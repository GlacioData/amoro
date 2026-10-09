/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.utils.DigestUtil;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Amoro-owned, bounded fixed-bucket wire format pinned to Paimon 1.4.2 metadata. */
public final class PaimonFixedBucketTaskSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(PaimonFixedBucketTaskSerializer.class);
  // Paimon ac223f47, table/sink/AppendCompactTaskSerializer.java:92 accepts only version 2.
  // A distinct outer version makes old executors reject before any native writer is created.
  public static final int VERSION = 1001;
  static final int MAX_FILES = 100_000;
  static final int MAX_PAYLOAD_BYTES = 32 * 1024 * 1024;
  private static final int MAX_SEGMENT_BYTES = 8 * 1024 * 1024;
  private static final int MAX_FIELDS = 65_536;
  private static final int MAGIC = 0x41464254;
  private static final int ENVELOPE_VERSION = 1;
  private static final int META_CODEC_VERSION = 1;
  private static final int CHECKSUM_BYTES = 32;
  private static final String META_SCHEMA_SHA256 =
      "460a0164724e85547b8d8e2ac2d20695f521187f3e32ef19376b0d3abf170786";

  public int getVersion() {
    return VERSION;
  }

  public byte[] serialize(PaimonFixedBucketTask task) {
    verifyMetadataSchema();
    try {
      ByteArrayOutputStream bytes = new BoundedBytes();
      DataOutputStream out = new DataOutputStream(bytes);
      writeHeader(out, task);
      for (DataFileMeta file : task.files()) {
        writeFile(out, file);
      }
      byte[] body = bytes.toByteArray();
      byte[] result = Arrays.copyOf(body, body.length + CHECKSUM_BYTES);
      System.arraycopy(checksum(body, body.length), 0, result, body.length, CHECKSUM_BYTES);
      return result;
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot encode fixed-bucket task", e);
    }
  }

  private static void writeHeader(DataOutputStream out, PaimonFixedBucketTask task)
      throws IOException {
    out.writeInt(MAGIC);
    out.writeInt(ENVELOPE_VERSION);
    out.writeInt(META_CODEC_VERSION);
    out.writeInt(task.files().size());
    out.writeInt(task.bucket());
    out.writeInt(task.totalBuckets());
    out.writeLong(task.snapshotId());
    out.writeLong(task.schemaId());
    out.writeByte((task.ordered() ? 1 : 0) | (task.rewriteAll() ? 2 : 0));
    out.writeByte(task.optimizingType().ordinal());
    out.writeByte(task.rewriteSource().ordinal());
    string(out, task.tableId());
    string(out, task.tablePath());
    string(out, task.rewriteFingerprint());
    row(out, task.partition());
  }

  /** Validate bounded input windows, then publish under both count and metadata byte limits. */
  public List<PaimonFixedBucketTask> splitForPublication(
      PaimonTable table,
      List<ManifestEntry> entries,
      long snapshotId,
      OptimizingType optimizingType,
      PaimonAppendRewriteOptions rewriteOptions) {
    require(entries != null && !entries.isEmpty(), "Fixed task requires manifest inputs");
    PaimonFixedBucketTask template = null;
    // Source:
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L885-L891
    // The native file-count option has a default, not a hard maximum. Preserve the wire limit
    // without constructing an oversized task. Validation windows must not become publication
    // barriers: a byte-tail singleton may need a partner from either adjacent count window.
    for (int start = 0; start < entries.size(); ) {
      int count = Math.min(MAX_FILES, entries.size() - start);
      PaimonFixedBucketTask task =
          PaimonFixedBucketTask.from(
              table,
              entries.subList(start, start + count),
              snapshotId,
              optimizingType,
              rewriteOptions);
      if (template == null) {
        template = task;
      } else {
        require(
            template.partition().equals(task.partition())
                && template.bucket() == task.bucket()
                && template.totalBuckets() == task.totalBuckets(),
            "Mixed manifest bucket layout across validation windows");
        require(
            !task.ordered()
                || entries.get(start - 1).file().maxSequenceNumber()
                    < entries.get(start).file().minSequenceNumber(),
            "Ordered fixed-bucket inputs overlap across validation windows");
      }
      start += count;
    }
    return splitForPublication(
        template, () -> entries.stream().map(ManifestEntry::file).iterator(), entries.size());
  }

  /** Split before publication using the same codec, without allocating encoded task buffers. */
  public List<PaimonFixedBucketTask> splitForPublication(PaimonFixedBucketTask task) {
    return splitForPublication(task, task.files(), task.files().size());
  }

  private List<PaimonFixedBucketTask> splitForPublication(
      PaimonFixedBucketTask task, Iterable<DataFileMeta> files, int inputCount) {
    verifyMetadataSchema();
    List<PaimonFixedBucketTask> result = new ArrayList<>();
    try {
      CountingBytes counter = new CountingBytes();
      DataOutputStream out = new DataOutputStream(counter);
      try {
        writeHeader(out, task);
      } catch (IllegalArgumentException oversized) {
        LOG.warn(
            "Skip fixed task: table={},bucket={},reason=METADATA_WIRE_HEADER_LIMIT",
            task.tableId(),
            task.bucket());
        return result;
      }
      long header = counter.count + CHECKSUM_BYTES;
      List<DataFileMeta> group = new ArrayList<>();
      long size = header;
      int rejected = 0;
      int firstGroupInRun = 0;
      // Source:
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/io/DataFileIndexWriter.java#L159-L175
      // Embedded indexes can dominate metadata independently of physical data-file size.
      // Protocol bounds are hard limits; the existing data-byte budget remains best effort.
      for (DataFileMeta file : files) {
        counter.count = 0;
        boolean encodable = true;
        try {
          writeFile(out, file);
        } catch (IllegalArgumentException oversized) {
          encodable = false;
        }
        long fileBytes = counter.count;
        if (!encodable || header + fileBytes > MAX_PAYLOAD_BYTES) {
          addGroup(result, task, group, header, firstGroupInRun);
          firstGroupInRun = result.size();
          group = new ArrayList<>();
          size = header;
          rejected++;
          // An unencodable input remains an ordered barrier, never bridge across it.
          continue;
        }
        if (group.size() == MAX_FILES || size + fileBytes > MAX_PAYLOAD_BYTES) {
          addGroup(result, task, group, header, firstGroupInRun);
          if (group.size() == 1) {
            // These adjacent inputs cannot share an envelope, even after rebalancing to the left.
            firstGroupInRun = result.size();
          }
          group = new ArrayList<>();
          size = header;
        }
        group.add(file);
        size += fileBytes;
      }
      addGroup(result, task, group, header, firstGroupInRun);
      int published = result.stream().mapToInt(part -> part.files().size()).sum();
      if (published != inputCount) {
        LOG.warn(
            "Fixed task metadata cannot publish all inputs: table={},bucket={},snapshot={},"
                + "reason=METADATA_WIRE_LIMIT,inputFiles={},publishedFiles={},oversizedFiles={}",
            task.tableId(),
            task.bucket(),
            task.snapshotId(),
            inputCount,
            published,
            rejected);
      }
      return result;
    } catch (IOException impossible) {
      throw new IllegalStateException("Cannot count fixed task metadata", impossible);
    }
  }

  private static void addGroup(
      List<PaimonFixedBucketTask> result,
      PaimonFixedBucketTask task,
      List<DataFileMeta> group,
      long header,
      int firstGroupInRun)
      throws IOException {
    if (group.size() > 1) {
      result.add(task.withFiles(group));
      return;
    }
    if (group.isEmpty()) {
      return;
    }
    // Source:
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/BaseAppendFileStoreWrite.java#L187-L222
    // Native rewrite consumes the exact input order. Rebalance only contiguous groups, retaining
    // at least two files per task. Two-file groups carry their first file left until a donor is
    // found; apply all boundary changes together, or retain the original publication unchanged.
    // The wire cap is Amoro-owned and includes the header/checksum, not physical data-file bytes.
    List<PaimonFixedBucketTask> replacements = new ArrayList<>();
    DataFileMeta carried = group.get(0);
    for (int i = result.size() - 1; i >= firstGroupInRun; i--) {
      List<DataFileMeta> previous = result.get(i).files();
      DataFileMeta last = previous.get(previous.size() - 1);
      CountingBytes counter = new CountingBytes();
      DataOutputStream out = new DataOutputStream(counter);
      writeFile(out, last);
      writeFile(out, carried);
      if (header + counter.count > MAX_PAYLOAD_BYTES) {
        return;
      }
      replacements.add(task.withFiles(Arrays.asList(last, carried)));
      if (previous.size() > 2) {
        PaimonFixedBucketTask donor = task.withFiles(previous.subList(0, previous.size() - 1));
        result.subList(i, result.size()).clear();
        result.add(donor);
        for (int j = replacements.size() - 1; j >= 0; j--) {
          result.add(replacements.get(j));
        }
        return;
      }
      carried = previous.get(0);
    }
  }

  private static final class CountingBytes extends OutputStream {
    private long count;

    @Override
    public void write(int value) {
      count++;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
      count += length;
    }
  }

  public PaimonFixedBucketTask deserialize(int version, byte[] bytes) {
    require(version == VERSION, "Unknown fixed task outer version");
    verifyMetadataSchema();
    require(
        bytes != null && bytes.length >= 43 + CHECKSUM_BYTES && bytes.length <= MAX_PAYLOAD_BYTES,
        "Invalid fixed task byte size");
    int bodyLength = bytes.length - CHECKSUM_BYTES;
    // Integrity checking detects accidental damage, not an authenticated sender.
    require(
        MessageDigest.isEqual(
            checksum(bytes, bodyLength), Arrays.copyOfRange(bytes, bodyLength, bytes.length)),
        "Fixed task checksum mismatch");
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bodyLength));
      require(in.readInt() == MAGIC, "Invalid fixed task magic");
      require(in.readInt() == ENVELOPE_VERSION, "Unknown fixed task envelope version");
      require(in.readInt() == META_CODEC_VERSION, "Unknown DataFileMeta codec version");
      int count = count(in, MAX_FILES, 1);
      int bucket = in.readInt();
      int totalBuckets = in.readInt();
      long snapshot = in.readLong();
      long schema = in.readLong();
      int flags = in.readUnsignedByte();
      require(flags <= 3, "Invalid fixed task flags");
      OptimizingType type = enumValue(in, OptimizingType.values());
      PaimonAppendRewriteOptions.Source source =
          enumValue(in, PaimonAppendRewriteOptions.Source.values());
      String tableId = string(in);
      String tablePath = string(in);
      String fingerprint = string(in);
      BinaryRow partition = row(in);
      List<DataFileMeta> files = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        files.add(readFile(in));
      }
      require(in.available() == 0, "Trailing fixed task bytes");
      return new PaimonFixedBucketTask(
          tableId,
          tablePath,
          partition,
          bucket,
          totalBuckets,
          snapshot,
          schema,
          (flags & 1) != 0,
          (flags & 2) != 0,
          type,
          source,
          fingerprint,
          files);
    } catch (IOException | RuntimeException e) {
      throw new IllegalArgumentException("Invalid fixed-bucket task payload", e);
    }
  }

  // Source:
  // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/io/DataFileMetaSerializer.java#L45-L100
  // Codec v1 maps all 20 fields explicitly. Do not call native deserializeList: it allocates
  // from unchecked counts. Every variable-length component is bounded before native decoding.
  private static void writeFile(DataOutputStream out, DataFileMeta f) throws IOException {
    string(out, f.fileName());
    out.writeLong(f.fileSize());
    out.writeLong(f.rowCount());
    row(out, f.minKey());
    row(out, f.maxKey());
    stats(out, f.keyStats());
    stats(out, f.valueStats());
    out.writeLong(f.minSequenceNumber());
    out.writeLong(f.maxSequenceNumber());
    out.writeLong(f.schemaId());
    out.writeInt(f.level());
    strings(out, f.extraFiles());
    out.writeLong(f.creationTime().getMillisecond());
    out.writeInt(f.creationTime().getNanoOfMillisecond());
    nullableLong(out, f.deleteRowCount().orElse(null));
    binary(out, f.embeddedIndex());
    out.writeByte(f.fileSource().map(FileSource::toByteValue).orElse((byte) -1));
    strings(out, f.valueStatsCols());
    string(out, f.externalPath().orElse(null));
    nullableLong(out, f.firstRowId());
    strings(out, f.writeCols());
  }

  private static DataFileMeta readFile(DataInputStream in) throws IOException {
    String name = string(in);
    long size = in.readLong();
    long rows = in.readLong();
    BinaryRow minKey = row(in);
    BinaryRow maxKey = row(in);
    SimpleStats keyStats = stats(in);
    SimpleStats valueStats = stats(in);
    long minSequence = in.readLong();
    long maxSequence = in.readLong();
    long schema = in.readLong();
    int level = in.readInt();
    List<String> extraFiles = strings(in);
    Timestamp creation = Timestamp.fromEpochMillis(in.readLong(), in.readInt());
    Long deletes = nullableLong(in);
    byte[] index = binary(in);
    byte source = in.readByte();
    FileSource fileSource = source == -1 ? null : FileSource.fromByteValue(source);
    List<String> statsCols = strings(in);
    String external = string(in);
    Long firstRowId = nullableLong(in);
    List<String> writeCols = strings(in);
    require(extraFiles != null, "Missing extra-files list");
    return DataFileMeta.create(
        name,
        size,
        rows,
        minKey,
        maxKey,
        keyStats,
        valueStats,
        minSequence,
        maxSequence,
        schema,
        level,
        extraFiles,
        creation,
        deletes,
        index,
        fileSource,
        statsCols,
        external,
        firstRowId,
        writeCols);
  }

  private static void stats(DataOutputStream out, SimpleStats stats) throws IOException {
    row(out, stats.minValues());
    row(out, stats.maxValues());
    int size = stats.nullCounts().size();
    require(size <= MAX_FIELDS, "Too many stats fields");
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      nullableLong(out, stats.nullCounts().isNullAt(i) ? null : stats.nullCounts().getLong(i));
    }
  }

  private static SimpleStats stats(DataInputStream in) throws IOException {
    BinaryRow min = row(in);
    BinaryRow max = row(in);
    int count = count(in, MAX_FIELDS, 1);
    Long[] nulls = new Long[count];
    for (int i = 0; i < count; i++) {
      nulls[i] = nullableLong(in);
    }
    return new SimpleStats(min, max, BinaryArray.fromLongArray(nulls));
  }

  private static void row(DataOutputStream out, BinaryRow row) throws IOException {
    require(
        row.getFieldCount() <= MAX_FIELDS && row.getSizeInBytes() <= MAX_SEGMENT_BYTES - 4,
        "Oversized binary row");
    binary(out, SerializationUtils.serializeBinaryRow(row));
  }

  private static BinaryRow row(DataInputStream in) throws IOException {
    byte[] value = binary(in);
    require(value != null && value.length >= 12, "Missing or short binary row");
    int arity = ByteBuffer.wrap(value).getInt();
    require(arity >= 0 && arity <= MAX_FIELDS, "Invalid binary row arity");
    require(
        value.length - 4 >= BinaryRow.calculateFixPartSizeInBytes(arity),
        "Truncated binary row fixed part");
    // Same SHA, utils/SerializationUtils.java:97. The arity and bytes are checked first.
    return SerializationUtils.deserializeBinaryRow(value);
  }

  private static void binary(DataOutputStream out, byte[] value) throws IOException {
    if (value == null) {
      out.writeInt(-1);
      return;
    }
    require(value.length <= MAX_SEGMENT_BYTES, "Oversized metadata segment");
    out.writeInt(value.length);
    out.write(value);
  }

  private static byte[] binary(DataInputStream in) throws IOException {
    int length = in.readInt();
    if (length == -1) {
      return null;
    }
    require(
        length >= 0 && length <= MAX_SEGMENT_BYTES && length <= in.available(),
        "Invalid metadata segment length");
    byte[] value = new byte[length];
    in.readFully(value);
    return value;
  }

  private static void string(DataOutputStream out, String value) throws IOException {
    require(value == null || value.length() <= MAX_SEGMENT_BYTES, "Oversized metadata string");
    binary(out, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
  }

  private static String string(DataInputStream in) throws IOException {
    byte[] bytes = binary(in);
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
  }

  private static void strings(DataOutputStream out, List<String> values) throws IOException {
    if (values == null) {
      out.writeInt(-1);
      return;
    }
    require(values.size() <= MAX_FIELDS, "Too many string elements");
    out.writeInt(values.size());
    for (String value : values) {
      string(out, value);
    }
  }

  private static List<String> strings(DataInputStream in) throws IOException {
    int size = in.readInt();
    if (size == -1) {
      return null;
    }
    require(
        size >= 0 && size <= MAX_FIELDS && size <= in.available() / 4,
        "Invalid string element count");
    List<String> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      String value = string(in);
      require(value != null, "Null metadata string element");
      result.add(value);
    }
    return result;
  }

  private static void nullableLong(DataOutputStream out, Long value) throws IOException {
    out.writeBoolean(value != null);
    if (value != null) {
      out.writeLong(value);
    }
  }

  private static Long nullableLong(DataInputStream in) throws IOException {
    int present = in.readUnsignedByte();
    require(present <= 1, "Invalid nullable-long flag");
    return present == 1 ? in.readLong() : null;
  }

  private static int count(DataInputStream in, int max, int minimumElementBytes)
      throws IOException {
    int count = in.readInt();
    require(
        count >= 0 && count <= max && count <= in.available() / minimumElementBytes,
        "Invalid metadata element count");
    return count;
  }

  private static <T> T enumValue(DataInputStream in, T[] values) throws IOException {
    int ordinal = in.readUnsignedByte();
    require(ordinal < values.length, "Unknown fixed task enum value");
    return values[ordinal];
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  private static void verifyMetadataSchema() {
    // Amoro codec v1 is pinned to the exact native 1.4.2 metadata layout, not a guessed version.
    require(
        META_SCHEMA_SHA256.equals(DigestUtil.sha256Hex(DataFileMeta.SCHEMA.toString())),
        "Paimon metadata schema changed; review and version the Amoro codec before upgrading");
  }

  private static byte[] checksum(byte[] bytes, int length) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(bytes, 0, length);
      return digest.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
    }
  }

  private static final class BoundedBytes extends ByteArrayOutputStream {
    @Override
    public synchronized void write(int value) {
      require(count < MAX_PAYLOAD_BYTES - CHECKSUM_BYTES, "Fixed task payload exceeds byte limit");
      super.write(value);
    }

    @Override
    public synchronized void write(byte[] bytes, int offset, int length) {
      require(
          length >= 0 && length <= MAX_PAYLOAD_BYTES - CHECKSUM_BYTES - count,
          "Fixed task payload exceeds byte limit");
      super.write(bytes, offset, length);
    }
  }
}
