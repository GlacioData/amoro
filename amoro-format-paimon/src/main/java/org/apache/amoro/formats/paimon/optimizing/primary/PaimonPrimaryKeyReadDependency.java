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

package org.apache.amoro.formats.paimon.optimizing.primary;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SerializationUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Immutable, format-private evidence of the files and DV state restored by an executor. */
final class PaimonPrimaryKeyReadDependency implements Serializable {
  private static final long serialVersionUID = 1L;
  final int version;
  final String location;
  final String uuid;
  final String branch;
  final long snapshotId;
  final List<Bucket> buckets;

  static boolean required(FileStoreTable table) {
    return table != null
        && table.bucketMode() == BucketMode.HASH_FIXED
        && CoreOptions.fromMap(table.options()).deletionVectorsEnabled();
  }

  PaimonPrimaryKeyReadDependency(
      FileStoreTable table, long snapshotId, Map<BinaryRow, Map<Integer, RestoreFiles>> restored) {
    this.version = 1;
    this.location = table.location().toString();
    this.uuid = table.uuid();
    this.branch = table.snapshotManager().branch();
    this.snapshotId = snapshotId;
    List<Bucket> captured = new ArrayList<>();
    restored.forEach(
        (partition, files) ->
            files.forEach((bucket, state) -> captured.add(new Bucket(partition, bucket, state))));
    this.buckets = Collections.unmodifiableList(captured);
  }

  static final class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;
    private final byte[] partitionBytes;
    final int bucket;
    final Integer totalBuckets;
    final Set<String> files;
    final Set<String> deletionVectors;

    Bucket(BinaryRow partition, int bucket, RestoreFiles restored) {
      this.partitionBytes = SerializationUtils.serializeBinaryRow(partition);
      this.bucket = bucket;
      this.totalBuckets = restored.totalBuckets();
      this.files =
          Collections.unmodifiableSet(
              restored.dataFiles().stream()
                  .map(PaimonPrimaryKeyReadDependency::fileIdentity)
                  .collect(Collectors.toSet()));
      this.deletionVectors =
          Collections.unmodifiableSet(dvIdentities(restored.deleteVectorsIndex()));
    }

    BinaryRow partition() {
      return SerializationUtils.deserializeBinaryRow(partitionBytes);
    }
  }

  // Statistics may be dropped by restore scans. Compare immutable read semantics instead.
  static String fileIdentity(DataFileMeta file) {
    return digest(
        out -> {
          string(out, file.fileName());
          string(out, file.externalPath().orElse(null));
          out.writeInt(file.level());
          out.writeLong(file.schemaId());
          out.writeLong(file.fileSize());
          out.writeLong(file.rowCount());
          out.writeLong(file.minSequenceNumber());
          out.writeLong(file.maxSequenceNumber());
        });
  }

  static Set<String> dvIdentities(List<IndexFileMeta> indexes) {
    Set<String> identities = new HashSet<>();
    if (indexes != null) {
      for (IndexFileMeta index : indexes) {
        identities.add(
            digest(
                out -> {
                  string(out, index.indexType());
                  string(out, index.fileName());
                  string(out, index.externalPath());
                  out.writeLong(index.fileSize());
                  out.writeLong(index.rowCount());
                  out.writeBoolean(index.dvRanges() != null);
                  if (index.dvRanges() != null) {
                    out.writeInt(index.dvRanges().size());
                    for (Map.Entry<String, DeletionVectorMeta> entry :
                        new TreeMap<>(index.dvRanges()).entrySet()) {
                      string(out, entry.getKey());
                      DeletionVectorMeta meta = entry.getValue();
                      string(out, meta.dataFileName());
                      out.writeInt(meta.offset());
                      out.writeInt(meta.length());
                      out.writeBoolean(meta.cardinality() != null);
                      if (meta.cardinality() != null) {
                        out.writeLong(meta.cardinality());
                      }
                    }
                  }
                }));
      }
    }
    return identities;
  }

  private static void string(DataOutputStream out, String value) throws IOException {
    out.writeBoolean(value != null);
    if (value != null) {
      byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  private static String digest(IdentityWriter writer) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        writer.write(out);
      }
      return Base64.getEncoder()
          .encodeToString(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Cannot encode primary-key read dependency", e);
    }
  }

  @FunctionalInterface
  private interface IdentityWriter {
    void write(DataOutputStream out) throws IOException;
  }
}
