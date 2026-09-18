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

package org.apache.amoro.formats.paimon;

import static org.apache.amoro.table.descriptor.SnapshotReadException.Reason.INVALID_REQUEST;
import static org.apache.amoro.table.descriptor.SnapshotReadException.Reason.READ_FAILED;

import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.SnapshotDetailPage;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaSerializer;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.VersionedObjectSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.function.Function;

/** Request-scoped delta reader. Neither a reader nor a decoded row survives the request. */
class PaimonSnapshotDetailReader {
  private static final int MAX_MANIFESTS = 25;
  private static final RowType META_TYPE =
      VersionedObjectSerializer.versionType(ManifestFileMeta.SCHEMA);
  private static final RowType ENTRY_TYPE =
      VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);
  private final PaimonSnapshotReadDeadline deadline;
  private final ManifestFileMetaSerializer metaSerializer = new ManifestFileMetaSerializer();
  private final ManifestEntrySerializer entrySerializer = new ManifestEntrySerializer();

  PaimonSnapshotDetailReader(PaimonSnapshotReadDeadline deadline) {
    this.deadline = deadline;
  }

  SnapshotDetailPage read(
      FileStore<?> store,
      Snapshot snapshot,
      String tableId,
      String ref,
      int limit,
      String token,
      Function<ManifestEntry, PartitionFileBaseInfo> toDto) {
    if (limit < 1 || limit > 25) {
      throw invalid("pageSize must be between 1 and 25");
    }
    return deadline.call(
        () -> {
          byte[] identity = identity(tableId, ref, snapshot);
          Cursor cursor = Cursor.decode(token, identity);
          List<PartitionFileBaseInfo> rows = new ArrayList<>(limit);
          String name = snapshot.deltaManifestList();
          if (name == null) {
            if (cursor.manifest != 0 || cursor.entry != 0) {
              throw invalid("Cursor is beyond the delta manifest list");
            }
            return new SnapshotDetailPage(rows, null);
          }
          FileStorePathFactory paths = store.pathFactory();
          ManifestList manifests = store.manifestListFactory().create();
          ManifestFile files = store.manifestFileFactory().create();
          boolean avro =
              CoreOptions.FILE_FORMAT_AVRO.equals(store.options().manifestFormatString());

          // Both levels share the skip-capable reader; Paimon's serializers keep version semantics.
          // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestList.java
          try (PaimonSnapshotRows metas =
              openRows(
                  manifests,
                  paths.toManifestListPath(name),
                  snapshot.deltaManifestListSize(),
                  META_TYPE,
                  avro)) {
            deadline.check();
            skip(metas, cursor.manifest, true);
            int converted = 0;
            // Check both budgets BEFORE hasNext: RecordReaderIterator.hasNext decodes the next row.
            // https://github.com/apache/paimon/blob/release-1.4.2/paimon-common/src/main/java/org/apache/paimon/reader/RecordReaderIterator.java
            while (rows.size() < limit && converted < MAX_MANIFESTS) {
              if (!hasNext(metas)) {
                if (cursor.entry != 0) {
                  throw invalid("Cursor is beyond the delta manifest list");
                }
                return new SnapshotDetailPage(rows, null);
              }
              ManifestFileMeta meta = deadline.call(() -> metaSerializer.fromRow(next(metas)));
              converted++;
              long count;
              try {
                count = Math.addExact(meta.numAddedFiles(), meta.numDeletedFiles());
              } catch (ArithmeticException e) {
                throw new SnapshotReadException(READ_FAILED, "Manifest entry count overflow", e);
              }
              if (meta.numAddedFiles() < 0 || meta.numDeletedFiles() < 0) {
                throw new SnapshotReadException(READ_FAILED, "Negative manifest entry count");
              }
              if (cursor.entry > count) {
                throw invalid("Cursor entry exceeds the manifest count");
              }
              if (cursor.entry < count) {
                deadline.check();
                try (PaimonSnapshotRows entries =
                    openRows(
                        files,
                        paths.toManifestFilePath(meta.fileName()),
                        meta.fileSize(),
                        ENTRY_TYPE,
                        avro)) {
                  deadline.check();
                  skip(entries, cursor.entry, false);
                  while (rows.size() < limit && cursor.entry < count) {
                    if (!hasNext(entries)) {
                      throw new SnapshotReadException(
                          READ_FAILED, "Manifest ended before its declared count");
                    }
                    ManifestEntry entry =
                        deadline.call(() -> entrySerializer.fromRow(next(entries)));
                    rows.add(deadline.call(() -> toDto.apply(entry)));
                    cursor.entry++;
                  }
                }
                deadline.check();
              }
              if (cursor.entry == count) {
                cursor.manifest = Math.addExact(cursor.manifest, 1);
                cursor.entry = 0;
              }
            }
            // No lookahead for a full/short budget-limited page. EOF may require one extra request.
            return new SnapshotDetailPage(rows, cursor.encode(identity));
          }
        });
  }

  private boolean hasNext(CloseableIterator<InternalRow> rows) {
    return deadline.call(rows::hasNext);
  }

  private InternalRow next(CloseableIterator<InternalRow> rows) {
    return deadline.call(rows::next);
  }

  PaimonSnapshotRows openRows(ObjectsFile<?> file, Path path, Long size, RowType type, boolean avro)
      throws IOException {
    return PaimonSnapshotRows.open(file, path, size, type, avro, deadline);
  }

  private void skip(PaimonSnapshotRows rows, long count, boolean list) {
    if (rows.skip(count) != count) {
      throw new SnapshotReadException(
          list ? INVALID_REQUEST : READ_FAILED,
          list
              ? "Cursor is beyond the delta manifest list"
              : "Manifest ended before cursor offset");
    }
  }

  private static byte[] identity(String table, String ref, Snapshot snapshot) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (String value :
        new String[] {
          "paimon",
          table,
          ref,
          Long.toString(snapshot.id()),
          String.valueOf(snapshot.deltaManifestList()),
          String.valueOf(snapshot.deltaManifestListSize())
        }) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
      digest.update(bytes);
    }
    return digest.digest();
  }

  private static SnapshotReadException invalid(String message) {
    return new SnapshotReadException(INVALID_REQUEST, message);
  }

  /** Fixed-width, versioned logical position, not a file path or a native seek offset. */
  private static final class Cursor {
    private long manifest;
    private long entry;

    private static Cursor decode(String token, byte[] identity) {
      Cursor cursor = new Cursor();
      if (token == null) {
        return cursor;
      }
      if (token.isEmpty() || token.length() > 4096) {
        throw invalid("Invalid snapshot cursor");
      }
      try {
        byte[] bytes = Base64.getUrlDecoder().decode(token);
        if (bytes.length != 49) {
          throw invalid("Invalid snapshot cursor");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.get() != 1) {
          throw invalid("Unsupported snapshot cursor version");
        }
        byte[] actual = new byte[32];
        buffer.get(actual);
        if (!Arrays.equals(identity, actual)) {
          throw invalid("Snapshot cursor belongs to another table, ref or snapshot");
        }
        cursor.manifest = buffer.getLong();
        cursor.entry = buffer.getLong();
        if (cursor.manifest < 0 || cursor.entry < 0 || cursor.manifest == Long.MAX_VALUE) {
          throw invalid("Invalid snapshot cursor position");
        }
        return cursor;
      } catch (IllegalArgumentException e) {
        throw invalid("Invalid snapshot cursor encoding");
      }
    }

    private String encode(byte[] identity) {
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(
              ByteBuffer.allocate(49)
                  .put((byte) 1)
                  .put(identity)
                  .putLong(manifest)
                  .putLong(entry)
                  .array());
    }
  }
}
