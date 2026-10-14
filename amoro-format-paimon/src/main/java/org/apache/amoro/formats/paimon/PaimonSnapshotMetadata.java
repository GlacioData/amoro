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
import static org.apache.amoro.table.descriptor.SnapshotReadException.Reason.NOT_FOUND;
import static org.apache.amoro.table.descriptor.SnapshotReadException.Reason.UNAVAILABLE;

import org.apache.amoro.api.CommitMetaProducer;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaSerializer;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.VersionedObjectSerializer;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Fixed candidate range with streaming delta-list statistics; never opens manifest entry files. */
public final class PaimonSnapshotMetadata {
  private PaimonSnapshotMetadata() {}

  public static String ref(String ref) {
    if (ref == null) {
      return "main";
    }
    if (ref.trim().isEmpty()
        || ref.equals(".")
        || ref.equals("..")
        || ref.contains("/")
        || ref.contains("\\")
        || ref.length() > 255
        || ref.chars().anyMatch(Character::isISOControl)) {
      throw new SnapshotReadException(INVALID_REQUEST, "Invalid snapshot ref");
    }
    return ref;
  }

  static boolean branch(FileStoreTable table, String ref, PaimonSnapshotReadDeadline deadline) {
    return BranchManager.isMainBranch(ref)
        || deadline.call(() -> table.branchManager().branchExists(ref));
  }

  static Snapshot resolve(
      FileStoreTable table, String ref, long id, PaimonSnapshotReadDeadline deadline) {
    if (id < 1) {
      throw new SnapshotReadException(INVALID_REQUEST, "Snapshot ID must be positive");
    }
    if (branch(table, ref, deadline)) {
      return deadline.call(
          () -> {
            try {
              return table.snapshotManager().copyWithBranch(ref).tryGetSnapshot(id);
            } catch (FileNotFoundException e) {
              throw new SnapshotReadException(NOT_FOUND, "Snapshot no longer exists", e);
            }
          });
    }
    Snapshot snapshot = tag(table, ref, deadline);
    if (snapshot.id() != id) {
      throw new SnapshotReadException(INVALID_REQUEST, "Snapshot ID does not match the tag");
    }
    return snapshot;
  }

  static Snapshot resolveBranch(
      FileStoreTable table, String ref, long id, PaimonSnapshotReadDeadline deadline) {
    if (!branch(table, ref, deadline)) {
      throw new SnapshotReadException(NOT_FOUND, "Snapshot branch no longer exists");
    }
    if (!deadline.call(() -> table.snapshotManager().copyWithBranch(ref).snapshotExists(id))) {
      throw new SnapshotReadException(NOT_FOUND, "Snapshot no longer exists in this branch");
    }
    return resolve(table, ref, id, deadline);
  }

  static Snapshot resolveTag(
      FileStoreTable table, String ref, long id, PaimonSnapshotReadDeadline deadline) {
    Snapshot snapshot = tag(table, ref, deadline);
    if (id < 1 || snapshot.id() != id) {
      throw new SnapshotReadException(INVALID_REQUEST, "Snapshot ID does not match the Tag");
    }
    return snapshot;
  }

  private static Snapshot tag(
      FileStoreTable table, String ref, PaimonSnapshotReadDeadline deadline) {
    return deadline.call(
        () ->
            table
                .tagManager()
                .get(ref)
                .orElseThrow(
                    () -> new SnapshotReadException(NOT_FOUND, "Snapshot tag no longer exists"))
                .trimToSnapshot());
  }

  static Pair<List<AmoroSnapshotsOfTable>, Long> page(
      FileStoreTable table,
      String ref,
      OperationType operation,
      int limit,
      int offset,
      PaimonSnapshotReadDeadline deadline) {
    if (limit < 1 || limit > 25 || offset < 0) {
      throw new SnapshotReadException(
          INVALID_REQUEST, "Invalid snapshot page; pageSize must be 1..25");
    }
    if (!branch(table, ref, deadline)) {
      Snapshot snapshot = tag(table, ref, deadline);
      return Pair.of(
          offset == 0 && matches(snapshot, operation)
              ? Collections.singletonList(summary(table, snapshot, deadline))
              : Collections.emptyList(),
          1L);
    }
    SnapshotManager manager = table.snapshotManager().copyWithBranch(ref);
    long[] range = range(manager, deadline);
    if (range == null) {
      return Pair.of(Collections.emptyList(), 0L);
    }
    long total = range[1] - range[0] + 1;
    List<AmoroSnapshotsOfTable> rows = new ArrayList<>(limit);
    if (offset >= total) {
      return Pair.of(rows, total);
    }
    long first = range[1] - offset;
    long candidates = Math.min((long) limit, first - range[0] + 1);
    for (long i = 0; i < candidates; i++) {
      final long id = first - i;
      Snapshot snapshot =
          deadline.call(
              () -> {
                try {
                  return manager.tryGetSnapshot(id);
                } catch (FileNotFoundException expired) {
                  return null; // Weak consistency: skip a hole, never refill outside this candidate
                  // range.
                }
              });
      if (snapshot != null && matches(snapshot, operation)) {
        rows.add(summary(table, snapshot, deadline));
      }
    }
    return Pair.of(rows, total);
  }

  static Snapshot latest(SnapshotManager manager, PaimonSnapshotReadDeadline deadline) {
    long[] bounds = range(manager, deadline);
    return bounds == null
        ? null
        : deadline.call(
            () -> {
              try {
                return manager.tryGetSnapshot(bounds[1]);
              } catch (FileNotFoundException e) {
                throw new SnapshotReadException(
                    UNAVAILABLE, "Snapshot boundary changed; refresh to retry", e);
              }
            });
  }

  private static long[] range(SnapshotManager manager, PaimonSnapshotReadDeadline deadline) {
    // Paimon 1.4.2 findLatest/findEarliest fall back to directory listing. Only readHint is
    // bounded.
    // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/utils/HintFileUtils.java
    Long min =
        deadline.call(
            () ->
                HintFileUtils.readHint(
                    manager.fileIO(), HintFileUtils.EARLIEST, manager.snapshotDirectory()));
    Long max =
        deadline.call(
            () ->
                HintFileUtils.readHint(
                    manager.fileIO(), HintFileUtils.LATEST, manager.snapshotDirectory()));
    if (min == null
        && max == null
        && !deadline.call(() -> manager.fileIO().exists(manager.snapshotDirectory()))) {
      return null;
    }
    if (min == null && max != null && max > 0) {
      if (deadline.call(
          () ->
              manager
                  .fileIO()
                  .exists(
                      new org.apache.paimon.fs.Path(
                          manager.snapshotDirectory(), HintFileUtils.EARLIEST)))) {
        throw new SnapshotReadException(UNAVAILABLE, "Snapshot earliest hint is unreadable");
      }
      min = 1L;
    }
    if (min == null || max == null || min < 1 || max < min) {
      throw new SnapshotReadException(
          UNAVAILABLE, "Snapshot boundaries unavailable; refresh to retry");
    }
    long earliest = min;
    long latest = max;
    if (!deadline.call(() -> manager.fileIO().exists(manager.snapshotPath(earliest)))
        || !deadline.call(() -> manager.fileIO().exists(manager.snapshotPath(latest)))) {
      throw new SnapshotReadException(UNAVAILABLE, "Snapshot boundary changed; refresh to retry");
    }
    return new long[] {earliest, latest};
  }

  private static boolean matches(Snapshot snapshot, OperationType operation) {
    return operation == null
        || operation == OperationType.ALL
        || (snapshot.commitKind() == Snapshot.CommitKind.COMPACT)
            == (operation == OperationType.OPTIMIZING);
  }

  static AmoroSnapshotsOfTable summary(
      FileStoreTable table, Snapshot snapshot, PaimonSnapshotReadDeadline deadline) {
    return deadline.call(
        () -> {
          AmoroSnapshotsOfTable row = summary(snapshot);
          long added = 0;
          long deleted = 0;
          long manifests = 0;
          long bytes = 0;
          String name = snapshot.deltaManifestList();
          if (name != null) {
            FileStore<?> store = table.store();
            ManifestList list = store.manifestListFactory().create();
            ManifestFileMetaSerializer serializer = new ManifestFileMetaSerializer();
            // Paimon 1.4.2 ManifestEntryWriter records ADD/DELETE counts and manifest bytes.
            // Never use readDeltaManifests/read(): they collect all metadata into a List.
            // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestFile.java
            // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestList.java
            try (PaimonSnapshotRows metas =
                PaimonSnapshotRows.open(
                    list,
                    store.pathFactory().toManifestListPath(name),
                    snapshot.deltaManifestListSize(),
                    VersionedObjectSerializer.versionType(ManifestFileMeta.SCHEMA),
                    CoreOptions.FILE_FORMAT_AVRO.equals(store.options().manifestFormatString()),
                    deadline)) {
              while (metas.hasNext()) {
                ManifestFileMeta meta = serializer.fromRow(metas.next());
                if (meta.numAddedFiles() < 0 || meta.numDeletedFiles() < 0 || meta.fileSize() < 0) {
                  throw new IllegalArgumentException("Negative delta manifest statistics");
                }
                added = Math.addExact(added, meta.numAddedFiles());
                deleted = Math.addExact(deleted, meta.numDeletedFiles());
                manifests = Math.addExact(manifests, 1);
                bytes = Math.addExact(bytes, meta.fileSize());
                deadline.check();
              }
            }
          }
          // Counts describe raw entries, not distinct file paths. Bytes are manifest bytes,
          // not data-file bytes. Publish only after EOF and successful reader cleanup.
          long entries = Math.addExact(added, deleted);
          row.setFileCount(Math.subtractExact(added, deleted));
          row.getSummary().put("addedFiles", Long.toString(added));
          row.getSummary().put("deletedFiles", Long.toString(deleted));
          row.getSummary().put("deltaEntries", Long.toString(entries));
          row.getSummary().put("deltaManifestCount", Long.toString(manifests));
          row.getSummary().put("deltaManifestBytes", Long.toString(bytes));
          // Reuse completed delta statistics; chart rendering must not read more manifests.
          Map<String, String> files = new LinkedHashMap<>();
          files.put("addedFiles", Long.toString(added));
          files.put("deletedFiles", Long.toString(deleted));
          files.put("netFileChange", Long.toString(row.getFileCount()));
          row.setFilesSummaryForChart(files);
          return row;
        });
  }

  static AmoroSnapshotsOfTable summary(Snapshot snapshot) {
    Map<String, String> summary = new LinkedHashMap<>();
    summary.put("commitUser", snapshot.commitUser());
    summary.put("commitIdentifier", String.valueOf(snapshot.commitIdentifier()));
    // Read only Snapshot JSON; never resolve manifest files to fill missing values.
    // Sizes are bytes and can be null for older snapshots (Paimon <= 1.0).
    // https://github.com/apache/paimon/blob/release-1.4.2/paimon-api/src/main/java/org/apache/paimon/Snapshot.java
    summary.put("schemaId", Long.toString(snapshot.schemaId()));
    summary.put("watermark", metadataValue(snapshot.watermark()));
    summary.put("changelogRecordCount", metadataValue(snapshot.changelogRecordCount()));
    summary.put("baseManifestListSize", metadataValue(snapshot.baseManifestListSize()));
    summary.put("deltaManifestListSize", metadataValue(snapshot.deltaManifestListSize()));
    if (snapshot.changelogManifestListSize() != null) {
      summary.put("changelogManifestListSize", snapshot.changelogManifestListSize().toString());
    }
    Map<String, String> records = new LinkedHashMap<>();
    records.put("total-records", String.valueOf(snapshot.totalRecordCount()));
    records.put("delta-records", String.valueOf(snapshot.deltaRecordCount()));
    if (snapshot.changelogRecordCount() != null) {
      records.put("changelog-records", snapshot.changelogRecordCount().toString());
    }
    summary.put("total-records", records.get("total-records"));
    summary.put("delta-records", records.get("delta-records"));
    AmoroSnapshotsOfTable row =
        new AmoroSnapshotsOfTable(
            Long.toString(snapshot.id()),
            0,
            0L,
            snapshot.totalRecordCount(),
            snapshot.timeMillis(),
            snapshot.commitKind().toString(),
            snapshot.commitKind() == Snapshot.CommitKind.COMPACT
                ? CommitMetaProducer.OPTIMIZE.name()
                : CommitMetaProducer.INGESTION.name(),
            summary);
    row.setFileCount(null);
    row.setFileSize(null);
    row.setRecordsSummaryForChart(records);
    row.setFilesSummaryForChart(Collections.emptyMap());
    return row;
  }

  private static String metadataValue(Long value) {
    return value == null ? "—" : value.toString();
  }
}
