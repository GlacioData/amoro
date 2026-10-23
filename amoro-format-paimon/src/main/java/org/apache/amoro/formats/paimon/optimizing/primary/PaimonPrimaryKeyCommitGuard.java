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

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.SnapshotManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Checks executor read dependencies against the parent of every native COMPACT publication. */
final class PaimonPrimaryKeyCommitGuard {
  enum Publication {
    PUBLISHED,
    ABSENT,
    UNKNOWN
  }

  private final FileStoreTable table;
  private final Collection<PaimonPrimaryKeyCompactionTask> tasks;
  private final String user;
  private final long identifier;
  private final long plannedSnapshot;
  private final Map<Pair<BinaryRow, Integer>, PaimonPrimaryKeyReadDependency.Bucket> buckets =
      new HashMap<>();
  private String verifiedSnapshot;
  private ReadConflict rejected;
  private boolean uncertain;
  private boolean published;

  PaimonPrimaryKeyCommitGuard(
      FileStoreTable table,
      Collection<PaimonPrimaryKeyCompactionTask> tasks,
      String user,
      long identifier,
      long plannedSnapshot) {
    this.table = table;
    this.tasks = tasks;
    this.user = user;
    this.identifier = identifier;
    this.plannedSnapshot = plannedSnapshot;
  }

  static boolean applies(FileStoreTable table, Collection<PaimonPrimaryKeyCompactionTask> tasks) {
    return PaimonPrimaryKeyReadDependency.required(table)
        || tasks.stream().anyMatch(t -> t.getOutput().getReadDependency() != null);
  }

  boolean publicationUncertain() {
    return uncertain;
  }

  boolean rejected() {
    return rejected != null;
  }

  Publication inspectPublication() throws IOException {
    if (published) {
      return Publication.PUBLISHED;
    }
    String uuid = table.uuid();
    long lower = Long.MAX_VALUE;
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      PaimonPrimaryKeyReadDependency dependency = task.getOutput().getReadDependency();
      if (dependency != null) {
        validateContext(task, dependency, uuid);
      }
      lower = Math.min(lower, dependency == null ? plannedSnapshot : dependency.snapshotId);
    }
    SnapshotManager manager = table.snapshotManager();
    Snapshot latest = manager.latestSnapshot();
    if (latest == null || lower < 1 || latest.id() < lower) {
      return Publication.UNKNOWN;
    }
    int probes = 0;
    for (long id = latest.id(); id > lower; id--) {
      if (++probes > PaimonPrimaryKeyTableCommit.MAX_IDENTITY_SNAPSHOT_PROBES) {
        return Publication.UNKNOWN;
      }
      Snapshot snapshot;
      try {
        snapshot = id == latest.id() ? latest : manager.tryGetSnapshot(id);
      } catch (FileNotFoundException expired) {
        return Publication.UNKNOWN;
      }
      if (snapshot == null) {
        return Publication.UNKNOWN;
      }
      if (user.equals(snapshot.commitUser())
          && identifier == snapshot.commitIdentifier()
          && snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
        return Publication.PUBLISHED;
      }
    }
    return Publication.ABSENT;
  }

  void prepare(List<CommitMessage> messages) {
    require(
        PaimonPrimaryKeyReadDependency.required(table),
        "Table no longer supports the captured DV mode");
    String uuid = table.uuid();
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      PaimonPrimaryKeyReadDependency d = task.getOutput().getReadDependency();
      require(d != null, "Missing executor read dependency; re-execute the old task");
      validateContext(task, d, uuid);
      Set<Pair<BinaryRow, Integer>> expected = new HashSet<>();
      for (PaimonBucketCompactionUnit unit : task.getInput().getUnits()) {
        require(
            expected.add(
                Pair.of(
                    SerializationUtils.deserializeBinaryRow(unit.getPartitionBytes()),
                    unit.getBucket())),
            "Duplicate input bucket");
      }
      for (PaimonPrimaryKeyReadDependency.Bucket bucket : d.buckets) {
        Pair<BinaryRow, Integer> key = Pair.of(bucket.partition(), bucket.bucket);
        require(
            expected.remove(key) && buckets.putIfAbsent(key, bucket) == null,
            "Duplicate or unexpected dependency bucket " + key);
      }
      require(expected.isEmpty(), "Incomplete executor read dependency");
    }
    Set<Pair<BinaryRow, Integer>> active = new HashSet<>();
    for (CommitMessage message : messages) {
      require(message instanceof CommitMessageImpl, "Unsupported native commit message");
      CommitMessageImpl nativeMessage = (CommitMessageImpl) message;
      require(nativeMessage.newFilesIncrement().isEmpty(), "Expected only COMPACT increments");
      if (nativeMessage.isEmpty()) {
        continue;
      }
      Pair<BinaryRow, Integer> key = Pair.of(message.partition(), message.bucket());
      PaimonPrimaryKeyReadDependency.Bucket bucket = buckets.get(key);
      require(bucket != null && active.add(key), "Missing or duplicate commit bucket " + key);
      require(
          Objects.equals(message.totalBuckets(), bucket.totalBuckets),
          "Bucket count changed: " + key);
      nativeMessage
          .compactIncrement()
          .compactBefore()
          .forEach(
              file ->
                  require(
                      bucket.files.contains(PaimonPrimaryKeyReadDependency.fileIdentity(file)),
                      "Compacted file was not restored: " + file.fileName()));
    }
    buckets.keySet().retainAll(active);
  }

  void preflight() {
    Snapshot snapshot = table.snapshotManager().latestSnapshot();
    require(snapshot != null, "Missing parent snapshot");
    validate(snapshot);
  }

  private void validateContext(
      PaimonPrimaryKeyCompactionTask task, PaimonPrimaryKeyReadDependency dependency, String uuid) {
    require(
        dependency.version == 1 && dependency.snapshotId >= task.getInput().getTargetSnapshotId(),
        "Invalid read dependency version or snapshot");
    require(
        Objects.equals(uuid, dependency.uuid)
            && table.location().toString().equals(dependency.location)
            && table.snapshotManager().branch().equals(dependency.branch),
        "Read dependency belongs to another table or branch");
  }

  FileStoreTable commitTable() {
    PrimaryKeyFileStoreTable guarded =
        new PrimaryKeyFileStoreTable(
            table.fileIO(),
            table.location(),
            table.schema(),
            new PaimonPrimaryKeyCommitEnvironment(table.catalogEnvironment(), this));
    guarded.setManifestCache(table.getManifestCache());
    return guarded;
  }

  SnapshotCommit wrap(SnapshotCommit delegate, SnapshotManager manager) {
    return new SnapshotCommit() {
      @Override
      public boolean commit(Snapshot candidate, String branch, List<PartitionStatistics> statistics)
          throws Exception {
        require(
            candidate.commitKind() == Snapshot.CommitKind.COMPACT
                && user.equals(candidate.commitUser())
                && identifier == candidate.commitIdentifier()
                && table.snapshotManager().branch().equals(branch),
            "Unexpected publication identity");
        // Paimon 1.4.2 chooses parent + 1, retries CAS failures and invokes this API on every
        // attempt.
        // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L687-L735
        if (rejected != null) {
          throw rejected;
        }
        validate(manager.snapshot(candidate.id() - 1));
        boolean previousUncertainty = uncertain;
        uncertain = true;
        boolean success = delegate.commit(candidate, branch, statistics);
        published |= success;
        uncertain = !published && previousUncertainty;
        return success;
      }

      @Override
      public void close() throws Exception {
        delegate.close();
      }
    };
  }

  private void validate(Snapshot snapshot) {
    if (rejected != null) {
      throw rejected;
    }
    String identity = snapshot.toJson();
    if (identity.equals(verifiedSnapshot)) {
      return;
    }
    Set<BinaryRow> partitions = new HashSet<>();
    Map<Pair<BinaryRow, Integer>, Set<String>> missing = new HashMap<>();
    buckets.forEach(
        (key, bucket) -> {
          partitions.add(key.getKey());
          missing.put(key, new HashSet<>(bucket.files));
        });
    if (!buckets.isEmpty()) {
      Iterator<ManifestEntry> entries =
          table
              .store()
              .newScan()
              .withSnapshot(snapshot)
              .withPartitionFilter(new ArrayList<>(partitions))
              .withTotalAwareBucketFilter(
                  (partition, bucket, total) -> buckets.containsKey(Pair.of(partition, bucket)))
              .dropStats()
              .readFileIterator();
      while (entries.hasNext()) {
        ManifestEntry entry = entries.next();
        Pair<BinaryRow, Integer> key = Pair.of(entry.partition(), entry.bucket());
        Set<String> remaining = missing.get(key);
        if (remaining != null) {
          require(
              Objects.equals(buckets.get(key).totalBuckets, entry.totalBuckets()),
              "Bucket count changed: " + key);
          remaining.remove(PaimonPrimaryKeyReadDependency.fileIdentity(entry.file()));
        }
      }
      Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> indexes =
          table.store().newIndexFileHandler().scan(snapshot, DELETION_VECTORS_INDEX, partitions);
      for (Map.Entry<Pair<BinaryRow, Integer>, PaimonPrimaryKeyReadDependency.Bucket> entry :
          buckets.entrySet()) {
        Pair<BinaryRow, Integer> key = entry.getKey();
        require(
            missing.get(key).isEmpty(),
            "Read files changed: bucket=" + key + " parent=" + snapshot.id());
        require(
            entry
                .getValue()
                .deletionVectors
                .equals(
                    PaimonPrimaryKeyReadDependency.dvIdentities(
                        indexes.getOrDefault(key, Collections.emptyList()))),
            "Deletion vectors changed: bucket=" + key + " parent=" + snapshot.id());
      }
    }
    verifiedSnapshot = identity;
  }

  private void require(boolean condition, String reason) {
    if (!condition) {
      if (rejected == null) {
        rejected =
            new ReadConflict(
                "Paimon primary-key read dependency rejected: table="
                    + table.name()
                    + " branch="
                    + table.snapshotManager().branch()
                    + " user="
                    + user
                    + " identifier="
                    + identifier
                    + " executionSnapshots="
                    + tasks.stream()
                        .map(task -> task.getOutput().getReadDependency())
                        .map(
                            dependency ->
                                dependency == null
                                    ? "missing"
                                    : String.valueOf(dependency.snapshotId))
                        .distinct()
                        .collect(java.util.stream.Collectors.joining(","))
                    + ": "
                    + reason);
      }
      throw rejected;
    }
  }

  static final class ReadConflict extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    private ReadConflict(String message) {
      super(message);
    }
  }
}
