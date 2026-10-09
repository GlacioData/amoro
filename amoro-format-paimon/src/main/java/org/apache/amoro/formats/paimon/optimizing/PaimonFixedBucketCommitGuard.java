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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ScanMode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Fixed-task ownership checks, kept outside the legacy unaware commit path. */
public final class PaimonFixedBucketCommitGuard {
  private final AppendOnlyFileStoreTable table;
  private final long sourceSnapshot;
  private final Map<FileEntry.Identifier, Integer> before = new HashMap<>();
  private final Map<FileEntry.Identifier, Integer> after = new HashMap<>();
  private final Set<Path> outputPaths = new HashSet<>();

  private PaimonFixedBucketCommitGuard(AppendOnlyFileStoreTable table, long sourceSnapshot) {
    this.table = table;
    this.sourceSnapshot = sourceSnapshot;
  }

  @Nullable
  public static PaimonFixedBucketCommitGuard validate(
      AppendOnlyFileStoreTable current,
      Collection<PaimonCompactionTask> tasks,
      List<CommitMessage> messages,
      String user,
      long identifier) {
    boolean fixed = tasks.stream().anyMatch(t -> isFixed(t.getInput()));
    if (!fixed) {
      return null;
    }
    require(tasks.size() == messages.size(), "Fixed task/message count mismatch");
    PaimonFixedBucketCommitGuard guard = null;
    Iterator<CommitMessage> outputs = messages.iterator();
    for (PaimonCompactionTask task : tasks) {
      PaimonCompactionInput input = task.getInput();
      require(isFixed(input), "Mixed fixed/unaware commit tasks");
      require(
          user.equals(input.getCommitUser()) && identifier == input.getCommitIdentifier(),
          "Fixed task commit identity mismatch");
      PaimonFixedBucketTask payload =
          new PaimonFixedBucketTaskSerializer()
              .deserialize(input.getSerializerVersion(), input.getTaskBytes());
      payload.validateBinding(input.getTable());
      require(payload.snapshotId() == input.getTargetSnapshotId(), "Fixed snapshot mismatch");
      require(payload.tablePath().equals(current.location().toString()), "Fixed table mismatch");
      AppendOnlyFileStoreTable bound = (AppendOnlyFileStoreTable) input.getTable().originalTable();
      if (guard == null) {
        // Paimon ac223f47, table/AbstractFileStoreTable.java:382-393 preserves bound options.
        // Commit the published task configuration; a later DDL belongs to the next plan.
        guard = new PaimonFixedBucketCommitGuard(bound, payload.snapshotId());
      } else {
        require(
            guard.sourceSnapshot == payload.snapshotId()
                && guard.table.schema().equals(bound.schema()),
            "Fixed tasks must share one snapshot and bound schema/options");
      }
      CommitMessage output = outputs.next();
      require(output instanceof CommitMessageImpl, "Unsupported fixed commit message");
      CommitMessageImpl message = (CommitMessageImpl) output;
      CompactIncrement compact = message.compactIncrement();
      require(
          payload.partition().equals(message.partition())
              && payload.bucket() == message.bucket()
              && Integer.valueOf(payload.totalBuckets()).equals(message.totalBuckets())
              && payload.files().equals(compact.compactBefore()),
          "Fixed output partition/bucket/totalBuckets/compactBefore mismatch");
      require(
          message.newFilesIncrement().isEmpty()
              && compact.changelogFiles().isEmpty()
              && compact.newIndexFiles().isEmpty()
              && compact.deletedIndexFiles().isEmpty()
              && !compact.compactAfter().isEmpty(),
          "Fixed output must contain only an ordinary nonempty rewrite");
      PaimonFixedBucketOutputValidator.validate(bound, payload, task.getOutput(), message);
      guard.addFiles(message, compact.compactBefore(), guard.before);
      guard.addFiles(message, compact.compactAfter(), guard.after);
      DataFilePathFactory paths =
          bound
              .store()
              .pathFactory()
              .createDataFilePathFactory(message.partition(), message.bucket());
      for (DataFileMeta file : compact.compactAfter()) {
        for (Path path : file.collectFiles(paths)) {
          require(guard.outputPaths.add(path), "Duplicate fixed output data/index path");
        }
      }
    }
    require(
        java.util.Collections.disjoint(guard.before.keySet(), guard.after.keySet()),
        "Fixed rewrite cannot reuse input files as output");
    return guard;
  }

  private static boolean isFixed(PaimonCompactionInput input) {
    return input != null && input.getSerializerVersion() == PaimonFixedBucketTaskSerializer.VERSION;
  }

  private void addFiles(
      CommitMessage message,
      List<DataFileMeta> files,
      Map<FileEntry.Identifier, Integer> destination) {
    for (DataFileMeta file : files) {
      FileEntry.Identifier key =
          ManifestEntry.create(
                  FileKind.ADD, message.partition(), message.bucket(), message.totalBuckets(), file)
              .identifier();
      require(destination.put(key, message.totalBuckets()) == null, "Duplicate fixed file owner");
    }
  }

  public AppendOnlyFileStoreTable table() {
    return table;
  }

  public void validateUncommitted() {
    require(
        table.snapshotManager().snapshotExists(sourceSnapshot), "Fixed source snapshot expired");
    Map<BinaryRow, Set<Integer>> units = new HashMap<>();
    for (FileEntry.Identifier file : after.keySet()) {
      units.computeIfAbsent(file.partition, ignored -> new HashSet<>()).add(file.bucket);
    }
    validateNoOutputAliases(table.snapshotManager().snapshot(sourceSnapshot), units);
    Snapshot latest = table.latestSnapshot().get();
    if (latest.id() != sourceSnapshot) {
      validateNoOutputAliases(latest, units);
    }
  }

  private void validateNoOutputAliases(Snapshot snapshot, Map<BinaryRow, Set<Integer>> units) {
    // Paimon ac223f47, manifest/FileEntry.java:85-159 includes index metadata in identity.
    // DataFileMeta.java:332-336 also aligns extra index paths with the data file directory.
    // Compare all physical paths: altered metadata must not disguise retained data or indexes
    // as fresh outputs. Only task-sized path sets are retained while scanning affected units.
    Iterator<ManifestEntry> active =
        table
            .store()
            .newScan()
            .withSnapshot(snapshot)
            .withPartitionFilter(new ArrayList<>(units.keySet()))
            // Source:
            // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreScan.java#L66-L70
            // Preserve exact partition/bucket pairs, not their Cartesian product. Do not filter
            // totalBuckets here: aliases from an older bucket layout must remain detectable.
            .withTotalAwareBucketFilter(
                (partition, bucket, total) ->
                    units.containsKey(partition) && units.get(partition).contains(bucket))
            .readFileIterator();
    while (active.hasNext()) {
      ManifestEntry file = active.next();
      DataFilePathFactory paths =
          table.store().pathFactory().createDataFilePathFactory(file.partition(), file.bucket());
      for (Path path : file.file().collectFiles(paths)) {
        require(!outputPaths.contains(path), "Fixed rewrite output reuses active data/index path");
      }
    }
  }

  public boolean validateReplay(String user, long identifier) {
    // Source:
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L246-L284
    // Native recovery compares only user/id. Prove the exact published delta before allowing
    // deduplication, and recognize completion BEFORE rejecting an expired source snapshot.
    Optional<Snapshot> committed = table.snapshotManager().latestSnapshotOfUser(user);
    if (!committed.isPresent()) {
      validateUncommitted();
      return false;
    }
    Snapshot snapshot = committed.get();
    require(
        snapshot.commitIdentifier() == identifier
            && snapshot.commitKind() == Snapshot.CommitKind.COMPACT,
        "Fixed commit identity was reused");
    Map<FileEntry.Identifier, Integer> remainingBefore = new HashMap<>(before);
    Map<FileEntry.Identifier, Integer> remainingAfter = new HashMap<>(after);
    // Same SHA, operation/ManifestsReader.java:139-149 and AbstractFileStoreScan.java:390-407.
    // DELTA exposes DELETE and ADD entries without merging; identifiers ignore dropped stats.
    Iterator<ManifestEntry> entries =
        table.store().newScan().withSnapshot(snapshot).withKind(ScanMode.DELTA).readFileIterator();
    while (entries.hasNext()) {
      ManifestEntry entry = entries.next();
      Map<FileEntry.Identifier, Integer> expected =
          entry.kind() == FileKind.DELETE ? remainingBefore : remainingAfter;
      require(
          Integer.valueOf(entry.totalBuckets()).equals(expected.remove(entry.identifier())),
          "Fixed committed delta differs from replay input/output");
    }
    require(
        remainingBefore.isEmpty() && remainingAfter.isEmpty(),
        "Fixed committed delta does not cover replay input/output");
    return true;
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
