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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TestPaimonFixedBucketTableCommit {
  @Test
  void commitGuardScansExactSparsePartitionBucketPairs(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      AppendOnlyFileStoreTable table = spy(fixture.table);
      List<PaimonCompactionTask> tasks = new ArrayList<>();
      tasks.add(execute(input(table, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER)));
      tasks.add(execute(input(table, fixture.units().get("1:1"), COMMIT_USER, COMMIT_IDENTIFIER)));
      PaimonFixedBucketCommitGuard guard =
          PaimonFixedBucketCommitGuard.validate(
              table,
              tasks,
              Arrays.asList(message(tasks.get(0)), message(tasks.get(1))),
              COMMIT_USER,
              COMMIT_IDENTIFIER);
      org.apache.paimon.AppendOnlyFileStore store = spy(table.store());
      org.mockito.Mockito.doReturn(store).when(table).store();
      List<ManifestEntry> visited = new ArrayList<>();
      doAnswer(
              invocation -> {
                org.apache.paimon.operation.FileStoreScan scan =
                    spy((org.apache.paimon.operation.FileStoreScan) invocation.callRealMethod());
                doAnswer(
                        read -> {
                          @SuppressWarnings("unchecked")
                          java.util.Iterator<ManifestEntry> entries =
                              (java.util.Iterator<ManifestEntry>) read.callRealMethod();
                          return new java.util.Iterator<ManifestEntry>() {
                            public boolean hasNext() {
                              return entries.hasNext();
                            }

                            public ManifestEntry next() {
                              ManifestEntry entry = entries.next();
                              visited.add(entry);
                              return entry;
                            }
                          };
                        })
                    .when(scan)
                    .readFileIterator();
                return scan;
              })
          .when(store)
          .newScan();
      // Paimon FileStoreScan.withTotalAwareBucketFilter is a joint predicate: the off-diagonal
      // active units exist, but must never reach synchronous commit path enumeration.
      guard.validateUncommitted();
      List<ManifestEntry> expected = new ArrayList<>(fixture.units().get("0:0"));
      expected.addAll(fixture.units().get("1:1"));
      assertEquals(expected.size(), visited.size());
      assertEquals(
          expected.stream().map(ManifestEntry::identifier).collect(Collectors.toSet()),
          visited.stream().map(ManifestEntry::identifier).collect(Collectors.toSet()));
    }
  }

  private static final String COMMIT_USER = "fixed-commit-test";
  private static final long COMMIT_IDENTIFIER = 101L;

  @Test
  void recoversAResponseLostAfterTheRealNativeCommitSucceeded(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      AppendOnlyFileStoreTable boundTable = spy(fixture.table);
      PaimonCompactionTask task =
          execute(input(boundTable, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER));
      long sourceSnapshot = fixture.table.latestSnapshot().get().id();
      List<String> expectedRows = fixture.snapshotRows();
      AtomicBoolean responseLost = new AtomicBoolean();
      AtomicLong committedSnapshot = new AtomicLong();
      AtomicReference<Set<String>> committedFiles = new AtomicReference<>();
      AtomicReference<Set<String>> referencedFiles = new AtomicReference<>();

      // Paimon ac223f47, table/sink/TableCommitImpl.java:212-213 commits the generated manifest.
      // Lose only the response after that REAL commit returns, not a pre-commit or close error.
      // The fallback and later recovery must recognize the already persisted exact identity.
      doAnswer(
              creation -> {
                StreamTableCommit nativeCommit = spy((StreamTableCommit) creation.callRealMethod());
                doAnswer(
                        commit -> {
                          Object result = commit.callRealMethod();
                          if (responseLost.compareAndSet(false, true)) {
                            Snapshot snapshot = fixture.table.latestSnapshot().get();
                            assertEquals(COMMIT_USER, snapshot.commitUser());
                            assertEquals(COMMIT_IDENTIFIER, snapshot.commitIdentifier());
                            committedSnapshot.set(snapshot.id());
                            committedFiles.set(files(warehouse));
                            referencedFiles.set(liveFileNames(fixture));
                            throw new IllegalStateException(
                                "Injected lost response after successful native commit");
                          }
                          return result;
                        })
                    .when(nativeCommit)
                    .commit(eq(COMMIT_IDENTIFIER), anyList());
                return nativeCommit;
              })
          .when(boundTable)
          .newCommit(COMMIT_USER);

      committer(boundTable, Collections.singletonList(task)).commit();

      assertTrue(responseLost.get(), "The post-publication failure must actually be injected");
      assertEquals(sourceSnapshot + 1, committedSnapshot.get());
      assertEquals(committedSnapshot.get(), fixture.table.latestSnapshot().get().id());
      assertEquals(committedFiles.get(), files(warehouse));
      assertEquals(referencedFiles.get(), liveFileNames(fixture));
      assertEquals(expectedRows, fixture.snapshotRows());

      committer(boundTable, Collections.singletonList(task)).commit(CommitMode.RECOVERY_REPLAY);

      assertEquals(committedSnapshot.get(), fixture.table.latestSnapshot().get().id());
      assertEquals(committedFiles.get(), files(warehouse));
      assertEquals(referencedFiles.get(), liveFileNames(fixture));
      assertEquals(expectedRows, fixture.snapshotRows());
    }
  }

  @ParameterizedTest
  @EnumSource(CommitMode.class)
  void commitsRealFilesAndRecoveryDoesNotPublishTwice(CommitMode mode, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      List<String> rows = fixture.snapshotRows();
      List<PaimonCompactionTask> tasks = new ArrayList<>();
      for (List<ManifestEntry> unit : fixture.units().values()) {
        tasks.add(execute(input(fixture.table, unit, COMMIT_USER, COMMIT_IDENTIFIER)));
      }
      assertEquals(4, tasks.size(), "Exercise both partitions and both nonempty buckets");
      long sourceSnapshot = fixture.table.latestSnapshot().get().id();

      committer(fixture.table, tasks).commit(mode);

      Snapshot committed = fixture.table.latestSnapshot().get();
      assertEquals(sourceSnapshot + 1, committed.id());
      assertEquals(Snapshot.CommitKind.COMPACT, committed.commitKind());
      assertEquals(COMMIT_USER, committed.commitUser());
      assertEquals(COMMIT_IDENTIFIER, committed.commitIdentifier());
      assertEquals(rows, fixture.snapshotRows());
      assertEquals(4, fixture.units().values().stream().mapToInt(List::size).sum());
      Set<String> committedFiles = files(warehouse);

      // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
      // paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java:277-290.
      // Recovery filters a stable identity before testing or publishing its old output files.
      committer(fixture.table, tasks).commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(committed.id(), fixture.table.latestSnapshot().get().id());
      assertEquals(rows, fixture.snapshotRows());
      assertEquals(committedFiles, files(warehouse));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"partition", "bucket", "totalBuckets", "compactBefore"})
  void rejectsMismatchedMessagesBeforeNativeCommitOrCleanup(
      String mismatch, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      AppendOnlyFileStoreTable observedTable = spy(fixture.table);
      List<ManifestEntry> unit = fixture.units().get("0:0");
      PaimonCompactionTask task =
          execute(input(observedTable, unit, COMMIT_USER, COMMIT_IDENTIFIER));
      CommitMessageImpl original = message(task);
      BinaryRow partition = original.partition();
      int bucket = original.bucket();
      int totalBuckets = original.totalBuckets();
      List<DataFileMeta> before = original.compactIncrement().compactBefore();
      if ("partition".equals(mismatch)) {
        partition = fixture.units().get("1:0").get(0).partition();
      } else if ("bucket".equals(mismatch)) {
        bucket = 1;
      } else if ("totalBuckets".equals(mismatch)) {
        totalBuckets = 3;
      } else {
        before = before.subList(0, 1);
      }
      setMessage(
          task,
          new CommitMessageImpl(
              partition,
              bucket,
              totalBuckets,
              DataIncrement.emptyIncrement(),
              new CompactIncrement(
                  before, original.compactIncrement().compactAfter(), Collections.emptyList())));
      List<String> rows = fixture.snapshotRows();
      long snapshot = fixture.table.latestSnapshot().get().id();
      Set<String> existingFiles = files(warehouse);

      // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
      // table/sink/CommitMessageImpl.java:51 and operation/FileStoreCommitImpl.java:633.
      // Native abort trusts message paths; Amoro must reject mismatched scope before that boundary.
      assertThrows(
          OptimizingCommitException.class,
          () -> committer(observedTable, Collections.singletonList(task)).commit());
      verify(observedTable, never()).newCommit(anyString());
      assertEquals(snapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(existingFiles, files(warehouse));
      assertEquals(rows, fixture.snapshotRows());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "dataNewFiles",
        "dataDeletedFiles",
        "dataChangelog",
        "dataNewIndexFiles",
        "dataDeletedIndexFiles",
        "compactChangelog",
        "compactNewIndexFiles",
        "compactDeletedIndexFiles"
      })
  void rejectsAdditionalIncrementsBeforeNativeCommitOrCleanup(
      String increment, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      AppendOnlyFileStoreTable observedTable = spy(fixture.table);
      PaimonCompactionTask task =
          execute(input(observedTable, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER));
      CommitMessageImpl original = message(task);
      CompactIncrement rewrite = original.compactIncrement();
      List<DataFileMeta> referenced = Collections.singletonList(rewrite.compactBefore().get(0));
      List<IndexFileMeta> indexes =
          Collections.singletonList(
              new IndexFileMeta("hash", "unexpected-index", 1L, 1L, null, null, null));
      DataIncrement data =
          new DataIncrement(
              "dataNewFiles".equals(increment) ? referenced : Collections.emptyList(),
              "dataDeletedFiles".equals(increment) ? referenced : Collections.emptyList(),
              "dataChangelog".equals(increment) ? referenced : Collections.emptyList(),
              "dataNewIndexFiles".equals(increment) ? indexes : Collections.emptyList(),
              "dataDeletedIndexFiles".equals(increment) ? indexes : Collections.emptyList());
      CompactIncrement compact =
          new CompactIncrement(
              rewrite.compactBefore(),
              rewrite.compactAfter(),
              "compactChangelog".equals(increment) ? referenced : Collections.emptyList(),
              "compactNewIndexFiles".equals(increment) ? indexes : Collections.emptyList(),
              "compactDeletedIndexFiles".equals(increment) ? indexes : Collections.emptyList());
      setMessage(
          task,
          new CommitMessageImpl(
              original.partition(), original.bucket(), original.totalBuckets(), data, compact));
      long snapshot = fixture.table.latestSnapshot().get().id();
      List<String> rows = fixture.snapshotRows();
      Set<String> existingFiles = files(warehouse);

      // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
      // paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageSerializer.java:145-162
      // preserves both data and compact increments. FileStoreCommitImpl.java:633-650 trusts
      // new/changelog paths during abort, so a malformed message must never reach that boundary.
      assertThrows(
          OptimizingCommitException.class,
          () -> committer(observedTable, Collections.singletonList(task)).commit());
      verify(observedTable, never()).newCommit(anyString());
      assertEquals(snapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(existingFiles, files(warehouse));
      assertEquals(rows, fixture.snapshotRows());
    }
  }

  @Test
  void rejectsTheSameInputClaimedByTwoTasksBeforePublishing(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      AppendOnlyFileStoreTable observedTable = spy(fixture.table);
      PaimonCompactionInput input =
          input(observedTable, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER);
      List<PaimonCompactionTask> tasks = Arrays.asList(execute(input), execute(input));
      long snapshot = fixture.table.latestSnapshot().get().id();
      Set<String> existingFiles = files(warehouse);
      List<String> rows = fixture.snapshotRows();

      // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
      // operation/commit/ConflictDetection.java:188-205. Native delta merging can fail late;
      // checking duplicate task ownership before opening a committer is Amoro's responsibility.
      assertThrows(OptimizingCommitException.class, () -> committer(observedTable, tasks).commit());
      verify(observedTable, never()).newCommit(anyString());
      assertEquals(snapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(existingFiles, files(warehouse));
      assertEquals(rows, fixture.snapshotRows());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rejectsAnOuterIdentityDifferentFromThePersistedTask(
      boolean wrongUser, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      AppendOnlyFileStoreTable observedTable = spy(fixture.table);
      PaimonCompactionTask task =
          execute(input(observedTable, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER));
      Set<String> existingFiles = files(warehouse);
      assertThrows(
          OptimizingCommitException.class,
          () ->
              new PaimonTableCommit(
                      observedTable,
                      Collections.singletonList(task),
                      wrongUser ? "different-user" : COMMIT_USER,
                      wrongUser ? COMMIT_IDENTIFIER : COMMIT_IDENTIFIER + 1)
                  .commit());
      verify(observedTable, never()).newCommit(anyString());
      assertEquals(existingFiles, files(warehouse));
      assertEquals(2, fixture.table.latestSnapshot().get().id());
    }
  }

  @Test
  void keepsConcurrentAppendOutsideTheExactRewriteScope(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PaimonCompactionTask task =
          execute(input(fixture.table, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER));
      // The external writer commits between rewrite completion and Amoro publication.
      // Paimon ac223f47, operation/commit/ConflictDetection.java:198-207 merges these live entries.
      fixture.append(2, 1);
      List<String> rowsWithAppend = fixture.snapshotRows();
      Set<String> inputNames =
          message(task).compactIncrement().compactBefore().stream()
              .map(DataFileMeta::fileName)
              .collect(Collectors.toSet());
      Set<String> retained = liveFileNames(fixture);
      retained.removeAll(inputNames);
      long snapshot = fixture.table.latestSnapshot().get().id();

      committer(fixture.table, Collections.singletonList(task)).commit();

      assertEquals(snapshot + 1, fixture.table.latestSnapshot().get().id());
      assertEquals(rowsWithAppend, fixture.snapshotRows());
      assertTrue(liveFileNames(fixture).containsAll(retained));
      assertTrue(Collections.disjoint(inputNames, liveFileNames(fixture)));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void refusesInputsRemovedByConcurrentOverwriteOrCompaction(
      boolean overwrite, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      List<ManifestEntry> selected = fixture.units().get("0:0");
      PaimonCompactionTask task =
          execute(input(fixture.table, selected, COMMIT_USER, COMMIT_IDENTIFIER));
      if (overwrite) {
        BatchWriteBuilder builder = fixture.table.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite writer = builder.newWrite();
            BatchTableCommit commit = builder.newCommit()) {
          writer.write(
              GenericRow.of(
                  0,
                  BinaryString.fromString("replacement"),
                  1,
                  999,
                  BinaryString.fromString("new-generation")));
          commit.commit(writer.prepareCommit());
        }
      } else {
        fixture.commit(fixture.rewrite(selected));
      }
      long concurrentSnapshot = fixture.table.latestSnapshot().get().id();
      Set<String> referencedFiles = liveFileNames(fixture);
      List<String> rows = fixture.snapshotRows();

      // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
      // operation/commit/ConflictDetection.java:198-210. A missing compact-before file is a
      // native conflict; fallback must not turn the rejected rewrite into a successful commit.
      assertThrows(
          OptimizingCommitException.class,
          () -> committer(fixture.table, Collections.singletonList(task)).commit());
      assertEquals(concurrentSnapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(referencedFiles, liveFileNames(fixture));
      assertEquals(rows, fixture.snapshotRows());
    }
  }

  @Test
  void recognizesCommittedIdentityBeforeRejectingAnExpiredSourceSnapshot(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PaimonCompactionTask task =
          execute(input(fixture.table, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER));
      long sourceSnapshot = task.getInput().getTargetSnapshotId();
      List<String> rows = fixture.snapshotRows();
      committer(fixture.table, Collections.singletonList(task)).commit();
      long committedSnapshot = fixture.table.latestSnapshot().get().id();
      expireBefore(fixture.table, committedSnapshot);
      assertFalse(fixture.table.snapshotManager().snapshotExists(sourceSnapshot));
      assertTrue(fixture.table.snapshotManager().snapshotExists(committedSnapshot));
      Set<String> remainingFiles = files(warehouse);

      // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
      // operation/FileStoreCommitImpl.java:246-284. The completed identity must win over an
      // expired source snapshot, otherwise a lost response becomes an unrecoverable failure.
      committer(fixture.table, Collections.singletonList(task)).commit(CommitMode.RECOVERY_REPLAY);

      assertEquals(committedSnapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(rows, fixture.snapshotRows());
      assertEquals(remainingFiles, files(warehouse));
    }
  }

  @ParameterizedTest
  @EnumSource(CommitMode.class)
  void rejectsAnUncommittedTaskWhoseSourceSnapshotExpired(CommitMode mode, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PaimonCompactionTask task =
          execute(input(fixture.table, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER));
      long sourceSnapshot = task.getInput().getTargetSnapshotId();
      fixture.append(2, 1);
      long snapshot = fixture.table.latestSnapshot().get().id();
      expireBefore(fixture.table, snapshot);
      assertFalse(fixture.table.snapshotManager().snapshotExists(sourceSnapshot));
      List<String> rows = fixture.snapshotRows();
      Set<String> remainingFiles = files(warehouse);

      // Paimon ac223f47, table/sink/TableCommitImpl.java:291-327 checks new output existence,
      // and deliberately skips compact-before files. Source-snapshot validity is Amoro-owned.
      assertThrows(
          OptimizingCommitException.class,
          () -> committer(fixture.table, Collections.singletonList(task)).commit(mode));
      assertEquals(snapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(rows, fixture.snapshotRows());
      assertEquals(remainingFiles, files(warehouse));
    }
  }

  @Test
  void rejectsACommittedIdentityReusedForDifferentInputs(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PaimonCompactionTask first =
          execute(input(fixture.table, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER));
      PaimonCompactionTask different =
          execute(input(fixture.table, fixture.units().get("0:1"), COMMIT_USER, COMMIT_IDENTIFIER));
      committer(fixture.table, Collections.singletonList(first)).commit();
      long snapshot = fixture.table.latestSnapshot().get().id();
      List<String> rows = fixture.snapshotRows();
      Set<String> existingFiles = files(warehouse);

      // Paimon ac223f47, operation/FileStoreCommitImpl.java:273-279 only compares identifiers.
      // Amoro must not accept a different exact input merely because its identity was reused.
      assertThrows(
          OptimizingCommitException.class,
          () ->
              committer(fixture.table, Collections.singletonList(different))
                  .commit(CommitMode.RECOVERY_REPLAY));
      assertEquals(snapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(rows, fixture.snapshotRows());
      assertEquals(existingFiles, files(warehouse));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rechecksTheIdentityPublishedBetweenRecoveryPreflightAndNativeFiltering(
      boolean sameScope, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      AppendOnlyFileStoreTable observedTable = spy(fixture.table);
      PaimonCompactionTask replay =
          execute(input(observedTable, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER));
      CommitMessageImpl concurrent =
          sameScope
              ? message(replay)
              : message(
                  execute(
                      input(
                          fixture.table,
                          fixture.units().get("0:1"),
                          COMMIT_USER,
                          COMMIT_IDENTIFIER)));
      List<String> rows = fixture.snapshotRows();
      assertFalse(fixture.table.snapshotManager().latestSnapshotOfUser(COMMIT_USER).isPresent());
      AtomicBoolean injected = new AtomicBoolean();
      AtomicLong committedSnapshot = new AtomicLong();
      AtomicReference<Set<String>> committedFiles = new AtomicReference<>();

      // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
      // paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java:246-284
      // and table/sink/TableCommitImpl.java:277-290. A real commit after Amoro's preflight can
      // make native filtering return zero; that result alone does not prove matching scope.
      doAnswer(
              invocation -> {
                if (injected.compareAndSet(false, true)) {
                  try (StreamTableCommit commit = fixture.table.newCommit(COMMIT_USER)) {
                    commit.commit(COMMIT_IDENTIFIER, Collections.singletonList(concurrent));
                  }
                  committedSnapshot.set(fixture.table.latestSnapshot().get().id());
                  committedFiles.set(files(warehouse));
                }
                return invocation.callRealMethod();
              })
          .when(observedTable)
          .newCommit(COMMIT_USER);

      if (sameScope) {
        committer(observedTable, Collections.singletonList(replay))
            .commit(CommitMode.RECOVERY_REPLAY);
      } else {
        assertThrows(
            OptimizingCommitException.class,
            () ->
                committer(observedTable, Collections.singletonList(replay))
                    .commit(CommitMode.RECOVERY_REPLAY));
      }

      assertTrue(injected.get(), "The concurrent commit must occur inside the recovery window");
      assertEquals(3L, committedSnapshot.get());
      assertEquals(committedSnapshot.get(), fixture.table.latestSnapshot().get().id());
      assertEquals(COMMIT_USER, fixture.table.latestSnapshot().get().commitUser());
      assertEquals(COMMIT_IDENTIFIER, fixture.table.latestSnapshot().get().commitIdentifier());
      assertEquals(committedFiles.get(), files(warehouse));
      assertEquals(rows, fixture.snapshotRows());
    }
  }

  @Test
  void commitsAndReplaysWithPublishedOptionsAfterAllowedDdl(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PaimonCompactionInput input =
          input(fixture.table, fixture.units().get("0:0"), COMMIT_USER, COMMIT_IDENTIFIER);
      List<String> rows = fixture.snapshotRows();
      fixture.catalog.alterTable(
          fixture.identifier,
          Arrays.asList(
              SchemaChange.setOption("bucket-append-ordered", "false"),
              SchemaChange.setOption("file-index.range-bitmap.columns", "payload"),
              SchemaChange.setOption("self-optimizing.enabled", "false")),
          false);
      AppendOnlyFileStoreTable refreshed =
          (AppendOnlyFileStoreTable) fixture.catalog.getTable(fixture.identifier);
      PaimonCompactionTask task = execute(input);

      // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
      // table/AbstractFileStoreTable.java:382-393. Published options remain frozen while a
      // newly loaded table observes DDL; a refreshed committer argument must not rebind the task.
      assertEquals("true", input.getTable().originalTable().options().get("bucket-append-ordered"));
      assertEquals(
          "hm", input.getTable().originalTable().options().get("file-index.range-bitmap.columns"));
      assertEquals("false", refreshed.options().get("bucket-append-ordered"));
      assertEquals("false", refreshed.options().get("self-optimizing.enabled"));
      assertEquals("payload", refreshed.options().get("file-index.range-bitmap.columns"));
      assertTrue(
          message(task).compactIncrement().compactAfter().stream()
              .allMatch(file -> file.schemaId() == fixture.table.schema().id()));

      committer(refreshed, Collections.singletonList(task)).commit();
      long committed = fixture.table.latestSnapshot().get().id();
      committer(refreshed, Collections.singletonList(task)).commit(CommitMode.RECOVERY_REPLAY);

      assertEquals(committed, fixture.table.latestSnapshot().get().id());
      assertEquals(rows, fixture.snapshotRows());
      assertEquals(COMMIT_USER, fixture.table.latestSnapshot().get().commitUser());
      assertEquals(COMMIT_IDENTIFIER, fixture.table.latestSnapshot().get().commitIdentifier());
    }
  }

  private static PaimonFixedBucketTestSupport fixture(Path warehouse) throws Exception {
    PaimonFixedBucketTestSupport fixture = new PaimonFixedBucketTestSupport(warehouse, true, true);
    fixture.append(0, 1);
    fixture.append(1, 1);
    return fixture;
  }

  private static PaimonCompactionInput input(
      AppendOnlyFileStoreTable table,
      List<ManifestEntry> selected,
      String commitUser,
      long commitIdentifier) {
    PaimonTable wrapper = new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), table);
    PaimonFixedBucketTask fixed =
        PaimonFixedBucketTask.from(
            wrapper,
            selected,
            table.latestSnapshot().get().id(),
            OptimizingType.MINOR,
            PaimonAppendRewriteOptions.from(wrapper.properties()));
    return PaimonCompactionInput.forFixedBucket(wrapper, fixed, commitUser, "", commitIdentifier);
  }

  private static PaimonCompactionTask execute(PaimonCompactionInput input) throws Exception {
    PaimonCompactionTask task = new PaimonCompactionTask(1L, "", input, Collections.emptyMap());
    setOutput(task, new PaimonCompactionExecutor(input).execute());
    return task;
  }

  private static void setOutput(PaimonCompactionTask task, PaimonCompactionOutput output) {
    ByteBuffer buffer = SerializationUtil.simpleSerialize(output);
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    task.setOutputBytes(bytes);
  }

  private static CommitMessageImpl message(PaimonCompactionTask task) throws Exception {
    PaimonCompactionOutput output = task.getOutput();
    return (CommitMessageImpl)
        new CommitMessageSerializer()
            .deserialize(output.getCommitMessageVersion(), output.getCommitMessageBytes());
  }

  private static void setMessage(PaimonCompactionTask task, CommitMessageImpl message)
      throws Exception {
    CommitMessageSerializer serializer = new CommitMessageSerializer();
    List<DataFileMeta> before = message.compactIncrement().compactBefore();
    List<DataFileMeta> after = message.compactIncrement().compactAfter();
    setOutput(
        task,
        new PaimonCompactionOutput(
            serializer.serialize(message),
            serializer.getVersion(),
            before.size(),
            before.stream().mapToLong(DataFileMeta::fileSize).sum(),
            after.size(),
            after.stream().mapToLong(DataFileMeta::fileSize).sum()));
  }

  private static PaimonTableCommit committer(
      AppendOnlyFileStoreTable table, List<PaimonCompactionTask> tasks) {
    return new PaimonTableCommit(table, tasks, COMMIT_USER, COMMIT_IDENTIFIER);
  }

  private static Set<String> liveFileNames(PaimonFixedBucketTestSupport fixture) {
    return fixture.units().values().stream()
        .flatMap(List::stream)
        .map(ManifestEntry::fileName)
        .collect(Collectors.toSet());
  }

  private static Set<String> files(Path warehouse) throws Exception {
    try (Stream<Path> paths = Files.walk(warehouse)) {
      return paths
          .filter(Files::isRegularFile)
          .map(path -> warehouse.relativize(path).toString())
          .collect(Collectors.toSet());
    }
  }

  private static void expireBefore(AppendOnlyFileStoreTable table, long retainedSnapshot) {
    // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
    // table/ExpireSnapshotsImpl.java:155-277. Real expiration removes obsolete data and manifests;
    // an exact end-exclusive snapshot makes the race deterministic without wall-clock sleeps.
    ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) table.newExpireSnapshots();
    assertTrue(
        expire.expireUntil(table.snapshotManager().earliestSnapshotId(), retainedSnapshot) > 0);
  }
}
