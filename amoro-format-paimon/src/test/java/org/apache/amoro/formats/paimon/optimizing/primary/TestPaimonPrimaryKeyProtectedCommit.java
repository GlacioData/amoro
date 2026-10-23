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

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.append;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.files;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyReadDependencies.decode;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyReadDependencies.fixture;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyReadDependencies.independentLookupCompact;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.exception.OptimizingCommitSkippedException;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.optimizing.TableOptimizingCommitter.ReconciliationResult;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

class TestPaimonPrimaryKeyProtectedCommit {
  @ParameterizedTest
  @ValueSource(strings = {"none", "lookup"})
  void retryRevalidatesAfterDvChangesBetweenCheckAndPublication(
      String producer, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, producer);
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.MINOR);
      AtomicInteger attempts = new AtomicInteger();
      FileStoreTable intercepted =
          intercept(
              t,
              (delegate, snapshot, branch, stats) -> {
                if (attempts.incrementAndGet() == 1) {
                  append(t, GenericRow.of(1, 11, 11, 2L));
                  independentLookupCompact(t);
                }
                return delegate.commit(snapshot, branch, stats);
              },
              false);
      assertThrows(OptimizingCommitException.class, () -> committer(intercepted, task).commit());
      assertEquals(1, attempts.get(), "Stale retries must not reach the catalog again");
      assertEquals(6L, t.snapshotManager().latestSnapshotId());
      assertEquals(1001, rows(t).size());
      assertTrue(rows(t).contains("1|11|11|2"));
      assertOutputExists(t, task);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"none", "lookup"})
  void retryAllowsConcurrentAppendWithoutChangingReadDependencies(
      String producer, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, producer);
      List<String> expected = new ArrayList<>(rows(t));
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.MAJOR);
      AtomicInteger attempts = new AtomicInteger();
      FileStoreTable intercepted =
          intercept(
              t,
              (delegate, snapshot, branch, stats) -> {
                if (attempts.incrementAndGet() == 1) {
                  append(t, GenericRow.of(9999, 99, 99, 2L));
                }
                return delegate.commit(snapshot, branch, stats);
              },
              false);
      committer(intercepted, task).commit();
      assertEquals(2, attempts.get());
      assertEquals(6L, t.snapshotManager().latestSnapshotId());
      assertEquals(1002L, t.latestSnapshot().get().totalRecordCount());
      // DV batch reads normally omit L0. Make the legitimate appended row visible first.
      assertTrue(files(t).stream().anyMatch(f -> f.level() == 0));
      independentLookupCompact(t);
      expected.add("9999|99|99|2");
      Collections.sort(expected);
      assertEquals(expected, rows(t));
      committer(t, task).commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(7L, t.snapshotManager().latestSnapshotId());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"direct", "replay", "lost-response", "fallback", "close"})
  void publicationAndReplayAreIdempotent(String fault, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "lookup");
      List<String> expected = rows(t);
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.FULL);
      AtomicInteger attempts = new AtomicInteger();
      FileStoreTable intercepted =
          intercept(
              t,
              (delegate, snapshot, branch, stats) -> {
                int attempt = attempts.incrementAndGet();
                if (fault.equals("fallback") && attempt == 1) {
                  throw new IOException("Injected failure before publication");
                }
                boolean published = delegate.commit(snapshot, branch, stats);
                if (fault.equals("lost-response") && attempt == 1 && published) {
                  throw new IOException("Injected lost response after publication");
                }
                return published;
              },
              fault.equals("close"));
      intercepted = intercepted.copy(Collections.singletonMap("commit.max-retries", "0"));
      PaimonPrimaryKeyTableCommit commit = committer(intercepted, task);
      commit.commit(fault.equals("replay") ? CommitMode.RECOVERY_REPLAY : CommitMode.NORMAL);
      assertEquals(5L, t.snapshotManager().latestSnapshotId());
      assertEquals(Snapshot.CommitKind.COMPACT, t.latestSnapshot().get().commitKind());
      assertEquals(expected, rows(t));
      assertEquals(fault.equals("fallback") ? 2 : 1, attempts.get());
      committer(t, task).commit(CommitMode.RECOVERY_REPLAY);
      committer(t, task).commit();
      assertEquals(ReconciliationResult.PUBLISHED, committer(t, task).reconcile());
      assertEquals(5L, t.snapshotManager().latestSnapshotId());
    }
  }

  @ParameterizedTest
  @EnumSource(OptimizingType.class)
  void oldOutputMustBeReexecutedUnlessExactCompactIsPublished(
      OptimizingType type, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "none");
      PaimonPrimaryKeyCompactionTask task = prepare(t, type);
      PaimonPrimaryKeyReadDependency evidence = task.getOutput().getReadDependency();
      task.getOutput().setReadDependency(null);
      OptimizingCommitException rejected =
          assertThrows(OptimizingCommitException.class, () -> committer(t, task).commit());
      assertEquals(
          type == OptimizingType.FULL, rejected instanceof OptimizingCommitSkippedException);
      assertEquals(4L, t.snapshotManager().latestSnapshotId());
      assertOutputExists(t, task);
      task.getOutput().setReadDependency(evidence);
      committer(t, task).commit();
      task.getOutput().setReadDependency(null);
      committer(t, task).commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(5L, t.snapshotManager().latestSnapshotId());
    }
  }

  @Test
  void filterReturningZeroForAppendDoesNotProveCompactPublication(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "none");
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.MINOR);
      StreamWriteBuilder builder =
          t.newStreamWriteBuilder().withCommitUser(task.getInput().getCommitUser());
      try (StreamTableWrite write = builder.newWrite();
          StreamTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(9999, 99, 99, 2L));
        commit.commit(
            task.getInput().getCommitIdentifier(),
            write.prepareCommit(false, task.getInput().getCommitIdentifier()));
      }
      assertEquals(Snapshot.CommitKind.APPEND, t.latestSnapshot().get().commitKind());
      assertThrows(
          OptimizingCommitException.class,
          () -> committer(t, task).commit(CommitMode.RECOVERY_REPLAY));
      assertEquals(5L, t.snapshotManager().latestSnapshotId());
      assertOutputExists(t, task);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"expired", "read-failure"})
  void unavailableHistoryRetainsFailureAndOutput(String fault, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "none");
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.FULL);
      append(t, GenericRow.of(9998, 98, 98, 2L));
      append(t, GenericRow.of(9999, 99, 99, 2L));
      FileStoreTable observed = t;
      if (fault.equals("expired")) {
        t.fileIO().delete(t.snapshotManager().snapshotPath(5L), false);
        t.snapshotManager().invalidateCache();
        assertFalse(t.snapshotManager().snapshotExists(5L));
      } else {
        observed = spy(t);
        SnapshotManager manager = spy(t.snapshotManager());
        doThrow(new java.io.UncheckedIOException(new IOException("Injected history read failure")))
            .when(manager)
            .tryGetSnapshot(5L);
        doReturn(manager).when(observed).snapshotManager();
        doReturn(observed).when(observed).copy(any(java.util.Map.class));
      }
      PaimonPrimaryKeyTableCommit commit = committer(observed, task);
      OptimizingCommitException failure =
          assertThrows(
              OptimizingCommitException.class, () -> commit.commit(CommitMode.RECOVERY_REPLAY));
      assertFalse(
          failure instanceof OptimizingCommitSkippedException,
          "Unknown publication must remain recoverable");
      assertThrows(OptimizingCommitException.class, commit::reconcile);
      assertEquals(6L, t.snapshotManager().latestSnapshotId());
      assertOutputExists(t, task);
    }
  }

  @Test
  void fullConflictSkipsWithoutPublicationButUncertainFailureRemainsRecoverable(
      @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "none");
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.FULL);
      FileStoreTable intercepted =
          intercept(
              t,
              (delegate, snapshot, branch, stats) -> {
                append(t, GenericRow.of(1, 11, 11, 2L));
                independentLookupCompact(t);
                return false;
              },
              false);
      OptimizingCommitSkippedException conflict =
          assertThrows(
              OptimizingCommitSkippedException.class, () -> committer(intercepted, task).commit());
      assertFalse(conflict.isPublicationUncertain());
      assertEquals(6L, t.snapshotManager().latestSnapshotId());
      assertOutputExists(t, task);

      PaimonPrimaryKeyCompactionTask retry = prepare(t, OptimizingType.FULL);
      FileStoreTable unavailable =
          intercept(
                  t,
                  (delegate, snapshot, branch, stats) -> {
                    throw new IOException("Unknown publication result");
                  },
                  false)
              .copy(Collections.singletonMap("commit.max-retries", "0"));
      OptimizingCommitException failure =
          assertThrows(
              OptimizingCommitException.class, () -> committer(unavailable, retry).commit());
      assertFalse(failure instanceof OptimizingCommitSkippedException);
      assertOutputExists(t, retry);
    }
  }

  @Test
  void protectedCommitRunsInsideTableAuthentication(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "none");
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.FULL);
      PaimonTable authenticated = mock(PaimonTable.class);
      AtomicInteger scope = new AtomicInteger();
      when(authenticated.doAs(any()))
          .thenAnswer(
              call -> {
                scope.incrementAndGet();
                try {
                  return call.getArgument(0, Callable.class).call();
                } finally {
                  scope.decrementAndGet();
                }
              });
      FileStoreTable observed =
          intercept(
              t,
              (delegate, snapshot, branch, stats) -> {
                assertEquals(1, scope.get());
                return delegate.commit(snapshot, branch, stats);
              },
              false);
      PaimonPrimaryKeyTableCommit commit =
          new PaimonPrimaryKeyTableCommit(authenticated, observed, Collections.singletonList(task));
      commit.commit();
      assertEquals(ReconciliationResult.PUBLISHED, commit.reconcile());
      verify(authenticated, times(2)).doAs(any());
    }
  }

  @Test
  void lostResponseAndReplayDoNotDuplicateLookupChangelog(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "lookup");
      org.apache.paimon.table.source.StreamTableScan changes = t.newReadBuilder().newStreamScan();
      changes.restore(5L);
      append(t, GenericRow.of(1, 11, 11, 2L));
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.FULL);
      FileStoreTable unavailable =
          intercept(
                  t,
                  (delegate, snapshot, branch, stats) -> {
                    boolean published = delegate.commit(snapshot, branch, stats);
                    if (published) {
                      throw new IOException("Lost COMPACT reply");
                    }
                    return false;
                  },
                  false)
              .copy(Collections.singletonMap("commit.max-retries", "0"));
      committer(unavailable, task).commit();
      assertEquals(
          java.util.Arrays.asList("+U:1:11:11:2", "-U:1:10:10:1"),
          TestPaimonPrimaryKeyProtectedSemantics.drain(t, changes));
      committer(t, task).commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(
          Collections.emptyList(), TestPaimonPrimaryKeyProtectedSemantics.drain(t, changes));
      assertEquals(6L, t.snapshotManager().latestSnapshotId());
    }
  }

  @Test
  void invalidContextRetainsItsOriginalDiagnostic(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, "none");
      PaimonPrimaryKeyCompactionTask task = prepare(t, OptimizingType.MINOR);
      java.lang.reflect.Field version =
          PaimonPrimaryKeyReadDependency.class.getDeclaredField("version");
      version.setAccessible(true);
      version.set(task.getOutput().getReadDependency(), 2);
      OptimizingCommitException failure =
          assertThrows(OptimizingCommitException.class, () -> committer(t, task).commit());
      assertTrue(failure.getCause() instanceof PaimonPrimaryKeyCommitGuard.ReadConflict);
      assertTrue(failure.getCause().getMessage().contains("Invalid read dependency version"));
      assertEquals(4L, t.snapshotManager().latestSnapshotId());
      assertOutputExists(t, task);
    }
  }

  static PaimonPrimaryKeyCompactionTask prepare(FileStoreTable table, OptimizingType type)
      throws Exception {
    PaimonPrimaryKeyCompactionInput input = input(table, type, 9901L);
    PaimonPrimaryKeyCompactionTask task =
        new PaimonPrimaryKeyCompactionTask(1L, "bucket", input, new HashMap<>());
    task.setOutput(
        SerializationUtil.simpleSerialize(new PaimonPrimaryKeyCompactionExecutor(input).execute()));
    assertNotNull(task.getOutput().getReadDependency());
    return task;
  }

  static PaimonPrimaryKeyTableCommit committer(
      FileStoreTable table, PaimonPrimaryKeyCompactionTask task) {
    return new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(task));
  }

  static void assertOutputExists(FileStoreTable table, PaimonPrimaryKeyCompactionTask task)
      throws Exception {
    for (org.apache.paimon.table.sink.CommitMessageImpl message :
        decode(task.getOutput().getCommitMessageBytesList())) {
      for (org.apache.paimon.io.DataFileMeta file : message.compactIncrement().compactAfter()) {
        assertTrue(
            table
                .fileIO()
                .exists(
                    table
                        .store()
                        .pathFactory()
                        .createDataFilePathFactory(message.partition(), message.bucket())
                        .toPath(file)),
            "Uncertain/rejected output must not be deleted");
      }
    }
  }

  static FileStoreTable intercept(
      FileStoreTable table, PublicationAction action, boolean failClose) {
    CatalogEnvironment environment = spy(table.catalogEnvironment());
    doAnswer(
            call -> {
              SnapshotCommit delegate = (SnapshotCommit) call.callRealMethod();
              return new SnapshotCommit() {
                @Override
                public boolean commit(
                    Snapshot snapshot, String branch, List<PartitionStatistics> stats)
                    throws Exception {
                  return action.commit(delegate, snapshot, branch, stats);
                }

                @Override
                public void close() throws Exception {
                  delegate.close();
                  if (failClose) {
                    throw new IOException("Injected close failure");
                  }
                }
              };
            })
        .when(environment)
        .snapshotCommit(any(SnapshotManager.class));
    return new PrimaryKeyFileStoreTable(
        table.fileIO(), table.location(), table.schema(), environment);
  }

  interface PublicationAction {
    boolean commit(
        SnapshotCommit delegate, Snapshot snapshot, String branch, List<PartitionStatistics> stats)
        throws Exception;
  }
}
