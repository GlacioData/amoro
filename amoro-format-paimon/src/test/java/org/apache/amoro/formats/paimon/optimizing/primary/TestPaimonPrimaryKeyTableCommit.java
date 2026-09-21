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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.exception.OptimizingCommitSkippedException;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonPrimaryKeyOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.optimizing.TableOptimizingCommitter.ReconciliationResult;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

@DisplayName("PaimonPrimaryKeyTableCommit")
class TestPaimonPrimaryKeyTableCommit {

  @Test
  void fullCommitAndReplayKeepAllFileIoInsideAuthentication(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = fsCatalog(warehouse)) {
      Map<String, String> options = primaryKeyOptions();
      options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
      Identifier id = createPrimaryKeyTable(catalog, "authenticated_full", options);
      writeCommits(catalog.getTable(id), 2);
      OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> plan =
          planner(
                  catalog,
                  id,
                  Collections.emptyMap(),
                  defaultConfig().setMinorLeastFileCount(99).setFullTriggerInterval(1))
              .plan();
      assertEquals(OptimizingType.FULL, plan.getOptimizingType());
      List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>(plan.getTasks());
      for (PaimonPrimaryKeyCompactionTask task : tasks) {
        setOutput(task, new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute());
      }
      FileStoreTable original = (FileStoreTable) catalog.getTable(id);
      AtomicBoolean armed = new AtomicBoolean(false);
      AtomicBoolean authenticated = new AtomicBoolean(false);
      java.util.concurrent.atomic.AtomicInteger ioCalls =
          new java.util.concurrent.atomic.AtomicInteger();
      org.apache.paimon.fs.FileIO guarded =
          (org.apache.paimon.fs.FileIO)
              java.lang.reflect.Proxy.newProxyInstance(
                  getClass().getClassLoader(),
                  new Class<?>[] {org.apache.paimon.fs.FileIO.class},
                  (proxy, method, args) -> {
                    if (armed.get() && method.getDeclaringClass() != Object.class) {
                      assertTrue(
                          authenticated.get(),
                          "FileIO outside authentication: " + method.getName());
                      ioCalls.incrementAndGet();
                    }
                    try {
                      return method.invoke(original.fileIO(), args);
                    } catch (java.lang.reflect.InvocationTargetException failure) {
                      throw failure.getCause();
                    }
                  });
      FileStoreTable table =
          org.apache.paimon.table.FileStoreTableFactory.create(
              guarded, original.location(), original.schema());
      PaimonTable wrapper = mock(PaimonTable.class);
      when(wrapper.doAs(any()))
          .thenAnswer(
              invocation -> {
                authenticated.set(true);
                try {
                  return ((Callable<?>) invocation.getArgument(0)).call();
                } finally {
                  authenticated.set(false);
                }
              });
      armed.set(true);
      PaimonPrimaryKeyTableCommit committer =
          new PaimonPrimaryKeyTableCommit(wrapper, table, tasks);
      committer.commit();
      committer.commit(CommitMode.RECOVERY_REPLAY);
      assertTrue(ioCalls.get() > 0);
      assertFalse(authenticated.get());
      verify(wrapper, times(2)).doAs(any());
    }
  }

  @Test
  @DisplayName("Committer commits executor output as a COMPACT snapshot")
  void committerCommitsExecutorOutput(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_commit_output", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 2);
    List<String> rowsBefore = readRows(catalog.getTable(id));
    List<PaimonPrimaryKeyCompactionTask> tasks = planAndExecute(catalog, id);
    assertFalse(tasks.isEmpty());
    FileStoreTable table = (FileStoreTable) catalog.getTable(id);
    long snapshotBefore = table.snapshotManager().latestSnapshot().id();
    AtomicBoolean insideDoAs = new AtomicBoolean(false);
    PaimonTable paimonTable = mock(PaimonTable.class);
    when(paimonTable.doAs(any()))
        .thenAnswer(
            invocation -> {
              insideDoAs.set(true);
              Callable<?> callable = invocation.getArgument(0);
              return callable.call();
            });

    PaimonPrimaryKeyTableCommit committer =
        new PaimonPrimaryKeyTableCommit(paimonTable, table, tasks);
    committer.commit();

    FileStoreTable afterCommit = (FileStoreTable) catalog.getTable(id);
    Snapshot snapshot = afterCommit.snapshotManager().latestSnapshot();
    assertEquals(snapshotBefore + 1, snapshot.id());
    assertEquals(Snapshot.CommitKind.COMPACT, snapshot.commitKind());
    assertEquals(7L, snapshot.commitIdentifier());
    org.apache.amoro.optimizing.FormatTableAnalysis historical =
        org.apache.amoro.formats.paimon.optimizing.health.PaimonHistoricalHealth.evaluate(
            afterCommit, id.toString(), snapshotBefore);
    assertEquals(Long.valueOf(snapshotBefore), historical.healthDetails().getSnapshotId());
    assertTrue(historical.pendingInput().getHealthScore() >= 0);
    assertEquals(
        snapshot.id(),
        org.apache.amoro.formats.paimon.optimizing.health.PaimonHistoricalHealth.findCommit(
                afterCommit, snapshotBefore, snapshot.commitUser(), 7L)
            .snapshot()
            .id());

    assertEquals(rowsBefore, readRows(afterCommit));
    assertTrue(insideDoAs.get());
    verify(paimonTable).doAs(any());
  }

  @Test
  @DisplayName("Planned-snapshot compaction preserves rows appended before commit")
  void plannedSnapshotCompactionPreservesConcurrentAppend(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id =
        createPrimaryKeyTable(catalog, "t_planned_snapshot_concurrent_append", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 2);
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, runtimeOptions("num-sorted-run.compaction-trigger", "2")).plan();
    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>(result.getTasks());

    writeCommit(catalog.getTable(id), 2, "name-2");
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      setOutput(task, new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute());
    }
    new PaimonPrimaryKeyTableCommit((FileStoreTable) catalog.getTable(id), tasks).commit();

    assertEquals(
        java.util.Arrays.asList("0|name-0", "1|name-1", "2|name-2"),
        readRows(catalog.getTable(id)));
  }

  @Test
  @DisplayName("Compaction preserves a concurrent primary-key update backed by deletion vectors")
  void compactionPreservesConcurrentDeletionVectorUpdate(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
    Identifier id = createPrimaryKeyTable(catalog, "t_concurrent_dv_update", options);
    writeCommits(catalog.getTable(id), 2);
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, runtimeOptions("num-sorted-run.compaction-trigger", "2")).plan();
    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>(result.getTasks());

    writeCommit(catalog.getTable(id), 0, "name-0-new");
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      setOutput(task, new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute());
    }
    new PaimonPrimaryKeyTableCommit((FileStoreTable) catalog.getTable(id), tasks).commit();

    assertEquals(
        java.util.Arrays.asList("0|name-0-new", "1|name-1"), readRows(catalog.getTable(id)));
  }

  @Test
  @DisplayName("Committer filters replayed commit identity")
  void committerFiltersReplayedCommitIdentity(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_replay_commit", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 2);
    List<PaimonPrimaryKeyCompactionTask> tasks = planAndExecute(catalog, id);
    assertFalse(tasks.isEmpty());
    FileStoreTable table = (FileStoreTable) catalog.getTable(id);

    new PaimonPrimaryKeyTableCommit(table, tasks).commit();
    long snapshotAfterFirstCommit =
        ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id();

    PaimonPrimaryKeyTableCommit replay =
        new PaimonPrimaryKeyTableCommit((FileStoreTable) catalog.getTable(id), tasks);
    replay.commit(CommitMode.RECOVERY_REPLAY);

    assertEquals(
        snapshotAfterFirstCommit,
        ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id());
  }

  @Test
  @DisplayName("NORMAL mode uses direct commit without historical filtering")
  void normalModeUsesDirectCommit() throws Exception {
    FileStoreTable sourceTable = mock(FileStoreTable.class);
    FileStoreTable commitTable = mock(FileStoreTable.class);
    TableCommitImpl directCommit = mock(TableCommitImpl.class);
    List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
    when(sourceTable.copy(anyMap())).thenReturn(commitTable);
    when(commitTable.name()).thenReturn("direct-primary-key-table");
    when(commitTable.newCommit("direct-user")).thenReturn(directCommit);

    new PaimonPrimaryKeyTableCommit(sourceTable, Collections.emptyList())
        .commitMessages(messages, "direct-user", 201L, CommitMode.NORMAL);

    verify(directCommit).commit(201L, messages);
    verify(directCommit, never()).filterAndCommit(anyMap());
    verify(directCommit).close();
    verify(commitTable).newCommit("direct-user");
  }

  @Test
  @DisplayName("NORMAL failure retries once with a fresh filterAndCommit")
  void normalFailureFallsBackWithFreshCommit() throws Exception {
    FileStoreTable sourceTable = mock(FileStoreTable.class);
    FileStoreTable commitTable = mock(FileStoreTable.class);
    TableCommitImpl directCommit = mock(TableCommitImpl.class);
    TableCommitImpl fallbackCommit = mock(TableCommitImpl.class);
    List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
    RuntimeException directFailure = new RuntimeException("direct-failure");
    when(sourceTable.copy(anyMap())).thenReturn(commitTable);
    when(commitTable.name()).thenReturn("fallback-primary-key-table");
    when(commitTable.newCommit("fallback-user")).thenReturn(directCommit, fallbackCommit);
    doThrow(directFailure).when(directCommit).commit(202L, messages);
    when(fallbackCommit.filterAndCommit(Collections.singletonMap(202L, messages))).thenReturn(1);

    new PaimonPrimaryKeyTableCommit(sourceTable, Collections.emptyList())
        .commitMessages(messages, "fallback-user", 202L, CommitMode.NORMAL);

    InOrder order = inOrder(commitTable, directCommit, fallbackCommit);
    order.verify(commitTable).newCommit("fallback-user");
    order.verify(directCommit).commit(202L, messages);
    order.verify(directCommit).close();
    order.verify(commitTable).newCommit("fallback-user");
    order.verify(fallbackCommit).filterAndCommit(Collections.singletonMap(202L, messages));
    order.verify(fallbackCommit).close();
    verify(commitTable, times(2)).newCommit("fallback-user");
  }

  @Test
  @DisplayName("RECOVERY_REPLAY mode only uses filterAndCommit")
  void recoveryReplayOnlyUsesFilterAndCommit() throws Exception {
    FileStoreTable sourceTable = mock(FileStoreTable.class);
    FileStoreTable commitTable = mock(FileStoreTable.class);
    TableCommitImpl replayCommit = mock(TableCommitImpl.class);
    List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
    when(sourceTable.copy(anyMap())).thenReturn(commitTable);
    when(commitTable.name()).thenReturn("replay-primary-key-table");
    when(commitTable.newCommit("replay-user")).thenReturn(replayCommit);
    when(replayCommit.filterAndCommit(Collections.singletonMap(203L, messages))).thenReturn(0);

    new PaimonPrimaryKeyTableCommit(sourceTable, Collections.emptyList())
        .commitMessages(messages, "replay-user", 203L, CommitMode.RECOVERY_REPLAY);

    verify(replayCommit, never()).commit(203L, messages);
    verify(replayCommit).filterAndCommit(Collections.singletonMap(203L, messages));
    verify(replayCommit).close();
    verify(commitTable).newCommit("replay-user");
  }

  @Test
  @DisplayName("A direct close failure triggers the idempotent fallback")
  void directCloseFailureTriggersFallback() throws Exception {
    FileStoreTable sourceTable = mock(FileStoreTable.class);
    FileStoreTable commitTable = mock(FileStoreTable.class);
    TableCommitImpl directCommit = mock(TableCommitImpl.class);
    TableCommitImpl fallbackCommit = mock(TableCommitImpl.class);
    List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
    Exception closeFailure = new Exception("direct-close-failure");
    when(sourceTable.copy(anyMap())).thenReturn(commitTable);
    when(commitTable.name()).thenReturn("close-failure-primary-key-table");
    when(commitTable.newCommit("close-failure-user")).thenReturn(directCommit, fallbackCommit);
    doThrow(closeFailure).when(directCommit).close();
    when(fallbackCommit.filterAndCommit(Collections.singletonMap(206L, messages))).thenReturn(0);

    new PaimonPrimaryKeyTableCommit(sourceTable, Collections.emptyList())
        .commitMessages(messages, "close-failure-user", 206L, CommitMode.NORMAL);

    verify(directCommit).commit(206L, messages);
    verify(fallbackCommit).filterAndCommit(Collections.singletonMap(206L, messages));
    verify(commitTable, times(2)).newCommit("close-failure-user");
  }

  @Test
  @DisplayName("Error bypasses fallback and propagates unchanged")
  void errorBypassesFallback() {
    FileStoreTable sourceTable = mock(FileStoreTable.class);
    FileStoreTable commitTable = mock(FileStoreTable.class);
    TableCommitImpl directCommit = mock(TableCommitImpl.class);
    List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
    AssertionError directError = new AssertionError("direct-error");
    when(sourceTable.copy(anyMap())).thenReturn(commitTable);
    when(commitTable.name()).thenReturn("error-primary-key-table");
    when(commitTable.newCommit("error-user")).thenReturn(directCommit);
    doThrow(directError).when(directCommit).commit(207L, messages);

    AssertionError failure =
        assertThrows(
            AssertionError.class,
            () ->
                new PaimonPrimaryKeyTableCommit(sourceTable, Collections.emptyList())
                    .commitMessages(messages, "error-user", 207L, CommitMode.NORMAL));

    assertSame(directError, failure);
    verify(commitTable).newCommit("error-user");
  }

  @Test
  @DisplayName("filterAndCommit rejects an impossible committed count")
  void filterAndCommitRejectsImpossibleCount() {
    FileStoreTable sourceTable = mock(FileStoreTable.class);
    FileStoreTable commitTable = mock(FileStoreTable.class);
    TableCommitImpl replayCommit = mock(TableCommitImpl.class);
    List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
    when(sourceTable.copy(anyMap())).thenReturn(commitTable);
    when(commitTable.name()).thenReturn("invalid-count-primary-key-table");
    when(commitTable.newCommit("invalid-count-user")).thenReturn(replayCommit);
    when(replayCommit.filterAndCommit(Collections.singletonMap(204L, messages))).thenReturn(-1);

    OptimizingCommitException failure =
        assertThrows(
            OptimizingCommitException.class,
            () ->
                new PaimonPrimaryKeyTableCommit(sourceTable, Collections.emptyList())
                    .commitMessages(
                        messages, "invalid-count-user", 204L, CommitMode.RECOVERY_REPLAY));

    assertTrue(failure.getMessage().contains("filterAndCommit"));
    assertTrue(failure.getMessage().contains("-1"));
  }

  @Test
  @DisplayName("Dual failure keeps fallback as cause and direct failure as suppressed")
  void dualFailurePreservesBothCauses() {
    FileStoreTable sourceTable = mock(FileStoreTable.class);
    FileStoreTable commitTable = mock(FileStoreTable.class);
    TableCommitImpl directCommit = mock(TableCommitImpl.class);
    TableCommitImpl fallbackCommit = mock(TableCommitImpl.class);
    List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
    RuntimeException directFailure = new RuntimeException("direct-failure");
    RuntimeException fallbackFailure = new RuntimeException("fallback-failure");
    when(sourceTable.copy(anyMap())).thenReturn(commitTable);
    when(commitTable.name()).thenReturn("dual-failure-primary-key-table");
    when(commitTable.newCommit("dual-failure-user")).thenReturn(directCommit, fallbackCommit);
    doThrow(directFailure).when(directCommit).commit(205L, messages);
    when(fallbackCommit.filterAndCommit(Collections.singletonMap(205L, messages)))
        .thenThrow(fallbackFailure);

    OptimizingCommitException failure =
        assertThrows(
            OptimizingCommitException.class,
            () ->
                new PaimonPrimaryKeyTableCommit(sourceTable, Collections.emptyList())
                    .commitMessages(messages, "dual-failure-user", 205L, CommitMode.NORMAL));

    assertSame(fallbackFailure, failure.getCause());
    assertEquals(1, failure.getSuppressed().length);
    assertSame(directFailure, failure.getSuppressed()[0]);
  }

  @Test
  @DisplayName("Committer skips stale FULL output when snapshot changed after execute")
  void committerSkipsStaleFullOutputWhenSnapshotChangedAfterExecute(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    Identifier id = createPrimaryKeyTable(catalog, "t_stale_full_commit", options);
    writeCommits(catalog.getTable(id), 2);
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                Collections.emptyMap(),
                defaultConfig().setMinorLeastFileCount(99).setFullTriggerInterval(1))
            .plan();
    assertEquals(OptimizingType.FULL, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>(result.getTasks());
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute();
      assertFalse(output.getCommitMessageBytesList().isEmpty());
      setOutput(task, output);
    }
    writeCommits(catalog.getTable(id), 1);
    long snapshotBeforeCommit =
        ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id();

    PaimonPrimaryKeyTableCommit normal =
        new PaimonPrimaryKeyTableCommit((FileStoreTable) catalog.getTable(id), tasks);
    assertThrows(OptimizingCommitSkippedException.class, normal::commit);
    PaimonPrimaryKeyTableCommit replay =
        new PaimonPrimaryKeyTableCommit((FileStoreTable) catalog.getTable(id), tasks);
    assertThrows(
        OptimizingCommitSkippedException.class, () -> replay.commit(CommitMode.RECOVERY_REPLAY));

    FileStoreTable afterCommit = (FileStoreTable) catalog.getTable(id);
    assertEquals(snapshotBeforeCommit, afterCommit.snapshotManager().latestSnapshot().id());
  }

  @Test
  @DisplayName("Committer rejects success task without output")
  void committerRejectsMissingOutput(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_missing_output", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 2);
    List<PaimonPrimaryKeyCompactionTask> tasks = planAndExecute(catalog, id);
    assertFalse(tasks.isEmpty());
    PaimonPrimaryKeyCompactionTask missingOutput =
        new PaimonPrimaryKeyCompactionTask(
            100L, "primary-key-buckets", tasks.get(0).getInput(), new HashMap<>());
    List<PaimonPrimaryKeyCompactionTask> tasksWithMissingOutput = new ArrayList<>(tasks);
    tasksWithMissingOutput.add(missingOutput);
    FileStoreTable table = (FileStoreTable) catalog.getTable(id);
    long snapshotBefore = table.snapshotManager().latestSnapshot().id();

    OptimizingCommitException ex =
        assertThrows(
            OptimizingCommitException.class,
            () -> new PaimonPrimaryKeyTableCommit(table, tasksWithMissingOutput).commit());

    assertTrue(ex.getMessage().contains("has no Paimon CommitMessage list"));
    assertEquals(
        snapshotBefore,
        ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id());
  }

  @Test
  @DisplayName("Committer rejects success task without table")
  void committerRejectsMissingTable(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_missing_table", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 2);
    List<PaimonPrimaryKeyCompactionTask> tasks = planAndExecute(catalog, id);
    assertFalse(tasks.isEmpty());

    OptimizingCommitException ex =
        assertThrows(
            OptimizingCommitException.class,
            () -> new PaimonPrimaryKeyTableCommit(null, tasks).commit());

    assertTrue(ex.getMessage().contains("have no Paimon table"));
  }

  @Test
  @DisplayName("Committer rejects stale FULL success task without output")
  void committerRejectsStaleFullMissingOutput(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    Identifier id = createPrimaryKeyTable(catalog, "t_stale_full_missing_output", options);
    writeCommits(catalog.getTable(id), 2);
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                Collections.emptyMap(),
                defaultConfig().setMinorLeastFileCount(99).setFullTriggerInterval(1))
            .plan();
    assertEquals(OptimizingType.FULL, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    PaimonPrimaryKeyCompactionTask missingOutput =
        new PaimonPrimaryKeyCompactionTask(
            100L, "primary-key-buckets", result.getTasks().get(0).getInput(), new HashMap<>());
    writeCommits(catalog.getTable(id), 1);
    FileStoreTable table = (FileStoreTable) catalog.getTable(id);
    long snapshotBefore = table.snapshotManager().latestSnapshot().id();

    OptimizingCommitException ex =
        assertThrows(
            OptimizingCommitException.class,
            () ->
                new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(missingOutput))
                    .commit());

    assertTrue(ex.getMessage().contains("has no Paimon CommitMessage list"));
    assertEquals(
        snapshotBefore,
        ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id());
  }

  @Test
  @DisplayName("Committer treats empty commit message list as no-op")
  void committerTreatsEmptyCommitMessageListAsNoOp(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_empty_messages", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 1);
    PaimonPrimaryKeyCompactionInput input =
        new PaimonPrimaryKeyCompactionInput(
            wrap(catalog.getTable(id), id.getObjectName()),
            Collections.singletonList(
                new PaimonBucketCompactionUnit(new byte[] {0}, 0, 1, 1, 1, 0)),
            OptimizingType.MINOR,
            false,
            latestSnapshotId(catalog, id),
            "user",
            1L);
    PaimonPrimaryKeyCompactionTask task =
        new PaimonPrimaryKeyCompactionTask(100L, "primary-key-buckets", input, new HashMap<>());
    setOutput(
        task, new PaimonPrimaryKeyCompactionOutput(Collections.emptyList(), 0, 0, 0, 0, 0, 0));
    FileStoreTable table = (FileStoreTable) catalog.getTable(id);
    long snapshotBefore = table.snapshotManager().latestSnapshot().id();

    new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(task)).commit();

    assertEquals(
        snapshotBefore,
        ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id());
  }

  @Test
  @DisplayName("Empty success task collection is a no-op")
  void emptySuccessTasksNoOp(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_noop", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 1);
    FileStoreTable table = (FileStoreTable) catalog.getTable(id);
    long snapshotBefore = table.snapshotManager().latestSnapshot().id();

    PaimonPrimaryKeyTableCommit committer =
        new PaimonPrimaryKeyTableCommit(table, Collections.emptyList());
    committer.commit();

    assertEquals(
        snapshotBefore,
        ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id());
  }

  @Test
  void recoveryOnlyInspectsPublicationWithoutChangingSnapshotsOrRows(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = fsCatalog(warehouse)) {
      Map<String, String> options = primaryKeyOptions();
      options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
      Identifier id = createPrimaryKeyTable(catalog, "readonly_recovery", options);
      writeCommits(catalog.getTable(id), 2);
      List<PaimonPrimaryKeyCompactionTask> tasks = fullTasks(catalog, id);
      FileStoreTable table = (FileStoreTable) catalog.getTable(id);
      PaimonPrimaryKeyTableCommit committer = new PaimonPrimaryKeyTableCommit(table, tasks);
      long before = latestSnapshotId(catalog, id);
      List<String> rows = readRows(catalog.getTable(id));
      assertEquals(ReconciliationResult.REPLAN, committer.reconcile());
      assertEquals(before, latestSnapshotId(catalog, id));
      assertEquals(rows, readRows(catalog.getTable(id)));
      committer.commit();
      writeCommit(catalog.getTable(id), 99, "later-writer");
      before = latestSnapshotId(catalog, id);
      rows = readRows(catalog.getTable(id));
      assertEquals(
          ReconciliationResult.PUBLISHED,
          new PaimonPrimaryKeyTableCommit((FileStoreTable) catalog.getTable(id), tasks)
              .reconcile());
      assertEquals(before, latestSnapshotId(catalog, id));
      assertEquals(rows, readRows(catalog.getTable(id)));
    }
  }

  @Test
  void readOnlyRecoveryPropagatesStorageFailureAndNeverCreatesCommitter() {
    FileStoreTable table = mockCommitTable();
    when(table.latestSnapshot()).thenThrow(new IllegalStateException("storage unavailable"));
    assertThrows(
        OptimizingCommitException.class,
        () ->
            new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(emptyFullTask()))
                .reconcile());
    verify(table, never()).newCommit(any());
  }

  @Test
  void fullReplayFindsExactIdentityBehindNewerWriterAndKeepsRows(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = fsCatalog(warehouse)) {
      Map<String, String> options = primaryKeyOptions();
      options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
      Identifier id = createPrimaryKeyTable(catalog, "full_replay", options);
      writeCommits(catalog.getTable(id), 2);
      List<PaimonPrimaryKeyCompactionTask> tasks = fullTasks(catalog, id);
      FileStoreTable table = (FileStoreTable) catalog.getTable(id);
      new PaimonPrimaryKeyTableCommit(table, tasks).commit();
      writeCommit(catalog.getTable(id), 99, "later-writer");
      List<String> rows = readRows(catalog.getTable(id));
      long snapshot = latestSnapshotId(catalog, id);
      new PaimonPrimaryKeyTableCommit((FileStoreTable) catalog.getTable(id), tasks)
          .commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(snapshot, latestSnapshotId(catalog, id));
      assertEquals(rows, readRows(catalog.getTable(id)));
    }
  }

  @Test
  void staleProofRequiresEqualIdentityAndStopsAtExpiredSnapshot() throws Exception {
    FileStoreTable table = mockCommitTable();
    org.apache.paimon.utils.SnapshotManager manager =
        mock(org.apache.paimon.utils.SnapshotManager.class);
    when(table.snapshotManager()).thenReturn(manager);
    Snapshot latest = snapshot(4, "user", 100);
    when(table.latestSnapshot()).thenReturn(java.util.Optional.of(latest));
    Snapshot exact = snapshot(3, "user", 7);
    when(manager.tryGetSnapshot(3)).thenReturn(exact);
    new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(emptyFullTask()))
        .commit(CommitMode.RECOVERY_REPLAY);
    verify(table, never()).newCommit(any());

    when(manager.tryGetSnapshot(3)).thenThrow(new java.io.FileNotFoundException("expired"));
    assertThrows(
        OptimizingCommitSkippedException.class,
        () ->
            new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(emptyFullTask()))
                .commit());
    verify(manager, never()).tryGetSnapshot(2);
    verify(table, never()).newCommit(any());
  }

  @Test
  void exactIdentityProbeIsBoundedAndIoFailureIsNotSkip() throws Exception {
    FileStoreTable table = mockCommitTable();
    org.apache.paimon.utils.SnapshotManager manager =
        mock(org.apache.paimon.utils.SnapshotManager.class);
    when(table.snapshotManager()).thenReturn(manager);
    Snapshot latest = snapshot(1000, "other", 7);
    when(table.latestSnapshot()).thenReturn(java.util.Optional.of(latest));
    when(manager.tryGetSnapshot(org.mockito.ArgumentMatchers.anyLong()))
        .thenAnswer(call -> snapshot(call.getArgument(0), "other", 7));
    OptimizingCommitSkippedException skipped =
        assertThrows(
            OptimizingCommitSkippedException.class,
            () ->
                new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(emptyFullTask()))
                    .commit());
    assertTrue(skipped.getMessage().contains("BUDGET_EXHAUSTED"));
    verify(manager, times(255)).tryGetSnapshot(org.mockito.ArgumentMatchers.anyLong());
    verify(table, never()).newCommit(any());
    RuntimeException denied = new RuntimeException("Permission denied");
    when(manager.tryGetSnapshot(999)).thenThrow(denied);
    OptimizingCommitException failed =
        assertThrows(
            OptimizingCommitException.class,
            () ->
                new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(emptyFullTask()))
                    .commit());
    assertFalse(failed instanceof OptimizingCommitSkippedException);
    assertSame(denied, failed.getCause());
  }

  @Test
  void explicitSkipCancelsWholeFullAndUnknownMarkerFailsClosed() throws Exception {
    FileStoreTable table = mockCommitTable();
    PaimonPrimaryKeyCompactionTask task = emptyFullTask();
    PaimonPrimaryKeyCompactionTask skipped = emptyFullTask();
    setOutput(skipped, PaimonPrimaryKeyCompactionOutput.staleFull());
    assertThrows(
        OptimizingCommitSkippedException.class,
        () ->
            new PaimonPrimaryKeyTableCommit(table, java.util.Arrays.asList(task, skipped))
                .commit());
    verify(table).latestSnapshot();
    verify(table, never()).newCommit(any());
    PaimonPrimaryKeyCompactionOutput invalid = PaimonPrimaryKeyCompactionOutput.staleFull();
    java.lang.reflect.Field field = invalid.getClass().getDeclaredField("skipReason");
    field.setAccessible(true);
    field.set(invalid, "FUTURE_REASON");
    setOutput(skipped, invalid);
    OptimizingCommitException failure =
        assertThrows(
            OptimizingCommitException.class,
            () ->
                new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(skipped))
                    .commit());
    assertFalse(failure instanceof OptimizingCommitSkippedException);
    assertTrue(failure.getMessage().contains("Invalid"));
  }

  @Test
  void executionSkipConflictingWithPublishedIdentityFails() {
    FileStoreTable table = mockCommitTable();
    Snapshot exact = snapshot(2, "user", 7);
    when(table.latestSnapshot()).thenReturn(java.util.Optional.of(exact));
    PaimonPrimaryKeyCompactionTask task = emptyFullTask();
    setOutput(task, PaimonPrimaryKeyCompactionOutput.staleFull());
    OptimizingCommitException failure =
        assertThrows(
            OptimizingCommitException.class,
            () -> new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(task)).commit());
    assertFalse(failure instanceof OptimizingCommitSkippedException);
    assertTrue(failure.getMessage().contains("conflicts"));
    verify(table, never()).newCommit(any());
  }

  @Test
  void directFailureRechecksFreshnessBeforeFallback(@TempDir Path warehouse) throws Exception {
    assertFullDirectFailure(warehouse.resolve("published"), true);
    assertFullDirectFailure(warehouse.resolve("unpublished"), false);
  }

  private static void assertFullDirectFailure(Path warehouse, boolean published) throws Exception {
    try (Catalog catalog = fsCatalog(warehouse)) {
      Map<String, String> options = primaryKeyOptions();
      options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
      Identifier id = createPrimaryKeyTable(catalog, "fallback", options);
      writeCommits(catalog.getTable(id), 2);
      List<PaimonPrimaryKeyCompactionTask> tasks = fullTasks(catalog, id);
      FileStoreTable real = (FileStoreTable) catalog.getTable(id);
      FileStoreTable guarded = org.mockito.Mockito.spy(real);
      when(guarded.copy(anyMap())).thenReturn(guarded);
      TableCommitImpl direct = mock(TableCommitImpl.class);
      org.mockito.Mockito.doAnswer(
              call -> {
                if (published) {
                  try (org.apache.paimon.table.sink.StreamTableCommit nativeCommit =
                      real.newCommit(tasks.get(0).getInput().getCommitUser())) {
                    nativeCommit.commit(call.getArgument(0), call.getArgument(1));
                  }
                }
                writeCommit(catalog.getTable(id), 99, "concurrent");
                throw new RuntimeException("ambiguous direct response");
              })
          .when(direct)
          .commit(org.mockito.ArgumentMatchers.anyLong(), any());
      org.mockito.Mockito.doReturn(direct).when(guarded).newCommit(any());
      PaimonPrimaryKeyTableCommit committer = new PaimonPrimaryKeyTableCommit(guarded, tasks);
      if (published) {
        committer.commit();
      } else {
        assertTrue(
            assertThrows(OptimizingCommitSkippedException.class, committer::commit)
                .isPublicationUncertain());
      }
      assertEquals(published ? 4L : 3L, latestSnapshotId(catalog, id));
      assertTrue(readRows(catalog.getTable(id)).contains("99|concurrent"));
      verify(guarded, times(1)).newCommit(any());
      verify(direct, never()).filterAndCommit(anyMap());
    }
  }

  private static List<PaimonPrimaryKeyCompactionTask> fullTasks(Catalog catalog, Identifier id)
      throws Exception {
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> plan =
        planner(
                catalog,
                id,
                Collections.emptyMap(),
                defaultConfig().setMinorLeastFileCount(99).setFullTriggerInterval(1))
            .plan();
    assertEquals(OptimizingType.FULL, plan.getOptimizingType());
    List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>(plan.getTasks());
    assertFalse(tasks.isEmpty());
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      setOutput(task, new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute());
    }
    return tasks;
  }

  private static FileStoreTable mockCommitTable() {
    FileStoreTable table = mock(FileStoreTable.class);
    when(table.copy(anyMap())).thenReturn(table);
    return table;
  }

  private static Snapshot snapshot(long id, String user, long identifier) {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(id);
    when(snapshot.commitUser()).thenReturn(user);
    when(snapshot.commitIdentifier()).thenReturn(identifier);
    return snapshot;
  }

  private static PaimonPrimaryKeyCompactionTask emptyFullTask() {
    PaimonPrimaryKeyCompactionTask task =
        new PaimonPrimaryKeyCompactionTask(
            1L,
            "p",
            new PaimonPrimaryKeyCompactionInput(
                null, Collections.emptyList(), OptimizingType.FULL, true, 1L, "user", 7L),
            new HashMap<>());
    setOutput(task, new PaimonPrimaryKeyCompactionOutput());
    return task;
  }

  private static List<PaimonPrimaryKeyCompactionTask> planAndExecute(Catalog catalog, Identifier id)
      throws Exception {
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, runtimeOptions("num-sorted-run.compaction-trigger", "2")).plan();
    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>(result.getTasks());
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute();
      assertFalse(output.getCommitMessageBytesList().isEmpty());
      setOutput(task, output);
    }
    return tasks;
  }

  private static void setOutput(
      PaimonPrimaryKeyCompactionTask task, PaimonPrimaryKeyCompactionOutput output) {
    ByteBuffer buffer = SerializationUtil.simpleSerialize(output);
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    task.setOutputBytes(bytes);
  }

  private static PaimonPrimaryKeyOptimizingPlanner planner(
      Catalog catalog, Identifier id, Map<String, String> runtimeOptions) throws Exception {
    return planner(catalog, id, runtimeOptions, defaultConfig());
  }

  private static PaimonPrimaryKeyOptimizingPlanner planner(
      Catalog catalog, Identifier id, Map<String, String> runtimeOptions, OptimizingConfig config)
      throws Exception {
    PaimonTable table = wrap(catalog.getTable(id).copy(runtimeOptions), id.getObjectName());
    return new PaimonPrimaryKeyOptimizingPlanner(
        table, 100L, 7L, 4.0, 64L * 1024 * 1024, config, 0L, 0L, 0L, null);
  }

  private static Catalog fsCatalog(Path warehouse) {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    return PaimonCatalogFactory.paimonCatalog(props, new Configuration());
  }

  private static Identifier createPrimaryKeyTable(
      Catalog catalog, String tableName, Map<String, String> options) throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .primaryKey("id");
    options.forEach(builder::option);
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return id;
  }

  private static void writeCommits(Table table, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      writeCommit(table, i, "name-" + i);
    }
  }

  private static void writeCommit(Table table, int id, String name) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite()) {
      write.write(GenericRow.of(id, BinaryString.fromString(name)));
      List<CommitMessage> messages = write.prepareCommit();
      try (BatchTableCommit commit = builder.newCommit()) {
        commit.commit(messages);
      }
    }
  }

  private static List<String> readRows(Table table) throws Exception {
    ReadBuilder readBuilder = table.newReadBuilder();
    List<String> rows = new ArrayList<>();
    RecordReader<InternalRow> reader =
        readBuilder.newRead().createReader(readBuilder.newScan().plan());
    reader.forEachRemaining(row -> rows.add(row.getInt(0) + "|" + row.getString(1).toString()));
    Collections.sort(rows);
    return rows;
  }

  private static long latestSnapshotId(Catalog catalog, Identifier id) throws Exception {
    return ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id();
  }

  private static Map<String, String> primaryKeyOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(CoreOptions.WRITE_ONLY.key(), "true");
    options.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");
    options.put("bucket", "1");
    return options;
  }

  private static Map<String, String> runtimeOptions(String... keyValues) {
    Map<String, String> options = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      options.put(keyValues[i], keyValues[i + 1]);
    }
    return options;
  }

  private static PaimonTable wrap(Table table, String name) {
    return new PaimonTable(TableIdentifier.of("test_catalog", "db1", name), table);
  }

  private static OptimizingConfig defaultConfig() {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastFileCount(2)
        .setMinorLeastInterval(0)
        .setFullTriggerInterval(-1)
        .setFullRewriteAllFiles(false)
        .setMaxTaskSize(64L * 1024 * 1024);
  }
}
