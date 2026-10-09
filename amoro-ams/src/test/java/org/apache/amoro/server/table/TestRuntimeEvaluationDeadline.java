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

package org.apache.amoro.server.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport;
import org.apache.amoro.formats.paimon.process.PaimonProcessFactory;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.optimizing.SchedulingPolicy;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.scheduler.inline.TableRuntimeRefreshExecutor;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Derby runtime deadline coverage, with a real Paimon flow and controlled concurrency cases. */
class TestRuntimeEvaluationDeadline extends PersistentBase {
  private static final StateKey<PaimonPendingInput> INPUT =
      StateKey.stateKey("pending_input")
          .jsonType(PaimonPendingInput.class)
          .defaultValue(new PaimonPendingInput());
  private final List<StateKey<?>> keys = new ArrayList<>(DefaultTableRuntime.REQUIRED_STATES);
  private ServerTableIdentifier id;
  private DefaultTableRuntimeStore store;
  private DefaultTableRuntime runtime;
  private PaimonTable table;
  private TableAnalysisKey key;

  @BeforeAll
  static void database() {
    FullSkipTestDatabase.initialize();
  }

  @BeforeEach
  void create() {
    keys.add(INPUT);
    id =
        ServerTableIdentifier.of(
            "deadline", "db", UUID.randomUUID().toString(), TableFormat.PAIMON);
    doAs(TableMetaMapper.class, m -> m.insertTable(id));
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("default");
    meta.setStatusCode(OptimizingStatus.IDLE.getCode());
    Map<String, String> config = new HashMap<>();
    config.put("self-optimizing.enabled", "true");
    config.put("write-only", "true");
    config.put("self-optimizing.full.trigger.interval", "-1");
    config.put("changelog-producer", "lookup");
    config.put("lookup-wait", "false");
    config.put("lookup-compact", "gentle");
    config.put("bucket", "1");
    config.put("self-optimizing.minor.trigger.interval", "0");
    meta.setTableConfig(config);
    doAs(TableRuntimeMapper.class, m -> m.insertRuntime(meta));
    store = new DefaultTableRuntimeStore(id, meta, keys, Collections.emptyList());
    table = mock(PaimonTable.class);
    when(table.supportsPendingInputRefresh()).thenReturn(true);
    when(table.properties()).thenAnswer(call -> new HashMap<>(store.getTableConfig()));
    key = key(10L, "config");
    when(table.currentAnalysisKey(any())).thenAnswer(call -> Optional.of(key));
    runtime = new DefaultTableRuntime(store, () -> table, INPUT);
    runtime.refresh(table);
    store
        .begin()
        .updateState(
            DefaultTableRuntime.OPTIMIZING_STATE_KEY,
            state -> {
              state.setCurrentSnapshotId(10L);
              state.setLastOptimizedSnapshotId(10L);
              state.setCurrentChangeSnapshotId(-1L);
              state.setLastOptimizedChangeSnapshotId(-1L);
              return state;
            })
        .commit();
  }

  @AfterEach
  void clean() {
    doAs(TableRuntimeMapper.class, m -> m.removeAllTableStates(id.getId()));
    doAs(TableRuntimeMapper.class, m -> m.deleteRuntime(id.getId()));
    doAs(TableMetaMapper.class, m -> m.deleteTableIdById(id.getId()));
  }

  @Test
  @Timeout(30)
  void realGentleTableRebuildsDeadlineAfterCatchUpAndEntersQueue(@TempDir Path warehouse)
      throws Exception {
    Map<String, String> options = PaimonPrimaryKeyIntervalTestSupport.options(false);
    options.put("changelog-producer", "lookup");
    options.put("lookup-wait", "false");
    options.put("lookup-compact", "gentle");
    options.put("num-sorted-run.compaction-trigger", "5");
    options.put("lookup-compact.max-interval", "100");
    options.put("self-optimizing.full.trigger.interval", "-1");
    options.put("self-optimizing.minor.trigger.interval", "5000");
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      FileStoreTable nativeTable =
          PaimonPrimaryKeyIntervalTestSupport.table(catalog, "gentle", options);
      PaimonPrimaryKeyIntervalTestSupport.append(nativeTable, GenericRow.of(1, 2, 3, 4L));
      table = PaimonPrimaryKeyIntervalTestSupport.wrap(nativeTable);
      runtime.refresh(table);
      // Warm up native manifest readers before starting the real MINOR interval window.
      assertTrue(table.evaluatePendingInput(runtime, 20).isPresent());
      long minorTime = System.currentTimeMillis();
      store
          .begin()
          .updateState(
              DefaultTableRuntime.OPTIMIZING_STATE_KEY,
              state -> {
                state.setLastMinorOptimizingTime(minorTime);
                state.setLastOptimizedSnapshotId(-1L);
                return state;
              })
          .commit();
      runtime.refresh(table);
      assertTrue(runtime.shouldEvaluateCurrentPlanningAnalysis());
      assertFalse(runtime.evaluatePendingInputAndTransition(table, 20));
      assertEquals(runtime.getCurrentSnapshotId(), runtime.getLastOptimizedSnapshotId());
      // Catching up the optimized snapshot changes the health key once. The next real scan
      // rebuilds the scheduling hint for that key; later idle refreshes do not rescan.
      runtime.refresh(table);
      assertTrue(runtime.shouldEvaluateCurrentPlanningAnalysis());
      assertFalse(runtime.evaluatePendingInputAndTransition(table, 20));
      runtime.refresh(table);
      assertFalse(runtime.shouldEvaluateCurrentPlanningAnalysis(minorTime + 5000));
      assertTrue(runtime.shouldEvaluateCurrentPlanningAnalysis(minorTime + 5001));
      long remaining = minorTime + 5001 - System.currentTimeMillis();
      if (remaining > 0L) {
        Thread.sleep(remaining);
      }
      assertFalse(runtime.shouldEvaluateCurrentAnalysis(), "Health facts remain current");
      TableRuntimeRefreshExecutor refresh =
          new TableRuntimeRefreshExecutor(mock(TableService.class), 1, 60000L, 20) {
            @Override
            protected AmoroTable<?> loadTable(TableRuntime ignored) {
              return table;
            }
          };
      refresh.execute(runtime);
      assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
      assertTrue(((PaimonPendingInput) runtime.getPendingInput()).isGentleMinorCompactionPending());
      SchedulingPolicy scheduler =
          new SchedulingPolicy(new ResourceGroup.Builder("default", "local").build());
      scheduler.addTable(runtime);
      assertEquals(
          Collections.singletonList(runtime), scheduler.scheduleTables(Collections.emptySet(), 1));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"interval", "disabled", "writer", "consumed", "empty"})
  void ordinaryMinorDemandIsRevalidatedBeforePlanning(String change, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      Map<String, String> options = PaimonPrimaryKeyIntervalTestSupport.options(false);
      options.put("self-optimizing.full.trigger.interval", "-1");
      options.put("self-optimizing.minor.trigger.interval", "0");
      options.put("num-sorted-run.compaction-trigger", "2");
      FileStoreTable nativeTable =
          PaimonPrimaryKeyIntervalTestSupport.table(catalog, "ordinary", options);
      PaimonPrimaryKeyIntervalTestSupport.append(nativeTable, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyIntervalTestSupport.append(nativeTable, GenericRow.of(1, 11, 21, 2L));
      table = PaimonPrimaryKeyIntervalTestSupport.wrap(nativeTable);
      runtime.refresh(table);
      store
          .begin()
          .updateState(
              DefaultTableRuntime.OPTIMIZING_STATE_KEY,
              state -> {
                state.setLastMinorOptimizingTime(System.currentTimeMillis() - 1000L);
                state.setLastOptimizedSnapshotId(state.getCurrentSnapshotId());
                return state;
              })
          .commit();
      runtime.refresh(table);
      assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
      assertEquals(runtime.getCurrentSnapshotId(), runtime.getLastOptimizedSnapshotId());
      assertTrue(((PaimonPendingInput) runtime.getPendingInput()).isSameSnapshotPlanningPending());
      PaimonProcessFactory factory = new PaimonProcessFactory();
      factory.open(Collections.singletonMap(PaimonProcessFactory.OPTIMIZER_ENABLED.key(), "true"));
      assertTrue(factory.createPlanner(runtime, table, 1, 64L * 1024 * 1024).isNecessary());
      if (change.equals("consumed")) {
        PaimonPrimaryKeyIntervalTestSupport.compact(nativeTable, OptimizingType.MINOR, 1L);
      } else if (change.equals("disabled")) {
        nativeTable =
            nativeTable.copy(Collections.singletonMap("self-optimizing.enabled", "false"));
      } else if (change.equals("writer")) {
        nativeTable = nativeTable.copy(Collections.singletonMap("write-only", "false"));
      } else {
        nativeTable =
            nativeTable.copy(
                Collections.singletonMap("self-optimizing.minor.trigger.interval", "600000"));
      }
      table = PaimonPrimaryKeyIntervalTestSupport.wrap(nativeTable);
      runtime.refresh(table);
      // Keep the stale persisted marker: the planner itself must revalidate it before any task.
      assertTrue(((PaimonPendingInput) runtime.getPendingInput()).isSameSnapshotPlanningPending());
      assertFalse(factory.createPlanner(runtime, table, 1, 64L * 1024 * 1024).isNecessary());
      if (change.equals("empty")) {
        store.begin().updateStatusCode(ignored -> OptimizingStatus.PLANNING.getCode()).commit();
        runtime.completeEmptyProcess();
      } else {
        assertFalse(runtime.evaluatePendingInputAndTransition(table, 20));
      }
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
      assertFalse(((PaimonPendingInput) runtime.getPendingInput()).isSameSnapshotPlanningPending());
      runtime.refresh(table);
      assertFalse(runtime.evaluatePendingInputAndTransition(table, 20));
      runtime.refresh(table);
      if (change.equals("disabled") || change.equals("writer")) {
        // Ineligible tables may still lack a health analysis; that is not MINOR demand.
        assertFalse(table.shouldEvaluatePendingInput(runtime, false, Long.MAX_VALUE));
        assertFalse(
            ((PaimonPendingInput) runtime.getPendingInput()).isSameSnapshotPlanningPending());
        assertFalse(factory.createPlanner(runtime, table, 1, 64L * 1024 * 1024).isNecessary());
      } else {
        assertFalse(runtime.shouldEvaluateCurrentPlanningAnalysis());
        assertEquals(change.equals("interval") || change.equals("empty"), due(Long.MAX_VALUE));
      }
      if (change.equals("interval")) {
        reload();
        runtime.refresh(table);
        assertTrue(runtime.shouldEvaluateCurrentPlanningAnalysis());
        assertFalse(runtime.evaluatePendingInputAndTransition(table, 20));
        runtime.refresh(table);
        assertFalse(runtime.shouldEvaluateCurrentPlanningAnalysis());
        assertTrue(
            due(Long.MAX_VALUE), "Restart reconstructs an ordinary MINOR deadline from real facts");
      }
    }
  }

  @Test
  void legacyConstructorsDoNotRequestReevaluation() {
    PaimonPendingInput input = new PaimonPendingInput();
    assertEquals(0L, new PendingInputResult(input, false).nextEvaluationTime());
    assertEquals(0L, new PendingInputResult(input, false, null).nextEvaluationTime());
    assertEquals(0L, new PendingInputResult(input, input, false).nextEvaluationTime());
    assertEquals(0L, new PendingInputResult(input, input, false, null).nextEvaluationTime());
  }

  @Test
  void explicitInvalidationClearsDeadline() {
    apply(1L);
    runtime.invalidateCurrentAnalysisKey();
    assertFalse(due(Long.MAX_VALUE));
    runtime.refresh(table);
    assertFalse(due(Long.MAX_VALUE));
  }

  @Test
  void noDemandDeadlineWakesWithoutNewSnapshot() {
    apply(1L);
    assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    assertEquals(runtime.getCurrentSnapshotId(), runtime.getLastOptimizedSnapshotId());
    assertTrue(
        runtime.shouldEvaluateCurrentPlanningAnalysis(),
        "A successful health result must not suppress the format deadline");
    assertEquals(90, runtime.getRuntimeHealthSnapshot().get().getHealthScore());
  }

  @Test
  void deadlineDoesNotBypassFailedScanBackoff() {
    apply(1L);
    when(table.evaluatePendingInput(any(), anyInt())).thenThrow(new IllegalStateException("scan"));
    assertThrows(
        IllegalStateException.class, () -> runtime.evaluatePendingInputAndTransition(table, 20));
    assertFalse(runtime.shouldEvaluateCurrentAnalysis());
    assertFalse(due(Long.MAX_VALUE));
  }

  @Test
  void deadlineBoundaryAndLegacyResultReplacement() throws Exception {
    apply(100L);
    assertFalse(due(99L));
    assertTrue(due(100L));
    assertTrue(due(101L));
    apply(0L);
    assertFalse(due(Long.MAX_VALUE));
  }

  @Test
  void restartRebuildsDeadlineFromFirstHealthEvaluation() {
    apply(Long.MAX_VALUE);
    assertFalse(runtime.shouldEvaluateCurrentPlanningAnalysis());
    reload();
    runtime.refresh(table);
    assertTrue(
        runtime.shouldEvaluateCurrentPlanningAnalysis(),
        "Restart must rebuild both in-memory health facts and scheduling hints");
    apply(1L);
    assertTrue(runtime.shouldEvaluateCurrentPlanningAnalysis());
  }

  @Test
  void analysisKeyChangeDropsOldDeadline() throws Exception {
    apply(1L);
    key = key(11L, "changed");
    runtime.refresh(table);
    assertTrue(
        runtime.shouldEvaluateCurrentPlanningAnalysis(),
        "A new analysis key still needs its initial health scan");
    apply(0L);
    assertFalse(due(Long.MAX_VALUE));
  }

  @ParameterizedTest
  @ValueSource(strings = {"owner", "plan", "minor", "disabled", "planning"})
  void invalidContextDropsDeadline(String change) throws Exception {
    apply(1L);
    change(change);
    assertFalse(due(Long.MAX_VALUE));
    if (change.equals("owner")) {
      store.begin().updateState(DefaultTableRuntime.PROCESS_ID_KEY, ignored -> 0L).commit();
    }
    if (change.equals("disabled")) {
      store.begin().updateTableConfig(c -> c.put("self-optimizing.enabled", "true")).commit();
    }
    assertFalse(
        due(Long.MAX_VALUE), "An invalidated hint must not reappear when the context is restored");
  }

  @ParameterizedTest
  @ValueSource(strings = {"owner", "plan", "minor", "disabled", "planning"})
  void concurrentChangeCannotPublishOldDeadline(String change) throws Exception {
    apply(0L);
    PendingInputResult delayed = result(1L, false);
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenAnswer(
            call -> {
              entered.countDown();
              assertTrue(release.await(10, TimeUnit.SECONDS));
              return Optional.of(delayed);
            });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> evaluation =
          executor.submit(() -> runtime.evaluatePendingInputAndTransition(table, 20));
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      change(change);
      release.countDown();
      evaluation.get(10, TimeUnit.SECONDS);
      if (change.equals("owner")) {
        store.begin().updateState(DefaultTableRuntime.PROCESS_ID_KEY, ignored -> 0L).commit();
      }
      if (change.equals("disabled")) {
        store.begin().updateTableConfig(c -> c.put("self-optimizing.enabled", "true")).commit();
      }
      assertFalse(due(Long.MAX_VALUE));
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void pendingTransitionClearsDeadlineAndSupportsFormatAdmission() throws Exception {
    apply(1L);
    // Confirm demand through the persisted format marker without changing the snapshot.
    PaimonPendingInput pending = new PaimonPendingInput();
    pending.setGentleMinorCompactionPending(true);
    pending.setDataFileCount(1);
    pending.setHealthScore(90);
    PendingInputResult evaluated = result(0L, true);
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(
            Optional.of(
                new PendingInputResult(
                    evaluated.pendingInput(), pending, true, evaluated.tableAnalysis().get())));
    assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
    assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
    assertFalse(due(Long.MAX_VALUE));
    SchedulingPolicy scheduler =
        new SchedulingPolicy(new ResourceGroup.Builder("default", "local").build());
    scheduler.addTable(runtime);
    assertEquals(
        Collections.singletonList(runtime), scheduler.scheduleTables(Collections.emptySet(), 1));
    store.begin().updateStatusCode(ignored -> OptimizingStatus.IDLE.getCode()).commit();
    assertFalse(due(Long.MAX_VALUE));
  }

  @Test
  void rejectedConfigurationCannotPublishDeadline() throws Exception {
    apply(0L);
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenAnswer(
            call -> {
              store.begin().updateTableConfig(c -> c.put("option", "changed")).commit();
              return Optional.of(result(1L, false));
            });
    runtime.evaluatePendingInputAndTransition(table, 20);
    assertFalse(due(Long.MAX_VALUE));
  }

  private void apply(long deadline) {
    PendingInputResult evaluation = result(deadline, false);
    when(table.evaluatePendingInput(any(), anyInt())).thenReturn(Optional.of(evaluation));
    assertFalse(runtime.evaluatePendingInputAndTransition(table, 20));
  }

  private PendingInputResult result(long deadline, boolean necessary) {
    PaimonPendingInput input = new PaimonPendingInput();
    input.setDataFileCount(1);
    input.setHealthScore(90);
    FormatTableAnalysis analysis = mock(FormatTableAnalysis.class);
    when(analysis.key()).thenReturn(key);
    when(analysis.pendingInput()).thenReturn(input);
    when(analysis.healthDetails())
        .thenReturn(
            new TableHealthDetails(
                "test-v1",
                key.getSnapshotId(),
                null,
                1L,
                key.getScoringConfigFingerprint(),
                key.encoded(),
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyList()));
    return new PendingInputResult(input, input, necessary, analysis, deadline);
  }

  private boolean due(long now) {
    return runtime.shouldEvaluateCurrentPlanningAnalysis(now);
  }

  private void change(String change) {
    if (change.equals("owner")) {
      store.begin().updateState(DefaultTableRuntime.PROCESS_ID_KEY, ignored -> 123L).commit();
    } else if (change.equals("plan")) {
      runtime.setLastPlanTime(runtime.getLastPlanTime() + 1);
    } else if (change.equals("minor")) {
      store
          .begin()
          .updateState(
              DefaultTableRuntime.OPTIMIZING_STATE_KEY,
              state -> {
                state.setLastMinorOptimizingTime(state.getLastMinorOptimizingTime() + 1);
                return state;
              })
          .commit();
    } else if (change.equals("planning")) {
      store.begin().updateStatusCode(ignored -> OptimizingStatus.PLANNING.getCode()).commit();
    } else {
      store.begin().updateTableConfig(c -> c.put("self-optimizing.enabled", "false")).commit();
    }
  }

  private TableAnalysisKey key(long snapshot, String config) {
    return new TableAnalysisKey(
        String.valueOf(id.getId()),
        TableFormat.PAIMON,
        snapshot,
        -1L,
        1L,
        config,
        "test-v1",
        -1L,
        0L);
  }

  private void reload() {
    store =
        new DefaultTableRuntimeStore(
            id,
            getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId())),
            keys,
            getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates).stream()
                .filter(s -> s.getTableId() == id.getId())
                .collect(Collectors.toList()));
    runtime = new DefaultTableRuntime(store, () -> table, INPUT);
  }
}
