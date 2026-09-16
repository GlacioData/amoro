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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.optimizing.SchedulingPolicy;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class TestPaimonFullScheduling extends PersistentBase {
  private static final StateKey<PaimonPendingInput> INPUT =
      StateKey.stateKey("pending_input")
          .jsonType(PaimonPendingInput.class)
          .defaultValue(new PaimonPendingInput());
  private ServerTableIdentifier id;
  private DefaultTableRuntimeStore store;
  private DefaultTableRuntime runtime;
  private PaimonTable table;
  private final List<StateKey<?>> keys = new ArrayList<>(DefaultTableRuntime.REQUIRED_STATES);

  @BeforeAll
  static void database() {
    FullSkipTestDatabase.initialize();
  }

  @BeforeEach
  void create() {
    keys.add(INPUT);
    id =
        ServerTableIdentifier.of(
            "full-scheduling", "db", UUID.randomUUID().toString(), TableFormat.PAIMON);
    doAs(TableMetaMapper.class, m -> m.insertTable(id));
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("default");
    meta.setStatusCode(OptimizingStatus.PENDING.getCode());
    Map<String, String> config = new HashMap<>();
    config.put("write-only", "true");
    config.put("self-optimizing.enabled", "true");
    config.put("self-optimizing.full.trigger.interval", "1");
    meta.setTableConfig(config);
    doAs(TableRuntimeMapper.class, m -> m.insertRuntime(meta));
    store = new DefaultTableRuntimeStore(id, meta, keys, Collections.emptyList());
    table = mock(PaimonTable.class);
    org.mockito.Mockito.when(table.supportsPendingInputRefresh()).thenCallRealMethod();
    org.mockito.Mockito.when(
            table.shouldEvaluatePendingInput(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.anyLong()))
        .thenCallRealMethod();
    doReturn(mock(PrimaryKeyFileStoreTable.class)).when(table).originalTable();
    runtime = new DefaultTableRuntime(store, () -> table, INPUT);
    store
        .begin()
        .updateState(
            DefaultTableRuntime.OPTIMIZING_STATE_KEY,
            state -> {
              state.setCurrentSnapshotId(10L);
              state.setLastOptimizedSnapshotId(10L);
              state.setCurrentChangeSnapshotId(10L);
              state.setLastOptimizedChangeSnapshotId(10L);
              return state;
            })
        .commit();
    when(table.evaluatePendingInput(any(), anyInt())).thenReturn(Optional.of(fullResult()));
  }

  @AfterEach
  void clean() {
    if (id != null) {
      doAs(TableRuntimeMapper.class, m -> m.removeAllTableStates(id.getId()));
      doAs(TableRuntimeMapper.class, m -> m.deleteRuntime(id.getId()));
      doAs(TableMetaMapper.class, m -> m.deleteTableIdById(id.getId()));
    }
  }

  private PendingInputResult fullResult() {
    PaimonPendingInput input = new PaimonPendingInput();
    input.setDataFileCount(2);
    input.setFullCompactionPending(true);
    return new PendingInputResult(input, true);
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

  private List<DefaultTableRuntime> scheduled(Set<ServerTableIdentifier> skip) {
    SchedulingPolicy policy =
        new SchedulingPolicy(new ResourceGroup.Builder("default", "local").build());
    policy.addTable(runtime);
    return policy.scheduleTables(skip, 1);
  }

  @Test
  void oldJsonPendingIsReevaluatedAndFullDemandSurvivesRestart() {
    store
        .begin()
        .updateState(INPUT, ignored -> INPUT.deserialize("{\"dataFileCount\":2}"))
        .commit();
    reload();
    assertFalse(((PaimonPendingInput) runtime.getPendingInput()).isFullCompactionPending());
    assertTrue(scheduled(Collections.emptySet()).isEmpty());
    assertTrue(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 100L));
    assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
    reload();
    assertTrue(((PaimonPendingInput) runtime.getPendingInput()).isFullCompactionPending());
    assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
    assertEquals(1, scheduled(Collections.emptySet()).size());
    assertTrue(scheduled(Collections.singleton(id)).isEmpty());
    store
        .begin()
        .updateTableConfig(c -> c.put("self-optimizing.min-plan-interval", "60000"))
        .commit();
    runtime.setLastPlanTime(System.currentTimeMillis());
    assertTrue(scheduled(Collections.emptySet()).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"write-only", "self-optimizing.enabled", "self-optimizing.full.trigger.interval"})
  void disablingFullRevokesPendingDemand(String option) {
    assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
    store
        .begin()
        .updateTableConfig(c -> c.put(option, option.endsWith("interval") ? "0" : "false"))
        .commit();
    when(table.evaluatePendingInput(any(), anyInt())).thenReturn(Optional.empty());
    assertTrue(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 100L));
    assertFalse(runtime.getPendingInput().canPlanWithoutSnapshotChange(runtime, 100L));
    assertFalse(runtime.evaluatePendingInputAndTransition(table, 20));
    reload();
    assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    assertFalse(((PaimonPendingInput) runtime.getPendingInput()).isFullCompactionPending());
    assertEquals(0L, runtime.getProcessId());
  }

  @Test
  void noCandidateClearsDemandButPlanningFailureRetainsIt() {
    assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
    runtime.beginPlanning();
    runtime.planFailed();
    reload();
    assertEquals(1, scheduled(Collections.emptySet()).size());
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(new PaimonPendingInput(), false)));
    assertFalse(runtime.evaluatePendingInputAndTransition(table, 20));
    reload();
    assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    assertFalse(((PaimonPendingInput) runtime.getPendingInput()).isFullCompactionPending());
  }

  @Test
  void fullBackoffDoesNotBlockSnapshotDrivenMinorDemand() {
    assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
    store
        .begin()
        .updateState(
            DefaultTableRuntime.OPTIMIZING_STATE_KEY,
            state -> {
              state.setFullRetryNotBefore(Long.MAX_VALUE);
              return state;
            })
        .commit();
    assertTrue(scheduled(Collections.emptySet()).isEmpty());
    store
        .begin()
        .updateState(
            DefaultTableRuntime.OPTIMIZING_STATE_KEY,
            state -> {
              state.setCurrentSnapshotId(11L);
              return state;
            })
        .commit();
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(new PaimonPendingInput(), true)));
    assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
    assertFalse(((PaimonPendingInput) runtime.getPendingInput()).isFullCompactionPending());
    assertEquals(1, scheduled(Collections.emptySet()).size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void evaluationCannotOverwriteConcurrentPlanningOrOwner(boolean owner) throws Exception {
    assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenAnswer(
            call -> {
              entered.countDown();
              assertTrue(release.await(10, TimeUnit.SECONDS));
              return Optional.of(new PendingInputResult(new PaimonPendingInput(), false));
            });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> evaluation =
          executor.submit(() -> runtime.evaluatePendingInputAndTransition(table, 20));
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      if (owner) {
        store.begin().updateState(DefaultTableRuntime.PROCESS_ID_KEY, ignored -> 123L).commit();
      } else {
        runtime.beginPlanning();
      }
      release.countDown();
      evaluation.get(10, TimeUnit.SECONDS);
      reload();
      assertEquals(
          owner ? OptimizingStatus.PENDING : OptimizingStatus.PLANNING,
          runtime.getOptimizingStatus());
      assertEquals(owner ? 123L : 0L, runtime.getProcessId());
      assertEquals(
          0,
          getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId()))
              .getTableSummary()
              .getTotalFileCount());
      assertEquals(
          2,
          getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId()))
              .getTableSummary()
              .getPendingFileCount());
      assertTrue(((PaimonPendingInput) runtime.getPendingInput()).isFullCompactionPending());
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = OptimizingStatus.class,
      names = {"PLANNING", "FULL_OPTIMIZING", "COMMITTING"})
  void activeProcessStillPublishesHealthWithoutChangingDemand(OptimizingStatus status) {
    assertTrue(runtime.evaluatePendingInputAndTransition(table, 20));
    TableAnalysisKey key =
        new TableAnalysisKey(
            String.valueOf(id.getId()),
            TableFormat.PAIMON,
            10L,
            TableAnalysisKey.NO_CHANGE_SNAPSHOT,
            1L,
            "config",
            "test-v1",
            TableAnalysisKey.NO_BASELINE,
            TableAnalysisKey.NO_BASELINE_TIME);
    when(table.properties()).thenReturn(runtime.getTableConfig());
    when(table.currentAnalysisKey(any())).thenReturn(Optional.of(key));
    runtime.refresh(table);
    store
        .begin()
        .updateStatusCode(ignored -> status.getCode())
        .updateState(DefaultTableRuntime.PROCESS_ID_KEY, ignored -> 123L)
        .commit();
    PaimonPendingInput healthy = new PaimonPendingInput();
    healthy.setDataFileCount(3);
    healthy.setHealthScore(90);
    TableHealthDetails details =
        new TableHealthDetails(
            key.getFormulaVersion(),
            key.getSnapshotId(),
            null,
            key.getSchemaId(),
            key.getScoringConfigFingerprint(),
            key.encoded(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyList());
    FormatTableAnalysis analysis = mock(FormatTableAnalysis.class);
    when(analysis.key()).thenReturn(key);
    when(analysis.pendingInput()).thenReturn(healthy);
    when(analysis.healthDetails()).thenReturn(details);
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(healthy, false, analysis)));
    assertTrue(runtime.shouldEvaluateCurrentAnalysis());
    runtime.evaluatePendingInputAndTransition(table, 20);
    assertFalse(runtime.shouldEvaluateCurrentAnalysis());
    assertEquals(status, runtime.getOptimizingStatus());
    assertEquals(123L, runtime.getProcessId());
    assertTrue(((PaimonPendingInput) runtime.getPendingInput()).isFullCompactionPending());
    TableRuntimeMeta persisted = getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId()));
    assertEquals(90, persisted.getTableSummary().getHealthScore());
    assertEquals(3, persisted.getTableSummary().getTotalFileCount());
    assertEquals(2, persisted.getTableSummary().getPendingFileCount());
  }

  @Test
  void changedConfigurationCannotPublishStaleFullEvaluation() {
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenAnswer(
            call -> {
              store.begin().updateTableConfig(c -> c.put("write-only", "false")).commit();
              return Optional.of(fullResult());
            });
    runtime.evaluatePendingInputAndTransition(table, 20);
    reload();
    assertFalse(((PaimonPendingInput) runtime.getPendingInput()).isFullCompactionPending());
    assertTrue(scheduled(Collections.emptySet()).isEmpty());
  }
}
