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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.optimizing.TableRuntimeOptimizingState;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.NestedSqlSession;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.table.TableProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class TestFullCompactionCompletion extends PersistentBase {
  private static final AtomicLong IDS = new AtomicLong(800000L);
  private ServerTableIdentifier id;
  private long processId;
  private DefaultTableRuntimeStore store;

  @BeforeAll
  static void initializeDatabase() {
    FullSkipTestDatabase.initialize();
  }

  private void create() {
    processId = IDS.incrementAndGet();
    id = ServerTableIdentifier.of("skip", "db", "table" + processId, TableFormat.ICEBERG);
    doAs(TableMetaMapper.class, mapper -> mapper.insertTable(id));
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("default");
    meta.setStatusCode(OptimizingStatus.COMMITTING.getCode());
    meta.setTableConfig(new HashMap<>(Collections.singletonMap("kept", "value")));
    meta.getTableSummary().setPendingFileCount(4);
    meta.getTableSummary().setPendingFileSize(40L);
    doAs(TableRuntimeMapper.class, mapper -> mapper.insertRuntime(meta));
    store =
        new DefaultTableRuntimeStore(
            id, meta, DefaultTableRuntime.REQUIRED_STATES, Collections.emptyList());
    store
        .begin()
        .updateState(DefaultTableRuntime.PROCESS_ID_KEY, old -> processId)
        .updateState(
            DefaultTableRuntime.OPTIMIZING_STATE_KEY,
            state -> {
              state.setCurrentSnapshotId(10L);
              state.setLastOptimizedSnapshotId(7L);
              state.setLastOptimizedChangeSnapshotId(6L);
              state.setLastFullOptimizingTime(30L);
              return state;
            })
        .commit();
    doAs(
        TableProcessMapper.class,
        mapper ->
            mapper.insertProcess(
                id.getId(),
                processId,
                "identity",
                ProcessStatus.RUNNING,
                "FULL",
                "planning",
                "AMORO",
                0,
                10L,
                Collections.singletonMap("existing", "parameter"),
                Collections.singletonMap("before", "metric")));
  }

  @Test
  void replanKeepsSuccessfulWatermarksAndRemovesBackoff() {
    create();
    new FullCompactionCompletion()
        .complete(store, processId, FullCompactionCompletion.Decision.replan(1000L, true), null);
    assertEquals(0L, owner());
    assertEquals(30L, persistedState().getLastFullOptimizingTime());
    assertEquals(7L, persistedState().getLastOptimizedSnapshotId());
    assertEquals(0L, persistedState().getFullRetryNotBefore());
    assertEquals(
        "REPLAN_PUBLISHED",
        getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId))
            .getProcessParameters()
            .get(FullCompactionCompletion.REASON_KEY));
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(
      value = ProcessStatus.class,
      names = {"SUCCESS", "FAILED", "CLOSED"})
  void terminalCompletionLostResponseReadsBackWithoutCreatingSkip(ProcessStatus terminal) {
    create();
    doAs(
        TableProcessMapper.class,
        m ->
            m.updateProcess(
                id.getId(),
                processId,
                "identity",
                terminal,
                "committing",
                0,
                1000L,
                null,
                Collections.emptyMap(),
                Collections.emptyMap()));
    doAs(
        TableRuntimeMapper.class,
        m -> {
          m.setStateValue(id.getId(), DefaultTableRuntime.PROCESS_ID_KEY.getKey(), "0");
          TableRuntimeMeta meta = m.selectRuntime(id.getId());
          meta.setStatusCode(OptimizingStatus.IDLE.getCode());
          m.updateRuntime(meta);
        });
    new FullCompactionCompletion().complete(store, processId, null, null);
    assertEquals(0L, store.getState(DefaultTableRuntime.PROCESS_ID_KEY));
    assertEquals(OptimizingStatus.IDLE.getCode(), store.getStatusCode());
    assertEquals(
        terminal, getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId)).getStatus());
    assertEquals(0L, persistedState().getFullStaleSkipCount());
  }

  @AfterEach
  void clean() {
    if (id != null) {
      doAs(TableProcessMapper.class, mapper -> mapper.deleteBefore(id.getId(), Long.MAX_VALUE));
      doAs(TableRuntimeMapper.class, mapper -> mapper.removeAllTableStates(id.getId()));
      doAs(TableRuntimeMapper.class, mapper -> mapper.deleteRuntime(id.getId()));
      doAs(TableMetaMapper.class, mapper -> mapper.deleteTableIdById(id.getId()));
    }
    assertFalse(NestedSqlSession.hasActiveSession());
  }

  private FullCompactionCompletion.Decision decision() {
    return new FullCompactionCompletion.Decision(1000L, 60_000L, 900_000L);
  }

  private void complete(FullCompactionCompletion completion) {
    completion.complete(store, processId, decision(), Collections.singletonMap("after", "metric"));
  }

  private TableRuntimeOptimizingState persistedState() {
    return getAs(
        TableRuntimeMapper.class,
        mapper ->
            DefaultTableRuntime.OPTIMIZING_STATE_KEY.deserialize(
                mapper
                    .getState(id.getId(), DefaultTableRuntime.OPTIMIZING_STATE_KEY.getKey())
                    .getStateValue()));
  }

  private long owner() {
    return getAs(
        TableRuntimeMapper.class,
        mapper ->
            Long.parseLong(
                mapper
                    .getState(id.getId(), DefaultTableRuntime.PROCESS_ID_KEY.getKey())
                    .getStateValue()));
  }

  private TableProcessMeta process() {
    return getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(processId));
  }

  private void unchangedWatermarks(TableRuntimeOptimizingState state) {
    assertEquals(7L, state.getLastOptimizedSnapshotId());
    assertEquals(6L, state.getLastOptimizedChangeSnapshotId());
    assertEquals(30L, state.getLastFullOptimizingTime());
  }

  @ParameterizedTest
  @ValueSource(strings = {"process", "state", "runtime", "owner"})
  void everyWriteFailureRollsBackDatabaseAndMemory(String failedStage) {
    create();
    assertThrows(
        PersistenceException.class,
        () ->
            complete(
                new FullCompactionCompletion(
                    stage -> {
                      assertEquals(
                          0L,
                          store
                              .getState(DefaultTableRuntime.OPTIMIZING_STATE_KEY)
                              .getFullRetryNotBefore());
                      assertEquals(OptimizingStatus.COMMITTING.getCode(), store.getStatusCode());
                      if (failedStage.equals(stage)) {
                        throw new IllegalStateException("injected " + stage);
                      }
                    })));
    assertEquals(ProcessStatus.RUNNING, process().getStatus());
    assertEquals(processId, owner());
    assertEquals(0L, persistedState().getFullStaleSkipCount());
    unchangedWatermarks(persistedState());
    complete(new FullCompactionCompletion());
    assertEquals(1L, persistedState().getFullStaleSkipCount());
  }

  @Test
  void durableSkipIsIdempotentAndRestoresNewStore() {
    create();
    complete(new FullCompactionCompletion());
    complete(new FullCompactionCompletion());
    TableProcessMeta process = process();
    assertTrue(FullCompactionCompletion.isSkipped(process));
    assertEquals("parameter", process.getProcessParameters().get("existing"));
    assertEquals("metric", process.getSummary().get("before"));
    assertEquals("metric", process.getSummary().get("after"));
    assertEquals("identity", process.getExternalProcessIdentifier());
    assertEquals(0L, owner());
    assertEquals(1L, persistedState().getFullStaleSkipCount());
    assertEquals(61000L, persistedState().getFullRetryNotBefore());
    unchangedWatermarks(persistedState());
    TableRuntimeMeta meta =
        getAs(TableRuntimeMapper.class, mapper -> mapper.selectRuntime(id.getId()));
    assertEquals("value", meta.getTableConfig().get("kept"));
    assertEquals("default", meta.getGroupName());
    assertEquals(0L, meta.getTableSummary().getPendingFileCount());
    DefaultTableRuntimeStore recovered =
        new DefaultTableRuntimeStore(
            id,
            meta,
            DefaultTableRuntime.REQUIRED_STATES,
            getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates).stream()
                .filter(s -> s.getTableId() == id.getId())
                .collect(java.util.stream.Collectors.toList()));
    assertEquals(
        61000L,
        recovered.getState(DefaultTableRuntime.OPTIMIZING_STATE_KEY).getFullRetryNotBefore());
  }

  @Test
  void lostCommitResponseReadsBackWithoutRecountingOrMovingDeadline() {
    create();
    assertThrows(
        PersistenceException.class,
        () ->
            complete(
                new FullCompactionCompletion(
                    stage -> {
                      if (stage.equals("committed")) {
                        throw new IllegalStateException("lost response");
                      }
                    })));
    assertEquals(ProcessStatus.CLOSED, process().getStatus());
    assertEquals(
        0L, store.getState(DefaultTableRuntime.OPTIMIZING_STATE_KEY).getFullRetryNotBefore());
    new FullCompactionCompletion()
        .complete(
            store,
            processId,
            new FullCompactionCompletion.Decision(999999L, 5L, 10L),
            Collections.emptyMap());
    assertEquals(61000L, persistedState().getFullRetryNotBefore());
    assertEquals(
        61000L, store.getState(DefaultTableRuntime.OPTIMIZING_STATE_KEY).getFullRetryNotBefore());
    assertEquals(1L, persistedState().getFullStaleSkipCount());
  }

  @Test
  void outerTransactionIsRejectedWithoutAnyWrites() {
    create();
    try (NestedSqlSession session = beginSession()) {
      assertTrue(NestedSqlSession.hasActiveSession());
      assertThrows(PersistenceException.class, () -> complete(new FullCompactionCompletion()));
      session.rollback();
    }
    assertEquals(processId, owner());
    assertEquals(ProcessStatus.RUNNING, process().getStatus());
  }

  @Test
  void newOwnerIsNeverReleasedOrOverwrittenByOldCompletion() {
    create();
    complete(new FullCompactionCompletion());
    store
        .begin()
        .updateState(DefaultTableRuntime.PROCESS_ID_KEY, old -> processId + 1)
        .updateStatusCode(old -> OptimizingStatus.FULL_OPTIMIZING.getCode())
        .commit();
    complete(new FullCompactionCompletion());
    assertEquals(processId + 1, owner());
    assertEquals(OptimizingStatus.FULL_OPTIMIZING.getCode(), store.getStatusCode());
    assertEquals(1L, persistedState().getFullStaleSkipCount());
  }

  @Test
  void activeProcessWithDifferentOwnerFailsClosed() {
    create();
    store.begin().updateState(DefaultTableRuntime.PROCESS_ID_KEY, old -> processId + 1).commit();
    assertThrows(PersistenceException.class, () -> complete(new FullCompactionCompletion()));
    assertEquals(processId + 1, owner());
    assertEquals(ProcessStatus.RUNNING, process().getStatus());
    assertEquals(0L, persistedState().getFullStaleSkipCount());
  }

  @Test
  void concurrentStoresCannotCountTheSameSkipTwice() throws Exception {
    create();
    DefaultTableRuntimeStore second =
        new DefaultTableRuntimeStore(
            id,
            getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId())),
            DefaultTableRuntime.REQUIRED_STATES,
            getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates).stream()
                .filter(s -> s.getTableId() == id.getId())
                .collect(java.util.stream.Collectors.toList()));
    java.util.concurrent.CyclicBarrier readTogether = new java.util.concurrent.CyclicBarrier(2);
    FullCompactionCompletion racing =
        new FullCompactionCompletion(
            stage -> {
              if (stage.equals("read")) {
                try {
                  readTogether.await(10, java.util.concurrent.TimeUnit.SECONDS);
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              }
            });
    java.util.concurrent.ExecutorService threads =
        java.util.concurrent.Executors.newFixedThreadPool(2);
    try {
      java.util.List<java.util.concurrent.Future<Boolean>> results = new java.util.ArrayList<>();
      for (DefaultTableRuntimeStore target : java.util.Arrays.asList(store, second)) {
        results.add(
            threads.submit(
                () -> {
                  try {
                    racing.complete(target, processId, decision(), Collections.emptyMap());
                    return true;
                  } catch (PersistenceException conflict) {
                    return false;
                  }
                }));
      }
      int successes = 0;
      for (java.util.concurrent.Future<Boolean> result : results) {
        if (result.get(20, java.util.concurrent.TimeUnit.SECONDS)) {
          successes++;
        }
      }
      assertEquals(1, successes);
      assertEquals(1L, persistedState().getFullStaleSkipCount());
      assertEquals(0L, owner());
      complete(new FullCompactionCompletion());
      assertEquals(1L, persistedState().getFullStaleSkipCount());
    } finally {
      threads.shutdownNow();
      assertTrue(threads.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(
      value = org.apache.amoro.optimizing.OptimizingType.class,
      names = {"MINOR", "MAJOR"})
  void ordinarySuccessPreservesFullRetry(org.apache.amoro.optimizing.OptimizingType type) {
    create();
    complete(new FullCompactionCompletion());
    long nextProcess = processId + 1;
    store.begin().updateState(DefaultTableRuntime.PROCESS_ID_KEY, old -> nextProcess).commit();
    DefaultTableRuntime runtime = new DefaultTableRuntime(store, () -> null);
    org.apache.amoro.server.optimizing.OptimizingProcess process =
        org.mockito.Mockito.mock(org.apache.amoro.server.optimizing.OptimizingProcess.class);
    org.mockito.Mockito.when(process.getProcessId()).thenReturn(nextProcess);
    org.mockito.Mockito.when(process.getOptimizingType()).thenReturn(type);
    org.mockito.Mockito.when(process.getPlanTime()).thenReturn(2000L);
    org.mockito.Mockito.when(process.getTargetSnapshotId()).thenReturn(10L);
    runtime.completeProcess(process, true);
    assertEquals(1, persistedState().getFullStaleSkipCount());
    assertEquals(61000, persistedState().getFullRetryNotBefore());
    assertEquals(30, persistedState().getLastFullOptimizingTime());
    assertEquals(10, persistedState().getLastOptimizedSnapshotId());
  }

  @Test
  void retrySequenceAndOverflowAreBounded() {
    TableRuntimeOptimizingState state = new TableRuntimeOptimizingState();
    long[] expected = {60_000L, 120_000L, 240_000L, 480_000L, 900_000L, 900_000L};
    for (int i = 0; i < expected.length; i++) {
      FullCompactionCompletion.advanceRetry(state, i + 1, decision());
      assertEquals(expected[i] + 1000L, state.getFullRetryNotBefore());
    }
    state.setFullStaleSkipCount(Long.MAX_VALUE);
    FullCompactionCompletion.advanceRetry(
        state, 20L, new FullCompactionCompletion.Decision(Long.MAX_VALUE - 1, 1L, Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, state.getFullRetryNotBefore());
    assertEquals(Long.MAX_VALUE, state.getFullStaleSkipCount());
  }

  @Test
  void legacyJsonAndConfigurationRemainCompatible() {
    TableRuntimeOptimizingState state =
        DefaultTableRuntime.OPTIMIZING_STATE_KEY.deserialize(
            "{\"lastFullOptimizingTime\":33,\"unknownLegacyField\":true}");
    assertEquals(33L, state.getLastFullOptimizingTime());
    assertEquals(0L, state.getFullRetryNotBefore());
    OptimizingConfig defaults = TableConfigurations.parseOptimizingConfig(Collections.emptyMap());
    assertEquals(60_000L, defaults.getFullSkipRetryInitialDelayMs());
    assertEquals(900_000L, defaults.getFullSkipRetryMaxDelayMs());
    for (String value : new String[] {"0", "-1", "abc", "9223372036854775808"}) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              TableConfigurations.parseOptimizingConfig(
                  Collections.singletonMap(
                      TableProperties.SELF_OPTIMIZING_FULL_SKIP_RETRY_INITIAL_DELAY_MS, value)));
    }
    Map<String, String> options = new HashMap<>();
    options.put(TableProperties.SELF_OPTIMIZING_FULL_SKIP_RETRY_INITIAL_DELAY_MS, "20");
    options.put(TableProperties.SELF_OPTIMIZING_FULL_SKIP_RETRY_MAX_DELAY_MS, "10");
    assertThrows(
        IllegalArgumentException.class, () -> TableConfigurations.parseOptimizingConfig(options));
  }
}
