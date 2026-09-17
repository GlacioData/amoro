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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.OptimizingTaskResult;
import org.apache.amoro.exception.OptimizingClosedException;
import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.exception.OptimizingCommitSkippedException;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionOutput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionTask;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingCommitter;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.optimizing.TableRuntimeOptimizingState;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.optimizing.OptimizingOwnership;
import org.apache.amoro.server.optimizing.OptimizingProcess;
import org.apache.amoro.server.optimizing.OptimizingQueue;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.optimizing.TaskRuntime;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.resource.OptimizerThread;
import org.apache.amoro.utils.SerializationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class TestPaimonFullSkipQueue extends PersistentBase {
  private static final AtomicLong IDS = new AtomicLong(900000);
  private ServerTableIdentifier id;
  private long processId;
  private DefaultTableRuntimeStore store;
  private DefaultTableRuntime runtime;
  private OptimizingQueue queue;
  private ExecutorService executor;
  private OptimizerThread assignedThread;
  private TaskRuntime<?> assignedTask;
  private ProcessFactory processFactory;
  private final AtomicInteger commits = new AtomicInteger();
  private final AtomicReference<OptimizingOwnership> ownership =
      new AtomicReference<>(OptimizingOwnership.OWNED);

  private static List<org.apache.amoro.table.StateKey<?>> requiredStates() {
    List<org.apache.amoro.table.StateKey<?>> keys =
        new ArrayList<>(DefaultTableRuntime.REQUIRED_STATES);
    keys.add(DefaultTableRuntime.DEFAULT_PENDING_INPUT_KEY);
    return keys;
  }

  @BeforeAll
  static void database() {
    FullSkipTestDatabase.initialize();
  }

  private void prepare(boolean successfulCommit) throws Exception {
    prepare(successfulCommit, true);
  }

  private void prepare(boolean successfulCommit, boolean completeTasks) throws Exception {
    processId = IDS.incrementAndGet();
    id = ServerTableIdentifier.of("skipqueue", "db", "t" + processId, TableFormat.PAIMON);
    doAs(TableMetaMapper.class, m -> m.insertTable(id));
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("skip-queue-" + processId);
    meta.setStatusCode(OptimizingStatus.PENDING.getCode());
    meta.setTableConfig(new HashMap<>());
    doAs(TableRuntimeMapper.class, m -> m.insertRuntime(meta));
    store = new DefaultTableRuntimeStore(id, meta, requiredStates(), Collections.emptyList());
    store
        .begin()
        .updateState(
            DefaultTableRuntime.OPTIMIZING_STATE_KEY,
            s -> {
              s.setCurrentSnapshotId(10);
              s.setLastOptimizedSnapshotId(7);
              s.setLastOptimizedChangeSnapshotId(6);
              s.setLastFullOptimizingTime(30);
              return s;
            })
        .commit();
    AmoroTable<?> table = mock(AmoroTable.class);
    runtime = spy(new DefaultTableRuntime(store, () -> table));
    doReturn(runtime).when(runtime).refresh(any());
    CatalogManager catalogs = mock(CatalogManager.class);
    doReturn(table).when(catalogs).loadTable(any());
    List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      PaimonPrimaryKeyCompactionInput input =
          new PaimonPrimaryKeyCompactionInput(
              null, Collections.emptyList(), OptimizingType.FULL, true, 10L, "user", processId);
      tasks.add(new PaimonPrimaryKeyCompactionTask(id.getId(), "p=" + i, input, new HashMap<>()));
    }
    tasks.forEach(
        task ->
            task.ensureExecutorFactoryImpl(
                org.apache.amoro.formats.paimon.optimizing.primary
                    .PaimonPrimaryKeyCompactionExecutorFactory.class
                    .getName()));
    TableOptimizingPlanner planner = mock(TableOptimizingPlanner.class);
    when(planner.isNecessary()).thenReturn(true);
    doReturn(
            new OptimizingPlanResult<>(
                processId,
                OptimizingType.FULL,
                System.currentTimeMillis(),
                10,
                6,
                tasks,
                Collections.emptyMap(),
                Collections.emptyMap()))
        .when(planner)
        .plan();
    ProcessFactory factory = mock(ProcessFactory.class);
    processFactory = factory;
    when(factory.supportedFormats()).thenReturn(Collections.singleton(TableFormat.PAIMON));
    when(factory.isOptimizingEligible(any())).thenReturn(true);
    when(factory.replanOnRecovery(any(), any())).thenReturn(true);
    when(factory.createPlanner(any(), any(), anyDouble(), anyLong())).thenReturn(planner);
    TableOptimizingCommitter committer =
        new TableOptimizingCommitter() {
          @Override
          public void commit() throws OptimizingCommitException {
            commits.incrementAndGet();
            if (!successfulCommit) {
              throw new OptimizingCommitSkippedException(
                  OptimizingCommitSkippedException.Reason.STALE_FULL, "stale");
            }
          }

          @Override
          public ReconciliationResult reconcile() {
            return successfulCommit && commits.get() > 0
                ? ReconciliationResult.PUBLISHED
                : ReconciliationResult.REPLAN;
          }
        };
    when(factory.createCommitter(any(), anyLong(), anyLong(), any(), any(), any()))
        .thenReturn(committer);
    executor = Executors.newSingleThreadExecutor();
    queue =
        new OptimizingQueue(
            catalogs,
            new ResourceGroup.Builder(meta.getGroupName(), "local").build(),
            ignored -> 1,
            executor,
            Collections.singletonList(runtime),
            1,
            new ProcessFactoryRouter(Collections.singletonList(factory)),
            ignored -> ownership.get());
    OptimizerThread thread =
        new OptimizerThread(1, null) {
          @Override
          public String getToken() {
            return "skip-token";
          }
        };
    for (int i = 0; i < tasks.size(); i++) {
      TaskRuntime<?> task = queue.pollTask(thread, 5000);
      assertNotNull(task);
      queue.ackTask(task.getTaskId(), thread);
      assignedTask = task;
      assignedThread = thread;
      if (!completeTasks) {
        continue;
      }
      queue.completeTask(
          thread,
          new OptimizingTaskResult(task.getTaskId(), 1)
              .setTaskOutput(
                  SerializationUtil.simpleSerialize(PaimonPrimaryKeyCompactionOutput.staleFull())));
    }
    assertEquals(
        completeTasks ? OptimizingStatus.COMMITTING : OptimizingStatus.FULL_OPTIMIZING,
        runtime.getOptimizingStatus());
  }

  @AfterEach
  void clean() throws Exception {
    if (queue != null) {
      queue.dispose();
    }
    if (executor != null) {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
    if (id != null) {
      doAs(
          OptimizingProcessMapper.class,
          m -> m.deleteTaskRuntimesBefore(id.getId(), Long.MAX_VALUE));
      doAs(
          OptimizingProcessMapper.class,
          m -> m.deleteProcessStateBefore(id.getId(), Long.MAX_VALUE));
      doAs(TableProcessMapper.class, m -> m.deleteBefore(id.getId(), Long.MAX_VALUE));
      doAs(TableRuntimeMapper.class, m -> m.removeAllTableStates(id.getId()));
      doAs(TableRuntimeMapper.class, m -> m.deleteRuntime(id.getId()));
      doAs(TableMetaMapper.class, m -> m.deleteTableIdById(id.getId()));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void commitFailureBeforeOrAfterDurabilityRetriesWithoutRecommitting(boolean afterCommit)
      throws Exception {
    prepare(false);
    AtomicBoolean fail = new AtomicBoolean(true);
    AtomicLong firstObserved = new AtomicLong();
    doAnswer(
            call -> {
              firstObserved.compareAndSet(
                  0, ((FullCompactionCompletion.Decision) call.getArgument(1)).getTime());
              if (!afterCommit && fail.getAndSet(false)) {
                throw new PersistenceException("before DB");
              }
              Object result = call.callRealMethod();
              if (afterCommit && fail.getAndSet(false)) {
                throw new PersistenceException("lost response");
              }
              return result;
            })
        .when(runtime)
        .completeSkippedFull(anyLong(), any(), any());
    OptimizingProcess process = runtime.getOptimizingProcess();
    assertThrows(PersistenceException.class, process::commit);
    assertEquals(afterCommit ? 0L : processId, runtime.getProcessId());
    store
        .begin()
        .updateTableConfig(
            config -> {
              config.put("self-optimizing.full.skip-retry.initial-delay-ms", "5");
              config.put("self-optimizing.full.skip-retry.max-delay-ms", "10");
            })
        .commit();
    queue.retryPendingRecoveries(Long.MAX_VALUE);
    assertEquals(1, commits.get());
    assertEquals(0L, runtime.getProcessId());
    assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    assertNull(runtime.getOptimizingProcess());
    TableProcessMeta persisted = getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId));
    assertTrue(FullCompactionCompletion.isSkipped(persisted));
    TableRuntimeOptimizingState state = store.getState(DefaultTableRuntime.OPTIMIZING_STATE_KEY);
    assertEquals(1, state.getFullStaleSkipCount());
    assertEquals(7L, state.getLastOptimizedSnapshotId());
    assertEquals(6L, state.getLastOptimizedChangeSnapshotId());
    assertEquals(30L, state.getLastFullOptimizingTime());
    assertTrue(queue.collectTasks().isEmpty());
    long deadline = state.getFullRetryNotBefore();
    assertEquals(firstObserved.get() + 60000L, deadline);
    process.commit();
    assertEquals(deadline, runtime.getFullRetryNotBefore());
    assertEquals(1, commits.get());
  }

  @Test
  void lostLocalDecisionBeforeDurabilityClosesWithoutReplayingTasks() throws Exception {
    prepare(false);
    AtomicBoolean fail = new AtomicBoolean(true);
    doAnswer(
            call -> {
              if (fail.getAndSet(false)) {
                throw new PersistenceException("before DB");
              }
              return call.callRealMethod();
            })
        .when(runtime)
        .completeSkippedFull(anyLong(), any(), any());
    OptimizingProcess old = runtime.getOptimizingProcess();
    assertThrows(PersistenceException.class, old::commit);
    runtime.detachOptimizingProcess(old);
    queue.retryPendingRecoveries(Long.MAX_VALUE);
    assertNull(runtime.getOptimizingProcess());
    assertEquals(
        1,
        commits.get(),
        "Takeover must only reconcile results and must not replay old task outputs");
    assertEquals(
        0L, store.getState(DefaultTableRuntime.OPTIMIZING_STATE_KEY).getFullStaleSkipCount());
    assertEquals(30L, runtime.getLastFullOptimizingTime());
    assertEquals(
        "REPLAN",
        getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId))
            .getProcessParameters()
            .get(FullCompactionCompletion.REASON_KEY));
    assertEquals(0L, runtime.getProcessId());
    assertNull(runtime.getOptimizingProcess());
  }

  @Test
  void detachClearsPendingRecoveryAndRejectsRetainedCommitReference() throws Exception {
    prepare(false);
    AtomicInteger completions = new AtomicInteger();
    doAnswer(
            call -> {
              completions.incrementAndGet();
              throw new PersistenceException("before DB");
            })
        .when(runtime)
        .completeSkippedFull(anyLong(), any(), any());
    OptimizingProcess old = runtime.getOptimizingProcess();
    assertThrows(PersistenceException.class, old::commit);
    queue.detachTable(runtime);
    queue.retryPendingRecoveries(Long.MAX_VALUE);
    old.commit();
    assertEquals(1, completions.get());
    assertEquals(1, commits.get());
    assertNull(runtime.getOptimizingProcess());
    assertEquals(processId, runtime.getProcessId());
  }

  @Test
  void replacementRuntimeOwnsRecoveryWithoutOldObject() throws Exception {
    prepare(false);
    doAnswer(
            call -> {
              throw new PersistenceException("old runtime unavailable");
            })
        .when(runtime)
        .completeSkippedFull(anyLong(), any(), any());
    assertThrows(PersistenceException.class, runtime.getOptimizingProcess()::commit);
    TableRuntimeMeta meta = getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId()));
    DefaultTableRuntimeStore replacementStore =
        new DefaultTableRuntimeStore(
            id,
            meta,
            requiredStates(),
            getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates).stream()
                .filter(state -> state.getTableId() == id.getId())
                .collect(Collectors.toList()));
    DefaultTableRuntime replacement =
        new DefaultTableRuntime(replacementStore, () -> mock(AmoroTable.class));
    queue.recoverOwnedTable(replacement);
    assertEquals(0L, replacement.getProcessId());
    assertEquals(OptimizingStatus.IDLE, replacement.getOptimizingStatus());
    assertEquals(1, commits.get());
    assertNull(replacement.getOptimizingProcess());
  }

  @Test
  void ownershipGainClosesCommittingProcessWithoutDispatchOrPublish() throws Exception {
    prepare(false);
    queue.detachTable(runtime);
    queue.recoverOwnedTable(runtime);
    assertEquals(0L, runtime.getProcessId());
    assertNull(runtime.getOptimizingProcess());
    assertEquals(0, commits.get());
    assertTrue(queue.collectTasks().isEmpty());
    assertEquals(30L, runtime.getLastFullOptimizingTime());
    assertEquals(7L, runtime.getLastOptimizedSnapshotId());
  }

  @Test
  void fullTaskFailureClosesAfterTaskTransactionCommits() throws Exception {
    prepare(false, false);
    doReturn(runtime.getOptimizingConfig().setMaxExecuteRetryCount(0))
        .when(runtime)
        .getOptimizingConfig();
    doReturn(false).when(runtime).isAllowPartialCommit();
    queue.completeTask(
        assignedThread,
        new OptimizingTaskResult(assignedTask.getTaskId(), 1).setErrorMessage("executor failed"));
    assertEquals(0L, runtime.getProcessId());
    assertEquals(
        ProcessStatus.CLOSED,
        getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId)).getStatus());
    assertEquals(0, commits.get());
  }

  @Test
  void fullWithoutSuccessfulTasksClosesWithoutBuildingCommitter() throws Exception {
    prepare(false, false);
    doReturn(runtime.getOptimizingConfig().setMaxExecuteRetryCount(0))
        .when(runtime)
        .getOptimizingConfig();
    doReturn(true).when(runtime).isAllowPartialCommit();
    queue.completeTask(
        assignedThread,
        new OptimizingTaskResult(assignedTask.getTaskId(), 1).setErrorMessage("executor failed"));
    assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
    runtime.getOptimizingProcess().commit();
    assertEquals(0L, runtime.getProcessId());
    TableProcessMeta meta = getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId));
    assertEquals(ProcessStatus.CLOSED, meta.getStatus());
    assertEquals("REPLAN", meta.getProcessParameters().get(FullCompactionCompletion.REASON_KEY));
    assertEquals(0, commits.get());
    org.mockito.Mockito.verify(processFactory, org.mockito.Mockito.never())
        .createCommitter(any(), anyLong(), anyLong(), any(), any(), any());
  }

  @Test
  void partialCommitWithRevokedEligibilityClosesOutsideTaskTransaction() throws Exception {
    prepare(false, false);
    when(processFactory.isOptimizingEligible(any())).thenReturn(false);
    // A failed callback with partial commit enabled also closes through eligibility; cover the same
    // nested callback boundary.
    doReturn(runtime.getOptimizingConfig().setMaxExecuteRetryCount(0))
        .when(runtime)
        .getOptimizingConfig();
    doReturn(true).when(runtime).isAllowPartialCommit();
    queue.completeTask(
        assignedThread,
        new OptimizingTaskResult(assignedTask.getTaskId(), 1).setErrorMessage("executor failed"));
    assertEquals(0L, runtime.getProcessId());
    assertEquals(
        ProcessStatus.CLOSED,
        getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId)).getStatus());
    assertEquals(0, commits.get());
  }

  @Test
  void retainedRpcAndCallbackCannotChangeDetachedProcess() throws Exception {
    prepare(false);
    OptimizingProcess old = runtime.getOptimizingProcess();
    Method taskMap = old.getClass().getDeclaredMethod("getTaskMap");
    taskMap.setAccessible(true);
    TaskRuntime<?> task =
        (TaskRuntime<?>) ((Map<?, ?>) taskMap.invoke(old)).values().iterator().next();
    queue.detachTable(runtime);
    queue.recoverOwnedTable(runtime);
    Method complete =
        old.getClass()
            .getDeclaredMethod("completeTask", OptimizerThread.class, OptimizingTaskResult.class);
    complete.setAccessible(true);
    InvocationTargetException rejected =
        assertThrows(
            InvocationTargetException.class,
            () ->
                complete.invoke(
                    old,
                    null,
                    new OptimizingTaskResult(task.getTaskId(), 1).setErrorMessage("late failure")));
    assertTrue(rejected.getCause() instanceof OptimizingClosedException);
    Method callback = old.getClass().getDeclaredMethod("acceptResult", TaskRuntime.class);
    callback.setAccessible(true);
    TaskRuntime<?> failed = mock(TaskRuntime.class);
    when(failed.getStatus()).thenReturn(TaskRuntime.Status.FAILED);
    when(failed.getRetry()).thenReturn(Integer.MAX_VALUE);
    callback.invoke(old, failed);
    TableProcessMeta meta = getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId));
    assertEquals(ProcessStatus.CLOSED, meta.getStatus());
    assertEquals("REPLAN", meta.getProcessParameters().get(FullCompactionCompletion.REASON_KEY));
    assertEquals(0L, runtime.getProcessId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void ownershipLossDropsRecoveryButUnknownKeepsIt(boolean lost) throws Exception {
    prepare(false);
    AtomicInteger completions = new AtomicInteger();
    doAnswer(
            call -> {
              if (completions.incrementAndGet() == 1) {
                throw new PersistenceException("before DB");
              }
              return call.callRealMethod();
            })
        .when(runtime)
        .completeSkippedFull(anyLong(), any(), any());
    assertThrows(PersistenceException.class, runtime.getOptimizingProcess()::commit);
    ownership.set(lost ? OptimizingOwnership.NOT_OWNED : OptimizingOwnership.UNKNOWN);
    queue.retryPendingRecoveries(Long.MAX_VALUE);
    ownership.set(OptimizingOwnership.OWNED);
    queue.retryPendingRecoveries(Long.MAX_VALUE);
    assertEquals(lost ? 1 : 2, completions.get());
    assertEquals(lost ? processId : 0L, runtime.getProcessId());
    assertEquals(1, commits.get());
  }

  @Test
  void configRefreshReplansDetachedFullWithoutLoadingRunnableTasks() throws Exception {
    prepare(false);
    queue.detachTable(runtime);
    queue.reconcileTableConfig(runtime);
    assertEquals(0L, runtime.getProcessId());
    assertNull(runtime.getOptimizingProcess());
    assertTrue(queue.collectTasks().isEmpty());
    assertEquals(0, commits.get());
  }

  @Test
  void releaseCompletesAttachedSkipMissingFromQueue() throws Exception {
    prepare(false);
    AtomicBoolean fail = new AtomicBoolean(true);
    doAnswer(
            call -> {
              if (fail.getAndSet(false)) {
                throw new PersistenceException("before DB");
              }
              return call.callRealMethod();
            })
        .when(runtime)
        .completeSkippedFull(anyLong(), any(), any());
    assertThrows(PersistenceException.class, runtime.getOptimizingProcess()::commit);
    queue.releaseTable(runtime);
    queue.retryPendingRecoveries(Long.MAX_VALUE);
    assertEquals(0L, runtime.getProcessId());
    assertNull(runtime.getOptimizingProcess());
    assertEquals(1, commits.get());
  }

  @Test
  void successfulFullResetsRetryButKeepsLastSkipIdentity() throws Exception {
    prepare(true);
    store
        .begin()
        .updateState(
            DefaultTableRuntime.OPTIMIZING_STATE_KEY,
            s -> {
              s.setFullStaleSkipCount(3);
              s.setFullRetryNotBefore(Long.MAX_VALUE);
              s.setLastFullStaleProcessId(123);
              return s;
            })
        .commit();
    runtime.getOptimizingProcess().commit();
    TableRuntimeOptimizingState state = store.getState(DefaultTableRuntime.OPTIMIZING_STATE_KEY);
    assertEquals(0L, state.getFullStaleSkipCount());
    assertEquals(0L, state.getFullRetryNotBefore());
    assertEquals(123L, state.getLastFullStaleProcessId());
    assertEquals(10L, state.getLastOptimizedSnapshotId());
    assertEquals(
        ProcessStatus.SUCCESS,
        getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId)).getStatus());
  }
}
