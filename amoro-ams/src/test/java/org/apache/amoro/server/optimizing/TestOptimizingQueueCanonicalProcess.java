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

package org.apache.amoro.server.optimizing;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.OptimizingTaskResult;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.RewriteFilesInput;
import org.apache.amoro.optimizing.RewriteFilesOutput;
import org.apache.amoro.optimizing.RewriteStageTask;
import org.apache.amoro.optimizing.TableOptimizingCommitter;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.optimizing.TableRuntimeOptimizingState;
import org.apache.amoro.optimizing.TaskProperties;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.persistence.NestedSqlSession;
import org.apache.amoro.server.persistence.OptimizingPayloadException;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.TaskFilesPersistence;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.resource.OptimizerThread;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.DefaultTableRuntimeFactory;
import org.apache.amoro.server.table.DefaultTableRuntimeStore;
import org.apache.amoro.server.table.DerbyPersistence;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.table.TableRuntimeFactory;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.ibatis.session.SqlSession;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/** Real queue, runtime, descriptor and Derby persistence; only the format SPI is stubbed. */
class TestOptimizingQueueCanonicalProcess extends PersistentBase {
  private final Database database = new Database();
  private final ExecutorService configExecutor = Executors.newFixedThreadPool(2);
  private final AtomicInteger commits = new AtomicInteger();
  private final AtomicLong processIds = new AtomicLong(100L);
  private final List<String> committedPartitions = new ArrayList<>();
  private final OptimizerThread optimizer =
      new OptimizerThread(1, null) {
        @Override
        public String getToken() {
          return "canonical-process-optimizer";
        }
      };
  private OptimizingQueue queue;
  private CatalogManager catalog;
  private ProcessFactoryRouter router;

  @AfterEach
  void cleanup() throws Exception {
    configExecutor.shutdownNow();
    assertTrue(configExecutor.awaitTermination(10, TimeUnit.SECONDS));
    if (queue != null) {
      queue.dispose();
    }
    database.clear();
  }

  @Test
  void idleRecoveryOwnershipChangeDoesNotCreateSpuriousRetry() {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    runtime.completeEmptyProcess();
    assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    assertEquals(0L, runtime.getProcessId());
    assertNull(runtime.getOptimizingProcess());
    AtomicInteger ownershipReads = new AtomicInteger();
    OptimizingQueue recoveryQueue =
        new OptimizingQueue(
            catalog,
            new ResourceGroup.Builder("idle-ownership-change", "local").build(),
            ignored -> 1,
            Runnable::run,
            Collections.emptyList(),
            1,
            router,
            ignored ->
                ownershipReads.incrementAndGet() == 1
                    ? OptimizingOwnership.OWNED
                    : OptimizingOwnership.NOT_OWNED);
    try {
      // The real recovery entry sees an idle table with no durable or local process. Ownership
      // changes at its second check; this is a normal skip, not a failed recovery to retry.
      recoveryQueue.recoverOwnedTable(runtime);
      assertEquals(2, ownershipReads.get());
      assertEquals(0, recoveryQueue.pendingRecoveryCount());
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
      assertEquals(0L, runtime.getProcessId());
      assertNull(runtime.getOptimizingProcess());
      assertTrue(recoveryQueue.collectTasks().isEmpty());
    } finally {
      recoveryQueue.dispose();
    }
  }

  @Test
  void configRecoveryMustNotReplaceTheProcessPublishedAfterItsInitialNullRead() throws Exception {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    CountDownLatch observedNoLocalProcess = new CountDownLatch(1);
    CountDownLatch publishedAndAcked = new CountDownLatch(1);
    AtomicReference<Thread> configThread = new AtomicReference<>();
    AtomicInteger configProcessReads = new AtomicInteger();

    doAnswer(
            invocation -> {
              // The first read belongs to reconcileTableConfig's quota probe. The next read is
              // localRunningProcess's owner check, after it has observed a null local process.
              if (Thread.currentThread() == configThread.get()
                  && configProcessReads.incrementAndGet() == 2) {
                observedNoLocalProcess.countDown();
                assertTrue(publishedAndAcked.await(10, TimeUnit.SECONDS));
              }
              return invocation.callRealMethod();
            })
        .when(runtime)
        .getProcessId();

    Future<?> configRefresh =
        configExecutor.submit(
            () -> {
              configThread.set(Thread.currentThread());
              queue.reconcileTableConfig(runtime);
            });
    OptimizingProcess published;
    TaskRuntime<?> task;
    try {
      assertTrue(observedNoLocalProcess.await(10, TimeUnit.SECONDS));
      // Normal scheduling publishes P to both the runtime and queue and persists its task.
      task = queue.pollTask(optimizer, 5000L);
      assertNotNull(task);
      published = runtime.getOptimizingProcess();
      assertNotNull(published);
      queue.ackTask(task.getTaskId(), optimizer);
      assertEquals(TaskRuntime.Status.ACKED, persistedTask(task.getProcessId()).getStatus());
    } finally {
      publishedAndAcked.countDown();
    }
    configRefresh.get(10, TimeUnit.SECONDS);
    OptimizingProcess commitTarget = runtime.getOptimizingProcess();

    // Canonical process identity is an Amoro-owned guarantee: RPC completion resolves P through
    // the queue, so recovery must not attach Q while process-id deduplication keeps P queued.
    // Paimon ac223f47 Snapshot#commitUser/#commitIdentifier (Snapshot.java:332-338) defines native
    // publication identity only; it cannot repair divergent AMS process instances before commit.
    queue.completeTask(
        optimizer,
        new OptimizingTaskResult(task.getTaskId(), optimizer.getThreadId())
            .setTaskOutput(
                SerializationUtil.simpleSerialize(new RewriteFilesOutput(null, null, null))));
    assertEquals(TaskRuntime.Status.SUCCESS, task.getStatus());
    assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(task.getProcessId()).getStatus());
    assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());

    // This is the production commit executor's entry condition and target, not an early commit.
    runtime.getOptimizingProcess().commit();

    assertAll(
        () -> assertSame(published, commitTarget, "recovery must retain the published instance"),
        () -> assertEquals(1, commits.get(), "the successful task must reach the format committer"),
        () ->
            assertEquals(
                ProcessStatus.SUCCESS,
                getAs(TableProcessMapper.class, m -> m.getProcessMeta(task.getProcessId()))
                    .getStatus()),
        () ->
            assertEquals(
                TaskRuntime.Status.SUCCESS,
                persistedTask(task.getProcessId()).getStatus(),
                "a stale ACKED recovery must not overwrite persisted SUCCESS with CANCELED"));
  }

  @Test
  void configReconciliationMustNotDetachAProcessBeforeItsPlanningTransactionCommits()
      throws Exception {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    CountDownLatch beginProcessReturned = new CountDownLatch(1);
    CountDownLatch allowPlanningCommit = new CountDownLatch(1);
    CountDownLatch configStarted = new CountDownLatch(1);
    AtomicReference<OptimizingProcess> published = new AtomicReference<>();
    AtomicReference<Thread> configThread = new AtomicReference<>();
    AtomicBoolean planningTransactionOpen = new AtomicBoolean();
    AtomicInteger prematureOwnerReads = new AtomicInteger();

    doAnswer(
            invocation -> {
              invocation.callRealMethod();
              assertTrue(NestedSqlSession.hasActiveSession());
              published.set(invocation.getArgument(0));
              planningTransactionOpen.set(true);
              beginProcessReturned.countDown();
              try {
                assertTrue(allowPlanningCommit.await(10, TimeUnit.SECONDS));
              } finally {
                planningTransactionOpen.set(false);
              }
              return null;
            })
        .when(runtime)
        .beginProcess(any());
    doAnswer(
            invocation -> {
              if (Thread.currentThread() == configThread.get() && planningTransactionOpen.get()) {
                // Derby blocks this read instead of returning MySQL's last committed owner.
                // Model only that observed database visibility difference, not queue behavior.
                prematureOwnerReads.incrementAndGet();
                return 0L;
              }
              return invocation.callRealMethod();
            })
        .when(runtime)
        .getProcessId();

    Future<TaskRuntime<?>> planning = configExecutor.submit(() -> queue.pollTask(optimizer, 5000L));
    Future<?> configRefresh = null;
    OptimizingProcess attachedBeforeCommit;
    try {
      assertTrue(beginProcessReturned.await(10, TimeUnit.SECONDS));
      assertSame(published.get(), runtime.getOptimizingProcess());
      assertEquals(OptimizingStatus.MINOR_OPTIMIZING, runtime.getOptimizingStatus());
      configRefresh =
          configExecutor.submit(
              () -> {
                configThread.set(Thread.currentThread());
                configStarted.countDown();
                queue.reconcileTableConfig(runtime);
              });
      assertTrue(configStarted.await(10, TimeUnit.SECONDS));
      awaitReconciliationCompletionOrProcessLock(
          configRefresh, published.get(), configThread.get());
      attachedBeforeCommit = runtime.getOptimizingProcess();
    } finally {
      allowPlanningCommit.countDown();
    }
    TaskRuntime<?> task = planning.get(10, TimeUnit.SECONDS);
    assertNotNull(task);
    assertNotNull(configRefresh);
    configRefresh.get(10, TimeUnit.SECONDS);

    // A later refresh must not materialize Q while the task queue still routes RPCs to P.
    queue.reconcileTableConfig(runtime);
    OptimizingProcess commitTarget = runtime.getOptimizingProcess();
    assertNotNull(commitTarget);
    queue.ackTask(task.getTaskId(), optimizer);
    completeSuccessfully(queue, task);
    assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(task).getStatus());
    assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
    runtime.getOptimizingProcess().commit();

    // Amoro owns publication across its outer SQL transaction, not just beginProcess's nested
    // store commit. Paimon ac223f47 Snapshot#commitUser/#commitIdentifier (332-338) only defines
    // native commit identity; it cannot reunify divergent AMS task and commit instances.
    assertAll(
        () -> assertEquals(0, prematureOwnerReads.get()),
        () -> assertSame(published.get(), attachedBeforeCommit),
        () -> assertSame(published.get(), commitTarget),
        () -> assertEquals(1, commits.get()),
        () -> assertEquals(ProcessStatus.SUCCESS, persistedProcess(task).getStatus()),
        () -> assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(task).getStatus()));
  }

  private void awaitReconciliationCompletionOrProcessLock(
      Future<?> reconciliation, OptimizingProcess process, Thread configThread) throws Exception {
    Field lockField = process.getClass().getDeclaredField("lock");
    lockField.setAccessible(true);
    ReentrantLock processLock = (ReentrantLock) lockField.get(process);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    // Observe a causal event rather than sleeping: the old path finishes and detaches, whereas
    // a publication fence parks this same reconciliation thread on the actual process lock.
    while (!reconciliation.isDone() && !processLock.hasQueuedThread(configThread)) {
      assertTrue(System.nanoTime() < deadline, "config reconciliation did not reach its fence");
      Thread.yield();
    }
  }

  @Test
  void rolledBackPlanningMustDetachItsUnpublishedProcessAndAllowARealRetry() throws Exception {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    AtomicBoolean failOnce = new AtomicBoolean(true);
    AtomicReference<OptimizingProcess> rolledBack = new AtomicReference<>();
    RuntimeException injectedFailure =
        new IllegalStateException("outer planning transaction failed");
    doAnswer(
            invocation -> {
              invocation.callRealMethod();
              if (failOnce.compareAndSet(true, false)) {
                assertTrue(NestedSqlSession.hasActiveSession());
                rolledBack.set(invocation.getArgument(0));
                // Fail after the real nested store commit but before the enclosing transaction
                // commits, so the real database owner, process and task inserts all roll back.
                throw injectedFailure;
              }
              return null;
            })
        .when(runtime)
        .beginProcess(any());

    Method plan =
        OptimizingQueue.class.getDeclaredMethod("planInternal", DefaultTableRuntime.class);
    plan.setAccessible(true);
    InvocationTargetException failure =
        assertThrows(InvocationTargetException.class, () -> plan.invoke(queue, runtime));
    Throwable cause = failure;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    assertSame(injectedFailure, cause);
    assertNotNull(rolledBack.get());
    long failedProcessId = rolledBack.get().getProcessId();

    // An Amoro transaction rollback must revoke its exact local candidate as well as its SQL
    // rows. Paimon ac223f47 Snapshot#commitUser/#commitIdentifier (332-338) cannot clean up an
    // AMS candidate that never became a persisted process or reached a native commit.
    assertAll(
        () -> assertEquals(0L, runtime.getProcessId()),
        () -> assertNull(runtime.getOptimizingProcess()),
        () -> assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus()),
        () -> assertTrue(queue.collectTasks().isEmpty()),
        () ->
            assertNull(
                getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(failedProcessId))),
        () ->
            assertTrue(
                getAs(
                        OptimizingProcessMapper.class,
                        mapper ->
                            mapper.selectOptimizeTaskMetas(
                                Collections.singletonList(failedProcessId)))
                    .isEmpty()));

    queue.reconcileTableConfig(runtime);
    TaskRuntime<?> retry = queue.pollTask(optimizer, 5000L);
    assertNotNull(retry);
    assertNotEquals(failedProcessId, retry.getProcessId());
    queue.ackTask(retry.getTaskId(), optimizer);
    completeSuccessfully(queue, retry);
    runtime.getOptimizingProcess().commit();
    assertEquals(1, commits.get());
    assertEquals(ProcessStatus.SUCCESS, persistedProcess(retry).getStatus());
    assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(retry).getStatus());
    assertNull(runtime.getOptimizingProcess());
    assertEquals(0L, runtime.getProcessId());
  }

  @Test
  void aRejectedProcessMustNotBeOfferedAgainBeforeARealRecovery() throws Exception {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    Method plan =
        OptimizingQueue.class.getDeclaredMethod("planInternal", DefaultTableRuntime.class);
    plan.setAccessible(true);
    OptimizingProcess rejected = (OptimizingProcess) plan.invoke(queue, runtime);
    assertNotNull(rejected);
    Method offer =
        OptimizingQueue.class.getDeclaredMethod(
            "offerPlannedProcess", DefaultTableRuntime.class, rejected.getClass());
    offer.setAccessible(true);
    assertTrue((Boolean) offer.invoke(queue, runtime, rejected));
    assertEquals(TaskRuntime.Status.PLANNED, persistedTask(rejected.getProcessId()).getStatus());

    AtomicInteger eligibilityChecks = new AtomicInteger();
    when(router.forFormat(TableFormat.ICEBERG).isOptimizingEligible(runtime))
        .thenAnswer(
            invocation -> {
              // Permit the first reconciliation fence, then make the actual offer fence UNKNOWN.
              if (eligibilityChecks.incrementAndGet() == 2) {
                throw new IllegalStateException("temporary eligibility read failure");
              }
              return true;
            });
    queue.reconcileTableConfig(runtime);
    assertEquals(2, eligibilityChecks.get());

    // This is the real final-planning offer entry point, delayed until after rejection. Amoro
    // must revoke this exact instance before permitting a new recovery; Paimon ac223f47
    // Snapshot#commitUser/#commitIdentifier (Snapshot.java:332-338) cannot fence a stale AMS offer.
    boolean offeredAgain = (Boolean) offer.invoke(queue, runtime, rejected);
    assertAll(
        () -> assertFalse(offeredAgain),
        () -> assertNull(runtime.getOptimizingProcess()),
        () -> assertTrue(queue.collectTasks().isEmpty()),
        () -> assertEquals(rejected.getProcessId(), runtime.getProcessId()),
        () ->
            assertEquals(
                ProcessStatus.RUNNING,
                getAs(
                        TableProcessMapper.class,
                        mapper -> mapper.getProcessMeta(rejected.getProcessId()))
                    .getStatus()),
        () ->
            assertEquals(
                TaskRuntime.Status.PLANNED, persistedTask(rejected.getProcessId()).getStatus()));

    queue.reconcileTableConfig(runtime);
    OptimizingProcess recovered = runtime.getOptimizingProcess();
    assertNotNull(recovered);
    assertNotSame(rejected, recovered);
    assertEquals(rejected.getProcessId(), recovered.getProcessId());
    TaskRuntime<?> task = queue.pollTask(optimizer, 5000L);
    assertNotNull(task);
    queue.ackTask(task.getTaskId(), optimizer);
    completeSuccessfully(queue, task);
    assertSame(recovered, runtime.getOptimizingProcess());
    recovered.commit();
    assertEquals(1, commits.get());
    assertEquals(ProcessStatus.SUCCESS, persistedProcess(task).getStatus());
    assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(task).getStatus());
  }

  @Test
  void genuinelyFailedOnlyPartialCommitStillRejectsAnEmptySuccessfulTaskSet() {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    TaskRuntime<?> task = queue.pollTask(optimizer, 5000L);
    assertNotNull(task);
    queue.ackTask(task.getTaskId(), optimizer);
    runtime
        .store()
        .begin()
        .updateTableConfig(
            config -> {
              config.put(TableProperties.SELF_OPTIMIZING_ALLOW_PARTIAL_COMMIT, "true");
              config.put(TableProperties.SELF_OPTIMIZING_EXECUTE_RETRY_NUMBER, "0");
            })
        .commit();

    // Reach COMMITTING through the actual exhausted-retry partial-commit callback. Fixing the
    // canonical instance race must not weaken the independent zero-success commit guard.
    queue.completeTask(
        optimizer,
        new OptimizingTaskResult(task.getTaskId(), optimizer.getThreadId())
            .setErrorMessage("deliberate optimizer failure"));
    assertEquals(TaskRuntime.Status.FAILED, task.getStatus());
    assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
    runtime.getOptimizingProcess().commit();

    TableProcessMeta process =
        getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(task.getProcessId()));
    assertEquals(ProcessStatus.FAILED, process.getStatus());
    assertTrue(process.getFailMessage().contains("No successful task"));
    assertEquals(0, commits.get());
  }

  @Test
  void losingConcurrentRecoveryCannotResetTheWinnersCompletedRetry() throws Exception {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    TaskRuntime<?> assigned = queue.pollTask(optimizer, 5000L);
    assertNotNull(assigned);
    queue.ackTask(assigned.getTaskId(), optimizer);
    queue.detachTable(runtime);
    assertNull(runtime.getOptimizingProcess());

    // Model a restart after the failure row was committed but before its process callback ran.
    // The mapper restores a real TaskRuntime with no process callback attached; its normal
    // complete() operation persists FAILED without fabricating a task or table status.
    TaskRuntime<?> interrupted =
        getAs(
                OptimizingProcessMapper.class,
                mapper -> mapper.selectTaskRuntimes(assigned.getTableId(), assigned.getProcessId()))
            .get(0);
    interrupted.complete(
        optimizer,
        new OptimizingTaskResult(assigned.getTaskId(), optimizer.getThreadId())
            .setErrorMessage("failure persisted before process recovery"));
    assertEquals(TaskRuntime.Status.FAILED, persistedTask(assigned).getStatus());
    assertEquals(ProcessStatus.RUNNING, persistedProcess(assigned).getStatus());

    CountDownLatch firstMaterialized = new CountDownLatch(1);
    CountDownLatch secondMaterialized = new CountDownLatch(1);
    CountDownLatch publishFirst = new CountDownLatch(1);
    CountDownLatch publishSecond = new CountDownLatch(1);
    AtomicInteger candidates = new AtomicInteger();
    AtomicReference<OptimizingProcess> firstCandidate = new AtomicReference<>();
    doAnswer(
            invocation -> {
              int candidate = candidates.incrementAndGet();
              if (candidate == 1) {
                firstCandidate.set(invocation.getArgument(0));
                firstMaterialized.countDown();
                assertTrue(publishFirst.await(10, TimeUnit.SECONDS));
              } else if (candidate == 2) {
                secondMaterialized.countDown();
                assertTrue(publishSecond.await(10, TimeUnit.SECONDS));
              }
              return invocation.callRealMethod();
            })
        .when(runtime)
        .attachRecoveredProcess(any());

    Future<?> first = configExecutor.submit(() -> queue.reconcileTableConfig(runtime));
    try {
      assertTrue(firstMaterialized.await(10, TimeUnit.SECONDS));
      Future<?> second = configExecutor.submit(() -> queue.reconcileTableConfig(runtime));
      assertTrue(secondMaterialized.await(10, TimeUnit.SECONDS));
      // Both candidates hold the same FAILED version before either can publish or reset it.
      assertEquals(TaskRuntime.Status.FAILED, persistedTask(assigned).getStatus());
      publishFirst.countDown();
      first.get(10, TimeUnit.SECONDS);
      assertSame(firstCandidate.get(), runtime.getOptimizingProcess());
      assertEquals(TaskRuntime.Status.PLANNED, persistedTask(assigned).getStatus());

      TaskRuntime<?> retried = queue.pollTask(optimizer, 5000L);
      assertNotNull(retried);
      assertEquals(assigned.getTaskId(), retried.getTaskId());
      assertEquals("2", retried.getProperties().get(TaskProperties.TASK_ATTEMPT_ID));
      queue.ackTask(retried.getTaskId(), optimizer);
      completeSuccessfully(queue, retried);
      assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(retried).getStatus());
      assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());

      // The late candidate still owns its stale FAILED copy. Activating it would reset the
      // durable SUCCESS row to PLANNED, even if runtime attachment itself were already fixed.
      publishSecond.countDown();
      second.get(10, TimeUnit.SECONDS);
      assertSame(firstCandidate.get(), runtime.getOptimizingProcess());
      assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(retried).getStatus());
      assertEquals(2, persistedTask(retried).getRetryNum());
      assertEquals(1, queue.collectTasks().size());
      assertSame(retried, queue.collectTasks().get(0));
      assertNull(queue.pollTask(optimizer, 0L));

      runtime.getOptimizingProcess().commit();
      assertEquals(1, commits.get());
      assertEquals(Collections.singletonList("p=0"), committedPartitions);
      assertEquals(ProcessStatus.SUCCESS, persistedProcess(retried).getStatus());
      assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(retried).getStatus());
    } finally {
      publishFirst.countDown();
      publishSecond.countDown();
    }
  }

  @Test
  void aDifferentQueueMustWaitForDetachBeforeRecoveringTheSameProcessId() {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    TaskRuntime<?> task = queue.pollTask(optimizer, 5000L);
    assertNotNull(task);
    queue.ackTask(task.getTaskId(), optimizer);
    OptimizingProcess original = runtime.getOptimizingProcess();
    OptimizingQueue nextQueue = newQueue("canonical-process-next");
    try {
      // Concurrent queue ownership observations do not authorize stealing the live instance.
      nextQueue.recoverOwnedTable(runtime);
      assertSame(original, runtime.getOptimizingProcess());
      assertTrue(nextQueue.collectTasks().isEmpty());
      assertEquals(1, nextQueue.pendingRecoveryCount());
      assertEquals(TaskRuntime.Status.ACKED, persistedTask(task).getStatus());

      queue.detachTable(runtime);
      assertNull(runtime.getOptimizingProcess());
      assertTrue(queue.collectTasks().isEmpty());
      assertEquals(ProcessStatus.RUNNING, persistedProcess(task).getStatus());
      nextQueue.retryPendingRecoveries(Long.MAX_VALUE);
      OptimizingProcess transferred = runtime.getOptimizingProcess();
      assertNotNull(transferred);
      assertNotSame(original, transferred);
      assertEquals(original.getProcessId(), transferred.getProcessId());
      assertEquals(0, nextQueue.pendingRecoveryCount());
      assertEquals(1, nextQueue.collectTasks().size());
      assertEquals(TaskRuntime.Status.ACKED, nextQueue.collectTasks().get(0).getStatus());

      completeSuccessfully(nextQueue, task);
      assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
      transferred.commit();
      assertEquals(1, commits.get());
      assertEquals(ProcessStatus.SUCCESS, persistedProcess(task).getStatus());
      assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(task).getStatus());
    } finally {
      nextQueue.dispose();
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false"})
  void lateMaterializationFailureMustNotClosePublishedCanonical(
      boolean competingQueue, boolean winnerAlreadyCompleted) throws Exception {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    TaskRuntime<?> assigned = queue.pollTask(optimizer, 5000L);
    assertNotNull(assigned);
    queue.ackTask(assigned.getTaskId(), optimizer);
    queue.detachTable(runtime);
    assertNull(runtime.getOptimizingProcess());

    OptimizingQueue recoveryQueue = competingQueue ? newQueue("canonical-failing-recovery") : queue;
    CountDownLatch materialized = new CountDownLatch(1);
    CountDownLatch winnerPublished = new CountDownLatch(1);
    AtomicReference<Thread> failingThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              if (Thread.currentThread() == failingThread.get()) {
                // The real recovery has read its descriptors and payloads. Inject a permanent
                // validation failure at the last boundary before publication or activation.
                // Its exception handler must recheck canonical identity before closing rows.
                materialized.countDown();
                assertTrue(winnerPublished.await(10, TimeUnit.SECONDS));
                throw new OptimizingPayloadException(
                    OptimizingPayloadException.FailureReason.DESERIALIZATION_FAILED,
                    "late recovery payload validation failure");
              }
              return invocation.callRealMethod();
            })
        .when(runtime)
        .attachRecoveredProcess(any());

    Future<?> losingRecovery =
        configExecutor.submit(
            () -> {
              failingThread.set(Thread.currentThread());
              recoveryQueue.recoverOwnedTable(runtime);
            });
    try {
      assertTrue(materialized.await(10, TimeUnit.SECONDS));
      // Config reconciliation is a separate real recovery entry, not an artificial attachment.
      queue.reconcileTableConfig(runtime);
      OptimizingProcess canonical = runtime.getOptimizingProcess();
      assertNotNull(canonical);
      assertEquals(1, queue.collectTasks().size());
      if (winnerAlreadyCompleted) {
        completeSuccessfully(queue, assigned);
        assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
      }
      TaskRuntime.Status expected =
          winnerAlreadyCompleted ? TaskRuntime.Status.SUCCESS : TaskRuntime.Status.ACKED;
      assertEquals(expected, persistedTask(assigned).getStatus());

      winnerPublished.countDown();
      losingRecovery.get(10, TimeUnit.SECONDS);
      assertAll(
          () -> assertSame(canonical, runtime.getOptimizingProcess()),
          () -> assertEquals(assigned.getProcessId(), runtime.getProcessId()),
          () -> assertEquals(ProcessStatus.RUNNING, persistedProcess(assigned).getStatus()),
          () -> assertEquals(expected, persistedTask(assigned).getStatus()),
          () -> assertEquals(1, queue.collectTasks().size()),
          () -> assertEquals(0, recoveryQueue.pendingRecoveryCount()));

      if (!winnerAlreadyCompleted) {
        completeSuccessfully(queue, assigned);
      }
      canonical.commit();
      assertEquals(1, commits.get());
      assertEquals(ProcessStatus.SUCCESS, persistedProcess(assigned).getStatus());
      assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(assigned).getStatus());
    } finally {
      winnerPublished.countDown();
      if (competingQueue) {
        recoveryQueue.dispose();
      }
    }
  }

  @Test
  void failedCorruptPayloadCloseCannotExposeAMetadataOnlyProcessToAConcurrentFollower()
      throws Exception {
    DefaultTableRuntime runtime = createRuntimeAndQueue();
    TaskRuntime<?> assigned = queue.pollTask(optimizer, 5000L);
    assertNotNull(assigned);
    queue.ackTask(assigned.getTaskId(), optimizer);
    queue.detachTable(runtime);

    CountDownLatch followerMaterialized = new CountDownLatch(1);
    CountDownLatch allowFollowerAttach = new CountDownLatch(1);
    CountDownLatch followerAttachFinished = new CountDownLatch(1);
    CountDownLatch closeTransactionStarted = new CountDownLatch(1);
    CountDownLatch allowCloseFailure = new CountDownLatch(1);
    AtomicReference<Thread> followerThread = new AtomicReference<>();
    AtomicReference<OptimizingProcess> followerObserved = new AtomicReference<>();
    AtomicBoolean failCloseOnce = new AtomicBoolean(true);

    doAnswer(
            invocation -> {
              if (Thread.currentThread() != followerThread.get()) {
                return invocation.callRealMethod();
              }
              // A real normal recovery has decoded healthy task inputs before the close starts.
              followerMaterialized.countDown();
              assertTrue(allowFollowerAttach.await(10, TimeUnit.SECONDS));
              try {
                Object attached = invocation.callRealMethod();
                followerObserved.set((OptimizingProcess) attached);
                return attached;
              } finally {
                followerAttachFinished.countDown();
              }
            })
        .when(runtime)
        .attachRecoveredProcess(any());
    doAnswer(
            invocation -> {
              if (failCloseOnce.compareAndSet(true, false)) {
                assertTrue(NestedSqlSession.hasActiveSession());
                closeTransactionStarted.countDown();
                assertTrue(allowCloseFailure.await(10, TimeUnit.SECONDS));
                throw new IllegalStateException("first strict close transaction fails");
              }
              return invocation.callRealMethod();
            })
        .when(runtime)
        .completeProcessStrict(any(), anyBoolean());

    Future<?> follower =
        configExecutor.submit(
            () -> {
              followerThread.set(Thread.currentThread());
              queue.reconcileTableConfig(runtime);
            });
    try {
      assertTrue(followerMaterialized.await(10, TimeUnit.SECONDS));
      corruptPersistedInput(assigned.getProcessId());
      assertThrows(
          OptimizingPayloadException.class,
          () -> TaskFilesPersistence.loadTaskInputs(assigned.getProcessId()));
      Future<?> closer = configExecutor.submit(() -> queue.recoverOwnedTable(runtime));
      assertTrue(closeTransactionStarted.await(10, TimeUnit.SECONDS));

      // Before durable closure, a follower must not receive the closer's payload-free CLOSED
      // object. Otherwise its later resetTableForRecovery can clear the still-live durable owner.
      allowFollowerAttach.countDown();
      assertTrue(followerAttachFinished.await(10, TimeUnit.SECONDS));
      allowCloseFailure.countDown();
      closer.get(10, TimeUnit.SECONDS);
      follower.get(10, TimeUnit.SECONDS);
      assertAll(
          () -> assertNull(followerObserved.get()),
          () -> assertNull(runtime.getOptimizingProcess()),
          () -> assertEquals(assigned.getProcessId(), runtime.getProcessId()),
          () -> assertEquals(ProcessStatus.RUNNING, persistedProcess(assigned).getStatus()),
          () -> assertEquals(TaskRuntime.Status.ACKED, persistedTask(assigned).getStatus()),
          () -> assertEquals(1, queue.pendingRecoveryCount()),
          () -> assertTrue(queue.collectTasks().isEmpty()),
          () -> assertEquals(0, commits.get()));

      queue.retryPendingRecoveries(Long.MAX_VALUE);
      assertEquals(0, queue.pendingRecoveryCount());
      assertNull(runtime.getOptimizingProcess());
      assertEquals(0L, runtime.getProcessId());
      assertEquals(ProcessStatus.CLOSED, persistedProcess(assigned).getStatus());
      assertTrue(persistedProcess(assigned).getFailMessage().contains("CORRUPT_GZIP"));
      assertEquals(TaskRuntime.Status.CANCELED, persistedTask(assigned).getStatus());
      assertTrue(queue.collectTasks().isEmpty());

      runtime.setPendingInput(new AbstractOptimizingEvaluator.PendingInput());
      runtime.setLastPlanTime(0L);
      TaskRuntime<?> replanned = queue.pollTask(optimizer, 5000L);
      assertNotNull(replanned);
      assertNotEquals(assigned.getProcessId(), replanned.getProcessId());
      assertNotNull(replanned.getTaskDescriptor().getInput());
      queue.ackTask(replanned.getTaskId(), optimizer);
      completeSuccessfully(queue, replanned);
      runtime.getOptimizingProcess().commit();
      assertEquals(ProcessStatus.SUCCESS, persistedProcess(replanned).getStatus());
      assertEquals(1, commits.get());
    } finally {
      allowFollowerAttach.countDown();
      allowCloseFailure.countDown();
    }
  }

  private void corruptPersistedInput(long processId) throws Exception {
    try (SqlSession session = SqlSessionFactoryProvider.getInstance().get().openSession(true);
        PreparedStatement statement =
            session
                .getConnection()
                .prepareStatement(
                    "UPDATE optimizing_process_state SET rewrite_input = ? WHERE process_id = ?")) {
      statement.setBytes(1, new byte[] {0x1f, (byte) 0x8b, 0x00});
      statement.setLong(2, processId);
      assertEquals(1, statement.executeUpdate());
    }
  }

  @Test
  void partialCommitPublishesOnlyTheSuccessfulTaskAndRetainsTheFailedTaskState() {
    DefaultTableRuntime runtime = createRuntimeAndQueue(2);
    TaskRuntime<?> successful = queue.pollTask(optimizer, 5000L);
    assertNotNull(successful);
    queue.ackTask(successful.getTaskId(), optimizer);
    completeSuccessfully(queue, successful);
    assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(successful).getStatus());

    TaskRuntime<?> failed = queue.pollTask(optimizer, 5000L);
    assertNotNull(failed);
    queue.ackTask(failed.getTaskId(), optimizer);
    runtime
        .store()
        .begin()
        .updateTableConfig(
            config -> {
              config.put(TableProperties.SELF_OPTIMIZING_ALLOW_PARTIAL_COMMIT, "true");
              config.put(TableProperties.SELF_OPTIMIZING_EXECUTE_RETRY_NUMBER, "0");
            })
        .commit();
    queue.completeTask(
        optimizer,
        new OptimizingTaskResult(failed.getTaskId(), optimizer.getThreadId())
            .setErrorMessage("only the second task failed"));
    assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
    runtime.getOptimizingProcess().commit();

    assertEquals(1, commits.get());
    assertEquals(
        Collections.singletonList(
            ((RewriteStageTask) successful.getTaskDescriptor()).getPartition()),
        committedPartitions);
    assertEquals(ProcessStatus.FAILED, persistedProcess(failed).getStatus());
    assertEquals(TaskRuntime.Status.SUCCESS, persistedTask(successful).getStatus());
    assertEquals(TaskRuntime.Status.FAILED, persistedTask(failed).getStatus());
  }

  private void completeSuccessfully(OptimizingQueue target, TaskRuntime<?> task) {
    target.completeTask(
        optimizer,
        new OptimizingTaskResult(task.getTaskId(), optimizer.getThreadId())
            .setTaskOutput(
                SerializationUtil.simpleSerialize(new RewriteFilesOutput(null, null, null))));
  }

  private TableProcessMeta persistedProcess(TaskRuntime<?> task) {
    return getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(task.getProcessId()));
  }

  private OptimizingTaskMeta persistedTask(TaskRuntime<?> task) {
    return getAs(
        OptimizingProcessMapper.class,
        mapper ->
            mapper.selectOptimizeTaskMeta(
                task.getTableId(), task.getProcessId(), task.getTaskId().getTaskId()));
  }

  private DefaultTableRuntime createRuntimeAndQueue() {
    return createRuntimeAndQueue(1);
  }

  private DefaultTableRuntime createRuntimeAndQueue(int taskCount) {
    ServerTableIdentifier identifier =
        ServerTableIdentifier.of("canonical", "db", "table", TableFormat.ICEBERG);
    doAs(TableMetaMapper.class, mapper -> mapper.insertTable(identifier));
    Map<String, String> options = new HashMap<>();
    options.put("self-optimizing.enabled", "true");
    options.put("self-optimizing.group", "canonical-process");
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(identifier.getId());
    meta.setGroupName("canonical-process");
    meta.setStatusCode(OptimizingStatus.PENDING.getCode());
    meta.setTableConfig(options);
    doAs(TableRuntimeMapper.class, mapper -> mapper.insertRuntime(meta));

    AmoroTable<?> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(options);
    when(table.refreshOptimizingState(any()))
        .thenAnswer(
            invocation -> {
              TableRuntimeOptimizingState state = invocation.getArgument(0);
              state.setCurrentSnapshotId(10L);
              return true;
            });
    DefaultTableRuntimeFactory runtimeFactory = new DefaultTableRuntimeFactory();
    runtimeFactory.withTableLoader(ignored -> table);
    TableRuntimeFactory.TableRuntimeCreator creator =
        runtimeFactory.accept(identifier, options).get();
    DefaultTableRuntimeStore store =
        new DefaultTableRuntimeStore(
            identifier, meta, creator.requiredStateKeys(), Collections.emptyList());
    DefaultTableRuntime runtime = spy((DefaultTableRuntime) creator.create(store));
    store.setTableRuntime(runtime);
    runtime.refresh(table);

    List<RewriteStageTask> descriptors = new ArrayList<>();
    for (int task = 0; task < taskCount; task++) {
      descriptors.add(
          new RewriteStageTask(
              identifier.getId(),
              "p=" + task,
              new RewriteFilesInput(
                  new DataFile[0],
                  new DataFile[0],
                  new ContentFile<?>[0],
                  new ContentFile<?>[0],
                  null),
              new HashMap<>()));
    }
    TableOptimizingPlanner planner = mock(TableOptimizingPlanner.class);
    when(planner.isNecessary()).thenReturn(true);
    when(planner.plan())
        .thenAnswer(
            ignored ->
                new OptimizingPlanResult<>(
                    processIds.incrementAndGet(),
                    OptimizingType.MINOR,
                    System.currentTimeMillis(),
                    10L,
                    -1L,
                    descriptors,
                    Collections.emptyMap(),
                    Collections.emptyMap()));
    ProcessFactory factory = mock(ProcessFactory.class);
    when(factory.supportedFormats()).thenReturn(Collections.singleton(TableFormat.ICEBERG));
    when(factory.isOptimizingEligible(runtime)).thenReturn(true);
    when(factory.createPlanner(any(), any(), anyDouble(), anyLong())).thenReturn(planner);
    when(factory.createCommitter(any(), anyLong(), anyLong(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Collection<?> successful = invocation.getArgument(3);
              List<String> partitions =
                  successful.stream()
                      .map(task -> ((RewriteStageTask) task).getPartition())
                      .collect(Collectors.toList());
              return (TableOptimizingCommitter)
                  () -> {
                    commits.incrementAndGet();
                    committedPartitions.addAll(partitions);
                  };
            });
    catalog = mock(CatalogManager.class);
    when(catalog.loadTable(any())).thenAnswer(ignored -> table);
    router = new ProcessFactoryRouter(Collections.singletonList(factory));
    queue = newQueue("canonical-process");
    queue.refreshTable(runtime);
    return runtime;
  }

  private OptimizingQueue newQueue(String group) {
    return new OptimizingQueue(
        catalog,
        new ResourceGroup.Builder(group, "local").build(),
        ignored -> 1,
        Runnable::run,
        Collections.emptyList(),
        1,
        router);
  }

  private OptimizingTaskMeta persistedTask(long processId) {
    return getAs(
            OptimizingProcessMapper.class,
            mapper -> mapper.selectOptimizeTaskMetas(Collections.singletonList(processId)))
        .get(0);
  }

  private static class Database extends DerbyPersistence {
    private void clear() {
      super.after();
    }
  }
}
