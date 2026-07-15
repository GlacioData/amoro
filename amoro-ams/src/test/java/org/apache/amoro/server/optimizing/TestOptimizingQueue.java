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

import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.GROUP_TAG;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_COMMITTING_TABLES;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_EXECUTING_TABLES;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_EXECUTING_TASKS;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_IDLE_TABLES;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_MEMORY_BYTES_ALLOCATED;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_OPTIMIZER_INSTANCES;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_PENDING_TABLES;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_PENDING_TASKS;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_PLANING_TABLES;
import static org.apache.amoro.server.optimizing.OptimizerGroupMetrics.OPTIMIZER_GROUP_THREADS;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableTestHelper;
import org.apache.amoro.api.OptimizerRegisterInfo;
import org.apache.amoro.api.OptimizingTaskId;
import org.apache.amoro.api.OptimizingTaskResult;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.catalog.CatalogTestHelper;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.exception.TaskRuntimeException;
import org.apache.amoro.io.MixedDataTestHelpers;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.RewriteFilesOutput;
import org.apache.amoro.optimizing.TableOptimizing;
import org.apache.amoro.optimizing.TableOptimizingCommitter;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.optimizing.TaskProperties;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.process.iceberg.IcebergProcessFactory;
import org.apache.amoro.server.resource.OptimizerInstance;
import org.apache.amoro.server.resource.OptimizerThread;
import org.apache.amoro.server.resource.QuotaProvider;
import org.apache.amoro.server.table.AMSTableTestBase;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.table.UnkeyedTable;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class TestOptimizingQueue extends AMSTableTestBase {

  private final Executor planExecutor = Executors.newSingleThreadExecutor();
  private final QuotaProvider quotaProvider = resourceGroup -> 1;
  private final ProcessFactoryRouter router =
      new ProcessFactoryRouter(Collections.singletonList(new IcebergProcessFactory()));
  private final long MAX_POLLING_TIME = 5000;

  private final OptimizerThread optimizerThread =
      new OptimizerThread(1, null) {

        @Override
        public String getToken() {
          return "aah";
        }
      };

  private final OptimizerThread optimizerThread2 =
      new OptimizerThread(2, null) {

        @Override
        public String getToken() {
          return "aah";
        }
      };

  public TestOptimizingQueue(CatalogTestHelper catalogTestHelper, TableTestHelper tableTestHelper) {
    super(catalogTestHelper, tableTestHelper, true);
  }

  @Parameterized.Parameters(name = "{0}, {1}")
  public static Object[] parameters() {
    return new Object[][] {
      {new BasicCatalogTestHelper(TableFormat.ICEBERG), new BasicTableTestHelper(false, true)}
    };
  }

  protected static ResourceGroup testResourceGroup() {
    return new ResourceGroup.Builder("test", "local").build();
  }

  @Before
  public void setUp() {
    // Clean up any existing metrics for the test resource group before each test
    // to avoid "Metric is already been registered" errors
    MetricRegistry registry = MetricManager.getInstance().getGlobalRegistry();
    String testGroupName = testResourceGroup().getName();

    // Unregister all metrics for the test resource group
    List<MetricKey> keysToRemove = new ArrayList<>();
    for (MetricKey key : registry.getMetrics().keySet()) {
      if (key.getDefine().getName().startsWith("optimizer_group_")
          && testGroupName.equals(key.valueOfTag(GROUP_TAG))) {
        keysToRemove.add(key);
      }
    }
    keysToRemove.forEach(registry::unregister);
  }

  protected OptimizingQueue buildOptimizingGroupService(DefaultTableRuntime tableRuntime) {
    return new OptimizingQueue(
        CATALOG_MANAGER,
        testResourceGroup(),
        quotaProvider,
        planExecutor,
        Collections.singletonList(tableRuntime),
        1,
        router);
  }

  private OptimizingQueue buildOptimizingGroupService() {
    return new OptimizingQueue(
        CATALOG_MANAGER,
        testResourceGroup(),
        quotaProvider,
        planExecutor,
        Collections.emptyList(),
        1,
        router);
  }

  private OptimizingQueue buildOptimizingGroupService(
      List<DefaultTableRuntime> tableRuntimes, Executor executor, int maxPlanningParallelism) {
    return new OptimizingQueue(
        CATALOG_MANAGER,
        testResourceGroup(),
        quotaProvider,
        executor,
        tableRuntimes,
        maxPlanningParallelism,
        router);
  }

  private OptimizingQueue buildOptimizingGroupService(
      CatalogManager catalogManager,
      List<DefaultTableRuntime> tableRuntimes,
      Executor executor,
      int maxPlanningParallelism) {
    return new OptimizingQueue(
        catalogManager,
        testResourceGroup(),
        quotaProvider,
        executor,
        tableRuntimes,
        maxPlanningParallelism,
        router);
  }

  @Test
  public void testPollNoTask() {
    DefaultTableRuntime tableRuntimeMeta =
        buildTableRuntimeMeta(OptimizingStatus.PENDING, defaultResourceGroup());
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntimeMeta);
    Assert.assertNull(queue.pollTask(optimizerThread, 0));
    queue.dispose();
  }

  @Test
  public void testRefreshAndReleaseTable() {
    OptimizingQueue queue = buildOptimizingGroupService();
    Assert.assertEquals(0, queue.getSchedulingPolicy().getTableRuntimeMap().size());
    DefaultTableRuntime tableRuntime =
        buildTableRuntimeMeta(OptimizingStatus.IDLE, defaultResourceGroup());
    queue.refreshTable(tableRuntime);
    Assert.assertEquals(1, queue.getSchedulingPolicy().getTableRuntimeMap().size());
    Assert.assertTrue(
        queue.getSchedulingPolicy().getTableRuntimeMap().containsKey(serverTableIdentifier()));

    queue.releaseTable(tableRuntime);
    Assert.assertEquals(0, queue.getSchedulingPolicy().getTableRuntimeMap().size());

    queue.refreshTable(tableRuntime);
    Assert.assertEquals(1, queue.getSchedulingPolicy().getTableRuntimeMap().size());
    queue.dispose();
  }

  @Test
  public void testPollTask() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);

    // 1.poll task
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);

    Assert.assertNotNull(task);
    Assert.assertEquals(TaskRuntime.Status.SCHEDULED, task.getStatus());
    Assert.assertNull(queue.pollTask(optimizerThread, 0));
    queue.dispose();
  }

  @Test
  public void testPollTaskWithOverQuotaDisabled() {
    DefaultTableRuntime tableRuntime = initTableWithPartitionedFiles();
    OptimizingQueue queue =
        new OptimizingQueue(
            CATALOG_MANAGER,
            testResourceGroup(),
            resourceGroup -> 2,
            planExecutor,
            Collections.singletonList(tableRuntime),
            1,
            router);

    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME, false);
    Assert.assertNotNull(task);
    Assert.assertEquals(TaskRuntime.Status.SCHEDULED, task.getStatus());
    queue.ackTask(task.getTaskId(), optimizerThread);
    Assert.assertEquals(
        1, queue.collectTasks(t -> t.getStatus() == TaskRuntime.Status.ACKED).size());
    Assert.assertNotNull(task);

    // Use a different thread to avoid resetStaleTasksForThread resetting the ACKED task
    TaskRuntime<?> task2 = queue.pollTask(optimizerThread2, MAX_POLLING_TIME, false);
    Assert.assertNull(task2);

    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId()));
    Assert.assertEquals(TaskRuntime.Status.SUCCESS, task.getStatus());

    TaskRuntime<?> retryTask = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(retryTask);

    queue.dispose();
  }

  @Test
  public void testPollTaskWithOverQuotaEnabled() {
    DefaultTableRuntime tableRuntime = initTableWithPartitionedFiles();
    OptimizingQueue queue =
        new OptimizingQueue(
            CATALOG_MANAGER,
            testResourceGroup(),
            resourceGroup -> 2,
            planExecutor,
            Collections.singletonList(tableRuntime),
            1,
            router);

    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    Assert.assertEquals(TaskRuntime.Status.SCHEDULED, task.getStatus());
    queue.ackTask(task.getTaskId(), optimizerThread);
    Assert.assertEquals(
        1, queue.collectTasks(t -> t.getStatus() == TaskRuntime.Status.ACKED).size());
    Assert.assertNotNull(task);

    // Use a different thread to avoid resetStaleTasksForThread resetting the ACKED task
    TaskRuntime<?> task2 = queue.pollTask(optimizerThread2, MAX_POLLING_TIME, true);
    Assert.assertNotNull(task2);

    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId()));
    Assert.assertEquals(TaskRuntime.Status.SUCCESS, task.getStatus());
    TaskRuntime<?> task4 = queue.pollTask(optimizerThread2, MAX_POLLING_TIME, false);
    Assert.assertNull(task4);
    TaskRuntime<?> retryTask = queue.pollTask(optimizerThread2, MAX_POLLING_TIME, true);
    Assert.assertNotNull(retryTask);
    queue.dispose();
  }

  @Test
  public void testQuotaSchedulePolicy() throws InterruptedException {
    DefaultTableRuntime tableRuntime = initTableWithFiles();

    OptimizingQueue queue =
        new OptimizingQueue(
            CATALOG_MANAGER,
            testResourceGroup(),
            resourceGroup -> 2,
            planExecutor,
            Collections.singletonList(tableRuntime),
            1,
            router);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    queue.ackTask(task.getTaskId(), optimizerThread);
    Assert.assertEquals(
        1, queue.collectTasks(t -> t.getStatus() == TaskRuntime.Status.ACKED).size());
    Assert.assertNotNull(task);
    Assert.assertTrue(tableRuntime.getTableIdentifier().getId() == task.getTableId());
    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId()));
    Assert.assertEquals(TaskRuntime.Status.SUCCESS, task.getStatus());
    OptimizingProcess optimizingProcess = tableRuntime.getOptimizingProcess();
    Assert.assertEquals(ProcessStatus.RUNNING, optimizingProcess.getStatus());
    optimizingProcess.commit();
    Assert.assertEquals(ProcessStatus.SUCCESS, optimizingProcess.getStatus());
    Assert.assertNull(tableRuntime.getOptimizingProcess());

    // waiting for min-plan-interval and minor-trigger-interval
    Thread.sleep(500);
    tableRuntime = initTableWithPartitionedFiles();
    ServerTableIdentifier serverTableIdentifier =
        ServerTableIdentifier.of(
            org.apache.amoro.table.TableIdentifier.of(
                serverTableIdentifier().getCatalog(), "db", "new_table"),
            TableFormat.ICEBERG);
    serverTableIdentifier.setId(100L);
    DefaultTableRuntime tableRuntime2 = createTable(serverTableIdentifier);
    queue.refreshTable(tableRuntime2);
    queue.refreshTable(tableRuntime);

    // Poll two tasks and verify they come from different tables
    // The order may vary due to async planning, so we check both possibilities
    TaskRuntime<?> task2 = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task2);
    TaskRuntime<?> task3 = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task3);

    // Verify that the two tasks come from the two different tables
    long tableId1 = tableRuntime.getTableIdentifier().getId();
    long tableId2 = tableRuntime2.getTableIdentifier().getId();
    long task2TableId = task2.getTableId();
    long task3TableId = task3.getTableId();

    Assert.assertTrue(
        "Task2 should come from one of the two tables",
        task2TableId == tableId1 || task2TableId == tableId2);
    Assert.assertTrue(
        "Task3 should come from one of the two tables",
        task3TableId == tableId1 || task3TableId == tableId2);
    Assert.assertNotEquals(
        "Task2 and Task3 should come from different tables", task2TableId, task3TableId);
    queue.dispose();
    tableRuntime2.dispose();
  }

  @Test
  public void testPollTaskFillsPlanningSlotsUpToMaxParallelism() throws Exception {
    DefaultTableRuntime tableRuntime1 = initTableWithFiles();
    DefaultTableRuntime tableRuntime2 = createTable(tableIdentifier("plan_fill_table_2", 200L));
    DefaultTableRuntime tableRuntime3 = createTable(tableIdentifier("plan_fill_table_3", 201L));
    CapturingPlanExecutor capturingExecutor = new CapturingPlanExecutor();
    OptimizingQueue queue =
        buildOptimizingGroupService(
            Lists.newArrayList(tableRuntime1, tableRuntime2, tableRuntime3), capturingExecutor, 3);

    Assert.assertNull(queue.pollTask(optimizerThread, 0));

    Assert.assertEquals(3, capturingExecutor.submittedCount());
    Set<ServerTableIdentifier> planningTables = readPlanningTables(queue);
    Assert.assertEquals(3, planningTables.size());
    Assert.assertEquals(3, new HashSet<>(planningTables).size());

    queue.dispose();
    tableRuntime2.dispose();
    tableRuntime3.dispose();
  }

  @Test
  public void testPollTaskDoesNotExceedMaxPlanningParallelism() throws Exception {
    DefaultTableRuntime tableRuntime1 = initTableWithFiles();
    DefaultTableRuntime tableRuntime2 = createTable(tableIdentifier("plan_fill_cap_table_2", 202L));
    DefaultTableRuntime tableRuntime3 = createTable(tableIdentifier("plan_fill_cap_table_3", 203L));
    CapturingPlanExecutor capturingExecutor = new CapturingPlanExecutor();
    OptimizingQueue queue =
        buildOptimizingGroupService(
            Lists.newArrayList(tableRuntime1, tableRuntime2, tableRuntime3), capturingExecutor, 2);

    Assert.assertNull(queue.pollTask(optimizerThread, 0));

    Assert.assertEquals(2, capturingExecutor.submittedCount());
    Assert.assertEquals(2, readPlanningTables(queue).size());

    queue.dispose();
    tableRuntime2.dispose();
    tableRuntime3.dispose();
  }

  @Test
  public void testCompletedPlanningProcessIsVisibleBeforeScheduleLockSignal() throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    CapturingPlanExecutor capturingExecutor = new CapturingPlanExecutor();
    OptimizingQueue queue =
        buildOptimizingGroupService(Collections.singletonList(tableRuntime), capturingExecutor, 1);

    Assert.assertNull(queue.pollTask(optimizerThread, 0));
    Assert.assertEquals(1, capturingExecutor.submittedCount());

    Lock scheduleLock = readScheduleLock(queue);
    scheduleLock.lock();
    CountDownLatch completed = new CountDownLatch(1);
    Thread plannerThread =
        new Thread(
            () -> {
              try {
                capturingExecutor.command(0).run();
              } finally {
                completed.countDown();
              }
            });
    plannerThread.start();

    long deadline = System.currentTimeMillis() + 5000;
    while (queue.collectTasks().isEmpty() && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    Assert.assertEquals(1, queue.collectTasks().size());
    Assert.assertEquals(1, readAvailablePlanningSlots(queue));
    Assert.assertTrue(readPlanningTables(queue).isEmpty());
    Assert.assertEquals(1, completed.getCount());

    scheduleLock.unlock();
    plannerThread.join(5000);
    Assert.assertEquals(0, completed.getCount());
    queue.dispose();
  }

  @Test
  public void testDirectExecutorPlanningDoesNotLoseSignal() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue =
        buildOptimizingGroupService(Collections.singletonList(tableRuntime), Runnable::run, 1);

    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);

    Assert.assertNotNull(task);
    queue.dispose();
  }

  @Test
  public void testConcurrentPollDoesNotSubmitDuplicatePlanningForSameTable() throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    CapturingPlanExecutor capturingExecutor = new CapturingPlanExecutor();
    OptimizingQueue queue =
        buildOptimizingGroupService(Collections.singletonList(tableRuntime), capturingExecutor, 2);

    CountDownLatch start = new CountDownLatch(1);
    Thread first =
        new Thread(
            () -> {
              awaitLatch(start);
              queue.pollTask(optimizerThread, 0);
            });
    Thread second =
        new Thread(
            () -> {
              awaitLatch(start);
              queue.pollTask(optimizerThread2, 0);
            });

    first.start();
    second.start();
    start.countDown();
    first.join(5000);
    second.join(5000);

    Assert.assertEquals(1, capturingExecutor.submittedCount());
    Assert.assertEquals(1, readPlanningTables(queue).size());
    queue.dispose();
  }

  @Test
  public void testRejectedPlanningSubmissionReleasesPlanningSlot() throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    Executor rejectingExecutor =
        command -> {
          throw new RejectedExecutionException("reject for test");
        };
    OptimizingQueue queue =
        buildOptimizingGroupService(Collections.singletonList(tableRuntime), rejectingExecutor, 1);

    Assert.assertNull(queue.pollTask(optimizerThread, 0));

    Assert.assertTrue(readPlanningTables(queue).isEmpty());
    Assert.assertEquals(1, readAvailablePlanningSlots(queue));
    Assert.assertEquals(OptimizingStatus.PENDING, tableRuntime.getOptimizingStatus());
    queue.dispose();
  }

  @Test
  public void testFailedPlanningReleasesSlotAndUpdatesLastPlanTime() throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    long beforePlanTime = tableRuntime.getLastPlanTime();
    CatalogManager failingCatalogManager = Mockito.mock(CatalogManager.class);
    Mockito.when(
            failingCatalogManager.loadTable(
                Mockito.any(org.apache.amoro.table.TableIdentifier.class)))
        .thenThrow(new RuntimeException("load table failed for test"));
    CapturingPlanExecutor capturingExecutor = new CapturingPlanExecutor();
    OptimizingQueue queue =
        buildOptimizingGroupService(
            failingCatalogManager, Collections.singletonList(tableRuntime), capturingExecutor, 1);

    Assert.assertNull(queue.pollTask(optimizerThread, 0));
    Assert.assertEquals(1, capturingExecutor.submittedCount());

    capturingExecutor.command(0).run();

    Assert.assertTrue(readPlanningTables(queue).isEmpty());
    Assert.assertEquals(1, readAvailablePlanningSlots(queue));
    Assert.assertEquals(OptimizingStatus.PENDING, tableRuntime.getOptimizingStatus());
    Assert.assertTrue(tableRuntime.getLastPlanTime() >= beforePlanTime);
    queue.dispose();
  }

  @Test
  public void testZeroPlanningParallelismDoesNotSchedulePlanning() throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    CapturingPlanExecutor capturingExecutor = new CapturingPlanExecutor();
    OptimizingQueue queue =
        buildOptimizingGroupService(Collections.singletonList(tableRuntime), capturingExecutor, 0);

    Assert.assertNull(queue.pollTask(optimizerThread, 0));

    Assert.assertEquals(0, capturingExecutor.submittedCount());
    Assert.assertTrue(readPlanningTables(queue).isEmpty());
    Assert.assertEquals(0, readAvailablePlanningSlots(queue));
    queue.dispose();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativePlanningParallelismIsRejected() {
    buildOptimizingGroupService(Collections.emptyList(), Runnable::run, -1);
  }

  @Test
  public void testRetryTask() {
    DefaultTableRuntime tableRuntimeMeta = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntimeMeta);

    // 1.poll task
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);

    for (int i = 0; i < TableProperties.SELF_OPTIMIZING_EXECUTE_RETRY_NUMBER_DEFAULT; i++) {
      queue.retryTask(task);
      TaskRuntime<?> retryTask = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
      Assert.assertEquals(retryTask.getTaskId(), task.getTaskId());
      queue.ackTask(task.getTaskId(), optimizerThread);
      queue.completeTask(
          optimizerThread,
          buildOptimizingTaskFailed(task.getTaskId(), optimizerThread.getThreadId()));
      Assert.assertEquals(TaskRuntime.Status.PLANNED, task.getStatus());
    }

    queue.retryTask(task);
    TaskRuntime<?> retryTask = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertEquals(retryTask.getTaskId(), task.getTaskId());
    queue.ackTask(task.getTaskId(), optimizerThread);
    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskFailed(task.getTaskId(), optimizerThread.getThreadId()));
    Assert.assertEquals(TaskRuntime.Status.FAILED, task.getStatus());
    queue.dispose();
  }

  // Issue #4235 fix: when the same optimizer thread polls again while one of its tasks is still
  // ACKED, pollTask -> resetStaleTasksForThread resets that task. With more than one task in the
  // process, the repoll schedules a *different* task, leaving the original PLANNED with token ==
  // null. The optimizer's in-flight ack for the reset task is rejected on purpose so the optimizer
  // skips executing this obsolete round; the task stays PLANNED to be re-polled by another thread.
  @Test
  public void testStaleAckAfterRepollIsRejected() {
    DefaultTableRuntime tableRuntime = initTableWithPartitionedFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);

    // 1. poll + ack task A -> ACKED (optimizer started executing it)
    TaskRuntime<?> taskA = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(taskA);
    queue.ackTask(taskA.getTaskId(), optimizerThread);
    Assert.assertEquals(TaskRuntime.Status.ACKED, taskA.getStatus());

    // 2. the same thread polls again; resetStaleTasksForThread resets the still-executing task A
    // and
    //    the repoll schedules a different task B. Task A is left PLANNED with no token.
    TaskRuntime<?> taskB = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(taskB);
    Assert.assertNotEquals(taskA.getTaskId(), taskB.getTaskId());
    Assert.assertEquals(TaskRuntime.Status.PLANNED, taskA.getStatus());
    Assert.assertNull(taskA.getToken());

    // 3. task A's in-flight ack is rejected so the optimizer skips this stale round (no duplicate
    //    execution). The exception still carries the message the optimizer client recognizes.
    TaskRuntimeException e =
        Assert.assertThrows(
            TaskRuntimeException.class, () -> queue.ackTask(taskA.getTaskId(), optimizerThread));
    Assert.assertTrue(e.getMessage().contains("has been reset or not yet scheduled"));

    // task A remains PLANNED, still waiting to be re-polled and re-executed
    Assert.assertEquals(TaskRuntime.Status.PLANNED, taskA.getStatus());
    queue.dispose();
  }

  // Issue #4235 fix, single-task variant: the repoll re-schedules the *same* task (its token is
  // restored), so the in-flight SUCCESS result for the previous run must be recognized as stale and
  // ignored -- not blow up the SCHEDULED -> SUCCESS transition. The freshly re-scheduled round is
  // left intact to be acked and completed normally.
  @Test
  public void testStaleCompleteAfterRepollIsIgnored() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);

    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    queue.ackTask(task.getTaskId(), optimizerThread);
    Assert.assertEquals(TaskRuntime.Status.ACKED, task.getStatus());

    // same thread polls again -> resetStaleTasksForThread resets the executing task, then the
    // single
    // task is re-scheduled back to the same thread (now SCHEDULED again, awaiting its own ack).
    TaskRuntime<?> repolled = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertSame(task, repolled);
    Assert.assertEquals(TaskRuntime.Status.SCHEDULED, task.getStatus());

    // the previous run's SUCCESS result arrives; it is stale and must be ignored (no exception)
    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId()));

    // the current re-scheduled round is untouched: still SCHEDULED, awaiting its own ack
    Assert.assertEquals(TaskRuntime.Status.SCHEDULED, task.getStatus());
    queue.dispose();
  }

  // The (token, threadId, status) checks alone cannot tell two attempts of the same task on the
  // same optimizer thread apart: once the rescheduled round is ACKED again, a delayed completion
  // of the previous attempt matches all three. Every schedule therefore stamps an attempt id into
  // the task properties, and a completion echoing a different attempt id must be ignored.
  @Test
  public void testStaleCompleteFromPreviousAttemptIsIgnored() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);

    // 1. first attempt: poll + ack, the optimizer starts executing
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    String firstAttempt =
        task.extractProtocolTask().getProperties().get(TaskProperties.TASK_ATTEMPT_ID);
    Assert.assertNotNull(firstAttempt);
    queue.ackTask(task.getTaskId(), optimizerThread);

    // 2. the same thread polls again: the task is reset and re-scheduled to the SAME thread with
    //    a new attempt id, then the second attempt is acked -> (token, threadId, ACKED) is now
    //    identical to what the first attempt's completion will carry
    TaskRuntime<?> repolled = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertSame(task, repolled);
    String secondAttempt =
        task.extractProtocolTask().getProperties().get(TaskProperties.TASK_ATTEMPT_ID);
    Assert.assertNotEquals(firstAttempt, secondAttempt);
    queue.ackTask(task.getTaskId(), optimizerThread);
    Assert.assertEquals(TaskRuntime.Status.ACKED, task.getStatus());

    // 3. the delayed completion of the FIRST attempt arrives; only the echoed attempt id can
    //    expose it as stale -> ignored, the second attempt keeps executing
    OptimizingTaskResult staleResult =
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId());
    staleResult.setSummary(ImmutableMap.of(TaskProperties.TASK_ATTEMPT_ID, firstAttempt));
    queue.completeTask(optimizerThread, staleResult);
    Assert.assertEquals(TaskRuntime.Status.ACKED, task.getStatus());

    // 4. the second attempt's own completion (echoing the current attempt id) is accepted
    OptimizingTaskResult currentResult =
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId());
    currentResult.setSummary(ImmutableMap.of(TaskProperties.TASK_ATTEMPT_ID, secondAttempt));
    queue.completeTask(optimizerThread, currentResult);
    Assert.assertEquals(TaskRuntime.Status.SUCCESS, task.getStatus());
    queue.dispose();
  }

  @Test
  public void testCommitTask() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    Assert.assertEquals(0, queue.collectTasks().size());

    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    queue.ackTask(task.getTaskId(), optimizerThread);
    Assert.assertEquals(
        1, queue.collectTasks(t -> t.getStatus() == TaskRuntime.Status.ACKED).size());
    Assert.assertNotNull(task);
    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId()));
    Assert.assertEquals(TaskRuntime.Status.SUCCESS, task.getStatus());

    // 7.commit
    OptimizingProcess optimizingProcess = tableRuntime.getOptimizingProcess();
    Assert.assertEquals(ProcessStatus.RUNNING, optimizingProcess.getStatus());
    optimizingProcess.commit();
    Assert.assertEquals(ProcessStatus.SUCCESS, optimizingProcess.getStatus());
    Assert.assertNull(tableRuntime.getOptimizingProcess());

    // 8.commit again, throw exceptions, and status not changed.
    Assert.assertThrows(IllegalStateException.class, optimizingProcess::commit);
    Assert.assertEquals(ProcessStatus.SUCCESS, optimizingProcess.getStatus());

    Assert.assertEquals(0, queue.collectTasks().size());
    queue.dispose();
  }

  @Test
  public void testCommitTaskWithFailed() {
    DefaultTableRuntime tableRuntime = initTableWithPartitionedFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    Assert.assertEquals(0, queue.collectTasks().size());

    TaskRuntime<?> firstTask = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    queue.ackTask(firstTask.getTaskId(), optimizerThread);
    Assert.assertEquals(
        1, queue.collectTasks(t -> t.getStatus() == TaskRuntime.Status.ACKED).size());
    Assert.assertNotNull(firstTask);
    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(firstTask.getTaskId(), optimizerThread.getThreadId()));
    Assert.assertEquals(TaskRuntime.Status.SUCCESS, firstTask.getStatus());

    queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);

    for (int i = 0; i < TableProperties.SELF_OPTIMIZING_EXECUTE_RETRY_NUMBER_DEFAULT; i++) {
      queue.retryTask(task);
      TaskRuntime<?> retryTask = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
      Assert.assertEquals(retryTask.getTaskId(), task.getTaskId());
      queue.ackTask(task.getTaskId(), optimizerThread);
      queue.completeTask(
          optimizerThread,
          buildOptimizingTaskFailed(task.getTaskId(), optimizerThread.getThreadId()));
      Assert.assertEquals(TaskRuntime.Status.PLANNED, task.getStatus());
    }

    queue.retryTask(task);
    TaskRuntime<?> retryTask = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertEquals(retryTask.getTaskId(), task.getTaskId());
    queue.ackTask(retryTask.getTaskId(), optimizerThread);

    OptimizingProcess optimizingProcess = tableRuntime.getOptimizingProcess();
    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskFailed(task.getTaskId(), optimizerThread.getThreadId()));
    Assert.assertEquals(TaskRuntime.Status.FAILED, task.getStatus());
    optimizingProcess.commit();
    Assert.assertEquals(ProcessStatus.FAILED, optimizingProcess.getStatus());
    Assert.assertNull(tableRuntime.getOptimizingProcess());
    Assert.assertEquals(0, queue.collectTasks().size());
    queue.dispose();
  }

  @Test
  public void testCollectingTasks() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    Assert.assertEquals(0, queue.collectTasks().size());

    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    Assert.assertEquals(1, queue.collectTasks().size());
    Assert.assertEquals(
        1, queue.collectTasks(t -> t.getStatus() == TaskRuntime.Status.SCHEDULED).size());
    queue.dispose();
  }

  @Test
  public void testTaskAndTableMetrics() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    MetricRegistry registry = MetricManager.getInstance().getGlobalRegistry();
    Map<String, String> tagValues = ImmutableMap.of(GROUP_TAG, testResourceGroup().getName());

    Gauge<Integer> queueTasksGauge =
        (Gauge<Integer>)
            registry.getMetrics().get(new MetricKey(OPTIMIZER_GROUP_PENDING_TASKS, tagValues));
    Gauge<Integer> executingTasksGauge =
        (Gauge<Integer>)
            registry.getMetrics().get(new MetricKey(OPTIMIZER_GROUP_EXECUTING_TASKS, tagValues));
    Gauge<Long> planingTablesGauge =
        (Gauge<Long>)
            registry.getMetrics().get(new MetricKey(OPTIMIZER_GROUP_PLANING_TABLES, tagValues));
    Gauge<Long> pendingTablesGauge =
        (Gauge<Long>)
            registry.getMetrics().get(new MetricKey(OPTIMIZER_GROUP_PENDING_TABLES, tagValues));
    Gauge<Long> executingTablesGauge =
        (Gauge<Long>)
            registry.getMetrics().get(new MetricKey(OPTIMIZER_GROUP_EXECUTING_TABLES, tagValues));
    Gauge<Long> idleTablesGauge =
        (Gauge<Long>)
            registry.getMetrics().get(new MetricKey(OPTIMIZER_GROUP_IDLE_TABLES, tagValues));
    Gauge<Long> committingTablesGauge =
        (Gauge<Long>)
            registry.getMetrics().get(new MetricKey(OPTIMIZER_GROUP_COMMITTING_TABLES, tagValues));

    Assert.assertEquals(0, queueTasksGauge.getValue().longValue());
    Assert.assertEquals(0, executingTasksGauge.getValue().longValue());
    Assert.assertEquals(0, planingTablesGauge.getValue().longValue());
    Assert.assertEquals(1, pendingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, executingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, idleTablesGauge.getValue().longValue());
    Assert.assertEquals(0, committingTablesGauge.getValue().longValue());

    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    Assert.assertEquals(1, queueTasksGauge.getValue().longValue());
    Assert.assertEquals(0, executingTasksGauge.getValue().longValue());
    Assert.assertEquals(0, planingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, pendingTablesGauge.getValue().longValue());
    Assert.assertEquals(1, executingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, idleTablesGauge.getValue().longValue());
    Assert.assertEquals(0, committingTablesGauge.getValue().longValue());

    queue.ackTask(task.getTaskId(), optimizerThread);
    Assert.assertEquals(0, queueTasksGauge.getValue().longValue());
    Assert.assertEquals(1, executingTasksGauge.getValue().longValue());
    Assert.assertEquals(0, planingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, pendingTablesGauge.getValue().longValue());
    Assert.assertEquals(1, executingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, idleTablesGauge.getValue().longValue());
    Assert.assertEquals(0, committingTablesGauge.getValue().longValue());

    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId()));
    Assert.assertEquals(0, queueTasksGauge.getValue().longValue());
    Assert.assertEquals(0, executingTasksGauge.getValue().longValue());
    Assert.assertEquals(0, planingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, pendingTablesGauge.getValue().longValue());
    Assert.assertEquals(1, executingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, idleTablesGauge.getValue().longValue());
    Assert.assertEquals(1, committingTablesGauge.getValue().longValue());

    OptimizingProcess optimizingProcess = tableRuntime.getOptimizingProcess();
    optimizingProcess.commit();
    Assert.assertEquals(0, queueTasksGauge.getValue().longValue());
    Assert.assertEquals(0, executingTasksGauge.getValue().longValue());
    Assert.assertEquals(0, planingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, pendingTablesGauge.getValue().longValue());
    Assert.assertEquals(0, executingTablesGauge.getValue().longValue());
    Assert.assertEquals(1, idleTablesGauge.getValue().longValue());
    Assert.assertEquals(0, committingTablesGauge.getValue().longValue());
    queue.dispose();
  }

  @Test
  public void testAddAndRemoveOptimizers() {

    OptimizingQueue queue = buildOptimizingGroupService();
    MetricRegistry registry = MetricManager.getInstance().getGlobalRegistry();
    Map<String, String> tagValues = ImmutableMap.of(GROUP_TAG, testResourceGroup().getName());
    OptimizerRegisterInfo optimizerRegisterInfo =
        new OptimizerRegisterInfo(
            2, 2048, System.currentTimeMillis(), testResourceGroup().getName());
    OptimizerInstance optimizer = new OptimizerInstance(optimizerRegisterInfo, "test_container");

    Gauge<Integer> optimizerCountGauge =
        (Gauge<Integer>)
            registry
                .getMetrics()
                .get(new MetricKey(OPTIMIZER_GROUP_OPTIMIZER_INSTANCES, tagValues));
    Gauge<Long> optimizerMemoryGauge =
        (Gauge<Long>)
            registry
                .getMetrics()
                .get(new MetricKey(OPTIMIZER_GROUP_MEMORY_BYTES_ALLOCATED, tagValues));
    Gauge<Long> optimizerThreadsGauge =
        (Gauge<Long>) registry.getMetrics().get(new MetricKey(OPTIMIZER_GROUP_THREADS, tagValues));

    queue.addOptimizer(optimizer);
    Assert.assertEquals(1, optimizerCountGauge.getValue().longValue());
    Assert.assertEquals((long) 2048 * 1024 * 1024, optimizerMemoryGauge.getValue().longValue());
    Assert.assertEquals(2, optimizerThreadsGauge.getValue().longValue());

    queue.removeOptimizer(optimizer);
    Assert.assertEquals(0, optimizerCountGauge.getValue().longValue());
    Assert.assertEquals(0, optimizerMemoryGauge.getValue().longValue());
    Assert.assertEquals(0, optimizerThreadsGauge.getValue().longValue());
    queue.dispose();
  }

  @Test
  public void testProcessCloseKeepsLastOptimizedSnapshotId() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    long snapshotIdBeforePlanning = tableRuntime.getLastOptimizedSnapshotId();
    long changeSnapshotIdBeforePlanning = tableRuntime.getLastOptimizedChangeSnapshotId();

    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);

    // Poll task to trigger planning → process creation
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);

    OptimizingProcess process = tableRuntime.getOptimizingProcess();
    Assert.assertNotNull(process);
    Assert.assertEquals(ProcessStatus.RUNNING, process.getStatus());

    // Close process without success (simulates group change / forced termination)
    process.close(false);

    // lastOptimizedSnapshotId and lastOptimizedChangeSnapshotId should NOT be updated
    Assert.assertEquals(snapshotIdBeforePlanning, tableRuntime.getLastOptimizedSnapshotId());
    Assert.assertEquals(
        changeSnapshotIdBeforePlanning, tableRuntime.getLastOptimizedChangeSnapshotId());
    Assert.assertEquals(OptimizingStatus.IDLE, tableRuntime.getOptimizingStatus());
    Assert.assertNull(tableRuntime.getOptimizingProcess());
    queue.dispose();
  }

  @Test
  public void testTableRescheduledAfterProcessClose() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();

    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);

    // Poll task to trigger planning → process creation
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);

    OptimizingProcess process = tableRuntime.getOptimizingProcess();
    Assert.assertNotNull(process);

    // Force close instead of commit (simulates group change)
    process.close(false);
    Assert.assertEquals(OptimizingStatus.IDLE, tableRuntime.getOptimizingStatus());

    // Verify snapshot marker was not updated — key condition for isTablePending()
    Assert.assertNotEquals(
        tableRuntime.getLastOptimizedSnapshotId(), tableRuntime.getCurrentSnapshotId());

    // Simulate setPendingInput() transitioning IDLE → PENDING (done by evaluator in production)
    tableRuntime
        .store()
        .begin()
        .updateStatusCode(code -> OptimizingStatus.PENDING.getCode())
        .commit();
    Assert.assertEquals(OptimizingStatus.PENDING, tableRuntime.getOptimizingStatus());

    // Set minPlanInterval to 0 so the table is immediately eligible for re-planning
    tableRuntime
        .store()
        .begin()
        .updateTableConfig(
            config -> config.put(TableProperties.SELF_OPTIMIZING_MIN_PLAN_INTERVAL, "0"))
        .commit();

    // Build a new queue with the PENDING table — scheduler should NOT skip it
    queue.dispose();
    OptimizingQueue queue2 = buildOptimizingGroupService(tableRuntime);
    TaskRuntime<?> task2 = queue2.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task2);
    queue2.dispose();
  }

  @Test
  public void testPlannedCommittingProcessIsNotRecoveryReplay() throws Exception {
    DefaultTableRuntime runtime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(runtime);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    task.ack(optimizerThread);
    task.complete(
        optimizerThread,
        new OptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId())
            .setTaskOutput(
                SerializationUtil.simpleSerialize(new RewriteFilesOutput(null, null, null))));

    Assert.assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
    OptimizingProcess process = runtime.getOptimizingProcess();
    Assert.assertNotNull(process);
    Assert.assertEquals(ProcessStatus.RUNNING, process.getStatus());

    Assert.assertFalse(canReplayPaimonCommittingProcess(queue, process));
    queue.dispose();
  }

  @Test
  public void testResolveRecoveryCommitModeRequiresAllReplayConditions() {
    Assert.assertEquals(
        CommitMode.RECOVERY_REPLAY,
        OptimizingQueue.resolveRecoveryCommitMode(
            TableFormat.PAIMON, OptimizingStatus.COMMITTING, ProcessStatus.RUNNING, true));
    Assert.assertEquals(
        CommitMode.NORMAL,
        OptimizingQueue.resolveRecoveryCommitMode(
            TableFormat.ICEBERG, OptimizingStatus.COMMITTING, ProcessStatus.RUNNING, true));
    Assert.assertEquals(
        CommitMode.NORMAL,
        OptimizingQueue.resolveRecoveryCommitMode(
            TableFormat.PAIMON, OptimizingStatus.MINOR_OPTIMIZING, ProcessStatus.RUNNING, true));
    Assert.assertEquals(
        CommitMode.NORMAL,
        OptimizingQueue.resolveRecoveryCommitMode(
            TableFormat.PAIMON, OptimizingStatus.COMMITTING, ProcessStatus.FAILED, true));
    Assert.assertEquals(
        CommitMode.NORMAL,
        OptimizingQueue.resolveRecoveryCommitMode(
            TableFormat.PAIMON, OptimizingStatus.COMMITTING, ProcessStatus.RUNNING, false));
  }

  @Test
  public void testRecoveredPaimonProcessPropagatesRecoveryReplayMode() throws Exception {
    DefaultTableRuntime runtime = initTableWithFiles();
    OptimizingQueue originalQueue = buildOptimizingGroupService(runtime);
    TaskRuntime<?> task = originalQueue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    task.ack(optimizerThread);
    task.complete(
        optimizerThread,
        new OptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId())
            .setTaskOutput(
                SerializationUtil.simpleSerialize(new RewriteFilesOutput(null, null, null))));
    Assert.assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());

    DefaultTableRuntime paimonRuntime = Mockito.spy(runtime);
    Mockito.doReturn(TableFormat.PAIMON).when(paimonRuntime).getFormat();
    RecordingCommitter recordingCommitter = new RecordingCommitter();
    ProcessFactory factory = Mockito.mock(ProcessFactory.class);
    Mockito.when(factory.supportedFormats()).thenReturn(Collections.singleton(TableFormat.PAIMON));
    Mockito.when(
            factory.createCommitter(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyCollection(),
                Mockito.anyMap(),
                Mockito.anyMap()))
        .thenReturn(recordingCommitter);
    ProcessFactoryRouter replayRouter =
        new ProcessFactoryRouter(Collections.singletonList(factory));
    OptimizingQueue recoveryQueue =
        new OptimizingQueue(
            CATALOG_MANAGER,
            new ResourceGroup.Builder("paimon-recovery-test", "local").build(),
            quotaProvider,
            planExecutor,
            Collections.singletonList(paimonRuntime),
            1,
            replayRouter);

    OptimizingProcess recoveredProcess = paimonRuntime.getOptimizingProcess();
    Assert.assertNotNull(recoveredProcess);
    Assert.assertEquals(CommitMode.RECOVERY_REPLAY, commitMode(recoveredProcess));
    Assert.assertTrue(canReplayPaimonCommittingProcess(recoveryQueue, recoveredProcess));

    recoveredProcess.commit();

    Assert.assertEquals(CommitMode.RECOVERY_REPLAY, recordingCommitter.commitMode);
    Assert.assertEquals(0, recordingCommitter.legacyCommitCount);
    recoveryQueue.dispose();
    originalQueue.dispose();
  }

  private OptimizingQueue recoveryQueue(
      DefaultTableRuntime paimonRuntime, RecordingCommitter recordingCommitter) {
    ProcessFactory factory = Mockito.mock(ProcessFactory.class);
    Mockito.when(factory.supportedFormats()).thenReturn(Collections.singleton(TableFormat.PAIMON));
    Mockito.when(
            factory.createCommitter(
                Mockito.any(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyCollection(),
                Mockito.anyMap(),
                Mockito.anyMap()))
        .thenReturn(recordingCommitter);
    return new OptimizingQueue(
        CATALOG_MANAGER,
        new ResourceGroup.Builder("paimon-evidence-test", "local").build(),
        quotaProvider,
        planExecutor,
        Collections.singletonList(paimonRuntime),
        1,
        new ProcessFactoryRouter(Collections.singletonList(factory)));
  }

  @Test
  public void testCompleteProcessWithExplicitProcessWorksWhenRuntimeProcessIsDetached()
      throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);

    OptimizingProcess process = tableRuntime.getOptimizingProcess();
    Assert.assertNotNull(process);

    setOptimizingProcess(tableRuntime, null);

    tableRuntime.completeProcess(process, true);

    Assert.assertNull(tableRuntime.getOptimizingProcess());
    Assert.assertEquals(OptimizingStatus.IDLE, tableRuntime.getOptimizingStatus());
    queue.dispose();
  }

  @Test
  public void testProcessOwnerCasAcquireAndRelease() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();

    Assert.assertTrue(tableRuntime.tryAcquireProcessOwner(1001L));
    Assert.assertEquals(1001L, tableRuntime.getProcessId());
    Assert.assertFalse(tableRuntime.tryAcquireProcessOwner(1002L));
    Assert.assertFalse(tableRuntime.tryReleaseProcessOwner(1002L));
    Assert.assertTrue(tableRuntime.tryReleaseProcessOwner(1001L));
    Assert.assertEquals(0L, tableRuntime.getProcessId());
  }

  @Test
  public void testPrepareOwnerForPlanningBlocksActiveOwner() throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);

    Assert.assertFalse(invokePrepareOwnerForPlanning(queue, tableRuntime));
    queue.dispose();
  }

  @Test
  public void testPrepareOwnerForPlanningNormalizesTerminalOwner() throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    queue.ackTask(task.getTaskId(), optimizerThread);
    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId()));
    OptimizingProcess process = tableRuntime.getOptimizingProcess();
    Assert.assertNotNull(process);
    process.commit();
    Assert.assertEquals(ProcessStatus.SUCCESS, process.getStatus());
    long completedProcessId = process.getProcessId();

    Assert.assertTrue(tableRuntime.tryAcquireProcessOwner(completedProcessId));
    Assert.assertEquals(completedProcessId, tableRuntime.getProcessId());

    Assert.assertTrue(invokePrepareOwnerForPlanning(queue, tableRuntime));
    Assert.assertEquals(0L, tableRuntime.getProcessId());
    queue.dispose();
  }

  @Test
  public void testRecycleStaleOwnerClearsOwner() throws Exception {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);
    queue.ackTask(task.getTaskId(), optimizerThread);
    queue.completeTask(
        optimizerThread,
        buildOptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId()));

    OptimizingProcess process = tableRuntime.getOptimizingProcess();
    Assert.assertNotNull(process);
    long ownerProcessId = process.getProcessId();

    TableProcessMeta staleMeta = new TableProcessMeta();
    staleMeta.setProcessId(ownerProcessId);
    staleMeta.setTableId(tableRuntime.getTableIdentifier().getId());
    staleMeta.setStatus(ProcessStatus.RUNNING);
    staleMeta.setCreateTime(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(16));
    staleMeta.setRetryNumber(0);
    staleMeta.setSummary(new HashMap<>());
    staleMeta.setProcessParameters(new HashMap<>());
    staleMeta.setExternalProcessIdentifier("");

    Assert.assertTrue(invokeRecycleStaleOwner(queue, tableRuntime, staleMeta, ownerProcessId));
    Assert.assertEquals(0L, tableRuntime.getProcessId());
    queue.dispose();
  }

  @Test
  public void testCompleteProcessSkipsWhenOwnerLost() {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);

    OptimizingProcess process = tableRuntime.getOptimizingProcess();
    Assert.assertNotNull(process);
    long processId = process.getProcessId();
    Assert.assertTrue(tableRuntime.tryReleaseProcessOwner(processId));
    Assert.assertEquals(0L, tableRuntime.getProcessId());

    OptimizingStatus statusBefore = tableRuntime.getOptimizingStatus();
    tableRuntime.completeProcess(process, false);
    Assert.assertEquals(statusBefore, tableRuntime.getOptimizingStatus());

    queue.dispose();
  }

  @Test
  public void testReleaseOrphanedPlanningTableOnRestart() {
    // Scenario: Table config was changed (optimizer group: "old_group" -> "default"),
    // the optimizing process was closed but table runtime is persisted with "old_group" and
    // PLANNING status
    ResourceGroup oldGroup = new ResourceGroup.Builder("old_group", "local").build();
    // reset optimizer group to "default" to simulate the scenario where the self-optimizing configs
    // have been cleared
    DefaultTableRuntime tableRuntime =
        buildTableRuntimeMeta(OptimizingStatus.PLANNING, defaultResourceGroup());
    Assert.assertEquals(OptimizingStatus.PLANNING, tableRuntime.getOptimizingStatus());
    Assert.assertEquals("default", tableRuntime.getGroupName());

    List<DefaultTableRuntime> released =
        simulateLoadOptimizingQueuesForNonExistentGroup(
            Collections.singletonList(tableRuntime), oldGroup);

    Assert.assertEquals(1, released.size());
    Assert.assertEquals(OptimizingStatus.IDLE, tableRuntime.getOptimizingStatus());
  }

  @Test
  public void testReleaseOrphanedPendingTableOnRestart() {
    // Scenario: Table config was changed (optimizer group: "old_group" -> "default"),
    // the optimizing process was closed but table runtime is persisted with "default" and PENDING
    // status
    ResourceGroup oldGroup = new ResourceGroup.Builder("old_group", "local").build();
    // reset optimizer group to "default" to simulate the scenario where the self-optimizing configs
    // have been cleared
    DefaultTableRuntime tableRuntime =
        buildTableRuntimeMeta(OptimizingStatus.PENDING, defaultResourceGroup());
    Assert.assertEquals(OptimizingStatus.PENDING, tableRuntime.getOptimizingStatus());
    Assert.assertEquals("default", tableRuntime.getGroupName());

    List<DefaultTableRuntime> released =
        simulateLoadOptimizingQueuesForNonExistentGroup(
            Collections.singletonList(tableRuntime), oldGroup);

    Assert.assertEquals(1, released.size());
    Assert.assertEquals(OptimizingStatus.IDLE, tableRuntime.getOptimizingStatus());
  }

  @Test
  public void testSkipPlanningForUnsupportedFormat() throws Exception {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    DefaultTableRuntime tableRuntime = Mockito.mock(DefaultTableRuntime.class);
    @SuppressWarnings("unchecked")
    AmoroTable<Object> paimonTable = Mockito.mock(AmoroTable.class);

    ServerTableIdentifier tableIdentifier =
        ServerTableIdentifier.of(
            org.apache.amoro.table.TableIdentifier.of("mock", "db", "unsupported_table"),
            TableFormat.PAIMON);
    tableIdentifier.setId(9999L);

    Mockito.when(tableRuntime.getTableIdentifier()).thenReturn(tableIdentifier);
    Mockito.when(tableRuntime.getFormat()).thenReturn(TableFormat.PAIMON);
    Mockito.when(tableRuntime.getOptimizingStatus()).thenReturn(OptimizingStatus.PENDING);
    Mockito.doReturn(paimonTable).when(catalogManager).loadTable(tableIdentifier.getIdentifier());
    Mockito.when(paimonTable.format()).thenReturn(TableFormat.PAIMON);

    OptimizingQueue queue =
        new OptimizingQueue(
            catalogManager,
            testResourceGroup(),
            quotaProvider,
            planExecutor,
            Collections.emptyList(),
            1,
            router);

    Method planInternal =
        OptimizingQueue.class.getDeclaredMethod("planInternal", DefaultTableRuntime.class);
    planInternal.setAccessible(true);
    Object process = planInternal.invoke(queue, tableRuntime);

    Assert.assertNull(process);
    Mockito.verify(tableRuntime).beginPlanning();
    Mockito.verify(tableRuntime).refresh(paimonTable);
    Mockito.verify(tableRuntime).completeEmptyProcess();
    Mockito.verify(tableRuntime, Mockito.never()).planFailed();

    queue.dispose();
  }

  @Test
  public void testRefreshTableSkipsSelfOptimizingDisabledRuntime() {
    DefaultTableRuntime tableRuntime = Mockito.mock(DefaultTableRuntime.class);
    ServerTableIdentifier tableIdentifier =
        ServerTableIdentifier.of(
            org.apache.amoro.table.TableIdentifier.of("mock", "db", "disabled_table"),
            TableFormat.ICEBERG);
    tableIdentifier.setId(10000L);

    Mockito.when(tableRuntime.getTableIdentifier()).thenReturn(tableIdentifier);
    Mockito.when(tableRuntime.getFormat()).thenReturn(TableFormat.ICEBERG);
    Mockito.when(tableRuntime.getOptimizingConfig())
        .thenReturn(new OptimizingConfig().setEnabled(false));

    OptimizingQueue queue =
        new OptimizingQueue(
            Mockito.mock(CatalogManager.class),
            testResourceGroup(),
            quotaProvider,
            planExecutor,
            Collections.emptyList(),
            1,
            router);

    queue.refreshTable(tableRuntime);

    Mockito.verify(tableRuntime, Mockito.never()).resetTaskQuotas(Mockito.anyLong());
    queue.dispose();
  }

  @Test
  public void testProcessCachesFormatAtConstruction() throws ReflectiveOperationException {
    DefaultTableRuntime tableRuntime = initTableWithFiles();
    OptimizingQueue queue = buildOptimizingGroupService(tableRuntime);

    // Drive a task through planning to create a TableOptimizingProcess.
    TaskRuntime<?> task = queue.pollTask(optimizerThread, MAX_POLLING_TIME);
    Assert.assertNotNull(task);

    OptimizingProcess process = tableRuntime.getOptimizingProcess();
    Assert.assertNotNull(process);

    // TableOptimizingProcess is a private inner class; reach the cached format
    // via reflection. Only the plan-time constructor is exercised here; the
    // recovery constructor sets this.format the same way from tableRuntime.getFormat().
    Method getFormat = process.getClass().getDeclaredMethod("getFormat");
    getFormat.setAccessible(true);
    Assert.assertEquals(tableRuntime.getFormat(), getFormat.invoke(process));
    Assert.assertEquals(CommitMode.NORMAL, commitMode(process));

    queue.dispose();
  }

  protected DefaultTableRuntime initTableWithFiles() {
    MixedTable mixedTable =
        (MixedTable) tableService().loadTable(serverTableIdentifier()).originalTable();
    mixedTable
        .updateProperties()
        .set(TableProperties.SELF_OPTIMIZING_MIN_PLAN_INTERVAL, "10")
        .set(TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL, "10")
        .commit();
    appendData(mixedTable.asUnkeyedTable(), 1);
    appendData(mixedTable.asUnkeyedTable(), 2);
    DefaultTableRuntime tableRuntime =
        buildTableRuntimeMeta(OptimizingStatus.PENDING, defaultResourceGroup());

    tableRuntime.refresh(tableService().loadTable(serverTableIdentifier()));
    return tableRuntime;
  }

  protected DefaultTableRuntime initTableWithPartitionedFiles() {
    MixedTable mixedTable =
        (MixedTable) tableService().loadTable(serverTableIdentifier()).originalTable();
    appendPartitionedData(mixedTable.asUnkeyedTable(), 1);
    appendPartitionedData(mixedTable.asUnkeyedTable(), 2);
    DefaultTableRuntime tableRuntime =
        buildTableRuntimeMeta(OptimizingStatus.PENDING, defaultResourceGroup());

    tableRuntime.refresh(tableService().loadTable(serverTableIdentifier()));
    return tableRuntime;
  }

  private DefaultTableRuntime buildTableRuntimeMeta(
      OptimizingStatus status, ResourceGroup resourceGroup) {
    DefaultTableRuntime tableRuntime =
        (DefaultTableRuntime) tableService().getRuntime(serverTableIdentifier().getId());
    tableRuntime
        .store()
        .begin()
        .updateStatusCode(code -> status.getCode())
        .updateGroup(any -> resourceGroup.getName())
        .commit();
    return tableRuntime;
  }

  private void setOptimizingProcess(DefaultTableRuntime runtime, OptimizingProcess process)
      throws Exception {
    Field field = DefaultTableRuntime.class.getDeclaredField("optimizingProcess");
    field.setAccessible(true);
    field.set(runtime, process);
  }

  private boolean canReplayPaimonCommittingProcess(OptimizingQueue queue, OptimizingProcess process)
      throws Exception {
    Method method =
        OptimizingQueue.class.getDeclaredMethod(
            "canReplayPaimonCommittingProcess", process.getClass());
    method.setAccessible(true);
    return (boolean) method.invoke(queue, process);
  }

  private CommitMode commitMode(OptimizingProcess process) throws ReflectiveOperationException {
    Method method = process.getClass().getDeclaredMethod("getCommitMode");
    method.setAccessible(true);
    return (CommitMode) method.invoke(process);
  }

  private static class RecordingCommitter implements TableOptimizingCommitter {
    private CommitMode commitMode;
    private int legacyCommitCount;
    private int commitCount;

    @Override
    public void commit() {
      legacyCommitCount++;
    }

    @Override
    public void commit(CommitMode mode) {
      commitMode = mode;
      commitCount++;
    }
  }

  private boolean invokePrepareOwnerForPlanning(
      OptimizingQueue queue, DefaultTableRuntime tableRuntime) throws Exception {
    Method method =
        OptimizingQueue.class.getDeclaredMethod(
            "prepareOwnerForPlanning", DefaultTableRuntime.class);
    method.setAccessible(true);
    return (boolean) method.invoke(queue, tableRuntime);
  }

  private boolean invokeRecycleStaleOwner(
      OptimizingQueue queue,
      DefaultTableRuntime tableRuntime,
      TableProcessMeta processMeta,
      long ownerProcessId)
      throws Exception {
    Method method =
        OptimizingQueue.class.getDeclaredMethod(
            "recycleStaleOwner", DefaultTableRuntime.class, TableProcessMeta.class, long.class);
    method.setAccessible(true);
    return (boolean) method.invoke(queue, tableRuntime, processMeta, ownerProcessId);
  }

  private void appendPartitionedData(UnkeyedTable table, int id) {
    ArrayList<Record> newRecords =
        Lists.newArrayList(
            MixedDataTestHelpers.createRecord(
                table.schema(), id, "111", 0L, "2022-01-01T12:00:00"));
    newRecords.add(
        MixedDataTestHelpers.createRecord(table.schema(), id, "222", 0L, "2022-01-02T12:00:00"));
    newRecords.add(
        MixedDataTestHelpers.createRecord(table.schema(), id, "333", 0L, "2022-01-03T12:00:00"));
    List<DataFile> dataFiles = MixedDataTestHelpers.writeBaseStore(table, 0L, newRecords, false);
    AppendFiles appendFiles = table.newAppend();
    dataFiles.forEach(appendFiles::appendFile);
    appendFiles.commit();
  }

  private void appendData(UnkeyedTable table, int id) {
    ArrayList<Record> newRecords =
        Lists.newArrayList(
            MixedDataTestHelpers.createRecord(
                table.schema(), id, "111", 0L, "2022-01-01T12:00:00"));
    List<DataFile> dataFiles = MixedDataTestHelpers.writeBaseStore(table, 0L, newRecords, false);
    AppendFiles appendFiles = table.newAppend();
    dataFiles.forEach(appendFiles::appendFile);
    appendFiles.commit();
  }

  private DefaultTableRuntime createTable(ServerTableIdentifier serverTableIdentifier) {
    org.apache.iceberg.catalog.Catalog catalog =
        catalogTestHelper().buildIcebergCatalog(catalogMeta());
    catalog.createTable(
        TableIdentifier.of(
            serverTableIdentifier.getDatabase(), serverTableIdentifier.getTableName()),
        tableTestHelper().tableSchema(),
        tableTestHelper().partitionSpec(),
        tableTestHelper().tableProperties());
    exploreTableRuntimes();
    serverTableIdentifier =
        tableManager()
            .getServerTableIdentifier(serverTableIdentifier.getIdentifier().buildTableIdentifier());

    MixedTable mixedTable =
        (MixedTable) tableService().loadTable(serverTableIdentifier).originalTable();
    appendPartitionedData(mixedTable.asUnkeyedTable(), 1);
    appendPartitionedData(mixedTable.asUnkeyedTable(), 2);

    DefaultTableRuntime tableRuntime =
        (DefaultTableRuntime) tableService().getRuntime(serverTableIdentifier.getId());

    tableRuntime
        .store()
        .begin()
        .updateGroup(any -> defaultResourceGroup().getName())
        .updateStatusCode(any -> OptimizingStatus.PENDING.getCode())
        .commit();

    tableRuntime.refresh(tableService().loadTable(serverTableIdentifier));

    return tableRuntime;
  }

  private ServerTableIdentifier tableIdentifier(String tableName, long id) {
    ServerTableIdentifier identifier =
        ServerTableIdentifier.of(
            org.apache.amoro.table.TableIdentifier.of(
                serverTableIdentifier().getCatalog(), "db", tableName),
            TableFormat.ICEBERG);
    identifier.setId(id);
    return identifier;
  }

  @SuppressWarnings("unchecked")
  private Set<ServerTableIdentifier> readPlanningTables(OptimizingQueue queue) throws Exception {
    Field field = OptimizingQueue.class.getDeclaredField("planningTables");
    field.setAccessible(true);
    return new HashSet<>((Set<ServerTableIdentifier>) field.get(queue));
  }

  private int readAvailablePlanningSlots(OptimizingQueue queue) throws Exception {
    Field field = OptimizingQueue.class.getDeclaredField("planningSlots");
    field.setAccessible(true);
    return ((Semaphore) field.get(queue)).availablePermits();
  }

  private Lock readScheduleLock(OptimizingQueue queue) throws Exception {
    Field field = OptimizingQueue.class.getDeclaredField("scheduleLock");
    field.setAccessible(true);
    return (Lock) field.get(queue);
  }

  private void awaitLatch(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static class CapturingPlanExecutor implements Executor {
    private final List<Runnable> commands = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void execute(Runnable command) {
      commands.add(command);
    }

    private int submittedCount() {
      return commands.size();
    }

    private Runnable command(int index) {
      return commands.get(index);
    }
  }

  private OptimizingTaskResult buildOptimizingTaskResult(OptimizingTaskId taskId, int threadId) {
    TableOptimizing.OptimizingOutput output = new RewriteFilesOutput(null, null, null);
    OptimizingTaskResult optimizingTaskResult = new OptimizingTaskResult(taskId, threadId);
    optimizingTaskResult.setTaskOutput(SerializationUtil.simpleSerialize(output));
    return optimizingTaskResult;
  }

  /**
   * Simulate the loadOptimizingQueues logic: tables whose persisted optimizer group no longer
   * exists (e.g., table config changed from an old deleted group to "default", but AMS restarted
   * before the optimizing process was closed) remain in the leftover groupToTableRuntimes map.
   * These PLANNING/PENDING tables should be released to IDLE via completeEmptyProcess().
   */
  private List<DefaultTableRuntime> simulateLoadOptimizingQueuesForNonExistentGroup(
      List<DefaultTableRuntime> tableRuntimes, ResourceGroup resourceGroup) {
    // Only the created resource group is returned
    List<ResourceGroup> existingGroups = Collections.singletonList(resourceGroup);

    // Group tables by their persisted optimizer group
    Map<String, List<DefaultTableRuntime>> groupToTableRuntimes =
        tableRuntimes.stream().collect(Collectors.groupingBy(DefaultTableRuntime::getGroupName));

    // Remove groups that exist — same logic as loadOptimizingQueues
    existingGroups.forEach(group -> groupToTableRuntimes.remove(group.getName()));

    // Release PLANNING/PENDING tables in non-existent groups — same logic as loadOptimizingQueues
    List<DefaultTableRuntime> released = new ArrayList<>();
    groupToTableRuntimes.forEach(
        (groupName, trs) ->
            trs.stream()
                .filter(
                    tr ->
                        tr.getOptimizingStatus() == OptimizingStatus.PLANNING
                            || tr.getOptimizingStatus() == OptimizingStatus.PENDING)
                .forEach(
                    tr -> {
                      tr.completeEmptyProcess();
                      released.add(tr);
                    }));
    return released;
  }

  @Test
  public void testPlanTakesCurrentAnalysisAfterRefreshAndPassesItToFactory() throws Exception {
    AnalysisHandoffFixture fixture = analysisHandoffFixture(TableFormat.PAIMON);
    try {
      TableAnalysisKey key = analysisKey(TableFormat.PAIMON, 11L);
      FormatTableAnalysis analysis = analysis(key);
      Mockito.when(fixture.runtime.getCurrentAnalysisKey()).thenReturn(Optional.of(key));
      Mockito.when(fixture.runtime.takeTableAnalysis(key)).thenReturn(Optional.of(analysis));

      Assert.assertNull(invokePlanInternal(fixture.queue, fixture.runtime));

      org.mockito.InOrder order =
          Mockito.inOrder(
              fixture.runtime, fixture.catalogManager, fixture.factory, fixture.planner);
      order.verify(fixture.runtime).beginPlanning();
      order.verify(fixture.catalogManager).loadTable(fixture.identifier.getIdentifier());
      order.verify(fixture.runtime).refresh(fixture.table);
      order.verify(fixture.runtime).getCurrentAnalysisKey();
      order.verify(fixture.runtime).takeTableAnalysis(key);
      order
          .verify(fixture.factory)
          .createPlanner(
              Mockito.eq(fixture.runtime),
              Mockito.eq(fixture.table),
              Mockito.anyDouble(),
              Mockito.anyLong(),
              Mockito.eq(Optional.of(analysis)));
      order.verify(fixture.planner).isNecessary();
      Mockito.verify(fixture.factory, Mockito.never())
          .createPlanner(Mockito.any(), Mockito.any(), Mockito.anyDouble(), Mockito.anyLong());
    } finally {
      fixture.queue.dispose();
    }
  }

  @Test
  public void testPlanWritesFallbackAnalysisOnlyThroughCurrentKeyGuard() throws Exception {
    AnalysisHandoffFixture fixture = analysisHandoffFixture(TableFormat.PAIMON);
    try {
      TableAnalysisKey currentKey = analysisKey(TableFormat.PAIMON, 12L);
      FormatTableAnalysis staleFallback = analysis(analysisKey(TableFormat.PAIMON, 11L));
      Mockito.when(fixture.runtime.getCurrentAnalysisKey()).thenReturn(Optional.of(currentKey));
      Mockito.when(fixture.runtime.takeTableAnalysis(currentKey)).thenReturn(Optional.empty());
      Mockito.when(fixture.planner.tableAnalysis()).thenReturn(Optional.of(staleFallback));
      Mockito.when(fixture.runtime.updateTableSummaryIfCurrent(fixture.table, staleFallback))
          .thenReturn(false);

      Assert.assertNull(invokePlanInternal(fixture.queue, fixture.runtime));

      Mockito.verify(fixture.factory)
          .createPlanner(
              Mockito.eq(fixture.runtime),
              Mockito.eq(fixture.table),
              Mockito.anyDouble(),
              Mockito.anyLong(),
              Mockito.eq(Optional.empty()));
      org.mockito.InOrder order = Mockito.inOrder(fixture.planner, fixture.runtime);
      order.verify(fixture.planner).isNecessary();
      order.verify(fixture.planner).tableAnalysis();
      order.verify(fixture.runtime).updateTableSummaryIfCurrent(fixture.table, staleFallback);
      Mockito.verify(fixture.runtime).completeEmptyProcess();
    } finally {
      fixture.queue.dispose();
    }
  }

  @Test
  public void testPlanUsesLegacyFactoryPathWhenFormatHasNoAnalysisKey() throws Exception {
    AnalysisHandoffFixture fixture = analysisHandoffFixture(TableFormat.ICEBERG);
    try {
      FormatTableAnalysis legacyAnalysis = analysis(analysisKey(TableFormat.ICEBERG, 21L));
      Mockito.when(fixture.runtime.getCurrentAnalysisKey()).thenReturn(Optional.empty());
      Mockito.when(fixture.planner.tableAnalysis()).thenReturn(Optional.of(legacyAnalysis));

      Assert.assertNull(invokePlanInternal(fixture.queue, fixture.runtime));

      Mockito.verify(fixture.factory)
          .createPlanner(
              Mockito.eq(fixture.runtime),
              Mockito.eq(fixture.table),
              Mockito.anyDouble(),
              Mockito.anyLong());
      Mockito.verify(fixture.factory, Mockito.never())
          .createPlanner(
              Mockito.any(), Mockito.any(), Mockito.anyDouble(), Mockito.anyLong(), Mockito.any());
      Mockito.verify(fixture.runtime, Mockito.never()).takeTableAnalysis(Mockito.any());
      Mockito.verify(fixture.runtime).updateTableSummaryIfCurrent(fixture.table, legacyAnalysis);
    } finally {
      fixture.queue.dispose();
    }
  }

  @Test
  public void testConcurrentPlansConsumeAnalysisSlotOnlyOnce() throws Exception {
    AnalysisHandoffFixture fixture = analysisHandoffFixture(TableFormat.PAIMON);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch release = new CountDownLatch(1);
    try {
      TableAnalysisKey key = analysisKey(TableFormat.PAIMON, 31L);
      FormatTableAnalysis analysis = analysis(key);
      AtomicReference<FormatTableAnalysis> slot = new AtomicReference<>(analysis);
      Queue<Optional<FormatTableAnalysis>> handedOff = new ConcurrentLinkedQueue<>();
      CountDownLatch bothReady = new CountDownLatch(2);

      Mockito.when(fixture.runtime.getCurrentAnalysisKey()).thenReturn(Optional.of(key));
      Mockito.when(fixture.runtime.takeTableAnalysis(key))
          .thenAnswer(
              ignored -> {
                bothReady.countDown();
                Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
                return Optional.ofNullable(slot.getAndSet(null));
              });
      Mockito.when(
              fixture.factory.createPlanner(
                  Mockito.any(),
                  Mockito.any(),
                  Mockito.anyDouble(),
                  Mockito.anyLong(),
                  Mockito.any()))
          .thenAnswer(
              invocation -> {
                @SuppressWarnings("unchecked")
                Optional<FormatTableAnalysis> value = invocation.getArgument(4);
                handedOff.add(value);
                return fixture.planner;
              });

      Future<Object> first =
          executor.submit(() -> invokePlanInternal(fixture.queue, fixture.runtime));
      Future<Object> second =
          executor.submit(() -> invokePlanInternal(fixture.queue, fixture.runtime));
      Assert.assertTrue(bothReady.await(5, TimeUnit.SECONDS));
      release.countDown();
      Assert.assertNull(first.get(5, TimeUnit.SECONDS));
      Assert.assertNull(second.get(5, TimeUnit.SECONDS));

      Assert.assertEquals(2, handedOff.size());
      Assert.assertEquals(1, handedOff.stream().filter(Optional::isPresent).count());
      Assert.assertEquals(1, handedOff.stream().filter(value -> !value.isPresent()).count());
      Assert.assertSame(
          analysis, handedOff.stream().filter(Optional::isPresent).findFirst().get().get());
      Mockito.verify(fixture.runtime, Mockito.times(2)).takeTableAnalysis(key);
    } finally {
      release.countDown();
      executor.shutdownNow();
      fixture.queue.dispose();
    }
  }

  private AnalysisHandoffFixture analysisHandoffFixture(TableFormat format) {
    ServerTableIdentifier identifier =
        ServerTableIdentifier.of(
            org.apache.amoro.table.TableIdentifier.of(
                "analysis_catalog", "analysis_database", "analysis_table"),
            format);
    identifier.setId(12345L);
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = Mockito.mock(AmoroTable.class);
    Mockito.doReturn(table).when(catalogManager).loadTable(identifier.getIdentifier());

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier()).thenReturn(identifier);
    Mockito.when(runtime.getFormat()).thenReturn(format);
    Mockito.when(runtime.refresh(table)).thenReturn(runtime);

    TableOptimizingPlanner planner = Mockito.mock(TableOptimizingPlanner.class);
    Mockito.when(planner.isNecessary()).thenReturn(false);

    ProcessFactory factory = Mockito.mock(ProcessFactory.class);
    Mockito.when(factory.supportedFormats()).thenReturn(Collections.singleton(format));
    Mockito.when(
            factory.createPlanner(
                Mockito.any(), Mockito.any(), Mockito.anyDouble(), Mockito.anyLong()))
        .thenReturn(planner);
    Mockito.when(
            factory.createPlanner(
                Mockito.any(),
                Mockito.any(),
                Mockito.anyDouble(),
                Mockito.anyLong(),
                Mockito.any()))
        .thenReturn(planner);
    ProcessFactoryRouter processFactoryRouter =
        new ProcessFactoryRouter(Collections.singletonList(factory));
    OptimizingQueue queue =
        new OptimizingQueue(
            catalogManager,
            testResourceGroup(),
            resourceGroup -> 1,
            Runnable::run,
            Collections.emptyList(),
            1,
            processFactoryRouter);
    return new AnalysisHandoffFixture(
        identifier, catalogManager, table, runtime, planner, factory, queue);
  }

  private static TableAnalysisKey analysisKey(TableFormat format, long snapshotId) {
    return new TableAnalysisKey(
        "analysis_catalog.analysis_database.analysis_table",
        format,
        snapshotId,
        TableAnalysisKey.NO_CHANGE_SNAPSHOT,
        1L,
        "config",
        "formula",
        TableAnalysisKey.NO_BASELINE,
        TableAnalysisKey.NO_BASELINE_TIME);
  }

  private static FormatTableAnalysis analysis(TableAnalysisKey key) {
    FormatTableAnalysis analysis = Mockito.mock(FormatTableAnalysis.class);
    Mockito.when(analysis.key()).thenReturn(key);
    return analysis;
  }

  private static Object invokePlanInternal(OptimizingQueue queue, DefaultTableRuntime tableRuntime)
      throws Exception {
    Method method =
        OptimizingQueue.class.getDeclaredMethod("planInternal", DefaultTableRuntime.class);
    method.setAccessible(true);
    try {
      return method.invoke(queue, tableRuntime);
    } catch (java.lang.reflect.InvocationTargetException exception) {
      if (exception.getCause() instanceof Exception) {
        throw (Exception) exception.getCause();
      }
      throw exception;
    }
  }

  private static final class AnalysisHandoffFixture {
    private final ServerTableIdentifier identifier;
    private final CatalogManager catalogManager;
    private final AmoroTable<?> table;
    private final DefaultTableRuntime runtime;
    private final TableOptimizingPlanner planner;
    private final ProcessFactory factory;
    private final OptimizingQueue queue;

    private AnalysisHandoffFixture(
        ServerTableIdentifier identifier,
        CatalogManager catalogManager,
        AmoroTable<?> table,
        DefaultTableRuntime runtime,
        TableOptimizingPlanner planner,
        ProcessFactory factory,
        OptimizingQueue queue) {
      this.identifier = identifier;
      this.catalogManager = catalogManager;
      this.table = table;
      this.runtime = runtime;
      this.planner = planner;
      this.factory = factory;
      this.queue = queue;
    }
  }

  private OptimizingTaskResult buildOptimizingTaskFailed(OptimizingTaskId taskId, int threadId) {
    OptimizingTaskResult optimizingTaskResult = new OptimizingTaskResult(taskId, threadId);
    optimizingTaskResult.setErrorMessage("error");
    return optimizingTaskResult;
  }
}
