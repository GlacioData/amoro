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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.io.MixedDataTestHelpers;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.process.iceberg.IcebergProcessFactory;
import org.apache.amoro.server.resource.OptimizerThread;
import org.apache.amoro.server.table.AMSTableTestBase;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.DefaultTableRuntimeStore;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.table.UnkeyedTable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class TestOptimizingPlanningEligibilityFence extends AMSTableTestBase {

  private static final String GROUP_NAME = "planning-eligibility-fence";
  private static final long TIMEOUT_SECONDS = 5L;
  private static final long POLL_WAIT_MILLIS = 500L;

  private final ExecutorService planExecutor = Executors.newSingleThreadExecutor();
  private final ExecutorService pollExecutor = Executors.newSingleThreadExecutor();
  private final Persistency persistency = new Persistency();
  private final OptimizerThread optimizerThread =
      new OptimizerThread(31, null) {
        @Override
        public String getToken() {
          return "planning-fence";
        }
      };

  public TestOptimizingPlanningEligibilityFence() {
    super(
        new BasicCatalogTestHelper(TableFormat.ICEBERG),
        new BasicTableTestHelper(false, true),
        true);
  }

  @Before
  public void clearMetrics() {
    MetricRegistry registry = MetricManager.getInstance().getGlobalRegistry();
    List<MetricKey> keys = new ArrayList<>();
    for (MetricKey key : registry.getMetrics().keySet()) {
      if (GROUP_NAME.equals(key.valueOfTag(OptimizerGroupMetrics.GROUP_TAG))) {
        keys.add(key);
      }
    }
    keys.forEach(registry::unregister);
  }

  @After
  public void shutdownExecutors() {
    planExecutor.shutdownNow();
    pollExecutor.shutdownNow();
  }

  @Test
  public void ineligibleCandidateNeverStartsPlanning() throws Exception {
    DefaultTableRuntime runtime = pendingRuntime();
    GatedIcebergProcessFactory factory = new GatedIcebergProcessFactory();
    OptimizingQueue queue = queue(runtime, factory, ignored -> OptimizingOwnership.OWNED);
    disableOptimizing(runtime);

    Assert.assertNull(queue.pollTask(optimizerThread, 200L));

    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    Assert.assertEquals(0L, runtime.getProcessId());
    Assert.assertEquals(0, factory.getPlannerCreationCount());
    assertPlanningResourcesReleased(queue);
    queue.dispose();
  }

  @Test
  public void revocationAfterRefreshStopsBeforePlannerCreation() throws Exception {
    DefaultTableRuntime runtime = pendingRuntime();
    Gate refreshGate = new Gate();
    Mockito.doAnswer(
            invocation -> {
              Object result = invocation.callRealMethod();
              refreshGate.entered.countDown();
              await(refreshGate.release);
              return result;
            })
        .when(runtime)
        .refresh(Mockito.any());
    GatedIcebergProcessFactory factory = new GatedIcebergProcessFactory();
    OptimizingQueue queue = queue(runtime, factory, ignored -> OptimizingOwnership.OWNED);

    Future<TaskRuntime<?>> poll =
        pollExecutor.submit(() -> queue.pollTask(optimizerThread, POLL_WAIT_MILLIS));
    Assert.assertTrue(refreshGate.entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    disableOptimizing(runtime);
    refreshGate.release.countDown();

    Assert.assertNull(poll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    Assert.assertEquals(0, factory.getPlannerCreationCount());
    assertClosedBeforePersistence(runtime, queue);
    queue.dispose();
  }

  @Test
  public void revocationDuringPlanningStopsBeforeProcessPersistence() throws Exception {
    DefaultTableRuntime runtime = pendingRuntime();
    Gate planGate = new Gate();
    GatedIcebergProcessFactory factory = new GatedIcebergProcessFactory();
    factory.blockNextPlan(planGate);
    OptimizingQueue queue = queue(runtime, factory, ignored -> OptimizingOwnership.OWNED);

    Future<TaskRuntime<?>> poll =
        pollExecutor.submit(() -> queue.pollTask(optimizerThread, POLL_WAIT_MILLIS));
    Assert.assertTrue(planGate.entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    queue.reconcileTableConfig(runtime);
    Assert.assertEquals(OptimizingStatus.PLANNING, runtime.getOptimizingStatus());
    Assert.assertEquals(0L, runtime.getProcessId());
    disableOptimizing(runtime);
    planGate.release.countDown();

    Assert.assertNull(poll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertClosedBeforePersistence(runtime, queue);
    queue.dispose();
  }

  @Test
  public void revocationAfterPersistenceStrictlyClosesBeforeOffer() throws Exception {
    DefaultTableRuntime runtime = pendingRuntime();
    Gate preOfferGate = new Gate();
    Function<DefaultTableRuntime, OptimizingOwnership> ownership =
        blockingPreOfferOwnership(preOfferGate, new AtomicReference<>(OptimizingOwnership.OWNED));
    OptimizingQueue queue = queue(runtime, new GatedIcebergProcessFactory(), ownership);

    Future<TaskRuntime<?>> poll =
        pollExecutor.submit(() -> queue.pollTask(optimizerThread, POLL_WAIT_MILLIS));
    Assert.assertTrue(preOfferGate.entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    long processId = runtime.getProcessId();
    Assert.assertTrue(processId > 0L);
    disableOptimizing(runtime);
    preOfferGate.release.countDown();

    Assert.assertNull(poll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    Assert.assertEquals(ProcessStatus.CLOSED, persistency.process(processId).getStatus());
    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    Assert.assertEquals(0L, runtime.getProcessId());
    Assert.assertNull(runtime.getOptimizingProcess());
    Assert.assertTrue(queue.collectTasks().isEmpty());
    assertPlanningResourcesReleased(queue);
    queue.dispose();
  }

  @Test
  public void ownershipLossAfterPersistenceDetachesWithoutClosing() throws Exception {
    assertOwnershipChangeAfterPersistenceDoesNotClose(OptimizingOwnership.NOT_OWNED);
  }

  @Test
  public void ownershipUnknownAfterPersistenceDetachesWithoutClosing() throws Exception {
    assertOwnershipChangeAfterPersistenceDoesNotClose(OptimizingOwnership.UNKNOWN);
  }

  @Test
  public void transientOwnershipUnknownAfterPersistenceRecoversWhenPlanningCompletes() {
    DefaultTableRuntime runtime = pendingRuntime();
    AtomicInteger finalFenceAttempts = new AtomicInteger();
    OptimizingQueue queue =
        queue(
            runtime,
            new GatedIcebergProcessFactory(),
            ignored -> {
              if (runtime.getProcessId() != 0L && finalFenceAttempts.getAndIncrement() == 0) {
                return OptimizingOwnership.UNKNOWN;
              }
              return OptimizingOwnership.OWNED;
            });

    TaskRuntime<?> recoveredTask = queue.pollTask(optimizerThread, POLL_WAIT_MILLIS);

    Assert.assertNotNull(recoveredTask);
    Assert.assertTrue(finalFenceAttempts.get() >= 2);
    Assert.assertEquals(runtime.getProcessId(), recoveredTask.getTaskId().getProcessId());
    Assert.assertEquals(
        ProcessStatus.RUNNING, persistency.process(runtime.getProcessId()).getStatus());
    queue.dispose();
  }

  @Test
  public void ownershipRecoveryReoffersPersistedProcessOnConfigReconciliation() throws Exception {
    DefaultTableRuntime runtime = pendingRuntime();
    Gate preOfferGate = new Gate();
    AtomicReference<OptimizingOwnership> ownership =
        new AtomicReference<>(OptimizingOwnership.OWNED);
    OptimizingQueue queue =
        queue(
            runtime,
            new GatedIcebergProcessFactory(),
            blockingPreOfferOwnership(preOfferGate, ownership));

    Future<TaskRuntime<?>> firstPoll =
        pollExecutor.submit(() -> queue.pollTask(optimizerThread, POLL_WAIT_MILLIS));
    Assert.assertTrue(preOfferGate.entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    long processId = runtime.getProcessId();
    Assert.assertTrue(processId > 0L);
    ownership.set(OptimizingOwnership.UNKNOWN);
    preOfferGate.release.countDown();

    Assert.assertNull(firstPoll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(processId).getStatus());
    Assert.assertNull(runtime.getOptimizingProcess());
    Assert.assertTrue(queue.collectTasks().isEmpty());

    ownership.set(OptimizingOwnership.OWNED);
    queue.reconcileTableConfig(runtime);
    queue.reconcileTableConfig(runtime);

    Assert.assertEquals(1, queue.collectTasks().size());
    TaskRuntime<?> recoveredTask = queue.pollTask(optimizerThread, POLL_WAIT_MILLIS);
    Assert.assertNotNull(recoveredTask);
    Assert.assertEquals(processId, recoveredTask.getTaskId().getProcessId());
    Assert.assertEquals(processId, runtime.getProcessId());
    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(processId).getStatus());
    queue.dispose();
  }

  @Test
  public void ownershipRecoveryNormalizesPlanningWithoutProcessOnOwnershipReconciliation()
      throws Exception {
    DefaultTableRuntime runtime = pendingRuntime();
    Gate refreshGate = new Gate();
    AtomicReference<OptimizingOwnership> ownership =
        new AtomicReference<>(OptimizingOwnership.OWNED);
    Mockito.doAnswer(
            invocation -> {
              Object result = invocation.callRealMethod();
              refreshGate.entered.countDown();
              await(refreshGate.release);
              return result;
            })
        .when(runtime)
        .refresh(Mockito.any());
    OptimizingQueue queue =
        queue(runtime, new GatedIcebergProcessFactory(), ignored -> ownership.get());

    Future<TaskRuntime<?>> poll =
        pollExecutor.submit(() -> queue.pollTask(optimizerThread, POLL_WAIT_MILLIS));
    Assert.assertTrue(refreshGate.entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    ownership.set(OptimizingOwnership.UNKNOWN);
    refreshGate.release.countDown();

    Assert.assertNull(poll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    Assert.assertEquals(OptimizingStatus.PLANNING, runtime.getOptimizingStatus());
    Assert.assertEquals(0L, runtime.getProcessId());

    ownership.set(OptimizingOwnership.OWNED);
    queue.recoverOwnedTable(runtime);

    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    Assert.assertEquals(0L, runtime.getProcessId());
    Assert.assertTrue(
        queue.getSchedulingPolicy().getTableRuntimeMap().containsKey(runtime.getTableIdentifier()));
    queue.dispose();
  }

  private void assertOwnershipChangeAfterPersistenceDoesNotClose(
      OptimizingOwnership deniedOwnership) throws Exception {
    DefaultTableRuntime runtime = pendingRuntime();
    Gate preOfferGate = new Gate();
    AtomicReference<OptimizingOwnership> ownership =
        new AtomicReference<>(OptimizingOwnership.OWNED);
    OptimizingQueue queue =
        queue(
            runtime,
            new GatedIcebergProcessFactory(),
            blockingPreOfferOwnership(preOfferGate, ownership));

    Future<TaskRuntime<?>> poll =
        pollExecutor.submit(() -> queue.pollTask(optimizerThread, POLL_WAIT_MILLIS));
    Assert.assertTrue(preOfferGate.entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    long processId = runtime.getProcessId();
    Assert.assertTrue(processId > 0L);
    ownership.set(deniedOwnership);
    preOfferGate.release.countDown();

    Assert.assertNull(poll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(processId).getStatus());
    Assert.assertEquals(processId, runtime.getProcessId());
    Assert.assertNull(runtime.getOptimizingProcess());
    Assert.assertTrue(queue.collectTasks().isEmpty());
    assertPlanningResourcesReleased(queue);
    queue.dispose();
  }

  private DefaultTableRuntime pendingRuntime() {
    MixedTable mixedTable =
        (MixedTable) tableService().loadTable(serverTableIdentifier()).originalTable();
    mixedTable
        .updateProperties()
        .set(TableProperties.SELF_OPTIMIZING_MIN_PLAN_INTERVAL, "10")
        .set(TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL, "10")
        .commit();
    appendData(mixedTable.asUnkeyedTable(), 1);
    appendData(mixedTable.asUnkeyedTable(), 2);
    DefaultTableRuntime runtime =
        Mockito.spy(getDefaultTableRuntime(serverTableIdentifier().getId()));
    runtime.refresh(tableService().loadTable(serverTableIdentifier()));
    ((DefaultTableRuntimeStore) runtime.store()).setRuntimeHandler(null);
    runtime
        .store()
        .begin()
        .updateStatusCode(ignored -> OptimizingStatus.PENDING.getCode())
        .commit();
    return runtime;
  }

  private void appendData(UnkeyedTable table, int id) {
    ArrayList<Record> records =
        Lists.newArrayList(
            MixedDataTestHelpers.createRecord(
                table.schema(), id, "111", 0L, "2022-01-01T12:00:00"));
    List<DataFile> dataFiles = MixedDataTestHelpers.writeBaseStore(table, 0L, records, false);
    AppendFiles appendFiles = table.newAppend();
    dataFiles.forEach(appendFiles::appendFile);
    appendFiles.commit();
  }

  private void disableOptimizing(DefaultTableRuntime runtime) {
    runtime
        .store()
        .begin()
        .updateTableConfig(config -> config.put(TableProperties.ENABLE_SELF_OPTIMIZING, "false"))
        .commit();
  }

  private OptimizingQueue queue(
      DefaultTableRuntime runtime,
      GatedIcebergProcessFactory factory,
      Function<DefaultTableRuntime, OptimizingOwnership> ownershipGuard) {
    return new OptimizingQueue(
        catalogManager(),
        new ResourceGroup.Builder(GROUP_NAME, "local").build(),
        ignored -> 1,
        planExecutor,
        Collections.singletonList(runtime),
        1,
        new ProcessFactoryRouter(Collections.singletonList(factory)),
        ownershipGuard);
  }

  private Function<DefaultTableRuntime, OptimizingOwnership> blockingPreOfferOwnership(
      Gate gate, AtomicReference<OptimizingOwnership> ownership) {
    return runtime -> {
      if (runtime.getProcessId() != 0L) {
        gate.entered.countDown();
        await(gate.release);
      }
      return ownership.get();
    };
  }

  private void assertClosedBeforePersistence(DefaultTableRuntime runtime, OptimizingQueue queue)
      throws Exception {
    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    Assert.assertEquals(0L, runtime.getProcessId());
    Assert.assertNull(runtime.getOptimizingProcess());
    Assert.assertTrue(queue.collectTasks().isEmpty());
    assertPlanningResourcesReleased(queue);
  }

  @SuppressWarnings("unchecked")
  private void assertPlanningResourcesReleased(OptimizingQueue queue) throws Exception {
    Field planningTablesField = OptimizingQueue.class.getDeclaredField("planningTables");
    planningTablesField.setAccessible(true);
    Set<ServerTableIdentifier> planningTables =
        (Set<ServerTableIdentifier>) planningTablesField.get(queue);
    Field planningSlotsField = OptimizingQueue.class.getDeclaredField("planningSlots");
    planningSlotsField.setAccessible(true);
    Semaphore planningSlots = (Semaphore) planningSlotsField.get(queue);
    Assert.assertTrue(planningTables.isEmpty());
    Assert.assertEquals(1, planningSlots.availablePermits());
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting for test latch");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for test latch", e);
    }
  }

  private static class Gate {
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
  }

  private static class GatedIcebergProcessFactory extends IcebergProcessFactory {
    private final java.util.concurrent.atomic.AtomicInteger plannerCreationCount =
        new java.util.concurrent.atomic.AtomicInteger();
    private volatile Gate nextPlanGate;

    private void blockNextPlan(Gate gate) {
      this.nextPlanGate = gate;
    }

    private int getPlannerCreationCount() {
      return plannerCreationCount.get();
    }

    @Override
    public TableOptimizingPlanner createPlanner(
        TableRuntime tableRuntime,
        AmoroTable<?> table,
        double availableCore,
        long maxInputSizePerThread) {
      plannerCreationCount.incrementAndGet();
      TableOptimizingPlanner planner =
          Mockito.spy(
              super.createPlanner(tableRuntime, table, availableCore, maxInputSizePerThread));
      Gate gate = nextPlanGate;
      if (gate != null) {
        nextPlanGate = null;
        Mockito.doAnswer(
                invocation -> {
                  gate.entered.countDown();
                  await(gate.release);
                  return invocation.callRealMethod();
                })
            .when(planner)
            .plan();
      }
      return planner;
    }
  }

  private static class Persistency extends PersistentBase {
    private TableProcessMeta process(long processId) {
      return getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(processId));
    }
  }
}
