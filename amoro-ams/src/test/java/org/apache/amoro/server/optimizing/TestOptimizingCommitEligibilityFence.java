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

import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.api.OptimizingTaskResult;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.io.MixedDataTestHelpers;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.optimizing.RewriteFilesOutput;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
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
import org.apache.amoro.utils.SerializationUtil;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestOptimizingCommitEligibilityFence extends AMSTableTestBase {

  private static final String GROUP_NAME = "commit-eligibility-fence";
  private static final long TIMEOUT_SECONDS = 5L;

  private final ExecutorService planExecutor = Executors.newSingleThreadExecutor();
  private final ExecutorService raceExecutor = Executors.newFixedThreadPool(2);
  private final Persistency persistency = new Persistency();
  private final FencedIcebergProcessFactory processFactory = new FencedIcebergProcessFactory();
  private final OptimizerThread optimizerThread =
      new OptimizerThread(7, null) {
        @Override
        public String getToken() {
          return "commit-fence";
        }
      };

  public TestOptimizingCommitEligibilityFence() {
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
    raceExecutor.shutdownNow();
  }

  @Test
  public void configPersistedFirstClosesInsteadOfEnteringCommitting() throws Exception {
    Fixture fixture = runningFixture(false);
    CountDownLatch configAppliedUnderLock = new CountDownLatch(1);
    CountDownLatch releaseConfigCommit = new CountDownLatch(1);

    Future<?> configUpdate =
        raceExecutor.submit(
            () ->
                fixture
                    .runtime
                    .store()
                    .begin()
                    .updateTableConfig(
                        config -> {
                          config.put(TableProperties.ENABLE_SELF_OPTIMIZING, "false");
                          configAppliedUnderLock.countDown();
                          await(releaseConfigCommit);
                        })
                    .commit());
    Assert.assertTrue(configAppliedUnderLock.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    Future<?> completion =
        raceExecutor.submit(
            () -> {
              fixture.queue.ackTask(fixture.task.getTaskId(), optimizerThread);
              fixture.queue.completeTask(optimizerThread, successfulResult(fixture.task));
            });
    releaseConfigCommit.countDown();

    configUpdate.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    completion.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    Assert.assertEquals(ProcessStatus.CLOSED, persistency.process(fixture.processId).getStatus());
    Assert.assertEquals(
        TaskRuntime.Status.SUCCESS, persistency.tasks(fixture.processId).get(0).getStatus());
    Assert.assertEquals(OptimizingStatus.IDLE, fixture.runtime.getOptimizingStatus());
    Assert.assertEquals(0L, fixture.runtime.getProcessId());
    fixture.queue.dispose();
  }

  @Test
  public void committingPersistedFirstWinsOverLaterConfigDisable() throws Exception {
    Fixture fixture = runningFixture(false);
    EligibilityGate gate = new EligibilityGate();
    processFactory.blockNextEligibility(gate);

    Future<?> completion =
        raceExecutor.submit(
            () -> {
              fixture.queue.ackTask(fixture.task.getTaskId(), optimizerThread);
              fixture.queue.completeTask(optimizerThread, successfulResult(fixture.task));
            });
    Assert.assertTrue(gate.entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

    CountDownLatch configApplied = new CountDownLatch(1);
    Future<?> configUpdate =
        raceExecutor.submit(
            () ->
                fixture
                    .runtime
                    .store()
                    .begin()
                    .updateTableConfig(
                        config -> {
                          config.put(TableProperties.ENABLE_SELF_OPTIMIZING, "false");
                          configApplied.countDown();
                        })
                    .commit());
    Assert.assertFalse(configApplied.await(200L, TimeUnit.MILLISECONDS));
    gate.release.countDown();

    completion.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    configUpdate.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    Assert.assertEquals(OptimizingStatus.COMMITTING, fixture.runtime.getOptimizingStatus());
    Assert.assertEquals(fixture.processId, fixture.runtime.getProcessId());
    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(fixture.processId).getStatus());
    Assert.assertFalse(fixture.runtime.getOptimizingConfig().isEnabled());
    fixture.queue.dispose();
  }

  @Test
  public void partialCommitUsesTheSameEligibilityFence() {
    Fixture fixture = runningFixture(true);
    fixture
        .runtime
        .store()
        .begin()
        .updateTableConfig(config -> config.put(TableProperties.ENABLE_SELF_OPTIMIZING, "false"))
        .commit();

    fixture.runtime.getOptimizingProcess().close(true);

    Assert.assertEquals(ProcessStatus.CLOSED, persistency.process(fixture.processId).getStatus());
    Assert.assertEquals(
        TaskRuntime.Status.CANCELED, persistency.tasks(fixture.processId).get(0).getStatus());
    Assert.assertEquals(OptimizingStatus.IDLE, fixture.runtime.getOptimizingStatus());
    Assert.assertEquals(0L, fixture.runtime.getProcessId());
    fixture.queue.dispose();
  }

  @Test
  public void ownershipLostCannotEnterCommitting() {
    assertOwnershipCannotEnterCommitting(OptimizingOwnership.NOT_OWNED);
  }

  @Test
  public void ownershipUnknownCannotEnterCommitting() {
    assertOwnershipCannotEnterCommitting(OptimizingOwnership.UNKNOWN);
  }

  @Test
  public void ownershipRecoveryRetriesEnteringCommittingOnConfigReconciliation() {
    AtomicReference<OptimizingOwnership> ownership =
        new AtomicReference<>(OptimizingOwnership.OWNED);
    Fixture fixture = runningFixture(false, ignored -> ownership.get());
    ownership.set(OptimizingOwnership.UNKNOWN);

    fixture.queue.ackTask(fixture.task.getTaskId(), optimizerThread);
    fixture.queue.completeTask(optimizerThread, successfulResult(fixture.task));

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(fixture.processId).getStatus());
    Assert.assertEquals(
        TaskRuntime.Status.SUCCESS, persistency.tasks(fixture.processId).get(0).getStatus());
    Assert.assertNotEquals(OptimizingStatus.COMMITTING, fixture.runtime.getOptimizingStatus());

    ownership.set(OptimizingOwnership.OWNED);
    fixture.queue.reconcileTableConfig(fixture.runtime);

    Assert.assertEquals(OptimizingStatus.COMMITTING, fixture.runtime.getOptimizingStatus());
    Assert.assertEquals(fixture.processId, fixture.runtime.getProcessId());
    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(fixture.processId).getStatus());
    fixture.queue.dispose();
  }

  private void assertOwnershipCannotEnterCommitting(OptimizingOwnership deniedOwnership) {
    AtomicReference<OptimizingOwnership> ownership =
        new AtomicReference<>(OptimizingOwnership.OWNED);
    Fixture fixture = runningFixture(false, ignored -> ownership.get());
    ownership.set(deniedOwnership);

    fixture.queue.ackTask(fixture.task.getTaskId(), optimizerThread);
    fixture.queue.completeTask(optimizerThread, successfulResult(fixture.task));

    Assert.assertEquals(ProcessStatus.RUNNING, persistency.process(fixture.processId).getStatus());
    Assert.assertEquals(
        TaskRuntime.Status.SUCCESS, persistency.tasks(fixture.processId).get(0).getStatus());
    Assert.assertNotEquals(OptimizingStatus.COMMITTING, fixture.runtime.getOptimizingStatus());
    Assert.assertEquals(fixture.processId, fixture.runtime.getProcessId());
    fixture.queue.dispose();
  }

  private Fixture runningFixture(boolean allowPartialCommit) {
    return runningFixture(allowPartialCommit, ignored -> OptimizingOwnership.OWNED);
  }

  private Fixture runningFixture(
      boolean allowPartialCommit,
      java.util.function.Function<DefaultTableRuntime, OptimizingOwnership> ownershipGuard) {
    DefaultTableRuntime runtime = runtimeWithProcessInput(allowPartialCommit);
    OptimizingQueue queue =
        new OptimizingQueue(
            catalogManager(),
            new ResourceGroup.Builder(GROUP_NAME, "local").build(),
            ignored -> 1,
            planExecutor,
            Collections.singletonList(runtime),
            1,
            new ProcessFactoryRouter(Collections.singletonList(processFactory)),
            ownershipGuard);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, TimeUnit.SECONDS.toMillis(5));
    Assert.assertNotNull(task);
    long processId = runtime.getProcessId();
    Assert.assertTrue(processId > 0L);
    return new Fixture(runtime, queue, task, processId);
  }

  private DefaultTableRuntime runtimeWithProcessInput(boolean allowPartialCommit) {
    MixedTable mixedTable =
        (MixedTable) tableService().loadTable(serverTableIdentifier()).originalTable();
    mixedTable
        .updateProperties()
        .set(TableProperties.SELF_OPTIMIZING_MIN_PLAN_INTERVAL, "10")
        .set(TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL, "10")
        .set(
            TableProperties.SELF_OPTIMIZING_ALLOW_PARTIAL_COMMIT,
            Boolean.toString(allowPartialCommit))
        .commit();
    appendData(mixedTable.asUnkeyedTable(), 1);
    appendData(mixedTable.asUnkeyedTable(), 2);
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime
        .store()
        .begin()
        .updateStatusCode(ignored -> OptimizingStatus.PENDING.getCode())
        .commit();
    runtime.refresh(tableService().loadTable(serverTableIdentifier()));
    ((DefaultTableRuntimeStore) runtime.store()).setRuntimeHandler(null);
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

  private OptimizingTaskResult successfulResult(TaskRuntime<?> task) {
    return new OptimizingTaskResult(task.getTaskId(), optimizerThread.getThreadId())
        .setTaskOutput(SerializationUtil.simpleSerialize(new RewriteFilesOutput(null, null, null)));
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

  private static class EligibilityGate {
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
  }

  private static class FencedIcebergProcessFactory extends IcebergProcessFactory {
    private volatile EligibilityGate nextGate;

    private void blockNextEligibility(EligibilityGate gate) {
      this.nextGate = gate;
    }

    @Override
    public boolean isOptimizingEligible(TableRuntime tableRuntime) {
      EligibilityGate gate = nextGate;
      if (gate != null) {
        nextGate = null;
        gate.entered.countDown();
        await(gate.release);
      }
      return true;
    }
  }

  private static class Fixture {
    private final DefaultTableRuntime runtime;
    private final OptimizingQueue queue;
    private final TaskRuntime<?> task;
    private final long processId;

    private Fixture(
        DefaultTableRuntime runtime, OptimizingQueue queue, TaskRuntime<?> task, long processId) {
      this.runtime = runtime;
      this.queue = queue;
      this.task = task;
      this.processId = processId;
    }
  }

  private static class Persistency extends PersistentBase {
    private TableProcessMeta process(long processId) {
      return getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(processId));
    }

    private List<OptimizingTaskMeta> tasks(long processId) {
      return getAs(
          OptimizingProcessMapper.class,
          mapper -> mapper.selectOptimizeTaskMetas(Collections.singletonList(processId)));
    }
  }
}
