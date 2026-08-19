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
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.io.MixedDataTestHelpers;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
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
import org.apache.amoro.server.table.OptimizingOwnerConflictException;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestOptimizingProcessStrictCompletion extends AMSTableTestBase {

  private static final String GROUP_NAME = "strict-completion";
  private final ExecutorService planExecutor = Executors.newSingleThreadExecutor();
  private final Persistency persistency = new Persistency();
  private final OptimizerThread optimizerThread =
      new OptimizerThread(1, null) {
        @Override
        public String getToken() {
          return "strict-test";
        }
      };

  public TestOptimizingProcessStrictCompletion() {
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
  public void shutdownExecutor() {
    planExecutor.shutdownNow();
  }

  @Test
  public void strictCloseWinnerAtomicallyClosesProcessTasksAndTable() {
    DefaultTableRuntime runtime = runtimeWithProcessInput();
    OptimizingQueue queue = queue(runtime);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, 5000L);
    Assert.assertNotNull(task);
    OptimizingProcess process = runtime.getOptimizingProcess();
    Assert.assertNotNull(process);

    queue.closeProcessStrict(runtime);

    Assert.assertEquals(
        ProcessStatus.CLOSED, persistency.process(process.getProcessId()).getStatus());
    Assert.assertEquals(
        TaskRuntime.Status.CANCELED, persistency.tasks(process.getProcessId()).get(0).getStatus());
    Assert.assertEquals(0L, runtime.getProcessId());
    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    queue.dispose();
  }

  @Test
  public void strictCloseCasLoserRollsBackProcessAndTaskUpdates() {
    DefaultTableRuntime runtime = runtimeWithProcessInput();
    OptimizingQueue queue = queue(runtime);
    TaskRuntime<?> task = queue.pollTask(optimizerThread, 5000L);
    Assert.assertNotNull(task);
    OptimizingProcess process = runtime.getOptimizingProcess();
    Assert.assertNotNull(process);
    long competingProcessId = process.getProcessId() + 1L;
    Assert.assertTrue(runtime.tryReleaseProcessOwner(process.getProcessId()));
    Assert.assertTrue(runtime.tryAcquireProcessOwner(competingProcessId));
    OptimizingStatus statusBefore = runtime.getOptimizingStatus();

    PersistenceException failure =
        Assert.assertThrows(PersistenceException.class, () -> queue.closeProcessStrict(runtime));

    Assert.assertTrue(hasCause(failure, OptimizingOwnerConflictException.class));
    Assert.assertEquals(
        ProcessStatus.RUNNING, persistency.process(process.getProcessId()).getStatus());
    Assert.assertEquals(
        TaskRuntime.Status.SCHEDULED, persistency.tasks(process.getProcessId()).get(0).getStatus());
    Assert.assertEquals(competingProcessId, runtime.getProcessId());
    Assert.assertEquals(statusBefore, runtime.getOptimizingStatus());
    Assert.assertNull(runtime.getOptimizingProcess());
    queue.dispose();
  }

  @Test
  public void overlappingOwnersAllowOnlyOneStrictCloseTransaction() throws Exception {
    DefaultTableRuntime firstRuntime = runtimeWithProcessInput();
    OptimizingQueue firstQueue = queue(firstRuntime);
    TaskRuntime<?> task = firstQueue.pollTask(optimizerThread, 5000L);
    Assert.assertNotNull(task);
    long processId = firstRuntime.getProcessId();

    DefaultTableRuntime secondRuntime =
        new DefaultTableRuntime(
            firstRuntime.store(), () -> tableService().loadTable(serverTableIdentifier()));
    OptimizingQueue secondQueue = queue(secondRuntime, GROUP_NAME + "-second-owner");
    Assert.assertEquals(processId, secondRuntime.getProcessId());
    Assert.assertNotNull(secondRuntime.getOptimizingProcess());

    ExecutorService closeExecutor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<Throwable> first =
          closeExecutor.submit(() -> closeOutcome(firstQueue, firstRuntime, start));
      Future<Throwable> second =
          closeExecutor.submit(() -> closeOutcome(secondQueue, secondRuntime, start));
      start.countDown();

      Throwable firstFailure = first.get(10L, TimeUnit.SECONDS);
      Throwable secondFailure = second.get(10L, TimeUnit.SECONDS);
      int conflicts =
          (hasCause(firstFailure, OptimizingOwnerConflictException.class) ? 1 : 0)
              + (hasCause(secondFailure, OptimizingOwnerConflictException.class) ? 1 : 0);

      Assert.assertEquals(1, conflicts);
      Assert.assertEquals(ProcessStatus.CLOSED, persistency.process(processId).getStatus());
      Assert.assertEquals(
          TaskRuntime.Status.CANCELED, persistency.tasks(processId).get(0).getStatus());
      Assert.assertEquals(0L, firstRuntime.getProcessId());
    } finally {
      closeExecutor.shutdownNow();
      firstQueue.dispose();
      secondQueue.dispose();
    }
  }

  private static Throwable closeOutcome(
      OptimizingQueue queue, DefaultTableRuntime runtime, CountDownLatch start) {
    try {
      start.await();
      queue.closeProcessStrict(runtime);
      return null;
    } catch (Throwable t) {
      return t;
    }
  }

  private DefaultTableRuntime runtimeWithProcessInput() {
    MixedTable mixedTable =
        (MixedTable) tableService().loadTable(serverTableIdentifier()).originalTable();
    mixedTable
        .updateProperties()
        .set(TableProperties.SELF_OPTIMIZING_MIN_PLAN_INTERVAL, "10")
        .set(TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL, "10")
        .commit();
    appendData(mixedTable.asUnkeyedTable(), 1);
    appendData(mixedTable.asUnkeyedTable(), 2);
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime
        .store()
        .begin()
        .updateStatusCode(ignored -> OptimizingStatus.PENDING.getCode())
        .updateGroup(ignored -> GROUP_NAME)
        .commit();
    runtime.refresh(tableService().loadTable(serverTableIdentifier()));
    return runtime;
  }

  private OptimizingQueue queue(DefaultTableRuntime runtime) {
    return queue(runtime, GROUP_NAME);
  }

  private OptimizingQueue queue(DefaultTableRuntime runtime, String groupName) {
    return new OptimizingQueue(
        catalogManager(),
        new ResourceGroup.Builder(groupName, "local").build(),
        ignored -> 1,
        planExecutor,
        Collections.singletonList(runtime),
        1,
        new ProcessFactoryRouter(Collections.singletonList(new IcebergProcessFactory())));
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

  private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
    Throwable current = throwable;
    while (current != null) {
      if (type.isInstance(current)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
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
