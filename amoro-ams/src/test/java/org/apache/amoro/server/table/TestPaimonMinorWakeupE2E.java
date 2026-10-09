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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.OptimizingTaskResult;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionExecutor;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport;
import org.apache.amoro.formats.paimon.process.PaimonProcessFactory;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.optimizing.OptimizingProcess;
import org.apache.amoro.server.optimizing.OptimizingQueue;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.optimizing.SchedulingPolicy;
import org.apache.amoro.server.optimizing.TaskRuntime;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.resource.OptimizerThread;
import org.apache.amoro.server.scheduler.inline.TableRuntimeRefreshExecutor;
import org.apache.amoro.table.TableRuntimeFactory.TableRuntimeCreator;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.TableScan;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class TestPaimonMinorWakeupE2E extends PersistentBase {
  @BeforeAll
  static void database() {
    FullSkipTestDatabase.initialize();
  }

  @ParameterizedTest
  @CsvSource({
    "none,1",
    "input,1",
    "lookup,1",
    "full-compaction,1",
    "none,-1",
    "input,-1",
    "lookup,-1",
    "full-compaction,-1"
  })
  @Timeout(60)
  void stoppedWritesWakePlanAndCommitMinor(String producer, String bucket, @TempDir Path warehouse)
      throws Exception {
    ServerTableIdentifier id =
        ServerTableIdentifier.of("minor-e2e", "db", "table", TableFormat.PAIMON);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    OptimizingQueue queue = null;
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      Map<String, String> options = PaimonPrimaryKeyIntervalTestSupport.options(false);
      options.put("bucket", bucket);
      options.put("changelog-producer", producer);
      options.put("lookup-compact", "radical");
      options.put("lookup-wait", "false");
      options.put("num-sorted-run.compaction-trigger", "2");
      options.put("self-optimizing.group", "minor-e2e");
      options.put("self-optimizing.minor.trigger.interval", "5000");
      options.put("self-optimizing.full.trigger.interval", "-1");
      options.put("paimon-optimizer.primary-key.partition-idle-time", "0s");
      FileStoreTable nativeTable =
          PaimonPrimaryKeyIntervalTestSupport.table(catalog, "table", options);
      FileStoreTable control =
          PaimonPrimaryKeyIntervalTestSupport.table(catalog, "control", options);
      for (FileStoreTable target : Arrays.asList(nativeTable, control)) {
        PaimonPrimaryKeyIntervalTestSupport.append(
            target, GenericRow.of(1, 10, 20, 1L), GenericRow.of(2, 20, 30, 1L));
        PaimonPrimaryKeyIntervalTestSupport.append(
            target, GenericRow.of(1, 11, 21, 2L), GenericRow.of(3, 30, 40, 2L));
      }
      PaimonTable table = PaimonPrimaryKeyIntervalTestSupport.wrap(nativeTable);
      doAs(TableMetaMapper.class, m -> m.insertTable(id));
      TableRuntimeMeta meta = new TableRuntimeMeta();
      meta.setTableId(id.getId());
      meta.setGroupName("minor-e2e");
      meta.setStatusCode(OptimizingStatus.IDLE.getCode());
      meta.setTableConfig(options);
      doAs(TableRuntimeMapper.class, m -> m.insertRuntime(meta));
      PaimonProcessFactory factory = new PaimonProcessFactory();
      factory.open(Collections.singletonMap(PaimonProcessFactory.OPTIMIZER_ENABLED.key(), "true"));
      DefaultTableRuntimeFactory runtimeFactory = new DefaultTableRuntimeFactory();
      runtimeFactory.initialize(Collections.singletonList(factory));
      runtimeFactory.withTableLoader(ignored -> table);
      TableRuntimeCreator creator = runtimeFactory.accept(id, options).get();
      DefaultTableRuntimeStore store =
          new DefaultTableRuntimeStore(
              id, meta, creator.requiredStateKeys(), Collections.emptyList());
      DefaultTableRuntime runtime = (DefaultTableRuntime) creator.create(store);
      runtime.refresh(table);
      // Warm up manifest readers before starting the interval window.
      assertTrue(table.evaluatePendingInput(runtime, 20).get().optimizingNecessary());
      long minorTime = System.currentTimeMillis();
      store
          .begin()
          .updateState(
              DefaultTableRuntime.OPTIMIZING_STATE_KEY,
              state -> {
                state.setLastMinorOptimizingTime(minorTime);
                return state;
              })
          .commit();
      refresh(table, runtime);
      refresh(table, runtime); // Rebuild the hint after catching up the optimized snapshot key.
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
      assertEquals(runtime.getCurrentSnapshotId(), runtime.getLastOptimizedSnapshotId());
      assertFalse(runtime.shouldEvaluateCurrentPlanningAnalysis(minorTime + 5000L));
      assertFalse(((PaimonPendingInput) runtime.getPendingInput()).isSameSnapshotPlanningPending());
      long snapshot = nativeTable.latestSnapshot().get().id();
      long remaining = minorTime + 5001L - System.currentTimeMillis();
      if (remaining > 0L) {
        Thread.sleep(remaining);
      }
      assertFalse(runtime.shouldEvaluateCurrentAnalysis());
      refresh(table, runtime);
      assertEquals(snapshot, nativeTable.latestSnapshot().get().id());
      assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
      assertFalse(
          ((PaimonPendingInput) runtime.getPendingInput()).isGentleMinorCompactionPending());
      // Persisted demand survives runtime reload without introducing another JSON field.
      DefaultTableRuntimeStore reloaded =
          new DefaultTableRuntimeStore(
              id,
              getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId())),
              creator.requiredStateKeys(),
              getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates).stream()
                  .filter(state -> state.getTableId() == id.getId())
                  .collect(Collectors.toList()));
      runtime = (DefaultTableRuntime) creator.create(reloaded);
      refresh(table, runtime);
      refresh(table, runtime);
      assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
      assertEquals(0L, runtime.getProcessId());
      SchedulingPolicy scheduler =
          new SchedulingPolicy(new ResourceGroup.Builder("minor-e2e", "local").build());
      scheduler.addTable(runtime);
      runtime.setLastPlanTime(System.currentTimeMillis());
      assertTrue(
          scheduler.scheduleTables(Collections.emptySet(), 1).isEmpty(),
          "minimum planning interval remains enforced");
      runtime.setLastPlanTime(0L);
      assertEquals(
          Collections.singletonList(runtime), scheduler.scheduleTables(Collections.emptySet(), 1));
      queue = queue(table, runtime, factory, executor);
      executeOne(queue, runtime);
      OptimizingProcess process = runtime.getOptimizingProcess();
      long processId = process.getProcessId();
      process.commit();
      assertEquals(
          ProcessStatus.SUCCESS,
          getAs(TableProcessMapper.class, m -> m.getProcessMeta(processId)).getStatus());
      assertEquals(0L, runtime.getProcessId());
      assertTrue(runtime.getLastMinorOptimizingTime() > minorTime);
      assertFalse(((PaimonPendingInput) runtime.getPendingInput()).isSameSnapshotPlanningPending());
      assertEquals(snapshot + 1L, nativeTable.latestSnapshot().get().id());
      nativeMinor(control);
      assertEquals(Arrays.asList("1:11:21:2", "2:20:30:1", "3:30:40:2"), rows(nativeTable));
      assertEquals(rows(control), rows(nativeTable));
      assertEquals(changelog(control), changelog(nativeTable));
      refresh(table, runtime);
      refresh(table, runtime);
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
      assertTrue(scheduler.scheduleTables(Collections.emptySet(), 1).isEmpty());
    } finally {
      if (queue != null) {
        queue.dispose();
      }
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      if (id.getId() != null) {
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
  }

  private static void nativeMinor(FileStoreTable table) throws Exception {
    BatchWriteBuilder builder =
        table.copy(Collections.singletonMap("write-only", "false")).newBatchWriteBuilder();
    try (IOManager io = IOManager.create(System.getProperty("java.io.tmpdir"));
        BatchTableWrite write = builder.newWrite().withIOManager(io);
        BatchTableCommit commit = builder.newCommit()) {
      write.compact(BinaryRow.EMPTY_ROW, 0, false);
      commit.commit(write.prepareCommit());
    }
  }

  private static List<String> rows(FileStoreTable table) throws Exception {
    ReadBuilder read = table.newReadBuilder();
    return readRows(read, read.newScan().plan(), false);
  }

  private static List<String> changelog(FileStoreTable table) throws Exception {
    List<String> result = new ArrayList<>();
    for (long id = 1; id <= table.latestSnapshot().get().id(); id++) {
      result.addAll(
          readRows(
              table.newReadBuilder(),
              table.newSnapshotReader().withSnapshot(id).withMode(ScanMode.CHANGELOG).read(),
              true));
    }
    return result;
  }

  private static List<String> readRows(ReadBuilder read, TableScan.Plan plan, boolean kind)
      throws Exception {
    List<String> result = new ArrayList<>();
    try (RecordReader<InternalRow> reader = read.newRead().createReader(plan)) {
      reader.forEachRemaining(
          row ->
              result.add(
                  (kind ? row.getRowKind().shortString() + ":" : "")
                      + row.getInt(0)
                      + ":"
                      + row.getInt(1)
                      + ":"
                      + row.getInt(2)
                      + ":"
                      + row.getLong(3)));
    }
    Collections.sort(result);
    return result;
  }

  private static void refresh(PaimonTable table, DefaultTableRuntime runtime) {
    TableService tables = mock(TableService.class);
    doReturn(table).when(tables).loadTable(any());
    TableRuntimeRefreshExecutor refresh = new TableRuntimeRefreshExecutor(tables, 1, 60000, 20);
    try {
      refresh.execute(runtime);
    } finally {
      refresh.gracefulShutdown();
    }
  }

  private OptimizingQueue queue(
      PaimonTable table,
      DefaultTableRuntime runtime,
      PaimonProcessFactory factory,
      ExecutorService executor) {
    CatalogManager catalogs = mock(CatalogManager.class);
    doReturn(table).when(catalogs).loadTable(any());
    return new OptimizingQueue(
        catalogs,
        new ResourceGroup.Builder("minor-e2e", "local").build(),
        ignored -> 1,
        executor,
        Collections.singletonList(runtime),
        1,
        new ProcessFactoryRouter(Collections.singletonList(factory)));
  }

  private static void executeOne(OptimizingQueue queue, DefaultTableRuntime runtime)
      throws Exception {
    OptimizerThread thread =
        new OptimizerThread(1, null) {
          @Override
          public String getToken() {
            return "minor-e2e";
          }
        };
    TaskRuntime<?> task = queue.pollTask(thread, 5000);
    assertNotNull(task);
    queue.ackTask(task.getTaskId(), thread);
    PaimonPrimaryKeyCompactionInput input =
        (PaimonPrimaryKeyCompactionInput) task.getTaskDescriptor().getInput();
    assertFalse(input.isFullCompaction());
    assertEquals(OptimizingType.MINOR, input.getOptimizingType());
    queue.completeTask(
        thread,
        new OptimizingTaskResult(task.getTaskId(), 1)
            .setTaskOutput(
                SerializationUtil.simpleSerialize(
                    new PaimonPrimaryKeyCompactionExecutor(input).execute())));
    assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
  }
}
