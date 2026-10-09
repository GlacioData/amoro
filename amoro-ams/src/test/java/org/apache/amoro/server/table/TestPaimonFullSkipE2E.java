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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.OptimizingTaskResult;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionExecutor;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionInput;
import org.apache.amoro.formats.paimon.process.PaimonProcessFactory;
import org.apache.amoro.optimizing.TableRuntimeOptimizingState;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.catalog.CatalogManager;
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
import org.apache.amoro.server.resource.OptimizerThread;
import org.apache.amoro.server.scheduler.inline.TableRuntimeRefreshExecutor;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableRuntimeFactory.TableRuntimeCreator;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class TestPaimonFullSkipE2E extends PersistentBase {
  @BeforeAll
  static void database() {
    FullSkipTestDatabase.initialize();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void fullWithoutNewSnapshotAndStaleRetryCommitRealRows(boolean staleSkip, @TempDir Path warehouse)
      throws Exception {
    runFullScenario(staleSkip, false, 0, "none", warehouse);
  }

  @ParameterizedTest
  @CsvSource({
    "false,1,none",
    "true,1,none",
    "false,2,none",
    "true,2,none",
    "true,1,before",
    "true,1,after"
  })
  void snapshotReadFailureClosesAndRetriesFull(
      boolean changelog, int readFailures, String persistenceFailure, @TempDir Path warehouse)
      throws Exception {
    runFullScenario(true, changelog, readFailures, persistenceFailure, warehouse);
  }

  private void runFullScenario(
      boolean staleSkip,
      boolean changelog,
      int readFailures,
      String persistenceFailure,
      Path warehouse)
      throws Exception {
    ServerTableIdentifier id =
        ServerTableIdentifier.of("full-e2e", "db", "table", TableFormat.PAIMON);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    OptimizingQueue queue = null;
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      Identifier paimonId = Identifier.create("db", "table");
      Map<String, String> options = new HashMap<>();
      options.put("bucket", "1");
      options.put("changelog-producer", changelog ? "full-compaction" : "none");
      options.put("write-only", "true");
      options.put("self-optimizing.enabled", "true");
      options.put("self-optimizing.group", "full-e2e");
      options.put("self-optimizing.minor.trigger.file-count", "99");
      // Isolate FULL retry deadlines from the independent minimum planning interval.
      options.put("self-optimizing.min-plan-interval", "0");
      options.put("self-optimizing.full.trigger.interval", "600000");
      options.put("paimon-optimizer.primary-key.partition-idle-time", "0s");
      options.put("num-sorted-run.compaction-trigger", "999");
      Schema.Builder schema =
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .column("value", DataTypes.INT())
              .primaryKey("id");
      options.forEach(schema::option);
      catalog.createTable(paimonId, schema.build(), true);
      FileStoreTable nativeTable = (FileStoreTable) catalog.getTable(paimonId);
      write(nativeTable, 1, 10);
      write(nativeTable, 2, 20);
      AtomicBoolean failSnapshotRead = new AtomicBoolean(false);
      AtomicInteger snapshotReadFailures = new AtomicInteger();
      FileStoreTable observedTable =
          readFailures == 0
              ? nativeTable
              : snapshotReadFailureTable(nativeTable, failSnapshotRead, snapshotReadFailures);
      PaimonTable table =
          new PaimonTable(TableIdentifier.of("full-e2e", "db", "table"), observedTable);
      doAs(TableMetaMapper.class, m -> m.insertTable(id));
      TableRuntimeMeta meta = new TableRuntimeMeta();
      meta.setTableId(id.getId());
      meta.setGroupName("full-e2e");
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
      DefaultTableRuntime runtime = spy((DefaultTableRuntime) creator.create(store));
      runtime.refresh(table);
      store
          .begin()
          .updateState(
              DefaultTableRuntime.OPTIMIZING_STATE_KEY,
              state -> {
                state.setLastFullOptimizingTime(System.currentTimeMillis());
                return state;
              })
          .commit();
      assertFalse(runtime.evaluatePendingInputAndTransition(table, 20, true));
      assertEquals(runtime.getCurrentSnapshotId(), runtime.getLastOptimizedSnapshotId());
      // The first evaluation without demand advances the health baseline; refresh again so only the
      // FULL deadline can wake scheduling.
      refresh(table, runtime);
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
      assertFalse(runtime.shouldEvaluateCurrentAnalysis());
      TableAnalysisKey unchangedKey = runtime.getCurrentAnalysisKey().get();
      // Advance only the persisted FULL clock, leaving both snapshot and health keys unchanged.
      store
          .begin()
          .updateState(
              DefaultTableRuntime.OPTIMIZING_STATE_KEY,
              state -> {
                state.setLastFullOptimizingTime(0L);
                return state;
              })
          .commit();
      long oldFull = runtime.getLastFullOptimizingTime();
      long oldOptimized = runtime.getLastOptimizedSnapshotId();
      long oldChange = runtime.getLastOptimizedChangeSnapshotId();
      refresh(table, runtime);
      assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
      assertEquals(unchangedKey, runtime.getCurrentAnalysisKey().get());
      assertEquals(runtime.getCurrentSnapshotId(), runtime.getLastOptimizedSnapshotId());
      assertEquals(
          runtime.getCurrentChangeSnapshotId(), runtime.getLastOptimizedChangeSnapshotId());
      queue = queue(table, runtime, factory, executor);
      executeOne(queue, runtime);
      OptimizingProcess first = runtime.getOptimizingProcess();
      long firstId = first.getProcessId();
      if (!staleSkip) {
        List<String> expected = rows(nativeTable);
        first.commit();
        assertEquals(
            ProcessStatus.SUCCESS,
            getAs(TableProcessMapper.class, m -> m.getProcessMeta(firstId)).getStatus());
        assertEquals(expected, rows(nativeTable));
        assertTrue(runtime.getLastFullOptimizingTime() > oldFull);
        assertEquals(0L, runtime.getFullRetryNotBefore());
        return;
      }
      if (readFailures == 0) {
        write(nativeTable, 1, 99);
      }
      List<String> visible = rows(nativeTable);
      long snapshot = nativeTable.latestSnapshot().get().id();
      AtomicBoolean failPersistence = new AtomicBoolean(!"none".equals(persistenceFailure));
      doAnswer(
              call -> {
                if ("before".equals(persistenceFailure) && failPersistence.getAndSet(false)) {
                  throw new PersistenceException("injected before FULL completion persistence");
                }
                Object result = call.callRealMethod();
                if ("after".equals(persistenceFailure) && failPersistence.getAndSet(false)) {
                  throw new PersistenceException("injected lost FULL completion response");
                }
                return result;
              })
          .when(runtime)
          .completeSkippedFull(anyLong(), any(), any());
      failSnapshotRead.set(readFailures > 0);
      try {
        if ("none".equals(persistenceFailure)) {
          first.commit();
        } else {
          assertThrows(PersistenceException.class, first::commit);
        }
      } finally {
        failSnapshotRead.set(false);
      }
      if (readFailures > 0) {
        assertTrue(snapshotReadFailures.get() > 0, "snapshot read fault must actually fire");
      }
      if (!"none".equals(persistenceFailure)) {
        queue.retryPendingRecoveries(Long.MAX_VALUE);
      }
      assertEquals(snapshot, nativeTable.latestSnapshot().get().id());
      assertEquals(
          "0",
          getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates).stream()
              .filter(
                  s ->
                      s.getTableId() == id.getId()
                          && s.getStateKey().equals(OptimizingCommitGuard.STATE_KEY.getKey()))
              .findFirst()
              .get()
              .getStateValue());
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
      assertEquals(visible, rows(nativeTable));
      assertEquals(oldFull, runtime.getLastFullOptimizingTime());
      assertEquals(oldOptimized, runtime.getLastOptimizedSnapshotId());
      assertEquals(oldChange, runtime.getLastOptimizedChangeSnapshotId());
      assertEquals(0, runtime.getProcessId());
      assertTrue(
          FullCompactionCompletion.isSkipped(
              getAs(TableProcessMapper.class, m -> m.getProcessMeta(firstId))));
      long deadline = runtime.getFullRetryNotBefore();
      assertTrue(deadline > System.currentTimeMillis());
      queue.dispose();
      queue = null;

      DefaultTableRuntimeStore restored =
          new DefaultTableRuntimeStore(
              id,
              getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId())),
              creator.requiredStateKeys(),
              getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates).stream()
                  .filter(s -> s.getTableId() == id.getId())
                  .collect(Collectors.toList()));
      runtime = (DefaultTableRuntime) creator.create(restored);
      assertEquals(deadline, runtime.getFullRetryNotBefore());
      assertFalse(factory.createPlanner(runtime, table, 1, 64L * 1024 * 1024).isNecessary());
      runtime.refresh(table);
      assertFalse(runtime.evaluatePendingInputAndTransition(table, 20, false));
      assertEquals(runtime.getCurrentSnapshotId(), runtime.getLastOptimizedSnapshotId());
      assertEquals(
          runtime.getCurrentChangeSnapshotId(), runtime.getLastOptimizedChangeSnapshotId());
      refresh(table, runtime);
      assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
      assertFalse(runtime.shouldEvaluateCurrentAnalysis());
      // Advance only the persisted deadline for a deterministic clock boundary; keep FULL time.
      restored
          .begin()
          .updateState(
              DefaultTableRuntime.OPTIMIZING_STATE_KEY,
              state -> {
                state.setFullRetryNotBefore(1);
                return state;
              })
          .commit();
      refresh(table, runtime);
      assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
      queue = queue(table, runtime, factory, executor);
      executeOne(queue, runtime);
      OptimizingProcess second = runtime.getOptimizingProcess();
      long secondId = second.getProcessId();
      if (readFailures > 1) {
        int previousFaults = snapshotReadFailures.get();
        failSnapshotRead.set(true);
        try {
          second.commit();
        } finally {
          failSnapshotRead.set(false);
        }
        assertTrue(snapshotReadFailures.get() > previousFaults);
        assertEquals(snapshot, nativeTable.latestSnapshot().get().id());
        assertEquals(oldFull, runtime.getLastFullOptimizingTime());
        assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
        assertEquals(0L, runtime.getProcessId());
        assertTrue(runtime.getFullRetryNotBefore() > System.currentTimeMillis());
        assertFalse(factory.createPlanner(runtime, table, 1, 64L * 1024 * 1024).isNecessary());
        restored
            .begin()
            .updateState(
                DefaultTableRuntime.OPTIMIZING_STATE_KEY,
                current -> {
                  current.setFullRetryNotBefore(1);
                  return current;
                })
            .commit();
        refresh(table, runtime);
        assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
        executeOne(queue, runtime);
        second = runtime.getOptimizingProcess();
      }
      long successfulId = second.getProcessId();
      second.commit();
      assertEquals(
          ProcessStatus.SUCCESS,
          getAs(TableProcessMapper.class, m -> m.getProcessMeta(successfulId)).getStatus());
      assertEquals(snapshot + 1, nativeTable.latestSnapshot().get().id());
      assertEquals(successfulId, nativeTable.latestSnapshot().get().commitIdentifier());
      assertEquals(visible, rows(nativeTable));
      if (changelog) {
        ReadBuilder read = nativeTable.newReadBuilder();
        List<String> changes = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
            read.newRead()
                .createReader(
                    nativeTable
                        .newSnapshotReader()
                        .withSnapshot(snapshot + 1)
                        .withMode(ScanMode.CHANGELOG)
                        .read())) {
          reader.forEachRemaining(
              row ->
                  changes.add(
                      row.getRowKind().shortString() + ":" + row.getInt(0) + ":" + row.getInt(1)));
        }
        Collections.sort(changes);
        assertEquals(Arrays.asList("+I:1:10", "+I:2:20"), changes);
      }
      TableRuntimeOptimizingState state =
          restored.getState(DefaultTableRuntime.OPTIMIZING_STATE_KEY);
      assertEquals(0, state.getFullStaleSkipCount());
      assertEquals(0, state.getFullRetryNotBefore());
      assertEquals(readFailures > 1 ? secondId : firstId, state.getLastFullStaleProcessId());
      assertTrue(state.getLastFullOptimizingTime() > oldFull);
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

  private static FileStoreTable snapshotReadFailureTable(
      FileStoreTable table, AtomicBoolean fail, AtomicInteger failures) {
    FileIO delegate = table.fileIO();
    FileIO faulty =
        (FileIO)
            Proxy.newProxyInstance(
                FileIO.class.getClassLoader(),
                new Class<?>[] {FileIO.class},
                (InvocationHandler & Serializable)
                    (proxy, method, args) -> {
                      if (fail.get()
                          && Arrays.asList(
                                  "exists",
                                  "listStatus",
                                  "getFileStatus",
                                  "newInputStream",
                                  "readFileUtf8")
                              .contains(method.getName())
                          && args != null
                          && args.length > 0
                          && args[0] instanceof org.apache.paimon.fs.Path
                          && (args[0].toString().endsWith("/snapshot")
                              || args[0].toString().contains("/snapshot/"))) {
                        failures.incrementAndGet();
                        throw new IOException("injected pre-commit snapshot read failure");
                      }
                      try {
                        return method.invoke(delegate, args);
                      } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                      }
                    });
    return FileStoreTableFactory.create(faulty, table.location(), table.schema());
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
        new ResourceGroup.Builder("full-e2e", "local").build(),
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
            return "full-e2e";
          }
        };
    TaskRuntime<?> task = queue.pollTask(thread, 5000);
    assertNotNull(task);
    queue.ackTask(task.getTaskId(), thread);
    PaimonPrimaryKeyCompactionInput input =
        (PaimonPrimaryKeyCompactionInput) task.getTaskDescriptor().getInput();
    assertTrue(input.isFullCompaction());
    queue.completeTask(
        thread,
        new OptimizingTaskResult(task.getTaskId(), 1)
            .setTaskOutput(
                SerializationUtil.simpleSerialize(
                    new PaimonPrimaryKeyCompactionExecutor(input).execute())));
    assertEquals(OptimizingStatus.COMMITTING, runtime.getOptimizingStatus());
  }

  private static void write(FileStoreTable table, int id, int value) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      write.write(GenericRow.of(id, value));
      commit.commit(write.prepareCommit());
    }
  }

  private static List<String> rows(FileStoreTable table) throws Exception {
    ReadBuilder read = table.newReadBuilder();
    List<String> rows = new ArrayList<>();
    try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
      reader.forEachRemaining(row -> rows.add(row.getInt(0) + ":" + row.getInt(1)));
    }
    Collections.sort(rows);
    return rows;
  }
}
