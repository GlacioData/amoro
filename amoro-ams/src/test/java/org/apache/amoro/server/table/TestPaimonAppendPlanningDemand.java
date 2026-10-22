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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonRuntimeSupport;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.formats.paimon.process.PaimonProcessFactory;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.scheduler.inline.TableRuntimeRefreshExecutor;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableRuntimeFactory;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;

class TestPaimonAppendPlanningDemand extends PersistentBase {
  private ServerTableIdentifier tableId;
  @TempDir Path warehouse;
  private Catalog catalog;
  private CountingTable table;
  private DefaultTableRuntime runtime;
  private DefaultTableRuntimeStore store;
  private TableRuntimeRefreshExecutor refresh;

  @BeforeAll
  static void initializeDatabase() {
    FullSkipTestDatabase.initialize();
  }

  @AfterEach
  void cleanup() throws Exception {
    if (refresh != null) {
      refresh.gracefulShutdown();
    }
    if (catalog != null) {
      catalog.close();
    }
    if (tableId != null) {
      doAs(TableRuntimeMapper.class, mapper -> mapper.removeAllTableStates(tableId.getId()));
      doAs(TableRuntimeMapper.class, mapper -> mapper.deleteRuntime(tableId.getId()));
      doAs(TableMetaMapper.class, mapper -> mapper.deleteTableIdById(tableId.getId()));
    }
  }

  @ParameterizedTest
  @CsvSource({"1,MINOR", "1,FULL", "-1,MINOR", "-1,FULL"})
  void dueDemandObservedDuringPlanningSurvivesOldEmptyPlan(int buckets, OptimizingType type)
      throws Exception {
    initialize(buckets, type, 2);
    refresh.execute(runtime);
    TableAnalysisKey before = runtime.getCurrentAnalysisKey().get();
    TableOptimizingPlanner oldPlanner = startPlanning();
    assertFalse(oldPlanner.isNecessary());

    // Pause between the old planner's empty decision and its completion callback. Moving the
    // persisted interval baseline gives deterministic before/due states without sleeping.
    setOptimizationTimes(1L);
    refresh.execute(runtime);
    TableAnalysisKey due = runtime.getCurrentAnalysisKey().get();
    assertNotEquals(before, due);
    assertEquals(OptimizingStatus.PLANNING, runtime.getOptimizingStatus());
    RuntimeHealthSnapshot health = runtime.getRuntimeHealthSnapshot().get();
    assertEquals(due.encoded(), health.getHealthDetails().getEvaluationKey());
    assertTrue(health.getCurrentHealthScore() >= 0);
    assertFalse(runtime.shouldEvaluateCurrentPlanningAnalysis());
    refresh.execute(runtime);
    assertEquals(2, table.evaluations, "unchanged PLANNING refresh must reuse health");

    finishEmpty(oldPlanner);
    assertFalse(pending().isSameSnapshotPlanningPending());
    refresh.execute(runtime);
    assertEquals(due, runtime.getCurrentAnalysisKey().get());
    assertEquals(runtime.getCurrentSnapshotId(), runtime.getLastOptimizedSnapshotId());
    assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
    assertTrue(pending().isSameSnapshotPlanningPending());
    assertPersistedDemand(OptimizingStatus.PENDING, true);
    assertTrue(pending().canPlanWithoutSnapshotChange(runtime, System.currentTimeMillis()));
    assertEquals(3, table.evaluations, "deferred demand needs exactly one new evaluation");
    refresh.execute(runtime);
    assertEquals(3, table.evaluations);

    TableOptimizingPlanner freshPlanner = startPlanning();
    assertTrue(freshPlanner.isNecessary());
    OptimizingPlanResult<?> plan = freshPlanner.plan();
    assertEquals(type, plan.getOptimizingType());
    assertFalse(plan.getTasks().isEmpty());
  }

  @ParameterizedTest
  @CsvSource({"1,MINOR", "1,FULL", "-1,MINOR", "-1,FULL"})
  void unchangedBeforeDueAndDueEmptyPlansDoNotRepeatScans(int buckets, OptimizingType type)
      throws Exception {
    initialize(buckets, type, 1);
    refresh.execute(runtime);
    finishEmpty(startPlanning());
    refresh.execute(runtime);
    refresh.execute(runtime);
    assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    assertEquals(1, table.evaluations);

    setOptimizationTimes(1L);
    refresh.execute(runtime);
    assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
    assertTrue(pending().isSameSnapshotPlanningPending());
    finishEmpty(startPlanning());
    refresh.execute(runtime);
    refresh.execute(runtime);
    assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    assertFalse(pending().isSameSnapshotPlanningPending());
    assertFalse(runtime.shouldEvaluateCurrentPlanningAnalysis());
    assertEquals(2, table.evaluations, "unchanged due empty plans must not poll manifests");
    assertPersistedDemand(OptimizingStatus.IDLE, false);
  }

  private void assertPersistedDemand(OptimizingStatus status, boolean pending) {
    TableRuntimeMeta meta =
        getAs(TableRuntimeMapper.class, mapper -> mapper.selectRuntime(tableId.getId()));
    assertEquals(status.getCode(), meta.getStatusCode());
    TableRuntimeState state =
        getAs(
            TableRuntimeMapper.class, mapper -> mapper.getState(tableId.getId(), "pending_input"));
    assertNotNull(state);
    PaimonPendingInput restored =
        new PaimonRuntimeSupport().pendingInputKey().deserialize(state.getStateValue());
    assertEquals(pending, restored.isSameSnapshotPlanningPending());
  }

  @Test
  void dataEvolutionNeverBecomesPendingAndDoesNotRepeatUnchangedScans() throws Exception {
    initialize(-1, OptimizingType.MINOR, 2, true);
    refresh.execute(runtime);
    assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    assertFalse(pending().isSameSnapshotPlanningPending());
    assertFalse(pending().canPlanWithoutSnapshotChange(runtime, System.currentTimeMillis()));
    assertPersistedDemand(OptimizingStatus.IDLE, false);
    assertTrue(runtime.getRuntimeHealthSnapshot().get().getCurrentHealthScore() >= 0);
    refresh.execute(runtime);
    refresh.execute(runtime);
    assertEquals(1, table.evaluations, "unsupported unchanged tables must not poll manifests");
  }

  private void initialize(int buckets, OptimizingType type, int batches) throws Exception {
    initialize(buckets, type, batches, false);
  }

  private void initialize(int buckets, OptimizingType type, int batches, boolean evolution)
      throws Exception {
    catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration());
    catalog.createDatabase("db", false);
    Identifier identifier = Identifier.create("db", "append_demand");
    Schema.Builder schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .option("bucket", String.valueOf(buckets))
            .option("write-only", "true")
            .option("target-file-size", "64 mb")
            .option("compaction.min.file-num", "100")
            .option("self-optimizing.enabled", "true")
            .option("self-optimizing.group", "default")
            .option(
                "self-optimizing.minor.trigger.interval", type == OptimizingType.MINOR ? "1" : "-1")
            .option(
                "self-optimizing.full.trigger.interval", type == OptimizingType.FULL ? "1" : "-1")
            .option("self-optimizing.full.rewrite-all-files", "false");
    if (buckets > 0) {
      schema.option("bucket-key", "id").option("bucket-append-ordered", "true");
    }
    if (evolution) {
      schema.option("row-tracking.enabled", "true").option("data-evolution.enabled", "true");
    }
    catalog.createTable(identifier, schema.build(), false);
    AppendOnlyFileStoreTable nativeTable = (AppendOnlyFileStoreTable) catalog.getTable(identifier);
    for (int batch = 0; batch < batches; batch++) {
      BatchWriteBuilder builder = nativeTable.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(batch));
        commit.commit(write.prepareCommit());
      }
    }
    table = new CountingTable(nativeTable);
    ServerTableIdentifier id =
        ServerTableIdentifier.of("test", "db", "append_demand", TableFormat.PAIMON);
    tableId = id;
    doAs(TableMetaMapper.class, mapper -> mapper.insertTable(id));
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("default");
    meta.setStatusCode(OptimizingStatus.IDLE.getCode());
    meta.setTableConfig(table.properties());
    doAs(TableRuntimeMapper.class, mapper -> mapper.insertRuntime(meta));
    DefaultTableRuntimeFactory factory = new DefaultTableRuntimeFactory();
    factory.withTableLoader(ignored -> table);
    TableRuntimeFactory.TableRuntimeCreator creator = factory.accept(id, table.properties()).get();
    store =
        new DefaultTableRuntimeStore(
            id, meta, creator.requiredStateKeys(), Collections.emptyList());
    runtime = (DefaultTableRuntime) creator.create(store);
    store.setTableRuntime(runtime);
    runtime.refresh(table);
    setOptimizationTimes(Long.MAX_VALUE);
    TableService service = mock(TableService.class);
    doReturn(table).when(service).loadTable(any());
    refresh = new TableRuntimeRefreshExecutor(service, 1, 60000, 100);
  }

  private void setOptimizationTimes(long time) {
    store
        .begin()
        .updateState(
            DefaultTableRuntime.OPTIMIZING_STATE_KEY,
            state -> {
              state.setLastOptimizedSnapshotId(state.getCurrentSnapshotId());
              state.setLastOptimizedChangeSnapshotId(state.getCurrentChangeSnapshotId());
              state.setLastMinorOptimizingTime(time);
              state.setLastFullOptimizingTime(time);
              return state;
            })
        .commit();
  }

  private TableOptimizingPlanner startPlanning() {
    runtime.beginPlanning();
    PaimonProcessFactory factory = new PaimonProcessFactory();
    factory.open(Collections.singletonMap("paimon-optimizer.enabled", "true"));
    return factory.createPlanner(runtime, table, 4, 64L * 1024 * 1024);
  }

  private void finishEmpty(TableOptimizingPlanner planner) {
    assertFalse(planner.isNecessary());
    planner
        .tableAnalysis()
        .ifPresent(analysis -> runtime.updateTableSummaryIfCurrent(table, analysis));
    runtime.completeEmptyProcess();
    runtime.setLastPlanTime(planner.plan().getPlanTime());
  }

  private PaimonPendingInput pending() {
    return (PaimonPendingInput) runtime.getPendingInput();
  }

  private static final class CountingTable extends PaimonTable {
    private int evaluations;

    private CountingTable(AppendOnlyFileStoreTable table) {
      super(TableIdentifier.of("test", "db", "append_demand"), table);
    }

    @Override
    public Optional<PendingInputResult> evaluatePendingInput(
        OptimizationContext context, int limit) {
      evaluations++;
      return super.evaluatePendingInput(context, limit);
    }
  }
}
