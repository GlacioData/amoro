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

package org.apache.amoro.formats.paimon.optimizing.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionExecutorFactory;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionTask;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluation;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluator;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptions;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeySnapshotAnalysis;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TaskProperties;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@DisplayName("PaimonPrimaryKeyOptimizingPlanner")
class TestPaimonPrimaryKeyOptimizingPlanner {

  @Test
  @DisplayName("matching HASH analysis is consumed without fallback snapshot scan")
  void matchingHashAnalysisAvoidsFallbackScan(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "2");
    Identifier id = createPrimaryKeyTable(catalog, "t_hash_analysis_reuse", options);
    writeCommits(catalog.getTable(id), 2);
    FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(id);
    PaimonTable paimonTable = wrap(fileStoreTable, id.getObjectName());
    OptimizingConfig config = defaultConfig();
    OptimizationContext runtimeContext = runtimeContext(config, 0L, 0L, 11L);
    PaimonHealthEvaluationContext healthContext =
        PaimonHealthEvaluationContext.capture(
            fileStoreTable, paimonTable.id().toString(), runtimeContext);
    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            fileStoreTable,
            paimonTable.id().getTableName(),
            healthContext,
            config,
            0L,
            0L,
            null,
            System.currentTimeMillis());
    PaimonPrimaryKeySnapshotAnalysis analysis = evaluation.analysis().get();

    PaimonPrimaryKeyOptimizingPlanner planner =
        new PaimonPrimaryKeyOptimizingPlanner(
            paimonTable,
            100L,
            7L,
            4.0,
            64L * 1024 * 1024,
            config,
            0L,
            0L,
            0L,
            null,
            runtimeContext,
            analysis);

    assertTrue(planner.isNecessary());
    assertEquals(0, planner.fallbackScanCount());
    assertTrue(planner.tableAnalysis().isPresent());
    assertTrue(planner.tableAnalysis().get() == analysis);
  }

  @Test
  @DisplayName("HASH planner recaptures a newer snapshot and falls back exactly once")
  void newerSnapshotBetweenCreationAndConsumptionFallsBackOnce(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "2");
    Identifier id = createPrimaryKeyTable(catalog, "t_hash_analysis_stale", options);
    Table table = catalog.getTable(id);
    writeCommits(table, 2);
    FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(id);
    PaimonTable paimonTable = wrap(fileStoreTable, id.getObjectName());
    OptimizingConfig config = defaultConfig();
    OptimizationContext runtimeContext = runtimeContext(config, 0L, 0L, 11L);
    PaimonHealthEvaluationContext initialContext =
        PaimonHealthEvaluationContext.capture(
            fileStoreTable, paimonTable.id().toString(), runtimeContext);
    PaimonPrimaryKeySnapshotAnalysis staleAnalysis =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                fileStoreTable,
                paimonTable.id().getTableName(),
                initialContext,
                config,
                0L,
                0L,
                null,
                System.currentTimeMillis())
            .analysis()
            .get();
    PaimonPrimaryKeyOptimizingPlanner planner =
        new PaimonPrimaryKeyOptimizingPlanner(
            paimonTable,
            100L,
            7L,
            4.0,
            64L * 1024 * 1024,
            config,
            0L,
            0L,
            0L,
            null,
            runtimeContext,
            staleAnalysis);

    writeCommits(table, 1);
    long currentSnapshotId = fileStoreTable.snapshotManager().latestSnapshot().id();

    assertTrue(planner.isNecessary());
    assertEquals(1, planner.fallbackScanCount());
    assertTrue(planner.tableAnalysis().isPresent());
    assertFalse(planner.tableAnalysis().get() == staleAnalysis);
    assertEquals(currentSnapshotId, planner.tableAnalysis().get().key().getSnapshotId());
  }

  @Test
  @DisplayName("HASH planner without restart-supplied facts scans once and exposes current key")
  void missingRestartAnalysisFallsBackOnce(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "2");
    Identifier id = createPrimaryKeyTable(catalog, "t_hash_analysis_missing", options);
    writeCommits(catalog.getTable(id), 2);
    FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(id);
    PaimonTable paimonTable = wrap(fileStoreTable, id.getObjectName());
    OptimizingConfig config = defaultConfig();
    OptimizationContext runtimeContext = runtimeContext(config, 0L, 0L, 15L);
    PaimonPrimaryKeyOptimizingPlanner planner =
        new PaimonPrimaryKeyOptimizingPlanner(
            paimonTable,
            100L,
            7L,
            4.0,
            64L * 1024 * 1024,
            config,
            0L,
            0L,
            0L,
            null,
            runtimeContext,
            null);

    assertTrue(planner.isNecessary());
    assertEquals(1, planner.fallbackScanCount());
    assertEquals(
        PaimonHealthEvaluationContext.capture(
                fileStoreTable, paimonTable.id().toString(), runtimeContext)
            .key(),
        planner.tableAnalysis().get().key());
  }

  @ParameterizedTest(name = "mismatched {0} rejects HASH analysis")
  @EnumSource(KeyField.class)
  @DisplayName("every TableAnalysisKey field participates in HASH reuse eligibility")
  void everyHashAnalysisKeyFieldMustMatch(KeyField keyField, @TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "2");
    String tableName = "t_hash_key_" + keyField.name().toLowerCase(java.util.Locale.ROOT);
    Identifier id = createPrimaryKeyTable(catalog, tableName, options);
    writeCommits(catalog.getTable(id), 2);
    FileStoreTable fileStoreTable = (FileStoreTable) catalog.getTable(id);
    PaimonTable paimonTable = wrap(fileStoreTable, tableName);
    PaimonHealthEvaluationContext currentContext =
        PaimonHealthEvaluationContext.capture(fileStoreTable, paimonTable.id().toString(), null);
    PaimonPrimaryKeySnapshotAnalysis supplied =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                fileStoreTable,
                paimonTable.id().getTableName(),
                currentContext,
                defaultConfig(),
                0L,
                0L,
                null,
                System.currentTimeMillis())
            .analysis()
            .get();
    replaceAnalysisKey(supplied, keyField.change(currentContext.key()));
    PaimonPrimaryKeyOptimizingPlanner planner =
        new PaimonPrimaryKeyOptimizingPlanner(
            paimonTable,
            100L,
            7L,
            4.0,
            64L * 1024 * 1024,
            defaultConfig(),
            0L,
            0L,
            0L,
            null,
            null,
            supplied);

    assertTrue(planner.isNecessary());
    assertEquals(1, planner.fallbackScanCount());
    assertEquals(currentContext.key(), planner.tableAnalysis().get().key());
  }

  @Test
  @DisplayName("real KEY_DYNAMIC primary-key table is never routed to HASH planner")
  void realKeyDynamicPrimaryKeyTableIsUnsupported(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "-1");
    Identifier id =
        createCrossPartitionPrimaryKeyTable(catalog, "t_key_dynamic_not_planned", options);
    Table raw = catalog.getTable(id);
    PaimonTable paimonTable = wrap(raw, id.getObjectName());

    assertTrue(raw instanceof PrimaryKeyFileStoreTable);
    assertEquals(BucketMode.KEY_DYNAMIC, ((FileStoreTable) raw).bucketMode());
    assertFalse(PaimonPrimaryKeyOptimizingPlanner.supports(paimonTable));
  }

  @Test
  @DisplayName("HASH_FIXED MINOR uses effective minor threshold")
  void hashFixedMinorUsesEffectiveMinorThreshold(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_fixed_minor", options);
    writeCommits(catalog.getTable(id), 3);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig().setMinorLeastFileCount(99),
                runtimeOptions("num-sorted-run.compaction-trigger", "3"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertTaskMetadata(result);
    for (PaimonPrimaryKeyCompactionTask task : result.getTasks()) {
      assertEquals(OptimizingType.MINOR, task.getInput().getOptimizingType());
      assertFalse(task.getInput().isFullCompaction());
      assertEquals(
          PaimonPrimaryKeyCompactionExecutorFactory.class.getName(),
          task.getProperties().get(TaskProperties.TASK_EXECUTOR_FACTORY_IMPL));
    }
  }

  @Test
  @DisplayName("explicit Paimon minor trigger can suppress lower Amoro minor file count")
  void explicitPaimonMinorTriggerSuppressesLowerAmoroMinorFileCount(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "3");
    Identifier id = createPrimaryKeyTable(catalog, "t_real_option_minor", options);
    writeCommits(catalog.getTable(id), 2);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, defaultConfig().setMinorLeastFileCount(2)).plan();

    assertTrue(result.getTasks().isEmpty());
  }

  @Test
  @DisplayName("HASH_FIXED MAJOR overrides MINOR")
  void hashFixedMajorOverridesMinor(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_fixed_major", options);
    writeCommits(catalog.getTable(id), 3);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions(
                    "num-sorted-run.compaction-trigger", "2", "num-sorted-run.stop-trigger", "2"))
            .plan();

    assertEquals(OptimizingType.MAJOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    for (PaimonPrimaryKeyCompactionTask task : result.getTasks()) {
      assertEquals(OptimizingType.MAJOR, task.getInput().getOptimizingType());
      assertFalse(task.getInput().isFullCompaction());
    }
  }

  @Test
  @DisplayName("R equal to explicit Paimon stop trigger remains MINOR")
  void sortedRunsEqualToExplicitStopTriggerRemainMinor(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_paimon_stop_major", options);
    writeCommits(catalog.getTable(id), 3);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions(
                    "num-sorted-run.compaction-trigger", "2", "num-sorted-run.stop-trigger", "3"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertFalse(result.getTasks().get(0).getInput().isFullCompaction());
  }

  @Test
  @DisplayName("R equal to default Paimon stop trigger remains MINOR")
  void sortedRunsEqualToDefaultStopTriggerRemainMinor(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_major_default_threshold", options);
    writeCommits(catalog.getTable(id), 5);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions("num-sorted-run.compaction-trigger", "2"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertFalse(result.getTasks().get(0).getInput().isFullCompaction());
  }

  @Test
  @DisplayName("HASH_DYNAMIC uses the same planner logic")
  void hashDynamicUsesSamePlannerLogic(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "-1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_dynamic_minor", options);
    writeCommits(catalog.getTable(id), 2, 0);
    assertEquals(BucketMode.HASH_DYNAMIC, ((FileStoreTable) catalog.getTable(id)).bucketMode());

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig().setMinorLeastFileCount(99),
                runtimeOptions("num-sorted-run.compaction-trigger", "2"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertFalse(result.getTasks().get(0).getInput().isFullCompaction());
  }

  @Test
  @DisplayName("single MAJOR plan caps all tasks by active bucket ratio")
  void singleMajorPlanCapsAllTasksByActiveBucketRatio(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "-1");
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_dynamic_major_cap", options);
    for (int bucket = 0; bucket < 10; bucket++) {
      writeBucketCommits(catalog.getTable(id), bucket, 2);
    }

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions(
                    "num-sorted-run.compaction-trigger", "1", "num-sorted-run.stop-trigger", "1"))
            .plan();

    assertEquals(OptimizingType.MAJOR, result.getOptimizingType());
    assertEquals(4, result.getTasks().size());
    Set<String> selectedBuckets = new HashSet<>();
    for (PaimonPrimaryKeyCompactionTask task : result.getTasks()) {
      assertEquals(1, task.getInput().getUnits().size());
      assertFalse(task.getInput().isFullCompaction());
      selectedBuckets.add(
          Arrays.toString(task.getInput().getUnits().get(0).getPartitionBytes())
              + ':'
              + task.getInput().getUnits().get(0).getBucket());
    }
    assertEquals(4, selectedBuckets.size());
  }

  @Test
  @DisplayName("non-empty filter returns empty plan")
  void nonEmptyFilterReturnsEmptyPlan(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_filter", options);
    writeCommits(catalog.getTable(id), 2);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, defaultConfig().setFilter("id = 1")).plan();

    assertTrue(result.getTasks().isEmpty());
  }

  @Test
  @DisplayName("partition filter returns empty plan")
  void partitionFilterReturnsEmptyPlan(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_partition_filter", options);
    writeCommits(catalog.getTable(id), 2);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions("num-sorted-run.compaction-trigger", "2"),
                0L,
                0L,
                org.mockito.Mockito.mock(Predicate.class))
            .plan();

    assertTrue(result.getTasks().isEmpty());
  }

  @Test
  @DisplayName("FULL without partition idle time returns empty plan")
  void fullWithoutPartitionIdleTimeReturnsEmptyPlan(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    Identifier id = createPrimaryKeyTable(catalog, "t_full_no_idle", options);
    writeCommits(catalog.getTable(id), 1);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, defaultConfig().setMinorLeastFileCount(99).setFullTriggerInterval(1))
            .plan();

    assertTrue(result.getTasks().isEmpty());
  }

  @Test
  @DisplayName("FULL with PT0S idle time plans idle buckets")
  void fullWithZeroIdleTimePlansIdleBuckets(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "PT0S");
    Identifier id = createPrimaryKeyTable(catalog, "t_full_idle", options);
    writeCommits(catalog.getTable(id), 1);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, defaultConfig().setMinorLeastFileCount(99).setFullTriggerInterval(1))
            .plan();

    assertEquals(OptimizingType.FULL, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertTaskMetadata(result);
    for (PaimonPrimaryKeyCompactionTask task : result.getTasks()) {
      assertEquals(OptimizingType.FULL, task.getInput().getOptimizingType());
      assertTrue(task.getInput().isFullCompaction());
    }
  }

  @Test
  @DisplayName("FULL with no cold buckets returns empty plan")
  void fullWithNoColdBucketsReturnsEmptyPlan(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "PT999999H");
    Identifier id = createPrimaryKeyTable(catalog, "t_full_no_cold_bucket", options);
    writeCommits(catalog.getTable(id), 1);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, defaultConfig().setMinorLeastFileCount(99).setFullTriggerInterval(1))
            .plan();

    assertTrue(result.getTasks().isEmpty());
  }

  @Test
  @DisplayName("FULL is not planned when MINOR candidates exist")
  void fullIsNotPlannedWhenMinorCandidatesExist(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "PT0S");
    Identifier id = createPrimaryKeyTable(catalog, "t_full_blocked_by_minor", options);
    writeCommits(catalog.getTable(id), 2);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig().setFullTriggerInterval(1),
                runtimeOptions("num-sorted-run.compaction-trigger", "2"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertFalse(result.getTasks().get(0).getInput().isFullCompaction());
  }

  @Test
  @DisplayName("primary-key planner keeps one unit per task")
  void primaryKeyPlannerPacksOneUnitPerTask(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPartitionedPrimaryKeyTable(catalog, "t_pack_default_one", options);
    writePartitionCommits(catalog.getTable(id), "p1", 2);
    writePartitionCommits(catalog.getTable(id), "p2", 2);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions("num-sorted-run.compaction-trigger", "2"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertTrue(result.getTasks().size() >= 2);
    for (PaimonPrimaryKeyCompactionTask task : result.getTasks()) {
      assertEquals(1, task.getInput().getUnits().size());
    }
  }

  @Test
  @DisplayName("removed max-buckets-per-task option does not change task packing")
  void removedMaxBucketsPerTaskOptionDoesNotChangePacking(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    options.put("paimon-optimizer.primary-key.max-buckets-per-task", "2");
    Identifier id = createPartitionedPrimaryKeyTable(catalog, "t_pack_removed_option", options);
    writePartitionCommits(catalog.getTable(id), "p1", 2);
    writePartitionCommits(catalog.getTable(id), "p2", 2);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions("num-sorted-run.compaction-trigger", "2"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertTrue(result.getTasks().size() >= 2);
    for (PaimonPrimaryKeyCompactionTask task : result.getTasks()) {
      assertEquals(1, task.getInput().getUnits().size());
    }
  }

  @Test
  @DisplayName("partition idle time does not filter MINOR candidates")
  void partitionIdleTimeDoesNotFilterMinorCandidates(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "PT999999H");
    Identifier id = createPartitionedPrimaryKeyTable(catalog, "t_idle_minor", options);
    writePartitionCommits(catalog.getTable(id), "p1", 2);
    writePartitionCommits(catalog.getTable(id), "p2", 2);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions("num-sorted-run.compaction-trigger", "2"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertFalse(result.getTasks().get(0).getInput().isFullCompaction());
  }

  @Test
  @DisplayName("partition idle time does not filter MAJOR candidates")
  void partitionIdleTimeDoesNotFilterMajorCandidates(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "PT999999H");
    Identifier id = createPartitionedPrimaryKeyTable(catalog, "t_idle_major", options);
    writePartitionCommits(catalog.getTable(id), "p1", 3);
    writePartitionCommits(catalog.getTable(id), "p2", 3);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions(
                    "num-sorted-run.compaction-trigger", "2", "num-sorted-run.stop-trigger", "2"))
            .plan();

    assertEquals(OptimizingType.MAJOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertFalse(result.getTasks().get(0).getInput().isFullCompaction());
  }

  @Test
  @DisplayName("minor interval throttles MINOR planning")
  void minorIntervalThrottlesMinorPlanning(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_minor_interval", options);
    writeCommits(catalog.getTable(id), 2);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig().setMinorLeastInterval(Integer.MAX_VALUE).setFullTriggerInterval(1),
                runtimeOptions("num-sorted-run.compaction-trigger", "2"),
                System.currentTimeMillis(),
                0L)
            .plan();

    assertTrue(result.getTasks().isEmpty());
  }

  @Test
  @DisplayName("MAJOR ignores minor interval under high pressure")
  void majorIgnoresMinorIntervalWhenHighPressure(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_major_interval", options);
    writeCommits(catalog.getTable(id), 3);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig().setMinorLeastInterval(Integer.MAX_VALUE),
                runtimeOptions(
                    "num-sorted-run.compaction-trigger", "2", "num-sorted-run.stop-trigger", "2"),
                System.currentTimeMillis(),
                0L)
            .plan();

    assertEquals(OptimizingType.MAJOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
  }

  @Test
  @DisplayName("deprecated private major threshold is ignored")
  void deprecatedPrivateMajorThresholdIsIgnored(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_bad_major_threshold", options);
    writeCommits(catalog.getTable(id), 3);

    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                defaultConfig(),
                runtimeOptions(
                    "num-sorted-run.compaction-trigger",
                    "3",
                    PaimonPrimaryKeyOptions.MAJOR_FILE_COUNT_THRESHOLD,
                    "2"))
            .plan();

    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    assertFalse(result.getTasks().get(0).getInput().isFullCompaction());
  }

  @Test
  @DisplayName("HASH table without primary key is rejected before planning")
  void nonPrimaryKeyHashTableIsRejectedBeforePlanning() {
    FileStoreTable table = mock(FileStoreTable.class);
    Map<String, String> options = primaryKeyOptions();
    when(table.options()).thenReturn(options);
    when(table.primaryKeys()).thenReturn(Collections.emptyList());
    when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(1L);
    when(table.latestSnapshot()).thenReturn(Optional.of(snapshot));
    PaimonTable paimonTable = wrap(table, "t_hash_without_pk");

    PaimonPrimaryKeyOptimizingPlanner planner =
        new PaimonPrimaryKeyOptimizingPlanner(
            paimonTable, 100L, 7L, 4.0, 64L * 1024 * 1024, defaultConfig(), 0L, 0L, 0L, null);

    assertFalse(PaimonPrimaryKeyOptimizingPlanner.supports(paimonTable));
    assertFalse(planner.isNecessary());
    assertTrue(planner.plan().getTasks().isEmpty());
  }

  private static PaimonPrimaryKeyOptimizingPlanner planner(
      Catalog catalog, Identifier id, OptimizingConfig config) throws Exception {
    return planner(catalog, id, config, Collections.emptyMap());
  }

  private static PaimonPrimaryKeyOptimizingPlanner planner(
      Catalog catalog, Identifier id, OptimizingConfig config, Map<String, String> runtimeOptions)
      throws Exception {
    return planner(catalog, id, config, runtimeOptions, 0L, 0L);
  }

  private static PaimonPrimaryKeyOptimizingPlanner planner(
      Catalog catalog,
      Identifier id,
      OptimizingConfig config,
      Map<String, String> runtimeOptions,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime)
      throws Exception {
    return planner(
        catalog, id, config, runtimeOptions, lastMinorOptimizingTime, lastFullOptimizingTime, null);
  }

  private static PaimonPrimaryKeyOptimizingPlanner planner(
      Catalog catalog,
      Identifier id,
      OptimizingConfig config,
      Map<String, String> runtimeOptions,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime,
      Predicate partitionFilter)
      throws Exception {
    PaimonTable table = wrap(catalog.getTable(id).copy(runtimeOptions), id.getObjectName());
    return new PaimonPrimaryKeyOptimizingPlanner(
        table,
        100L,
        7L,
        4.0,
        64L * 1024 * 1024,
        config,
        lastMinorOptimizingTime,
        0L,
        lastFullOptimizingTime,
        partitionFilter);
  }

  private static Catalog fsCatalog(Path warehouse) {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    return PaimonCatalogFactory.paimonCatalog(props, new Configuration());
  }

  private static Identifier createPrimaryKeyTable(
      Catalog catalog, String tableName, Map<String, String> options) throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .primaryKey("id");
    options.forEach(builder::option);
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return id;
  }

  private static Identifier createPartitionedPrimaryKeyTable(
      Catalog catalog, String tableName, Map<String, String> options) throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("dt", DataTypes.STRING())
            .partitionKeys("dt")
            .primaryKey("dt", "id");
    options.forEach(builder::option);
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return id;
  }

  private static Identifier createCrossPartitionPrimaryKeyTable(
      Catalog catalog, String tableName, Map<String, String> options) throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("pt", DataTypes.STRING())
            .column("name", DataTypes.STRING())
            .partitionKeys("pt")
            .primaryKey("id");
    options.forEach(builder::option);
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return id;
  }

  private static void writeCommits(Table table, int count) throws Exception {
    writeCommits(table, count, null);
  }

  private static void writeCommits(Table table, int count, Integer bucket) throws Exception {
    for (int i = 0; i < count; i++) {
      writeRecords(
          table,
          Collections.singletonList(GenericRow.of(i, BinaryString.fromString("name-" + i))),
          bucket);
    }
  }

  private static void writePartitionCommits(Table table, String partition, int count)
      throws Exception {
    for (int i = 0; i < count; i++) {
      writeRecords(
          table,
          Collections.singletonList(
              GenericRow.of(
                  i,
                  BinaryString.fromString(partition + "-" + i),
                  BinaryString.fromString(partition))),
          null);
    }
  }

  private static void writeBucketCommits(Table table, int bucket, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      int id = bucket * 100 + i;
      writeRecords(
          table,
          Collections.singletonList(GenericRow.of(id, BinaryString.fromString("name-" + id))),
          bucket);
    }
  }

  private static void writeRecords(Table table, List<GenericRow> rowsInOneCommit) throws Exception {
    writeRecords(table, rowsInOneCommit, null);
  }

  private static void writeRecords(Table table, List<GenericRow> rowsInOneCommit, Integer bucket)
      throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite()) {
      for (GenericRow row : rowsInOneCommit) {
        if (bucket == null) {
          write.write(row);
        } else {
          write.write(row, bucket);
        }
      }
      List<CommitMessage> messages = write.prepareCommit();
      try (BatchTableCommit commit = builder.newCommit()) {
        commit.commit(messages);
      }
    }
  }

  private static Map<String, String> primaryKeyOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    return options;
  }

  private static Map<String, String> runtimeOptions(String... keyValues) {
    Map<String, String> options = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      options.put(keyValues[i], keyValues[i + 1]);
    }
    return options;
  }

  private static PaimonTable wrap(Table table, String name) {
    return new PaimonTable(TableIdentifier.of("test_catalog", "db1", name), table);
  }

  private static OptimizingConfig defaultConfig() {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastFileCount(2)
        .setMinorLeastInterval(0)
        .setFullTriggerInterval(-1)
        .setFullRewriteAllFiles(false)
        .setMaxTaskSize(64L * 1024 * 1024);
  }

  private static OptimizationContext runtimeContext(
      OptimizingConfig config, long lastMinor, long lastFull, long lastOptimizedSnapshotId) {
    OptimizationContext context = mock(OptimizationContext.class);
    when(context.getOptimizingConfig()).thenReturn(config);
    when(context.getLastMinorOptimizingTime()).thenReturn(lastMinor);
    when(context.getLastFullOptimizingTime()).thenReturn(lastFull);
    when(context.getLastOptimizedSnapshotId()).thenReturn(lastOptimizedSnapshotId);
    return context;
  }

  private static void replaceAnalysisKey(
      PaimonPrimaryKeySnapshotAnalysis analysis, TableAnalysisKey replacement) throws Exception {
    Field field = PaimonPrimaryKeySnapshotAnalysis.class.getDeclaredField("key");
    field.setAccessible(true);
    field.set(analysis, replacement);
  }

  private enum KeyField {
    TABLE_ID,
    TABLE_FORMAT,
    SNAPSHOT,
    SCHEMA,
    FINGERPRINT,
    FORMULA,
    BASELINE_ID,
    BASELINE_TIME;

    private TableAnalysisKey change(TableAnalysisKey key) {
      return new TableAnalysisKey(
          this == TABLE_ID ? key.getTableId() + "-other" : key.getTableId(),
          this == TABLE_FORMAT ? org.apache.amoro.TableFormat.ICEBERG : key.getTableFormat(),
          this == SNAPSHOT ? key.getSnapshotId() + 1L : key.getSnapshotId(),
          key.getChangeSnapshotId(),
          this == SCHEMA ? key.getSchemaId() + 1L : key.getSchemaId(),
          this == FINGERPRINT
              ? key.getScoringConfigFingerprint() + "-other"
              : key.getScoringConfigFingerprint(),
          this == FORMULA ? key.getFormulaVersion() + "-other" : key.getFormulaVersion(),
          this == BASELINE_ID
              ? key.getSuccessfulOptimizationBaselineId() + 1L
              : key.getSuccessfulOptimizationBaselineId(),
          this == BASELINE_TIME
              ? key.getSuccessfulOptimizationBaselineTimeMillis() + 1L
              : key.getSuccessfulOptimizationBaselineTimeMillis());
    }
  }

  private static void assertTaskMetadata(
      OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result) {
    assertEquals(7L, result.getProcessId());
    assertTrue(result.getPlanTime() > 0);
    assertTrue(result.getTargetSnapshotId() > 0);
    assertEquals(-1L, result.getTargetChangeSnapshotId());
    for (PaimonPrimaryKeyCompactionTask task : result.getTasks()) {
      assertNotNull(task.getInput().getCommitUser());
      assertEquals(7L, task.getInput().getCommitIdentifier());
      assertEquals(result.getTargetSnapshotId(), task.getInput().getTargetSnapshotId());
    }
  }
}
