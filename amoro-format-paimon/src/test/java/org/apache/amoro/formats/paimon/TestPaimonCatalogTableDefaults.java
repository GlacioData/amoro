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

package org.apache.amoro.formats.paimon;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonPrimaryKeyOptimizingPlanner;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonBucketCompactionUnit;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionTask;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluation;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluator;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.table.health.TableHealthStructure;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

class TestPaimonCatalogTableDefaults {

  private static final String SCAN_PARALLELISM = CoreOptions.SCAN_MANIFEST_PARALLELISM.key();
  private static final String CATALOG_DEFAULT =
      Catalog.TABLE_DEFAULT_OPTION_PREFIX + SCAN_PARALLELISM;
  private static final Identifier TABLE_ID = Identifier.create("db", "existing_table");

  @TempDir private Path warehouse;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void appliesRuntimeDefaultWithoutChangingExistingTable(boolean primaryKey) throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, primaryKey, Collections.emptyMap());
      Map<String, String> originalOptions = new HashMap<>(original.options());
      PaimonTable wrapper =
          (PaimonTable)
              catalog(nativeCatalog, "4")
                  .loadTable(TABLE_ID.getDatabaseName(), TABLE_ID.getObjectName());
      FileStoreTable effective = (FileStoreTable) wrapper.originalTable();

      assertEquals(4, effective.store().newScan().parallelism());
      assertEquals("4", wrapper.properties().get(SCAN_PARALLELISM));
      assertNotSame(original, effective);
      assertEquals(original.schema().id(), effective.schema().id());
      assertEquals(original.schema().fields(), effective.schema().fields());
      assertEquals(original.primaryKeys(), effective.primaryKeys());
      assertEquals(original.partitionKeys(), effective.partitionKeys());
      assertEquals(original.location(), effective.location());
      assertSame(original.fileIO(), effective.fileIO());
      assertSame(original.catalogEnvironment(), effective.catalogEnvironment());
      assertSame(original.getManifestCache(), effective.getManifestCache());
      Map<String, String> expectedOptions = new HashMap<>(originalOptions);
      expectedOptions.put(SCAN_PARALLELISM, "4");
      assertEquals(expectedOptions, effective.options());
      assertEquals(originalOptions, original.options());
      nativeCatalog.invalidateTable(TABLE_ID);
      assertFalse(nativeCatalog.getTable(TABLE_ID).options().containsKey(SCAN_PARALLELISM));

      ByteBuffer serialized = SerializationUtil.simpleSerialize(wrapper);
      byte[] bytes = new byte[serialized.remaining()];
      serialized.get(bytes);
      PaimonTable restored = SerializationUtil.simpleDeserialize(bytes);
      assertEquals(4, ((FileStoreTable) restored.originalTable()).store().newScan().parallelism());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void explicitTableOptionWinsWithoutCopyingCachedTable(boolean primaryKey) throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original =
          createTable(nativeCatalog, primaryKey, Collections.singletonMap(SCAN_PARALLELISM, "8"));
      PaimonTable wrapper =
          (PaimonTable) catalogReturning(original, "4").loadTable("db", "existing_table");

      assertSame(original, wrapper.originalTable());
      assertEquals(8, ((FileStoreTable) wrapper.originalTable()).store().newScan().parallelism());
    }
  }

  @Test
  void absentDefaultKeepsOriginalTable() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, true, Collections.emptyMap());
      PaimonTable wrapper =
          (PaimonTable) catalogReturning(original, null).loadTable("db", "existing_table");

      assertSame(original, wrapper.originalTable());
      assertNull(original.store().newScan().parallelism());
    }
  }

  @Test
  void reloadingChangedAndRemovedDefaultsDoesNotPolluteCachedTable() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, true, Collections.emptyMap());
      for (String value : new String[] {"4", "2", null}) {
        Map<String, String> defaults = new HashMap<>();
        if (value != null) {
          defaults.put(CATALOG_DEFAULT, value);
          defaults.put("table-default.custom.runtime-option", value);
        }
        Catalog cachedCatalog = mock(Catalog.class);
        when(cachedCatalog.getTable(any())).thenReturn(original);
        FileStoreTable effective =
            (FileStoreTable)
                new PaimonCatalog(cachedCatalog, "test", defaults)
                    .loadTable("db", "existing_table")
                    .originalTable();
        assertEquals(
            value == null ? null : Integer.valueOf(value),
            effective.store().newScan().parallelism());
        assertEquals(value, effective.options().get("custom.runtime-option"));
        assertFalse(original.options().containsKey(SCAN_PARALLELISM));
        assertFalse(original.options().containsKey("custom.runtime-option"));
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void tagReloadUsesSameRuntimeDefault() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      createTable(nativeCatalog, true, Collections.emptyMap());
      Map<String, String> defaults = new HashMap<>();
      defaults.put(CATALOG_DEFAULT, "4");
      defaults.put("table-default.write-buffer-size", "32mb");
      PaimonTable wrapper =
          (PaimonTable)
              new PaimonCatalog(nativeCatalog, "test", defaults).loadTable("db", "existing_table");
      Field refreshField = PaimonTable.class.getDeclaredField("tagRefresh");
      refreshField.setAccessible(true);
      Supplier<Table> refresh = (Supplier<Table>) refreshField.get(wrapper);

      FileStoreTable reloaded = (FileStoreTable) refresh.get();
      assertEquals(4, reloaded.store().newScan().parallelism());
      assertEquals(32L * 1024 * 1024, reloaded.coreOptions().writeBufferSize());
      assertFalse(nativeCatalog.getTable(TABLE_ID).options().containsKey(SCAN_PARALLELISM));
      assertFalse(nativeCatalog.getTable(TABLE_ID).options().containsKey("write-buffer-size"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void appliesAllTableDefaultsToNativeTable(boolean primaryKey) throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, primaryKey, Collections.emptyMap());
      Map<String, String> defaults = new HashMap<>();
      defaults.put(CATALOG_DEFAULT, "4");
      defaults.put("table-default.write-buffer-size", "32mb");
      defaults.put("table-default.num-sorted-run.compaction-trigger", "7");
      defaults.put("table-default.self-optimizing.min-plan-interval", "18000000");
      defaults.put("table-default.custom.runtime-option", " value=kept ");
      defaults.put("table-default.bucket", "8");
      defaults.put("table-default.write-only", "false");
      defaults.put("table.scan.manifest.parallelism", "12");
      PaimonTable wrapper =
          (PaimonTable)
              new PaimonCatalog(nativeCatalog, "test", defaults).loadTable("db", "existing_table");
      FileStoreTable effective = (FileStoreTable) wrapper.originalTable();

      Map<String, String> expected = new HashMap<>(original.options());
      expected.put(SCAN_PARALLELISM, "4");
      expected.put("write-buffer-size", "32mb");
      expected.put("num-sorted-run.compaction-trigger", "7");
      expected.put("self-optimizing.min-plan-interval", "18000000");
      expected.put("custom.runtime-option", " value=kept ");
      assertEquals(expected, effective.options());
      assertEquals(4, effective.store().newScan().parallelism());
      assertEquals(32L * 1024 * 1024, effective.coreOptions().writeBufferSize());
      assertEquals(7, effective.coreOptions().numSortedRunCompactionTrigger());
      assertTrue(effective.coreOptions().writeOnly());
      assertEquals(original.options().get("bucket"), effective.options().get("bucket"));
      assertEquals("18000000", wrapper.properties().get("self-optimizing.min-plan-interval"));
      assertFalse(effective.options().containsKey("table-default.write-buffer-size"));
      assertFalse(original.options().containsKey("write-buffer-size"));
      assertSame(original.fileIO(), effective.fileIO());
      assertSame(original.catalogEnvironment(), effective.catalogEnvironment());
      assertSame(original.getManifestCache(), effective.getManifestCache());
      nativeCatalog.invalidateTable(TABLE_ID);
      assertFalse(nativeCatalog.getTable(TABLE_ID).options().containsKey("write-buffer-size"));

      ByteBuffer serialized = SerializationUtil.simpleSerialize(wrapper);
      byte[] bytes = new byte[serialized.remaining()];
      serialized.get(bytes);
      PaimonTable restored = SerializationUtil.simpleDeserialize(bytes);
      assertEquals(expected, restored.originalTable().options());
      assertEquals(
          32L * 1024 * 1024,
          ((FileStoreTable) restored.originalTable()).coreOptions().writeBufferSize());
    }
  }

  @Test
  void appliesDefaultsWithoutManifestParallelism() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, true, Collections.emptyMap());
      Map<String, String> defaults = new HashMap<>();
      defaults.put("table-default.target-file-size", "64mb");
      defaults.put("table-default.custom.runtime-option", "value");
      FileStoreTable effective =
          (FileStoreTable)
              new PaimonCatalog(nativeCatalog, "test", defaults)
                  .loadTable("db", "existing_table")
                  .originalTable();

      assertNotSame(original, effective);
      assertEquals(64L * 1024 * 1024, effective.coreOptions().targetFileSize(true));
      assertEquals("value", effective.options().get("custom.runtime-option"));
      assertNull(effective.store().newScan().parallelism());
    }
  }

  @Test
  void explicitOptionsWinForEveryDefaultWithoutCopyingCachedTable() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, true, Collections.emptyMap());
      Catalog cachedCatalog = mock(Catalog.class);
      when(cachedCatalog.getTable(any())).thenReturn(original);
      Map<String, String> defaults = new HashMap<>();
      defaults.put("table-default.bucket", "8");
      defaults.put("table-default.write-only", "false");
      PaimonTable wrapper =
          (PaimonTable)
              new PaimonCatalog(cachedCatalog, "test", defaults).loadTable("db", "existing_table");

      assertSame(original, wrapper.originalTable());
      assertEquals("1", wrapper.originalTable().options().get("bucket"));
      assertTrue(((FileStoreTable) wrapper.originalTable()).coreOptions().writeOnly());
    }
  }

  @Test
  void explicitManifestParallelismDoesNotBlockOtherDefaults() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      createTable(nativeCatalog, true, Collections.singletonMap(SCAN_PARALLELISM, "8"));
      Map<String, String> defaults = new HashMap<>();
      defaults.put(CATALOG_DEFAULT, "4");
      defaults.put("table-default.write-buffer-size", "32mb");
      FileStoreTable effective =
          (FileStoreTable)
              new PaimonCatalog(nativeCatalog, "test", defaults)
                  .loadTable("db", "existing_table")
                  .originalTable();

      assertEquals(8, effective.store().newScan().parallelism());
      assertEquals(32L * 1024 * 1024, effective.coreOptions().writeBufferSize());
    }
  }

  @Test
  void ignoresCatalogOptionsWithoutExactDefaultPrefix() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, true, Collections.emptyMap());
      Catalog cachedCatalog = mock(Catalog.class);
      when(cachedCatalog.getTable(any())).thenReturn(original);
      Map<String, String> options = new HashMap<>();
      options.put(SCAN_PARALLELISM, "4");
      options.put("table.scan.manifest.parallelism", "8");
      options.put("table-defaultish.custom-option", "value");
      options.put("custom.table-default.write-buffer-size", "32mb");
      PaimonTable wrapper =
          (PaimonTable)
              new PaimonCatalog(cachedCatalog, "test", options).loadTable("db", "existing_table");

      assertSame(original, wrapper.originalTable());
      assertNull(original.store().newScan().parallelism());
    }
  }

  @Test
  void rejectsDefaultWithEmptyTableOptionName() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new PaimonCatalog(
                    mock(Catalog.class),
                    "test",
                    Collections.singletonMap(Catalog.TABLE_DEFAULT_OPTION_PREFIX, "value")));
    assertTrue(failure.getMessage().contains(Catalog.TABLE_DEFAULT_OPTION_PREFIX));
  }

  @Test
  void preservesDefaultValueWhileUsingNativeParsing() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      createTable(nativeCatalog, true, Collections.emptyMap());
      FileStoreTable effective =
          (FileStoreTable)
              catalog(nativeCatalog, "04").loadTable("db", "existing_table").originalTable();
      assertEquals("04", effective.options().get(SCAN_PARALLELISM));
      assertEquals(4, effective.store().newScan().parallelism());
    }
  }

  @Test
  void rejectsDefaultsThatChangeImmutableOptions() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, true, Collections.emptyMap());
      assertFalse(original.options().containsKey("merge-engine"));
      PaimonCatalog catalog =
          new PaimonCatalog(
              nativeCatalog,
              "test",
              Collections.singletonMap("table-default.merge-engine", "partial-update"));

      UnsupportedOperationException failure =
          assertThrows(
              UnsupportedOperationException.class, () -> catalog.loadTable("db", "existing_table"));
      assertTrue(failure.getMessage().contains("merge-engine"));
      assertEquals(CoreOptions.MergeEngine.DEDUPLICATE, original.coreOptions().mergeEngine());
      assertFalse(nativeCatalog.getTable(TABLE_ID).options().containsKey("merge-engine"));
    }
  }

  @Test
  void rejectsDefaultsThatViolateNativeSchemaValidation() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      createTable(nativeCatalog, true, Collections.emptyMap());
      PaimonCatalog catalog =
          new PaimonCatalog(
              nativeCatalog,
              "test",
              Collections.singletonMap("table-default.snapshot.num-retained.min", "0"));

      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class, () -> catalog.loadTable("db", "existing_table"));
      assertTrue(failure.getMessage().contains("snapshot.num-retained.min"));
      assertFalse(
          nativeCatalog.getTable(TABLE_ID).options().containsKey("snapshot.num-retained.min"));
    }
  }

  @Test
  void snapshotDefaultDoesNotRebindCapturedSchema() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original = createTable(nativeCatalog, true, Collections.emptyMap());
      BatchWriteBuilder builder = original.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(1, BinaryString.fromString("value")));
        commit.commit(write.prepareCommit());
      }
      long snapshotId = original.snapshotManager().latestSnapshotId();
      nativeCatalog.alterTable(
          TABLE_ID,
          Collections.singletonList(SchemaChange.addColumn("added", DataTypes.INT())),
          false);
      nativeCatalog.invalidateTable(TABLE_ID);
      FileStoreTable latest = (FileStoreTable) nativeCatalog.getTable(TABLE_ID);
      assertTrue(latest.schema().id() > original.schema().id());

      FileStoreTable effective =
          (FileStoreTable)
              new PaimonCatalog(
                      nativeCatalog,
                      "test",
                      Collections.singletonMap(
                          "table-default.scan.snapshot-id", Long.toString(snapshotId)))
                  .loadTable("db", "existing_table")
                  .originalTable();

      assertEquals(latest.schema().id(), effective.schema().id());
      assertEquals(latest.schema().fields(), effective.schema().fields());
      assertEquals(Long.toString(snapshotId), effective.options().get("scan.snapshot-id"));
      assertFalse(latest.options().containsKey("scan.snapshot-id"));
    }
  }

  @Test
  void preservesHealthCandidatesAndTaskPackingForPopulatedTable() throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original =
          createTable(
              nativeCatalog,
              true,
              Collections.singletonMap("num-sorted-run.compaction-trigger", "2"));
      for (int id = 0; id < 2; id++) {
        BatchWriteBuilder builder = original.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
          write.write(GenericRow.of(id, BinaryString.fromString("value-" + id)));
          List<CommitMessage> messages = write.prepareCommit();
          try (BatchTableCommit commit = builder.newCommit()) {
            commit.commit(messages);
          }
        }
      }
      PaimonTable baseline =
          (PaimonTable) catalog(nativeCatalog, null).loadTable("db", "existing_table");
      PaimonTable injected =
          (PaimonTable) catalog(nativeCatalog, "4").loadTable("db", "existing_table");
      OptimizingConfig config =
          new OptimizingConfig()
              .setEnabled(true)
              .setMinorLeastFileCount(2)
              .setMinorLeastInterval(0)
              .setFullTriggerInterval(-1)
              .setFullRewriteAllFiles(false)
              .setMaxTaskSize(64L * 1024 * 1024);
      long planTime = System.currentTimeMillis();
      PaimonPrimaryKeyOptimizingEvaluation expected = evaluate(baseline, config, planTime);
      PaimonPrimaryKeyOptimizingEvaluation actual = evaluate(injected, config, planTime);

      assertTrue(expected.necessary());
      assertEquals(expected.optimizingType(), actual.optimizingType());
      assertEquals(expected.fullCompaction(), actual.fullCompaction());
      assertEquals(expected.targetSnapshotId(), actual.targetSnapshotId());
      assertEquals(expected.nextEvaluationTime(), actual.nextEvaluationTime());
      assertUnitsEqual(expected.units(), actual.units());
      TableHealthDetails expectedHealth = expected.analysis().get().healthDetails();
      TableHealthDetails actualHealth = actual.analysis().get().healthDetails();
      TableHealthStructure structure = actualHealth.getStructure();
      // Only the collection wall-clock time may differ between two equivalent scans.
      assertEquals(
          expectedHealth,
          actualHealth.withStructure(
              new TableHealthStructure(
                  structure.getTableShape(),
                  structure.getAvailability(),
                  expectedHealth.getStructure().getEvaluatedAtMillis(),
                  structure.getSnapshotTimeMillis(),
                  structure.getTotalUnits(),
                  structure.getAffectedUnits(),
                  structure.getSevereUnits(),
                  structure.getBands(),
                  structure.getProblems())));

      OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> baselinePlan =
          new PaimonPrimaryKeyOptimizingPlanner(
                  baseline, 1L, 2L, 1D, Long.MAX_VALUE, config, 0L, 0L, 0L, null)
              .plan();
      OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> injectedPlan =
          new PaimonPrimaryKeyOptimizingPlanner(
                  injected, 1L, 2L, 1D, Long.MAX_VALUE, config, 0L, 0L, 0L, null)
              .plan();
      assertFalse(baselinePlan.getTasks().isEmpty());
      assertEquals(baselinePlan.getTargetSnapshotId(), injectedPlan.getTargetSnapshotId());
      assertEquals(baselinePlan.getTasks().size(), injectedPlan.getTasks().size());
      for (int index = 0; index < baselinePlan.getTasks().size(); index++) {
        PaimonPrimaryKeyCompactionTask expectedTask = baselinePlan.getTasks().get(index);
        PaimonPrimaryKeyCompactionTask actualTask = injectedPlan.getTasks().get(index);
        assertEquals(expectedTask.getPartition(), actualTask.getPartition());
        assertEquals(
            expectedTask.getInput().getOptimizingType(), actualTask.getInput().getOptimizingType());
        assertEquals(
            expectedTask.getInput().isFullCompaction(), actualTask.getInput().isFullCompaction());
        assertEquals(
            expectedTask.getInput().getTargetSnapshotId(),
            actualTask.getInput().getTargetSnapshotId());
        assertUnitsEqual(expectedTask.getInput().getUnits(), actualTask.getInput().getUnits());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "0", "-1", "abc", "1.5", "2147483648"})
  void explicitTableOptionWinsOverInvalidCatalogDefault(String value) throws Exception {
    try (Catalog nativeCatalog = nativeCatalog()) {
      FileStoreTable original =
          createTable(nativeCatalog, true, Collections.singletonMap(SCAN_PARALLELISM, "8"));
      PaimonTable wrapper =
          (PaimonTable) catalogReturning(original, value).loadTable("db", "existing_table");

      assertSame(original, wrapper.originalTable());
      assertEquals(8, ((FileStoreTable) wrapper.originalTable()).store().newScan().parallelism());
    }
  }

  @Test
  void nonFileStoreTableIsUnchanged() throws Exception {
    Table table = mock(Table.class);
    assertSame(
        table, catalogReturning(table, "4").loadTable("db", "existing_table").originalTable());
  }

  private Catalog nativeCatalog() {
    Map<String, String> options = new HashMap<>();
    options.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    options.put(CatalogOptions.METASTORE.key(), "filesystem");
    return PaimonCatalogFactory.paimonCatalog(options, new Configuration());
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      PaimonTable wrapper, OptimizingConfig config, long planTime) {
    FileStoreTable table = (FileStoreTable) wrapper.originalTable();
    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.captureAt(
            table, wrapper.id().toString(), null, table.options(), planTime);
    return PaimonPrimaryKeyOptimizingEvaluator.evaluate(
        table, wrapper.id().toString(), context, config, 0L, 0L, null, planTime);
  }

  private static void assertUnitsEqual(
      List<PaimonBucketCompactionUnit> expected, List<PaimonBucketCompactionUnit> actual) {
    assertEquals(expected.size(), actual.size());
    for (int index = 0; index < expected.size(); index++) {
      PaimonBucketCompactionUnit left = expected.get(index);
      PaimonBucketCompactionUnit right = actual.get(index);
      assertArrayEquals(left.getPartitionBytes(), right.getPartitionBytes());
      assertEquals(left.getBucket(), right.getBucket());
      assertEquals(left.getFileCount(), right.getFileCount());
      assertEquals(left.getSortedRunCount(), right.getSortedRunCount());
      assertEquals(left.getFileSizeInBytes(), right.getFileSizeInBytes());
      assertEquals(left.getRecordCount(), right.getRecordCount());
      assertEquals(left.getLastFileCreationTime(), right.getLastFileCreationTime());
    }
  }

  private static FileStoreTable createTable(
      Catalog catalog, boolean primaryKey, Map<String, String> tableOptions) throws Exception {
    catalog.createDatabase(TABLE_ID.getDatabaseName(), true);
    Schema.Builder schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("value", DataTypes.STRING())
            .option("bucket", primaryKey ? "1" : "-1")
            .option("write-only", "true")
            .option("self-optimizing.enabled", "true");
    if (primaryKey) {
      schema.primaryKey("id");
    }
    tableOptions.forEach(schema::option);
    catalog.createTable(TABLE_ID, schema.build(), false);
    return (FileStoreTable) catalog.getTable(TABLE_ID);
  }

  private static PaimonCatalog catalog(Catalog catalog, String value) {
    return new PaimonCatalog(
        catalog,
        "test",
        value == null ? Collections.emptyMap() : Collections.singletonMap(CATALOG_DEFAULT, value));
  }

  private static PaimonCatalog catalogReturning(Table table, String value) throws Exception {
    Catalog cachedCatalog = mock(Catalog.class);
    when(cachedCatalog.getTable(any())).thenReturn(table);
    return catalog(cachedCatalog, value);
  }
}
