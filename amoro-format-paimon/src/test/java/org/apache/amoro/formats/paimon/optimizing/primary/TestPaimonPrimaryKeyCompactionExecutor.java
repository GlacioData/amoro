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

package org.apache.amoro.formats.paimon.optimizing.primary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonPrimaryKeyOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizingExecutor;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@DisplayName("PaimonPrimaryKeyCompactionExecutor")
class TestPaimonPrimaryKeyCompactionExecutor {

  @Test
  @DisplayName("Factory creates a primary-key compaction executor")
  void factoryCreatesExecutor() {
    PaimonPrimaryKeyCompactionExecutorFactory factory =
        new PaimonPrimaryKeyCompactionExecutorFactory();
    factory.initialize(new HashMap<>());

    OptimizingExecutor<PaimonPrimaryKeyCompactionOutput> executor =
        factory.createExecutor(new PaimonPrimaryKeyCompactionInput());

    assertNotNull(executor);
    assertTrue(executor instanceof PaimonPrimaryKeyCompactionExecutor);
  }

  @Test
  @DisplayName("execute rejects missing required input")
  void executorRejectsMissingInput() {
    IllegalStateException nullInput =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(null).execute());
    assertTrue(nullInput.getMessage().contains("missing required fields"));

    IllegalStateException emptyInput =
        assertThrows(
            IllegalStateException.class,
            () ->
                new PaimonPrimaryKeyCompactionExecutor(new PaimonPrimaryKeyCompactionInput())
                    .execute());
    assertTrue(emptyInput.getMessage().contains("missing required fields"));
  }

  @Test
  @DisplayName("execute rejects missing commit identity")
  void executorRejectsMissingCommitIdentity(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_bad_identity", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 2);
    PaimonPrimaryKeyCompactionInput valid = planMinorTasks(catalog, id).get(0).getInput();

    PaimonPrimaryKeyCompactionInput missingUser =
        copyInput(valid, valid.getUnits(), "", valid.getCommitIdentifier());
    IllegalStateException missingUserEx =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(missingUser).execute());
    assertTrue(missingUserEx.getMessage().contains("missing commitUser"));

    PaimonPrimaryKeyCompactionInput invalidIdentifier =
        copyInput(valid, valid.getUnits(), valid.getCommitUser(), 0L);
    IllegalStateException invalidIdentifierEx =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(invalidIdentifier).execute());
    assertTrue(invalidIdentifierEx.getMessage().contains("invalid commitIdentifier"));
  }

  @Test
  @DisplayName("execute rejects inconsistent optimizing type and compaction mode")
  void executorRejectsInconsistentOptimizingMode(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_bad_mode", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 2);
    PaimonPrimaryKeyCompactionInput valid = planMinorTasks(catalog, id).get(0).getInput();

    PaimonPrimaryKeyCompactionInput minorFull =
        copyInput(
            valid,
            valid.getUnits(),
            OptimizingType.MINOR,
            true,
            valid.getCommitUser(),
            valid.getCommitIdentifier());
    IllegalStateException minorFullEx =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(minorFull).execute());
    assertTrue(minorFullEx.getMessage().contains("requires fullCompaction=false"));

    PaimonPrimaryKeyCompactionInput majorFull =
        copyInput(
            valid,
            valid.getUnits(),
            OptimizingType.MAJOR,
            true,
            valid.getCommitUser(),
            valid.getCommitIdentifier());
    IllegalStateException majorFullEx =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(majorFull).execute());
    assertTrue(majorFullEx.getMessage().contains("requires fullCompaction=false"));

    PaimonPrimaryKeyCompactionInput fullNonFull =
        copyInput(
            valid,
            valid.getUnits(),
            OptimizingType.FULL,
            false,
            valid.getCommitUser(),
            valid.getCommitIdentifier());
    IllegalStateException fullNonFullEx =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(fullNonFull).execute());
    assertTrue(fullNonFullEx.getMessage().contains("requires fullCompaction=true"));
  }

  @Test
  @DisplayName("executor forwards exact native fullCompaction flag")
  void executorForwardsExactNativeFullCompactionFlag() throws Exception {
    assertNativeFullCompactionFlag(OptimizingType.MINOR, false);
    assertNativeFullCompactionFlag(OptimizingType.MAJOR, false);
    assertNativeFullCompactionFlag(OptimizingType.FULL, true);
  }

  @Test
  @DisplayName("execute rejects append-only table")
  void executorRejectsAppendOnlyTable(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createNonPrimaryKeyHashTable(catalog, "t_non_primary_key_hash");
    PaimonPrimaryKeyCompactionInput input =
        new PaimonPrimaryKeyCompactionInput(
            wrap(catalog.getTable(id), id.getObjectName()),
            Collections.singletonList(
                new PaimonBucketCompactionUnit(new byte[] {0}, 0, 1L, 1L, 1L, 1L)),
            OptimizingType.MINOR,
            false,
            1L,
            "user",
            1L);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(input).execute());

    assertTrue(ex.getMessage().contains("requires non-append FileStoreTable"), ex::getMessage);
  }

  @Test
  @DisplayName("execute rejects primary-key non-hash bucket mode")
  void executorRejectsPrimaryKeyNonHashBucketMode(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPostponePrimaryKeyTable(catalog, "t_postpone_primary_key");
    PaimonPrimaryKeyCompactionInput input =
        new PaimonPrimaryKeyCompactionInput(
            wrap(catalog.getTable(id), id.getObjectName()),
            Collections.singletonList(
                new PaimonBucketCompactionUnit(new byte[] {0}, -2, 1L, 1L, 1L, 1L)),
            OptimizingType.MINOR,
            false,
            1L,
            "user",
            1L);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(input).execute());

    assertTrue(ex.getMessage().contains("primary-key HASH_FIXED/HASH_DYNAMIC"), ex::getMessage);
    assertTrue(ex.getMessage().contains("POSTPONE_MODE"), ex::getMessage);
  }

  @Test
  @DisplayName("execute rejects PK clustering override before creating a writer")
  void executorRejectsPkClusteringOverrideBeforeCreatingWriter(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("deletion-vectors.enabled", "true");
    options.put("clustering.columns", "name");
    options.put("pk-clustering-override", "true");
    Identifier id = createPrimaryKeyTable(catalog, "t_clustering_override", options);
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    PaimonPrimaryKeyCompactionInput input =
        new PaimonPrimaryKeyCompactionInput(
            wrap(table, id.getObjectName()),
            Collections.singletonList(
                new PaimonBucketCompactionUnit(new byte[] {0}, 0, 1L, 1L, 1L, 1L)),
            OptimizingType.MINOR,
            false,
            1L,
            "user",
            1L);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonPrimaryKeyCompactionExecutor(input).execute());

    assertTrue(ex.getMessage().contains("pk-clustering-override"), ex::getMessage);
    verify(table, never()).copy(anyMap());
  }

  @Test
  @DisplayName("MINOR executor continues after a newer snapshot without committing it")
  void minorExecutorContinuesAfterNewerSnapshot(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_minor_execute", primaryKeyOptions());
    writeCommits(catalog.getTable(id), 2);
    List<PaimonPrimaryKeyCompactionTask> tasks = planMinorTasks(catalog, id);
    assertFalse(tasks.isEmpty());
    PaimonPrimaryKeyCompactionTask task = tasks.get(0);
    assertEquals(task.getInput().getTargetSnapshotId(), latestSnapshotId(catalog, id));
    writeCommits(catalog.getTable(id), 1);
    long concurrentSnapshotId = latestSnapshotId(catalog, id);
    assertTrue(concurrentSnapshotId > task.getInput().getTargetSnapshotId());

    PaimonPrimaryKeyCompactionOutput output =
        new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute();

    assertFalse(output.getCommitMessageBytesList().isEmpty());
    assertEquals(task.getInput().getUnits().size(), output.getCompactedBucketCount());
    assertEquals(sumFiles(task.getInput().getUnits()), output.getCompactedFileCount());
    assertEquals(sumBytes(task.getInput().getUnits()), output.getCompactedFileSize());
    assertEquals(sumRecords(task.getInput().getUnits()), output.getCompactedRecordCount());
    assertTrue(output.getProducedFileCount() > 0);
    assertTrue(output.getProducedFileSize() > 0);
    assertEquals(concurrentSnapshotId, latestSnapshotId(catalog, id));
  }

  @Test
  @DisplayName("MAJOR executor continues after a newer snapshot with normal compaction")
  void majorExecutorContinuesAfterNewerSnapshot(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_major_execute", options);
    writeCommits(catalog.getTable(id), 3);
    List<PaimonPrimaryKeyCompactionTask> tasks = planMajorTasks(catalog, id);
    assertFalse(tasks.isEmpty());
    PaimonPrimaryKeyCompactionTask task = tasks.get(0);
    assertFalse(task.getInput().isFullCompaction());
    assertEquals(task.getInput().getTargetSnapshotId(), latestSnapshotId(catalog, id));
    writeCommits(catalog.getTable(id), 1);
    long concurrentSnapshotId = latestSnapshotId(catalog, id);
    assertTrue(concurrentSnapshotId > task.getInput().getTargetSnapshotId());

    PaimonPrimaryKeyCompactionOutput output =
        new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute();

    assertFalse(output.getCommitMessageBytesList().isEmpty());
    assertEquals(task.getInput().getUnits().size(), output.getCompactedBucketCount());
    assertTrue(output.getCompactedFileCount() >= output.getProducedFileCount());
    assertTrue(output.getCompactedFileSize() > 0);
    assertTrue(output.getProducedFileSize() > 0);
    assertEquals(concurrentSnapshotId, latestSnapshotId(catalog, id));
  }

  @Test
  @DisplayName("MAJOR executor compacts write-only table by forcing write-only false")
  void majorExecutorCompactsWriteOnlyTable(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_write_only_major_execute", options);
    writeCommits(catalog.getTable(id), 3);
    List<PaimonPrimaryKeyCompactionTask> tasks = planMajorTasks(catalog, id);
    assertFalse(tasks.isEmpty());

    PaimonPrimaryKeyCompactionOutput output =
        new PaimonPrimaryKeyCompactionExecutor(tasks.get(0).getInput()).execute();

    assertFalse(output.getCommitMessageBytesList().isEmpty());
    assertTrue(output.getProducedFileCount() > 0);
  }

  @Test
  @DisplayName("FULL executor returns no-op output when snapshot changed after planning")
  void fullExecutorNoOpsWhenSnapshotChangedAfterPlanning(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    Identifier id = createPrimaryKeyTable(catalog, "t_full_stale_snapshot", options);
    writeCommits(catalog.getTable(id), 1);
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, Collections.emptyMap(), defaultConfig().setFullTriggerInterval(1))
            .plan();
    assertEquals(OptimizingType.FULL, result.getOptimizingType());
    assertFalse(result.getTasks().isEmpty());
    writeCommits(catalog.getTable(id), 1);

    PaimonPrimaryKeyCompactionOutput output =
        new PaimonPrimaryKeyCompactionExecutor(result.getTasks().get(0).getInput()).execute();

    assertTrue(output.getCommitMessageBytesList().isEmpty());
    assertEquals(0, output.getCompactedBucketCount());
    assertEquals(0L, output.getCompactedFileCount());
    assertEquals(0L, output.getProducedFileCount());
  }

  @Test
  @DisplayName("FULL executor returns no-op output when latest snapshot is missing")
  void fullExecutorNoOpsWhenLatestSnapshotIsMissing() {
    FileStoreTable table = mock(FileStoreTable.class);
    when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
    when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    when(table.options()).thenReturn(Collections.emptyMap());
    when(table.latestSnapshot()).thenReturn(Optional.empty());
    PaimonPrimaryKeyCompactionInput input =
        new PaimonPrimaryKeyCompactionInput(
            wrap(table, "t_full_missing_snapshot"),
            Collections.singletonList(
                new PaimonBucketCompactionUnit(
                    SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW), 0, 1L, 1L, 1L, 1L)),
            OptimizingType.FULL,
            true,
            10L,
            "user",
            1L);

    PaimonPrimaryKeyCompactionOutput output =
        new PaimonPrimaryKeyCompactionExecutor(input).execute();

    assertTrue(output.getCommitMessageBytesList().isEmpty());
    assertEquals(0, output.getCompactedBucketCount());
    assertEquals(0L, output.getCompactedFileCount());
    assertEquals(0L, output.getProducedFileCount());
    verify(table, never()).copy(anyMap());
  }

  private static PaimonPrimaryKeyCompactionInput copyInput(
      PaimonPrimaryKeyCompactionInput input,
      List<PaimonBucketCompactionUnit> units,
      String commitUser,
      long commitIdentifier) {
    return copyInput(
        input,
        units,
        input.getOptimizingType(),
        input.isFullCompaction(),
        commitUser,
        commitIdentifier);
  }

  private static void assertNativeFullCompactionFlag(
      OptimizingType optimizingType, boolean expectedFullCompaction) throws Exception {
    FileStoreTable table = mock(FileStoreTable.class);
    when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
    when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    when(table.options()).thenReturn(Collections.emptyMap());
    FileStoreTable compactTable = mock(FileStoreTable.class);
    when(table.copy(anyMap())).thenReturn(compactTable);
    BatchWriteBuilder writeBuilder = mock(BatchWriteBuilder.class);
    BatchTableWrite write = mock(BatchTableWrite.class);
    when(compactTable.newBatchWriteBuilder()).thenReturn(writeBuilder);
    when(writeBuilder.newWrite()).thenReturn(write);
    when(write.withIOManager(any())).thenReturn(write);
    when(write.prepareCommit()).thenReturn(Collections.emptyList());

    long targetSnapshotId = 10L;
    if (optimizingType == OptimizingType.FULL) {
      Snapshot snapshot = mock(Snapshot.class);
      when(snapshot.id()).thenReturn(targetSnapshotId);
      when(table.latestSnapshot()).thenReturn(Optional.of(snapshot));
    }
    PaimonPrimaryKeyCompactionInput input =
        new PaimonPrimaryKeyCompactionInput(
            wrap(table, "t_native_" + optimizingType.name().toLowerCase()),
            Collections.singletonList(
                new PaimonBucketCompactionUnit(
                    SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW), 0, 1L, 1L, 1L, 1L)),
            optimizingType,
            expectedFullCompaction,
            targetSnapshotId,
            "user",
            1L);

    new PaimonPrimaryKeyCompactionExecutor(input).execute();

    verify(write).compact(any(BinaryRow.class), eq(0), eq(expectedFullCompaction));
    verify(write).prepareCommit();
  }

  private static PaimonPrimaryKeyCompactionInput copyInput(
      PaimonPrimaryKeyCompactionInput input,
      List<PaimonBucketCompactionUnit> units,
      OptimizingType optimizingType,
      boolean fullCompaction,
      String commitUser,
      long commitIdentifier) {
    return new PaimonPrimaryKeyCompactionInput(
        input.getTable(),
        units,
        optimizingType,
        fullCompaction,
        input.getTargetSnapshotId(),
        commitUser,
        commitIdentifier);
  }

  private static List<PaimonPrimaryKeyCompactionTask> planMinorTasks(Catalog catalog, Identifier id)
      throws Exception {
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(catalog, id, runtimeOptions("num-sorted-run.compaction-trigger", "2")).plan();
    assertEquals(OptimizingType.MINOR, result.getOptimizingType());
    return result.getTasks();
  }

  private static List<PaimonPrimaryKeyCompactionTask> planMajorTasks(Catalog catalog, Identifier id)
      throws Exception {
    OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> result =
        planner(
                catalog,
                id,
                runtimeOptions(
                    "num-sorted-run.compaction-trigger", "2", "num-sorted-run.stop-trigger", "2"))
            .plan();
    assertEquals(OptimizingType.MAJOR, result.getOptimizingType());
    return result.getTasks();
  }

  private static PaimonPrimaryKeyOptimizingPlanner planner(
      Catalog catalog, Identifier id, Map<String, String> runtimeOptions) throws Exception {
    return planner(catalog, id, runtimeOptions, defaultConfig());
  }

  private static PaimonPrimaryKeyOptimizingPlanner planner(
      Catalog catalog, Identifier id, Map<String, String> runtimeOptions, OptimizingConfig config)
      throws Exception {
    PaimonTable table = wrap(catalog.getTable(id).copy(runtimeOptions), id.getObjectName());
    return new PaimonPrimaryKeyOptimizingPlanner(
        table, 100L, 7L, 4.0, 64L * 1024 * 1024, config, 0L, 0L, 0L, null);
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

  private static Identifier createNonPrimaryKeyHashTable(Catalog catalog, String tableName)
      throws Exception {
    catalog.createDatabase("db1", true);
    Schema schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .option("bucket", "1")
            .option("bucket-key", "id")
            .build();
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, schema, true);
    return id;
  }

  private static Identifier createPostponePrimaryKeyTable(Catalog catalog, String tableName)
      throws Exception {
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "-2");
    return createPrimaryKeyTable(catalog, tableName, options);
  }

  private static void writeCommits(Table table, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      BatchWriteBuilder builder = table.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite()) {
        write.write(GenericRow.of(i, BinaryString.fromString("name-" + i)));
        List<CommitMessage> messages = write.prepareCommit();
        try (BatchTableCommit commit = builder.newCommit()) {
          commit.commit(messages);
        }
      }
    }
  }

  private static long latestSnapshotId(Catalog catalog, Identifier id) throws Exception {
    return ((FileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id();
  }

  private static long sumFiles(List<PaimonBucketCompactionUnit> units) {
    return units.stream().mapToLong(PaimonBucketCompactionUnit::getFileCount).sum();
  }

  private static long sumBytes(List<PaimonBucketCompactionUnit> units) {
    return units.stream().mapToLong(PaimonBucketCompactionUnit::getFileSizeInBytes).sum();
  }

  private static long sumRecords(List<PaimonBucketCompactionUnit> units) {
    return units.stream().mapToLong(PaimonBucketCompactionUnit::getRecordCount).sum();
  }

  private static Map<String, String> primaryKeyOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    options.put("bucket", "1");
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
}
