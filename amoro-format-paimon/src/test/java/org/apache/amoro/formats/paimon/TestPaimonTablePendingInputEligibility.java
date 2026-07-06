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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptions;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.table.TableIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DisplayName("Paimon pending input eligibility")
class TestPaimonTablePendingInputEligibility {

  @Test
  @DisplayName("append-only BUCKET_UNAWARE table can request optimizing")
  void appendOnlyBucketUnawareTableIsOptimizingNecessary(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_append", new HashMap<>());
    PaimonTable paimonTable = wrap(table, "t_append");

    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(true), 10)
            .orElseThrow(AssertionError::new);

    assertTrue(result.optimizingNecessary());
  }

  @Test
  @DisplayName("primary-key HASH_FIXED table is not bound to optimizing queue by default")
  void primaryKeyHashFixedTableIsNotOptimizingNecessaryByDefault(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_pk_default", new HashMap<>());
    PaimonTable paimonTable = wrap(catalog.getTable(id), "t_pk");

    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(true), 10)
            .orElseThrow(AssertionError::new);

    assertFalse(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("primary-key HASH_FIXED table does not request optimizing before trigger")
  void primaryKeyHashFixedTableDoesNotRequestOptimizingBeforeTrigger(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_pk_enabled", options);
    PaimonTable paimonTable =
        wrap(
            catalog.getTable(id).copy(runtimeOptions("num-sorted-run.compaction-trigger", "2")),
            "t_pk_enabled");

    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(true), 10)
            .orElseThrow(AssertionError::new);

    assertFalse(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("primary-key HASH_FIXED table requests optimizing after minor trigger")
  void primaryKeyHashFixedTableRequestsOptimizingAfterMinorTrigger(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_pk_minor", options);
    writeCommits(catalog.getTable(id), 2);
    PaimonTable paimonTable =
        wrap(
            catalog.getTable(id).copy(runtimeOptions("num-sorted-run.compaction-trigger", "2")),
            "t_pk_minor");

    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(true), 10)
            .orElseThrow(AssertionError::new);

    assertTrue(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("primary-key HASH_DYNAMIC table requests optimizing after minor trigger")
  void primaryKeyHashDynamicTableRequestsOptimizingAfterMinorTrigger(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "-1");
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_pk_dynamic_enabled", options);
    writeCommits(catalog.getTable(id), 2, 0);
    PaimonTable paimonTable =
        wrap(
            catalog.getTable(id).copy(runtimeOptions("num-sorted-run.compaction-trigger", "2")),
            "t_pk_dynamic_enabled");

    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(true), 10)
            .orElseThrow(AssertionError::new);

    assertTrue(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("primary-key HASH table respects minor interval in pre-check")
  void primaryKeyHashTableRespectsMinorIntervalInPreCheck(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_pk_minor_interval", options);
    writeCommits(catalog.getTable(id), 2);
    PaimonTable paimonTable =
        wrap(
            catalog.getTable(id).copy(runtimeOptions("num-sorted-run.compaction-trigger", "2")),
            "t_pk_minor_interval");

    long now = System.currentTimeMillis();
    OptimizingConfig config =
        new OptimizingConfig()
            .setEnabled(true)
            .setMinorLeastFileCount(2)
            .setMinorLeastInterval(60_000)
            .setFullTriggerInterval(-1)
            .setFullRewriteAllFiles(false)
            .setMaxTaskSize(64L * 1024 * 1024);
    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(config, now, 0L), 10)
            .orElseThrow(AssertionError::new);

    assertFalse(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("primary-key HASH table rejects non-empty self-optimizing filter in pre-check")
  void primaryKeyHashTableRejectsFilterInPreCheck(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "99");
    Identifier id = createPrimaryKeyTable(catalog, "t_pk_filter", options);
    writeCommits(catalog.getTable(id), 2);
    PaimonTable paimonTable =
        wrap(
            catalog.getTable(id).copy(runtimeOptions("num-sorted-run.compaction-trigger", "2")),
            "t_pk_filter");

    OptimizingConfig config = new OptimizingConfig().setEnabled(true).setFilter("id > 1");
    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(config), 10)
            .orElseThrow(AssertionError::new);

    assertFalse(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("primary-key HASH table can request FULL optimizing after idle interval")
  void primaryKeyHashTableRequestsFullAfterIdleInterval(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    Identifier id = createPrimaryKeyTable(catalog, "t_pk_full", options);
    writeCommits(catalog.getTable(id), 1);
    PaimonTable paimonTable = wrap(catalog.getTable(id), "t_pk_full");

    OptimizingConfig config =
        new OptimizingConfig()
            .setEnabled(true)
            .setMinorLeastFileCount(10)
            .setMinorLeastInterval(0)
            .setFullTriggerInterval(1)
            .setFullRewriteAllFiles(false)
            .setMaxTaskSize(64L * 1024 * 1024);
    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(config), 10)
            .orElseThrow(AssertionError::new);

    assertTrue(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("primary-key HASH table respects full interval in pre-check")
  void primaryKeyHashTableRespectsFullIntervalInPreCheck(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    Identifier id = createPrimaryKeyTable(catalog, "t_pk_full_interval", options);
    writeCommits(catalog.getTable(id), 1);
    PaimonTable paimonTable = wrap(catalog.getTable(id), "t_pk_full_interval");

    long now = System.currentTimeMillis();
    OptimizingConfig config =
        new OptimizingConfig()
            .setEnabled(true)
            .setMinorLeastFileCount(10)
            .setMinorLeastInterval(0)
            .setFullTriggerInterval(60_000)
            .setFullRewriteAllFiles(false)
            .setMaxTaskSize(64L * 1024 * 1024);
    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(config, 0L, now), 10)
            .orElseThrow(AssertionError::new);

    assertFalse(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("HASH table without primary key is not bound to optimizing queue")
  void nonPrimaryKeyHashTableIsNotOptimizingNecessaryWhenEnabled() {
    FileStoreTable table = mock(FileStoreTable.class);
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    when(table.options()).thenReturn(options);
    when(table.primaryKeys()).thenReturn(Collections.emptyList());
    when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    PaimonTable paimonTable = wrap(table, "t_hash_without_pk");

    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(true), 10)
            .orElseThrow(AssertionError::new);

    assertFalse(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("fixed-bucket append-only table is not bound to optimizing queue")
  void fixedBucketAppendOnlyTableIsNotOptimizingNecessary(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    options.put("bucket-key", "id");
    Table table = createAppendOnlyTable(catalog, "t_fixed_bucket", options);
    PaimonTable paimonTable = wrap(table, "t_fixed_bucket");

    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(true), 10)
            .orElseThrow(AssertionError::new);

    assertFalse(result.optimizingNecessary());
    assertEmptyPendingInput(result);
  }

  @Test
  @DisplayName("self-optimizing disabled table does not request optimizing")
  void disabledSelfOptimizingIsNotOptimizingNecessary(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_disabled", new HashMap<>());
    PaimonTable paimonTable = wrap(table, "t_disabled");

    PendingInputResult result =
        paimonTable
            .evaluatePendingInput(optimizationContext(false), 10)
            .orElseThrow(AssertionError::new);

    assertFalse(result.optimizingNecessary());
  }

  private static Catalog fsCatalog(Path warehouse) {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    return PaimonCatalogFactory.paimonCatalog(props, new Configuration());
  }

  private static Table createAppendOnlyTable(
      Catalog catalog, String tableName, Map<String, String> extraOptions) throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .option("bucket", "-1");
    extraOptions.forEach(builder::option);
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return catalog.getTable(id);
  }

  private static Identifier createPrimaryKeyTable(
      Catalog catalog, String tableName, Map<String, String> extraOptions) throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .primaryKey("id")
            .option("bucket", "2");
    extraOptions.forEach(builder::option);
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return id;
  }

  private static PaimonTable wrap(Table table, String name) {
    return new PaimonTable(TableIdentifier.of("test_catalog", "db1", name), table);
  }

  private static Map<String, String> runtimeOptions(String... keyValues) {
    Map<String, String> options = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      options.put(keyValues[i], keyValues[i + 1]);
    }
    return options;
  }

  private static OptimizationContext optimizationContext(boolean enabled) {
    return optimizationContext(new OptimizingConfig().setEnabled(enabled));
  }

  private static OptimizationContext optimizationContext(OptimizingConfig config) {
    return optimizationContext(config, 0L, 0L);
  }

  private static OptimizationContext optimizationContext(
      OptimizingConfig config, long lastMinorOptimizingTime, long lastFullOptimizingTime) {
    OptimizationContext context = mock(OptimizationContext.class);
    when(context.getOptimizingConfig()).thenReturn(config);
    when(context.getLastMinorOptimizingTime()).thenReturn(lastMinorOptimizingTime);
    when(context.getLastFullOptimizingTime()).thenReturn(lastFullOptimizingTime);
    return context;
  }

  private static void writeCommits(Table table, int count) throws Exception {
    writeCommits(table, count, null);
  }

  private static void writeCommits(Table table, int count, Integer bucket) throws Exception {
    for (int i = 0; i < count; i++) {
      BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
      try (BatchTableWrite write = writeBuilder.newWrite();
          BatchTableCommit commit = writeBuilder.newCommit()) {
        GenericRow row = GenericRow.of(i, BinaryString.fromString("name-" + i));
        if (bucket == null) {
          write.write(row);
        } else {
          write.write(row, bucket);
        }
        List<CommitMessage> messages = write.prepareCommit();
        commit.commit(messages);
      }
    }
  }

  private static void assertEmptyPendingInput(PendingInputResult result) {
    PaimonPendingInput pendingInput = (PaimonPendingInput) result.pendingInput();
    assertEquals(0, pendingInput.getDataFileCount());
    assertEquals(0L, pendingInput.getDataFileSize());
    assertEquals(0, pendingInput.getSmallFileCount());
  }
}
