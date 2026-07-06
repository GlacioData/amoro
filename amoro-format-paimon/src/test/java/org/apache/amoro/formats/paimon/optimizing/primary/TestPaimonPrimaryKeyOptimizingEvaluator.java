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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DisplayName("Paimon primary-key optimizing evaluator")
class TestPaimonPrimaryKeyOptimizingEvaluator {

  @Test
  @DisplayName("enabled table without snapshot is not necessary")
  void noSnapshotIsNotNecessary(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    Identifier id = createPrimaryKeyTable(catalog, "t_no_snapshot", options);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig(), 0, 0, System.currentTimeMillis());

    assertFalse(evaluation.necessary());
    assertEquals(-1L, evaluation.targetSnapshotId());
  }

  @Test
  @DisplayName("minor candidates use Amoro trigger when Paimon native trigger is absent")
  void minorUsesAmoroTriggerWhenPaimonNativeTriggerIsAbsent(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    Identifier id = createPrimaryKeyTable(catalog, "t_minor_amoro_trigger", options);
    writeCommits(catalog.getTable(id), 2);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig().setMinorLeastFileCount(2), 0, 0, now());

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.MINOR, evaluation.optimizingType());
    assertFalse(evaluation.fullCompaction());
  }

  @Test
  @DisplayName("explicit Paimon native trigger suppresses lower Amoro trigger")
  void explicitPaimonNativeTriggerSuppressesLowerAmoroTrigger(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "3");
    Identifier id = createPrimaryKeyTable(catalog, "t_minor_native_trigger", options);
    writeCommits(catalog.getTable(id), 2);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig().setMinorLeastFileCount(2), 0, 0, now());

    assertFalse(evaluation.necessary());
  }

  @Test
  @DisplayName("major candidates have higher priority than minor candidates")
  void majorHasHigherPriorityThanMinor(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put(PaimonPrimaryKeyOptions.MAJOR_FILE_COUNT_THRESHOLD, "3");
    Identifier id = createPrimaryKeyTable(catalog, "t_major", options);
    writeCommits(catalog.getTable(id), 3);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig(), 0, 0, now());

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.MAJOR, evaluation.optimizingType());
    assertTrue(evaluation.fullCompaction());
  }

  @Test
  @DisplayName("full candidates require explicit partition idle time")
  void fullRequiresPartitionIdleTime(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    Identifier id = createPrimaryKeyTable(catalog, "t_full_no_idle", options);
    writeCommits(catalog.getTable(id), 1);

    OptimizingConfig config = defaultConfig().setMinorLeastFileCount(10).setFullTriggerInterval(1);

    PaimonPrimaryKeyOptimizingEvaluation evaluation = evaluate(catalog, id, config, 0, 0, now());

    assertFalse(evaluation.necessary());
    assertTrue(evaluation.targetSnapshotId() > 0);
  }

  @Test
  @DisplayName("full candidates are planned after interval and idle time")
  void fullPlansAfterIntervalAndIdleTime(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    Identifier id = createPrimaryKeyTable(catalog, "t_full_idle", options);
    writeCommits(catalog.getTable(id), 1);

    OptimizingConfig config = defaultConfig().setMinorLeastFileCount(10).setFullTriggerInterval(1);

    PaimonPrimaryKeyOptimizingEvaluation evaluation = evaluate(catalog, id, config, 0, 0, now());

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    assertTrue(evaluation.fullCompaction());
  }

  @Test
  @DisplayName("non-empty self-optimizing filter is rejected")
  void filterIsRejected(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("num-sorted-run.compaction-trigger", "2");
    Identifier id = createPrimaryKeyTable(catalog, "t_filter", options);
    writeCommits(catalog.getTable(id), 2);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig().setFilter("id > 1"), 0, 0, now());

    assertFalse(evaluation.necessary());
  }

  private static long now() {
    return System.currentTimeMillis();
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      Catalog catalog,
      Identifier id,
      OptimizingConfig config,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime,
      long planTime)
      throws Exception {
    return PaimonPrimaryKeyOptimizingEvaluator.evaluate(
        (FileStoreTable) catalog.getTable(id),
        id.getObjectName(),
        config,
        lastMinorOptimizingTime,
        lastFullOptimizingTime,
        null,
        planTime);
  }

  private static Catalog fsCatalog(Path warehouse) {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    return PaimonCatalogFactory.paimonCatalog(props, new Configuration());
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

  private static void writeCommits(Table table, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      writeRecords(table, GenericRow.of(i, BinaryString.fromString("name-" + i)));
    }
  }

  private static void writeRecords(Table table, GenericRow row) throws Exception {
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit()) {
      write.write(row);
      List<CommitMessage> messages = write.prepareCommit();
      commit.commit(messages);
    }
  }

  private static Map<String, String> primaryKeyOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(PaimonPrimaryKeyOptions.ENABLED, "true");
    return options;
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
