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

package org.apache.amoro.formats.paimon.optimizing.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendRewriteOptions;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonAppendFileScanner;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeySnapshotAnalysis;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestPaimonHealthFingerprint {
  @TempDir Path warehouse;

  @ParameterizedTest
  @ValueSource(strings = {"compaction.offpeak.start.hour", "compaction.offpeak-ratio"})
  void appendIgnoresUnrelatedOffPeakValues(String option) throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      AppendOnlyFileStoreTable table = fixedTable(catalog);
      FileStoreTable invalidOffPeak = table.copy(Collections.singletonMap(option, "invalid"));
      PaimonHealthEvaluationContext original =
          PaimonHealthEvaluationContext.captureAt(table, "t", null, null, 0);
      PaimonHealthEvaluationContext captured =
          PaimonHealthEvaluationContext.captureAt(invalidOffPeak, "t", null, null, 0);
      assertEquals(0, captured.offPeakRatioNow());
      assertEquals(original.key(), captured.key());
      assertFalse(captured.configurationError().isPresent());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 2})
  void clockTransitionsInvalidateOnlyPlanningAndAreConsumedOnce(int buckets) throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      AppendOnlyFileStoreTable table = appendTable(catalog, buckets);
      write(table, 1);
      OptimizationContext runtime = mock(OptimizationContext.class);
      when(runtime.getOptimizingConfig())
          .thenReturn(
              new OptimizingConfig().setMinorLeastInterval(100).setFullTriggerInterval(200));
      when(runtime.getLastMinorOptimizingTime()).thenReturn(1000L);
      when(runtime.getLastFullOptimizingTime()).thenReturn(1000L);
      PaimonHealthEvaluationContext before =
          PaimonHealthEvaluationContext.captureAt(table, "t", runtime, null, 1100);
      PaimonHealthEvaluationContext minorDue =
          PaimonHealthEvaluationContext.captureAt(table, "t", runtime, null, 1101);
      PaimonHealthEvaluationContext fullDue =
          PaimonHealthEvaluationContext.captureAt(table, "t", runtime, null, 1201);
      assertNotEquals(before.key(), minorDue.key());
      assertNotEquals(minorDue.key(), fullDue.key());
      assertEquals(
          minorDue.key(),
          PaimonHealthEvaluationContext.captureAt(table, "t", runtime, null, 1200).key());
      assertEquals(
          fullDue.key(),
          PaimonHealthEvaluationContext.captureAt(table, "t", runtime, null, Long.MAX_VALUE).key());
      assertEquals(
          PaimonHealthEvaluationContext.healthIdentity(before.scoringConfigFingerprint()),
          PaimonHealthEvaluationContext.healthIdentity(fullDue.scoringConfigFingerprint()));
      // Restart and empty-plan completion do not alter persisted due flags by elapsed time alone.
      when(runtime.getLastPlanTime()).thenReturn(1300L);
      assertEquals(
          fullDue.key(),
          PaimonHealthEvaluationContext.captureAt(table, "t", runtime, null, 1400).key());
      when(runtime.getLastPlanTime()).thenReturn(0L);
      assertEquals(
          fullDue.key(),
          PaimonHealthEvaluationContext.captureAt(table, "t", runtime, null, 1400).key());
      // Successful optimization resets only its own threshold for the next interval.
      when(runtime.getLastFullOptimizingTime()).thenReturn(1400L);
      assertEquals(
          minorDue.key(),
          PaimonHealthEvaluationContext.captureAt(table, "t", runtime, null, 1500).key());
      assertFalse(PaimonHealthEvaluationContext.intervalReached(1000, 1000, 0));
      assertTrue(PaimonHealthEvaluationContext.intervalReached(1001, 1000, 0));
      assertFalse(PaimonHealthEvaluationContext.intervalReached(999, 1000, 0));
      assertFalse(PaimonHealthEvaluationContext.intervalReached(1001, 1000, -1));
    }
  }

  // Paimon ac223f47, table/AbstractFileStoreTable.java:396-408 and schema/TableSchema.java:317
  // copy the native schema/options without committing a new schema ID. This is not a DDL test.
  @ParameterizedTest
  @CsvSource({
    "bucket-append-ordered, false, true",
    "file-index.range-bitmap.columns, id, true",
    "file-index.in-manifest-threshold, 1 b, true",
    "clustering.incremental, true, true",
    "clustering.columns, id, true",
    "compaction.force-rewrite-all-files, true, true",
    "compaction.force-rewrite-all-files, false, true",
    "self-optimizing.full.rewrite-all-files, true, true",
    "self-optimizing.full.rewrite-all-files, false, true",
    "self-optimizing.full.rewrite-all-files, invalid, true",
    "compaction.force-rewrite-all-files, invalid, true",
    "bucket-key, other, true",
    "bucket, 4, true",
    "file.format, parquet, true",
    "file.compression, gzip, true",
    "deletion-vectors.enabled, true, false",
    "target-file-size, 3 mb, false"
  })
  void fixedWriterOptionsInvalidatePlanningWithoutChangingStructuralHealth(
      String option, String value, boolean sameHealth) throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      AppendOnlyFileStoreTable table = fixedTable(catalog);
      write(table, 1);
      FormatTableAnalysis before = analyze(table, null);
      Map<String, String> options = new HashMap<>(table.options());
      options.put(option, value);
      FileStoreTable copied = table.copy(table.schema().copy(options));
      PaimonHealthEvaluationContext changed =
          PaimonHealthEvaluationContext.capture(copied, "t", null);

      assertEquals(before.key().getSnapshotId(), changed.key().getSnapshotId());
      assertEquals(table.schema().id(), copied.schema().id());
      assertNotEquals(before.key(), changed.key());
      assertEquals(
          sameHealth, new PaimonHealthSupport().sameInput(changed.key(), before.healthDetails()));
    }
  }

  @Test
  void unchangedNativeOptionsCopyKeepsTheWholeAnalysisKey() throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      AppendOnlyFileStoreTable table = fixedTable(catalog);
      write(table, 1);
      assertEquals(
          PaimonHealthEvaluationContext.capture(table, "t", null).key(),
          PaimonHealthEvaluationContext.capture(table.copy(Collections.emptyMap()), "t", null)
              .key());
    }
  }

  // Paimon ac223f47, CoreOptions.java:236-241 defines native rewrite default false. Amoro
  // resolves explicit merged catalog/table properties before defaults erase their provenance.
  @Test
  void inheritedAmoroRewriteOverridesNativePolicyInThePlanningKey() throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      FileStoreTable table =
          fixedTable(catalog)
              .copy(Collections.singletonMap("compaction.force-rewrite-all-files", "true"));
      write(table, 1);
      FormatTableAnalysis nativeAnalysis = analyze(table, null);
      Map<String, String> inherited = new HashMap<>(table.options());
      inherited.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, "false");
      PaimonAppendRewriteOptions policy = PaimonAppendRewriteOptions.from(inherited);
      PaimonHealthEvaluationContext changed =
          PaimonHealthEvaluationContext.capture(table, "t", null, inherited);
      FileStoreTable explicit =
          table.copy(
              Collections.singletonMap(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, "false"));

      assertEquals(PaimonAppendRewriteOptions.Source.AMORO_EXPLICIT, policy.source());
      assertFalse(policy.fullRewriteAllFiles());
      assertNotEquals(nativeAnalysis.key(), changed.key());
      assertEquals(PaimonHealthEvaluationContext.capture(explicit, "t", null).key(), changed.key());
      assertTrue(
          new PaimonHealthSupport().sameInput(changed.key(), nativeAnalysis.healthDetails()));
    }
  }

  private static AppendOnlyFileStoreTable fixedTable(Catalog catalog) throws Exception {
    return appendTable(catalog, 2);
  }

  private static AppendOnlyFileStoreTable appendTable(Catalog catalog, int buckets)
      throws Exception {
    catalog.createDatabase("d", false);
    Identifier id = Identifier.create("d", "fixed");
    Schema.Builder schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("other", DataTypes.INT())
            .option("bucket", String.valueOf(buckets))
            .option("write-only", "true");
    if (buckets > 0) {
      schema.option("bucket-key", "id");
    }
    catalog.createTable(id, schema.build(), false);
    return (AppendOnlyFileStoreTable) catalog.getTable(id);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void runtimeFactsAreReusedWhilePlanningKeysRemainStrict(boolean primaryKey) throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("d", false);
      Identifier id = Identifier.create("d", "t");
      Schema.Builder schema =
          Schema.newBuilder().column("id", DataTypes.INT()).option("write-only", "true");
      if (primaryKey) {
        schema.primaryKey("id").option("bucket", "1");
      }
      catalog.createTable(id, schema.build(), false);
      FileStoreTable table = (FileStoreTable) catalog.getTable(id);
      OptimizationContext runtime = mock(OptimizationContext.class);
      OptimizingConfig config = new OptimizingConfig();
      when(runtime.getOptimizingConfig()).thenReturn(config);
      write(table, 1);
      FormatTableAnalysis before = analyze(table, runtime);
      write(table, 2);
      FormatTableAnalysis after = analyze(table, runtime);
      Snapshot committed = table.snapshotManager().latestSnapshot();
      PaimonHealthComparisonState.Entry entry = new PaimonHealthComparisonState.Entry();
      entry.setInputSnapshotId(before.key().getSnapshotId());
      entry.setSchemaId(committed.schemaId());
      entry.setCommitUser(committed.commitUser());
      entry.setCommitIdentifier(committed.commitIdentifier());
      entry.setBefore(before.healthDetails());
      entry.setBeforeScore(before.pendingInput().getHealthScore());
      PaimonHealthSupport.build(
          entry, table, "t", after.healthDetails(), after.pendingInput().getHealthScore());
      assertEquals("READY", entry.getStatus());
      // A rescan creates new immutable details, so identity proves both native scans were skipped.
      assertSame(before.healthDetails(), entry.getBefore());
      assertSame(after.healthDetails(), entry.getAfter());

      PaimonHealthSupport support = new PaimonHealthSupport();
      config.setMinorLeastInterval(123);
      config.setFullTriggerInterval(456);
      config.setFullRewriteAllFiles(true);
      config.setFilter("id > 0");
      PaimonHealthEvaluationContext changed =
          PaimonHealthEvaluationContext.capture(table, "t", runtime);
      assertNotEquals(after.key(), changed.key());
      assertTrue(support.sameInput(changed.key(), after.healthDetails()));
      assertTrue(
          PaimonHealthSupport.reusable(
              after.healthDetails(), after.pendingInput().getHealthScore(), table, "t", committed));

      FileStoreTable differentScore =
          table.copy(Collections.singletonMap("target-file-size", "3 mb"));
      PaimonHealthEvaluationContext different =
          PaimonHealthEvaluationContext.capture(differentScore, "t", runtime);
      assertFalse(support.sameInput(different.key(), after.healthDetails()));
      FormatTableAnalysis differentFacts = analyze(differentScore, runtime);
      assertFalse(
          PaimonHealthSupport.reusable(
              differentFacts.healthDetails(),
              differentFacts.pendingInput().getHealthScore(),
              table,
              "t",
              committed));

      TableHealthDetails d = after.healthDetails();
      String legacyDigest =
          d.getScoringConfigFingerprint()
              .substring(d.getScoringConfigFingerprint().lastIndexOf(':') + 1);
      TableHealthDetails legacy =
          new TableHealthDetails(
              d.getFormulaVersion(),
              d.getSnapshotId(),
              d.getChangeSnapshotId(),
              d.getSchemaId(),
              legacyDigest,
              d.getEvaluationKey(),
              d.getComponents(),
              d.getMetrics(),
              d.getReasonCodes(),
              d.getStructure());
      assertFalse(support.sameInput(after.key(), legacy));
      assertFalse(
          PaimonHealthSupport.reusable(
              legacy, after.pendingInput().getHealthScore(), table, "t", committed));
    }
  }

  private static FormatTableAnalysis analyze(FileStoreTable table, OptimizationContext runtime) {
    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.capture(table, "t", runtime);
    return table instanceof PrimaryKeyFileStoreTable
        ? PaimonPrimaryKeySnapshotAnalysis.analyze(table, context)
        : PaimonAppendFileScanner.analyze((AppendOnlyFileStoreTable) table, context, null);
  }

  private static void write(FileStoreTable table, int value) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      write.write(
          table.schema().fields().size() == 2 ? GenericRow.of(value, value) : GenericRow.of(value));
      commit.commit(write.prepareCommit());
    }
  }
}
