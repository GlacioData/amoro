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

package org.apache.amoro.formats.paimon.optimizing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendHealthEvaluator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.PlannerFile;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.ScanTotals;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonAppendFileScanner;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.table.TableIdentifier;
import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

class TestPaimonAppendDemandScope {
  @Test
  void missingManifestAfterSuccessfulPrimaryScanRequestsRetry(@TempDir Path warehouse)
      throws Exception {
    Map<String, String> options = options();
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, false, true, options)) {
      AppendOnlyFileStoreTable table = fixture.table;
      append(table, 1, "small");
      append(table, 2, largePayload());

      long now = 10_000L;
      OptimizationContext runtime =
          runtime(
              new OptimizingConfig()
                  .setEnabled(true)
                  .setMinorLeastInterval(-1)
                  .setFullTriggerInterval(0),
              0L,
              0L);
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.captureAt(
              table, table.fullName(), runtime, table.options(), now);
      PaimonAppendSnapshotAnalysis analysis =
          PaimonAppendFileScanner.analyze(table, health, runtime, null, now);
      assertTrue(analysis.plannerFactsAvailable());
      assertEquals(2L, analysis.fullSnapshotFileCount());
      assertEquals(1, analysis.plannerFiles().size());
      assertTrue(analysis.pendingInput().getHealthScore() >= 0);

      org.apache.paimon.fs.Path deltaList =
          table
              .store()
              .pathFactory()
              .manifestListFactory()
              .toPath(analysis.snapshot().deltaManifestList());
      table.setManifestCache(null);
      assertTrue(table.fileIO().delete(deltaList, false));
      PaimonAppendFileScanner.AppendDemandEvaluation demand =
          PaimonAppendFileScanner.evaluateAppendDemand(
              wrap(table), health, analysis, table.options(), runtime, now);
      assertTrue(demand.retryPlanning());
      assertFalse(demand.necessary());
      assertEquals(now + 5_000L, demand.nextEvaluationTime());
      assertTrue(analysis.pendingInput().getHealthScore() >= 0);
    }
  }

  @Test
  void fullBeforeMinorProbesTheScopeOnce(@TempDir Path warehouse) throws Exception {
    Map<String, String> options = options();
    options.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, "false");
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, false, true, options)) {
      append(fixture.table, 1, "small");
      append(fixture.table, 2, largePayload());
      append(fixture.table, 3, "small");
      AppendOnlyFileStoreTable table = spy(fixture.table);
      AppendOnlyFileStore store = spy(fixture.table.store());
      doReturn(store).when(table).store();
      long now = 10_000L;
      OptimizationContext runtime =
          runtime(
              new OptimizingConfig()
                  .setEnabled(true)
                  .setMinorLeastInterval(1_000)
                  .setFullTriggerInterval(500),
              now,
              now);
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.captureAt(
              table, table.fullName(), runtime, table.options(), now);
      PaimonAppendSnapshotAnalysis analysis =
          PaimonAppendFileScanner.analyze(table, health, runtime, null, now);
      assertEquals(3L, analysis.fullSnapshotFileCount());
      assertEquals(2, analysis.plannerFiles().size());
      clearInvocations(store);
      PaimonAppendFileScanner.AppendDemandEvaluation demand =
          PaimonAppendFileScanner.evaluateAppendDemand(
              wrap(table), health, analysis, table.options(), runtime, now);
      assertFalse(demand.necessary());
      assertEquals(0L, demand.nextEvaluationTime());
      verify(store, times(1)).newScan();
    }
  }

  @Test
  void fixedAdmissionRejectsAnUnpublishableOrderedGroup(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, false, true, options())) {
      AppendOnlyFileStoreTable table = fixture.table;
      append(table, 1, "snapshot");
      PaimonTable amoroTable = wrap(table);
      OptimizingConfig config =
          new OptimizingConfig()
              .setEnabled(true)
              .setMinorLeastInterval(-1)
              .setFullTriggerInterval(0);
      OptimizationContext runtime = runtime(config, 0L, 0L);
      long now = System.currentTimeMillis();
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.captureAt(
              table, amoroTable.id().toString(), runtime, table.options(), now);
      List<DataFileMeta> files =
          Arrays.asList(file(0, null), file(1, new byte[8 * 1024 * 1024 + 1]));
      ScanTotals totals = new ScanTotals();
      files.forEach(file -> totals.addFile(file, 16 * 1024, 32 * 1024, 0L));
      PaimonAppendSnapshotAnalysis analysis =
          PaimonAppendSnapshotAnalysis.createForScan(
              health,
              PaimonAppendHealthEvaluator.Result.invalid("TEST"),
              null,
              totals,
              Arrays.asList(
                  new PlannerFile(BinaryRow.EMPTY_ROW, 0, 2, files.get(0), null, null),
                  new PlannerFile(BinaryRow.EMPTY_ROW, 0, 2, files.get(1), null, null)),
              true,
              true,
              health.snapshot().get(),
              1);

      PaimonAppendFileScanner.AppendDemandEvaluation demand =
          PaimonAppendFileScanner.evaluateAppendDemand(
              amoroTable, health, analysis, table.options(), runtime, now);
      assertFalse(demand.necessary());
      assertFalse(demand.retryPlanning());
      PaimonOptimizingPlanner planner =
          new PaimonOptimizingPlanner(
              amoroTable,
              1L,
              1L,
              1.0D,
              Long.MAX_VALUE,
              config,
              0L,
              0L,
              0L,
              null,
              runtime,
              analysis);
      assertFalse(planner.isNecessary());
      assertSame(analysis, planner.tableAnalysis().get());
    }
  }

  private static DataFileMeta file(int sequence, byte[] index) {
    return DataFileMeta.forAppend(
        "file-" + sequence,
        1024L,
        10L,
        SimpleStats.EMPTY_STATS,
        10L * sequence,
        10L * sequence + 9L,
        0L,
        Collections.emptyList(),
        index,
        null,
        null,
        null,
        null,
        null);
  }

  private static PaimonTable wrap(AppendOnlyFileStoreTable table) {
    return new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), table);
  }

  private static Map<String, String> options() {
    Map<String, String> properties = new HashMap<>();
    properties.put("target-file-size", "32 kb");
    properties.put("compaction.min.file-num", "100");
    properties.put("source.split.open-file-cost", "10 b");
    return properties;
  }

  private static OptimizationContext runtime(
      OptimizingConfig config, long lastMinor, long lastFull) {
    OptimizationContext runtime = mock(OptimizationContext.class);
    when(runtime.getOptimizingConfig()).thenReturn(config);
    when(runtime.getLastMinorOptimizingTime()).thenReturn(lastMinor);
    when(runtime.getLastFullOptimizingTime()).thenReturn(lastFull);
    return runtime;
  }

  private static String largePayload() {
    byte[] bytes = new byte[128 * 1024];
    new Random(77L).nextBytes(bytes);
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static void append(AppendOnlyFileStoreTable table, int sequence, String payload)
      throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite writer = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      writer.write(
          GenericRow.of(
              0, BinaryString.fromString("key"), 0, sequence, BinaryString.fromString(payload)));
      commit.commit(writer.prepareCommit());
    }
  }
}
