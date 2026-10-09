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

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.append;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.config;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.table.StateKey;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class TestPaimonPrimaryKeyMinorWakeup {
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
  void ordinaryCandidatesWaitThenConfirmSameSnapshotDemand(
      String producer, String bucket, @TempDir Path dir) throws Exception {
    try (Catalog catalog = catalog(dir)) {
      Map<String, String> options = options(false);
      options.put("bucket", bucket);
      options.put("changelog-producer", producer);
      options.put("lookup-compact", "radical");
      options.put("lookup-wait", "false");
      options.put("num-sorted-run.compaction-trigger", "2");
      FileStoreTable table = table(catalog, "minor", options);
      OptimizingConfig config = config().setMinorLeastInterval(1000);
      OptimizationContext context = mock(OptimizationContext.class);
      when(context.getOptimizingConfig()).thenReturn(config);
      when(context.getTableConfig()).thenReturn(options);
      when(context.getLastMinorOptimizingTime()).thenReturn(1000L);
      assertEquals(0L, wrap(table).evaluatePendingInput(context, 1).get().nextEvaluationTime());
      append(table, GenericRow.of(1, 10, 20, 1L));
      append(table, GenericRow.of(1, 11, 21, 2L));
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.capture(table, table.name(), context);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(table, health);
      for (long now : new long[] {1999L, 2000L, 2001L}) {
        for (boolean reused : new boolean[] {false, true}) {
          PaimonPrimaryKeyOptimizingEvaluation result =
              reused
                  ? PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                      table, table.name(), health, analysis, config, 1000L, 0L, null, now)
                  : PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                      table, table.name(), config, 1000L, 0L, null, now);
          assertEquals(now > 2000L, result.necessary());
          assertEquals(OptimizingType.MINOR, result.optimizingType());
          assertEquals(now <= 2000L ? 2001L : 0L, result.nextEvaluationTime());
        }
      }
      long future = System.currentTimeMillis() + 60000L;
      when(context.getLastMinorOptimizingTime()).thenReturn(future);
      PendingInputResult waiting = wrap(table).evaluatePendingInput(context, 1).get();
      assertFalse(waiting.optimizingNecessary());
      assertEquals(future + 1001L, waiting.nextEvaluationTime());
      assertFalse(
          ((PaimonPendingInput) waiting.optimizingPendingInput()).isSameSnapshotPlanningPending());
      when(context.getLastMinorOptimizingTime()).thenReturn(0L);
      PendingInputResult ready = wrap(table).evaluatePendingInput(context, 1).get();
      assertTrue(ready.optimizingNecessary());
      PaimonPendingInput pending = (PaimonPendingInput) ready.optimizingPendingInput();
      assertTrue(pending.isSameSnapshotPlanningPending());
      assertFalse(pending.isGentleMinorCompactionPending());
      StateKey<PaimonPendingInput> key =
          StateKey.stateKey("pending_input")
              .jsonType(PaimonPendingInput.class)
              .defaultValue(new PaimonPendingInput());
      assertTrue(
          key.deserialize(key.serialize(pending)).canPlanWithoutSnapshotChange(context, 2001L));
      assertFalse(key.deserialize("{\"dataFileCount\":2}").isSameSnapshotPlanningPending());
      config.setMinorLeastInterval(-1);
      assertNoWakeup(table, health, analysis, config);
      config.setMinorLeastInterval(1000).setEnabled(false);
      assertNoWakeup(table, health, analysis, config);
    }
  }

  @Test
  void deadlineArithmeticPreservesStrictBoundaryWithoutOverflow() {
    assertEquals(0L, PaimonPrimaryKeyOptimizingEvaluator.nextMinorEvaluationTime(1000L, -1));
    assertEquals(1001L, PaimonPrimaryKeyOptimizingEvaluator.nextMinorEvaluationTime(1000L, 0));
    assertEquals(
        Long.MAX_VALUE,
        PaimonPrimaryKeyOptimizingEvaluator.nextMinorEvaluationTime(Long.MAX_VALUE - 2L, 1000L));
    assertEquals(
        Long.MAX_VALUE,
        PaimonPrimaryKeyOptimizingEvaluator.nextMinorEvaluationTime(0L, Long.MAX_VALUE));
  }

  @ParameterizedTest
  @CsvSource({
    "chain-table.enabled,true",
    "pk-clustering-override,true",
    "write-only,false",
    "self-optimizing.enabled,false"
  })
  void excludedOptionsDoNotGainDeadlineOrSameSnapshotDemand(
      String option, String value, @TempDir Path dir) throws Exception {
    try (Catalog catalog = catalog(dir)) {
      Map<String, String> options = options(false);
      options.put("num-sorted-run.compaction-trigger", "2");
      FileStoreTable original = table(catalog, "excluded", options);
      append(original, GenericRow.of(1, 10, 20, 1L));
      append(original, GenericRow.of(1, 11, 21, 2L));
      // These excluded shapes have native creation constraints unrelated to scheduling. Expose
      // their options on real snapshot facts to test only the new wakeup/admission boundary.
      FileStoreTable table = spy(original);
      Map<String, String> excludedOptions = new HashMap<>(original.options());
      excludedOptions.put(option, value);
      doReturn(excludedOptions).when(table).options();
      doReturn(CoreOptions.fromMap(excludedOptions)).when(table).coreOptions();
      OptimizationContext context = mock(OptimizationContext.class);
      OptimizingConfig config = config().setMinorLeastInterval(1000);
      when(context.getOptimizingConfig()).thenReturn(config);
      when(context.getTableConfig()).thenReturn(excludedOptions);
      when(context.getLastMinorOptimizingTime()).thenReturn(System.currentTimeMillis() + 60000L);
      Optional<PendingInputResult> waiting = wrap(table).evaluatePendingInput(context, 1);
      waiting.ifPresent(result -> assertEquals(0L, result.nextEvaluationTime()));
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.capture(table, table.name(), context);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(table, health);
      assertNoWakeup(table, health, analysis, config);
      when(context.getLastMinorOptimizingTime()).thenReturn(0L);
      Optional<PendingInputResult> ready = wrap(table).evaluatePendingInput(context, 1);
      ready.ifPresent(
          result ->
              assertFalse(
                  ((PaimonPendingInput) result.optimizingPendingInput())
                      .isSameSnapshotPlanningPending()));
    }
  }

  private static void assertNoWakeup(
      FileStoreTable table,
      PaimonHealthEvaluationContext health,
      PaimonPrimaryKeySnapshotAnalysis analysis,
      OptimizingConfig config) {
    for (boolean reused : new boolean[] {false, true}) {
      PaimonPrimaryKeyOptimizingEvaluation result =
          reused
              ? PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                  table, table.name(), health, analysis, config, 1000L, 0L, null, 2000L)
              : PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                  table, table.name(), config, 1000L, 0L, null, 2000L);
      assertFalse(result.necessary());
      assertEquals(0L, result.nextEvaluationTime());
    }
  }
}
