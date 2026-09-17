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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluation;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluator;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeySnapshotAnalysis;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestPaimonPrimaryKeyCompactionIntervalPlanner {
  @Test
  void everyAnalysisEntryUsesSameManagedPolicy(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      FileStoreTable table =
          PaimonPrimaryKeyIntervalTestSupport.table(
              catalog, "entries", PaimonPrimaryKeyIntervalTestSupport.options(true));
      PaimonPrimaryKeyIntervalTestSupport.history(table);
      PaimonPrimaryKeyIntervalTestSupport.increments(table, 1, 1);
      PaimonTable wrapped = PaimonPrimaryKeyIntervalTestSupport.wrap(table);
      PaimonHealthEvaluationContext context =
          PaimonHealthEvaluationContext.capture(table, wrapped.id().toString(), null);
      Map<String, String> original = new HashMap<>(context.coreOptions().get().toMap());
      OptimizingConfig config = PaimonPrimaryKeyIntervalTestSupport.config();
      long now = System.currentTimeMillis();
      PaimonPrimaryKeyOptimizingEvaluation health =
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
              table, table.name(), context, config, 0, 0, null, now);
      PaimonPrimaryKeySnapshotAnalysis analysis = health.analysis().get();
      assertTrue(analysis.validForPlanning());
      assertFalse(health.necessary());
      assertFalse(PaimonPrimaryKeyIntervalTestSupport.evaluate(table).necessary());
      assertFalse(
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                  table, table.name(), context, analysis, config, 0, 0, null, now)
              .necessary());
      for (PaimonPrimaryKeySnapshotAnalysis supplied : Arrays.asList(analysis, null)) {
        PaimonPrimaryKeyOptimizingPlanner planner =
            new PaimonPrimaryKeyOptimizingPlanner(
                wrapped, 1L, 1L, 1.0, 64L * 1024 * 1024, config, 0, 0, 0, null, null, supplied);
        assertFalse(planner.isNecessary());
        assertEquals(supplied == null ? 1 : 0, planner.fallbackScanCount());
        assertEquals(context.key(), planner.tableAnalysis().get().key());
      }
      assertEquals(original, context.coreOptions().get().toMap());
      assertEquals("1h", table.options().get("compaction.optimization-interval"));
    }
  }

  @Test
  void fullRequiresAmoroIntervalAndIdleAndStillExecutes(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      FileStoreTable table =
          PaimonPrimaryKeyIntervalTestSupport.table(
              catalog, "full", PaimonPrimaryKeyIntervalTestSupport.options(true));
      PaimonPrimaryKeyIntervalTestSupport.history(table);
      PaimonPrimaryKeyIntervalTestSupport.increments(table, 1, 1);
      long now = System.currentTimeMillis() + 86400000L;
      OptimizingConfig config =
          PaimonPrimaryKeyIntervalTestSupport.config().setFullTriggerInterval(1000);
      assertFalse(
          evaluate(table, config, 0, 0, now).necessary(),
          "FULL must not be planned without an idle interval");
      FileStoreTable idle =
          table.copy(
              Collections.singletonMap("paimon-optimizer.primary-key.partition-idle-time", "0s"));
      assertFalse(evaluate(idle, config, 0, now - 999, now).necessary());
      assertEquals(
          OptimizingType.FULL, evaluate(idle, config, 0, now - 1000, now).optimizingType());
      for (int interval : new int[] {-1, 0}) {
        config.setFullTriggerInterval(interval);
        assertFalse(evaluate(idle, config, 0, 0, now).necessary());
      }
      config.setFullTriggerInterval(1000);
      FileStoreTable busy =
          table.copy(
              Collections.singletonMap("paimon-optimizer.primary-key.partition-idle-time", "2d"));
      assertFalse(evaluate(busy, config, 0, 0, now).necessary());
      FileStoreTable rested =
          table.copy(
              Collections.singletonMap("paimon-optimizer.primary-key.partition-idle-time", "1h"));
      assertEquals(OptimizingType.FULL, evaluate(rested, config, 0, 0, now).optimizingType());
      assertTrue(
          PaimonPrimaryKeyIntervalTestSupport.compact(idle, OptimizingType.FULL, 2L)
                  .getCompactedFileCount()
              > 1);
      assertEquals("1h", table.options().get("compaction.optimization-interval"));
    }
  }

  @Test
  void nativeSizeTriggersAndAmoroPriorityRemainEffective(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      FileStoreTable table =
          PaimonPrimaryKeyIntervalTestSupport.table(
              catalog, "priority", PaimonPrimaryKeyIntervalTestSupport.options(true));
      PaimonPrimaryKeyIntervalTestSupport.history(table);
      PaimonPrimaryKeyIntervalTestSupport.increments(table, 1, 1);
      for (String key :
          Arrays.asList(
              "compaction.total-size-threshold", "compaction.incremental-size-threshold")) {
        String value = key.contains("total") ? "64mb" : "1b";
        FileStoreTable sized = table.copy(Collections.singletonMap(key, value));
        assertTrue(PaimonPrimaryKeyIntervalTestSupport.evaluate(sized).necessary());
        assertTrue(
            PaimonPrimaryKeyIntervalTestSupport.evaluate(
                    sized.copy(Collections.singletonMap("compaction.optimization-interval", null)))
                .necessary());
      }
      PaimonPrimaryKeyIntervalTestSupport.increments(table, 2, 2);
      long now = System.currentTimeMillis() + 100000L;
      OptimizingConfig config =
          PaimonPrimaryKeyIntervalTestSupport.config().setMinorLeastInterval(1000);
      assertFalse(evaluate(table, config, now, 0, now).necessary());
      assertFalse(evaluate(table, config, now - 1000, 0, now).necessary());
      PaimonPrimaryKeyOptimizingEvaluation minor = evaluate(table, config, now - 1001, 0, now);
      assertTrue(minor.necessary());
      assertEquals(OptimizingType.MINOR, minor.optimizingType());
      FileStoreTable idle =
          table.copy(
              Collections.singletonMap("paimon-optimizer.primary-key.partition-idle-time", "0s"));
      config.setFullTriggerInterval(1);
      assertTrue(evaluate(idle, config, 0, 0, now).necessary());
      assertEquals(OptimizingType.MINOR, evaluate(idle, config, 0, 0, now).optimizingType());
      FileStoreTable pressure =
          idle.copy(Collections.singletonMap("num-sorted-run.stop-trigger", "3"));
      assertEquals(OptimizingType.MAJOR, evaluate(pressure, config, now, 0, now).optimizingType());
    }
  }

  @Test
  void retryDeadlineGatesAllEvaluationEntriesAndPreservesMinorMajor(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      FileStoreTable base =
          PaimonPrimaryKeyIntervalTestSupport.table(
              catalog, "deadline", PaimonPrimaryKeyIntervalTestSupport.options(true));
      PaimonPrimaryKeyIntervalTestSupport.history(base);
      PaimonPrimaryKeyIntervalTestSupport.increments(base, 1, 1);
      FileStoreTable table =
          base.copy(
              Collections.singletonMap("paimon-optimizer.primary-key.partition-idle-time", "0s"));
      PaimonTable wrapped = PaimonPrimaryKeyIntervalTestSupport.wrap(table);
      PaimonHealthEvaluationContext context =
          PaimonHealthEvaluationContext.capture(table, wrapped.id().toString(), null);
      OptimizingConfig config =
          PaimonPrimaryKeyIntervalTestSupport.config().setFullTriggerInterval(1);
      long now = System.currentTimeMillis() + 86400000L;
      PaimonPrimaryKeyOptimizingEvaluation initial =
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
              table, table.name(), context, config, 0, 0, null, now);
      assertEquals(OptimizingType.FULL, initial.optimizingType());
      PaimonPrimaryKeySnapshotAnalysis analysis = initial.analysis().get();
      for (long deadline : new long[] {now, now + 1}) {
        for (PaimonPrimaryKeyOptimizingEvaluation evaluation :
            Arrays.asList(
                PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                    table, table.name(), config, 0, 0, null, now, deadline),
                PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                    table, table.name(), context, config, 0, 0, null, now, deadline),
                PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                    table, table.name(), context, analysis, config, 0, 0, null, now, deadline))) {
          assertEquals(deadline == now, evaluation.necessary());
        }
      }
      org.apache.amoro.optimizing.OptimizationContext runtime =
          org.mockito.Mockito.mock(org.apache.amoro.optimizing.OptimizationContext.class);
      org.mockito.Mockito.when(runtime.getOptimizingConfig()).thenReturn(config);
      org.mockito.Mockito.when(runtime.getFullRetryNotBefore()).thenReturn(Long.MAX_VALUE);
      for (PaimonPrimaryKeySnapshotAnalysis supplied : Arrays.asList(analysis, null)) {
        PaimonPrimaryKeyOptimizingPlanner planner =
            new PaimonPrimaryKeyOptimizingPlanner(
                wrapped, 1, 2, 1, 64L * 1024 * 1024, config, 0, 0, 0, null, runtime, supplied);
        assertFalse(planner.isNecessary());
      }
      PaimonPrimaryKeyIntervalTestSupport.increments(base, 2, 2);
      assertEquals(
          OptimizingType.MINOR,
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                  table, table.name(), config, 0, 0, null, now, now + 1)
              .optimizingType());
      FileStoreTable pressure =
          table.copy(Collections.singletonMap("num-sorted-run.stop-trigger", "3"));
      assertEquals(
          OptimizingType.MAJOR,
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                  pressure, pressure.name(), config, now, 0, null, now, now + 1)
              .optimizingType());
    }
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      FileStoreTable table, OptimizingConfig config, long minor, long full, long now) {
    return PaimonPrimaryKeyOptimizingEvaluator.evaluate(
        table, table.name(), config, minor, full, null, now);
  }
}
