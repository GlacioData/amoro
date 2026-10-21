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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.mergetree.compact.OffPeakHours;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

class TestPaimonPrimaryKeyOffPeakWakeup {
  static long hour(int hour) {
    return LocalDate.of(2026, 10, 21)
        .atTime(hour, 0)
        .atZone(ZoneId.systemDefault())
        .toInstant()
        .toEpochMilli();
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "-1"})
  void realFilesUseCapturedRatioOnColdAndReusedPaths(String bucket, @TempDir Path dir)
      throws Exception {
    try (Catalog catalog = catalog(dir)) {
      Map<String, String> options = options(false);
      options.put("bucket", bucket);
      options.put("num-sorted-run.compaction-trigger", "2");
      options.put("compaction.offpeak.start.hour", "1");
      options.put("compaction.offpeak.end.hour", "2");
      options.put("compaction.offpeak-ratio", "100000");
      FileStoreTable table = table(catalog, "offpeak", options);
      GenericRow[] large = new GenericRow[1000];
      for (int i = 0; i < large.length; i++) {
        large[i] = GenericRow.of(i, i * 73, i * 97, 1L);
      }
      append(table, large);
      append(table, GenericRow.of(1, 11, 21, 2L));
      PaimonHealthEvaluationContext outside =
          PaimonHealthEvaluationContext.captureAt(table, "t", null, null, hour(3));
      PaimonHealthEvaluationContext inside =
          PaimonHealthEvaluationContext.captureAt(table, "t", null, null, hour(1));
      assertNotEquals(outside.key(), inside.key());
      assertEquals(
          outside.key(),
          PaimonHealthEvaluationContext.captureAt(table, "t", null, null, hour(4)).key());
      assertEquals(
          inside.key(),
          PaimonHealthEvaluationContext.captureAt(table, "t", null, null, hour(1) + 3599999L)
              .key());
      Map<String, String> original = new HashMap<>(table.options());
      OptimizingConfig config = config();
      for (PaimonHealthEvaluationContext captured :
          new PaimonHealthEvaluationContext[] {outside, inside, outside}) {
        boolean necessary = captured == inside;
        PaimonPrimaryKeySnapshotAnalysis facts =
            PaimonPrimaryKeySnapshotAnalysis.analyze(table, captured);
        for (long evaluationTime : new long[] {hour(1), hour(3), hour(1)}) {
          PaimonPrimaryKeyOptimizingEvaluation result =
              PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                  table, "t", captured, facts, config, 0, 0, null, evaluationTime);
          assertEquals(
              necessary,
              result.necessary(),
              "candidate selection must retain the captured window, including ABA");
          if (necessary) {
            assertEquals(OptimizingType.MINOR, result.optimizingType());
          }
        }
        assertEquals(
            necessary,
            PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                    table, "t", captured, config, 0, 0, null, hour(3))
                .necessary());
      }
      assertEquals(original, table.options());
      // chain does not gain a clock wakeup, but its pre-existing native candidate semantics
      // must remain identical on direct and context-based paths.
      FileStoreTable chain = spy(table);
      Map<String, String> chainOptions = new HashMap<>(table.options());
      chainOptions.put("chain-table.enabled", "true");
      doReturn(chainOptions).when(chain).options();
      doReturn(CoreOptions.fromMap(chainOptions)).when(chain).coreOptions();
      PaimonHealthEvaluationContext chainInside =
          PaimonHealthEvaluationContext.captureAt(chain, "t", null, null, hour(1));
      assertEquals(
          chainInside.key(),
          PaimonHealthEvaluationContext.captureAt(chain, "t", null, null, hour(3)).key());
      assertTrue(
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                  chain, "t", chainInside, config, 0, 0, null, hour(1))
              .necessary());
      assertTrue(
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(chain, "t", config, 0, 0, null, hour(1))
              .necessary());
      assertFalse(
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(table, "t", config, 0, 0, null, hour(3))
              .necessary());
      assertTrue(
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(table, "t", config, 0, 0, null, hour(1))
              .necessary());
      config.setMinorLeastInterval(1000);
      PaimonPrimaryKeyOptimizingEvaluation waiting =
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
              table, "t", inside, config, hour(1), 0, null, hour(1) + 1000);
      assertFalse(waiting.necessary());
      assertEquals(hour(1) + 1001, waiting.nextEvaluationTime());
    }
  }

  @ParameterizedTest
  @CsvSource({"1,2,100", "23,2,100", "0,24,100", "-1,-1,100", "1,1,100", "1,2,0"})
  void planningIdentityChangesOnlyWhenNativeEffectiveRatioChanges(
      int start, int end, int ratio, @TempDir Path dir) throws Exception {
    try (Catalog catalog = catalog(dir)) {
      Map<String, String> options = options(false);
      options.put("compaction.offpeak.start.hour", Integer.toString(start));
      options.put("compaction.offpeak.end.hour", Integer.toString(end));
      options.put("compaction.offpeak-ratio", Integer.toString(ratio));
      FileStoreTable table = table(catalog, "hours", options);
      OffPeakHours nativeHours = OffPeakHours.create(start, end, ratio);
      PaimonHealthEvaluationContext previous = null;
      int previousRatio = 0;
      for (int hour = 0; hour < 24; hour++) {
        PaimonHealthEvaluationContext current =
            PaimonHealthEvaluationContext.captureAt(table, "t", null, null, hour(hour));
        int effective = nativeHours == null ? 0 : nativeHours.currentRatio(hour);
        if (previous != null) {
          assertEquals(previousRatio == effective, previous.key().equals(current.key()));
        }
        previous = current;
        previousRatio = effective;
      }
    }
  }
}
