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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.evaluate;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

class TestPaimonPrimaryKeyManagedGentle {
  @ParameterizedTest
  @CsvSource({
    "first-row,none,1",
    "first-row,none,-1",
    "deduplicate,lookup,1",
    "deduplicate,lookup,-1"
  })
  void managedGentleHasCandidateWithoutAccumulatingPicks(
      String merge, String changelog, String bucket, @TempDir Path dir) throws Exception {
    try (Catalog c = catalog(dir)) {
      Map<String, String> opts = options(false);
      opts.put("merge-engine", merge);
      opts.put("changelog-producer", changelog);
      opts.put("bucket", bucket);
      opts.put("lookup-compact", "gentle");
      opts.put("lookup-compact.max-interval", "100");
      opts.put("num-sorted-run.compaction-trigger", "5");
      FileStoreTable t = table(c, "gentle", opts);
      append(t, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyOptimizingEvaluation result = evaluate(t);
      assertTrue(result.necessary());
      assertEquals(OptimizingType.MINOR, result.optimizingType());
      assertFalse(result.fullCompaction());
      assertEquals("gentle", t.options().get("lookup-compact"));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "affected,true",
    "radical,false",
    "none,false",
    "unmanaged,false",
    "writer,false",
    "chain,false",
    "clustering,false",
    "postpone,false",
    "first-row,true",
    "dv,true",
    "force,true"
  })
  void optionsAndSchedulingStayInsideOriginalGentleScope(String variant, boolean affected) {
    Map<String, String> raw = gentleOptions();
    if (variant.equals("radical")) {
      raw.put("lookup-compact", "radical");
    }
    if (variant.equals("none")) {
      raw.put("changelog-producer", "none");
    }
    if (variant.equals("unmanaged")) {
      raw.put("self-optimizing.enabled", "false");
    }
    if (variant.equals("writer")) {
      raw.put("write-only", "false");
    }
    if (variant.equals("chain")) {
      raw.put("chain-table.enabled", "true");
    }
    if (variant.equals("clustering")) {
      raw.put("pk-clustering-override", "true");
    }
    if (variant.equals("postpone")) {
      raw.put("bucket", "-2");
    }
    if (variant.equals("first-row")) {
      raw.put("changelog-producer", "none");
      raw.put("merge-engine", "first-row");
    }
    if (variant.equals("dv")) {
      raw.put("changelog-producer", "none");
      raw.put("deletion-vectors.enabled", "true");
    }
    if (variant.equals("force")) {
      raw.put("changelog-producer", "none");
      raw.put("force-lookup", "true");
    }
    Map<String, String> before = new HashMap<>(raw);
    CoreOptions effective =
        PaimonPrimaryKeyOptions.compactionStrategyOptions(CoreOptions.fromMap(raw));
    Map<String, String> expected = new HashMap<>(raw);
    if (affected) {
      expected.put("lookup-compact", "radical");
    }
    assertEquals(expected, effective.toMap());
    Map<String, String> overrides = PaimonPrimaryKeyOptions.compactionWriteOverrides(raw);
    assertEquals(affected, overrides.containsKey("lookup-compact"));
    assertEquals(before, raw);
    OptimizationContext context = context(raw, 1000L, 1000);
    PaimonPendingInput input = new PaimonPendingInput();
    assertFalse(input.canPlanWithoutSnapshotChange(context, 2001L));
    input.setGentleMinorCompactionPending(true);
    assertFalse(input.canPlanWithoutSnapshotChange(context, 2000L));
    assertEquals(affected, input.canPlanWithoutSnapshotChange(context, 2001L));
  }

  @ParameterizedTest
  @CsvSource({"gentle,true", "radical,false"})
  void delayHintsPreserveStrictIntervalAndGentleDemandScope(
      String mode, boolean affected, @TempDir Path dir) throws Exception {
    try (Catalog c = catalog(dir)) {
      Map<String, String> opts = gentleOptions();
      opts.put("lookup-compact", mode);
      FileStoreTable t = table(c, "timed", opts);
      append(t, GenericRow.of(1, 10, 20, 1L));
      OptimizationContext context = context(opts, 1000L, 1000);
      OptimizingConfig config = context.getOptimizingConfig();
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.capture(t, t.name(), context);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(t, health);
      for (long now : new long[] {1999L, 2000L, 2001L}) {
        PaimonPrimaryKeyOptimizingEvaluation direct =
            PaimonPrimaryKeyOptimizingEvaluator.evaluate(t, t.name(), config, 1000L, 0L, null, now);
        PaimonPrimaryKeyOptimizingEvaluation reused =
            PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                t, t.name(), health, analysis, config, 1000L, 0L, null, now);
        assertEquals(now > 2000L, direct.necessary());
        assertEquals(direct.necessary(), reused.necessary());
        assertEquals(now <= 2000L ? 2001L : 0L, direct.nextEvaluationTime());
        assertEquals(direct.nextEvaluationTime(), reused.nextEvaluationTime());
      }
      config.setMinorLeastInterval(-1);
      assertEquals(
          0L,
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(t, t.name(), config, 1000L, 0L, null, 3000L)
              .nextEvaluationTime());
      assertFalse(
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(t, t.name(), config, 1000L, 0L, null, 3000L)
              .necessary());
      config.setMinorLeastInterval(1000);
      long future = System.currentTimeMillis() + 60000L;
      when(context.getLastMinorOptimizingTime()).thenReturn(future);
      PendingInputResult delayed = wrap(t).evaluatePendingInput(context, 1).get();
      assertFalse(delayed.optimizingNecessary());
      assertEquals(future + 1001L, delayed.nextEvaluationTime());
      assertFalse(
          ((PaimonPendingInput) delayed.optimizingPendingInput()).isGentleMinorCompactionPending());
      when(context.getLastMinorOptimizingTime()).thenReturn(0L);
      PendingInputResult due = wrap(t).evaluatePendingInput(context, 1).get();
      assertTrue(due.optimizingNecessary());
      assertEquals(0L, due.nextEvaluationTime());
      assertEquals(
          affected,
          ((PaimonPendingInput) due.optimizingPendingInput()).isGentleMinorCompactionPending());
    }
  }

  @Test
  void emptyAppendAndLegacyInputCannotGainGentleDemand(@TempDir Path dir) throws Exception {
    assertFalse(PaimonManagedGentle.isAffected(mock(AppendOnlyFileStoreTable.class)));
    StateKey<PaimonPendingInput> key =
        StateKey.stateKey("pending_input")
            .jsonType(PaimonPendingInput.class)
            .defaultValue(new PaimonPendingInput());
    assertFalse(key.deserialize("{\"dataFileCount\":2}").isGentleMinorCompactionPending());
    try (Catalog c = catalog(dir)) {
      FileStoreTable t = table(c, "empty", gentleOptions());
      PendingInputResult result =
          wrap(t).evaluatePendingInput(context(t.options(), 0L, 1000), 1).get();
      assertFalse(result.optimizingNecessary());
      assertEquals(0L, result.nextEvaluationTime());
    }
    assertEquals(
        Long.MAX_VALUE,
        PaimonPrimaryKeyOptimizingEvaluator.nextMinorEvaluationTime(Long.MAX_VALUE - 2, 1000));
  }

  @Test
  void crossPartitionKeyDynamicNeverGetsGentlePlanning(@TempDir Path dir) throws Exception {
    try (Catalog c = catalog(dir)) {
      c.createDatabase("db", true);
      Map<String, String> opts = gentleOptions();
      opts.put("bucket", "-1");
      Schema.Builder schema =
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .column("p", DataTypes.INT())
              .primaryKey("id")
              .partitionKeys("p");
      opts.forEach(schema::option);
      Identifier id = Identifier.create("db", "global_key");
      c.createTable(id, schema.build(), false);
      FileStoreTable t = (FileStoreTable) c.getTable(id);
      assertEquals(BucketMode.KEY_DYNAMIC, t.bucketMode());
      assertFalse(PaimonManagedGentle.isAffected(t));
      assertFalse(evaluate(t).necessary());
      assertEquals(0L, evaluate(t).nextEvaluationTime());
      PendingInputResult result = wrap(t).evaluatePendingInput(context(opts, 0L, 0), 1).get();
      assertFalse(result.optimizingNecessary());
      assertEquals(0L, result.nextEvaluationTime());
      assertFalse(
          ((PaimonPendingInput) result.optimizingPendingInput()).isGentleMinorCompactionPending());
      assertEquals("gentle", t.options().get("lookup-compact"));
    }
  }

  private static Map<String, String> gentleOptions() {
    Map<String, String> opts = options(false);
    opts.put("changelog-producer", "lookup");
    opts.put("lookup-compact", "gentle");
    opts.put("lookup-compact.max-interval", "100");
    opts.put("num-sorted-run.compaction-trigger", "5");
    return opts;
  }

  private static OptimizationContext context(
      Map<String, String> opts, long lastMinor, int interval) {
    OptimizationContext context = mock(OptimizationContext.class);
    when(context.getOptimizingConfig()).thenReturn(config().setMinorLeastInterval(interval));
    when(context.getTableConfig()).thenReturn(opts);
    when(context.getLastMinorOptimizingTime()).thenReturn(lastMinor);
    return context;
  }
}
