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
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.Map;

class TestPaimonPrimaryKeyMajorWakeup {
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
  void lowerThresholdsAdmitMajorWithoutANewSnapshot(
      String producer, String bucket, @TempDir Path dir) throws Exception {
    try (Catalog catalog = catalog(dir)) {
      Map<String, String> options = options(false);
      options.put("bucket", bucket);
      options.put("changelog-producer", producer);
      options.put("lookup-compact", "radical");
      options.put("lookup-wait", "false");
      options.put("num-sorted-run.compaction-trigger", "10");
      FileStoreTable table = table(catalog, "major", options);
      OptimizingConfig config = config().setMinorLeastInterval(-1);
      OptimizationContext context = mock(OptimizationContext.class);
      when(context.getOptimizingConfig()).thenReturn(config);
      when(context.getTableConfig()).thenReturn(options);
      assertFalse(wrap(table).evaluatePendingInput(context, 1).get().optimizingNecessary());
      for (int i = 0; i < 3; i++) {
        append(table, GenericRow.of(1, i, i, (long) i));
      }
      long snapshot = table.latestSnapshot().get().id();
      PendingInputResult before = wrap(table).evaluatePendingInput(context, 1).get();
      assertFalse(before.optimizingNecessary());
      assertFalse(
          ((PaimonPendingInput) before.optimizingPendingInput()).isSameSnapshotPlanningPending());
      Identifier id = Identifier.create("db", "major");
      catalog.alterTable(
          id, SchemaChange.setOption("num-sorted-run.compaction-trigger", "2"), false);
      catalog.alterTable(id, SchemaChange.setOption("num-sorted-run.stop-trigger", "2"), false);
      FileStoreTable changed = (FileStoreTable) catalog.getTable(id);
      when(context.getTableConfig()).thenReturn(changed.options());
      assertEquals(snapshot, changed.latestSnapshot().get().id());
      assertEquals(OptimizingType.MAJOR, evaluate(changed).optimizingType());
      PendingInputResult after = wrap(changed).evaluatePendingInput(context, 1).get();
      assertTrue(after.optimizingNecessary());
      PaimonPendingInput pending = (PaimonPendingInput) after.optimizingPendingInput();
      assertTrue(pending.isSameSnapshotPlanningPending());
      assertTrue(pending.canPlanWithoutSnapshotChange(context, System.currentTimeMillis()));
      assertFalse(pending.isFullCompactionPending());
      assertFalse(pending.isGentleMinorCompactionPending());
      config.setEnabled(false);
      PendingInputResult disabled = wrap(changed).evaluatePendingInput(context, 1).get();
      assertFalse(disabled.optimizingNecessary());
      assertFalse(
          ((PaimonPendingInput) disabled.optimizingPendingInput()).isSameSnapshotPlanningPending());
    }
  }
}
