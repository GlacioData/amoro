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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.commit;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.files;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class TestPaimonPrimaryKeySchemaFailure {
  @ParameterizedTest
  @ValueSource(strings = {"behind", "missing", "io"})
  void failsBeforeWriterWhenSchemaCannotBeLoaded(String mode, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable old = table(catalog, "schema", options(true));
      append(old, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyCompactionInput planned = input(old, OptimizingType.MINOR, 99L);
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Collections.singletonList(SchemaChange.addColumn("new_col", DataTypes.INT())),
          false);
      FileStoreTable current = (FileStoreTable) catalog.getTable(Identifier.create("db", "schema"));
      append(current, GenericRow.of(1, 11, 21, 2L, 12345));
      FileStoreTable tracked = spy(old);
      if (mode.equals("io")) {
        doThrow(new IllegalStateException("schema IO unavailable"))
            .when(tracked)
            .copyWithLatestSchema();
      } else if (mode.equals("missing")) {
        SchemaManager manager = spy(old.schemaManager());
        doReturn(Optional.empty()).when(manager).latest();
        doReturn(manager).when(tracked).schemaManager();
      } else {
        doReturn(tracked).when(tracked).copyWithLatestSchema();
      }
      long snapshot = current.latestSnapshot().get().id();
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () -> new PaimonPrimaryKeyCompactionExecutor(withTable(planned, tracked)).execute());
      assertTrue(error.getMessage().contains("requiredSchemaId=1"));
      assertTrue(error.getMessage().contains("refreshAttempts=" + (mode.equals("io") ? 1 : 3)));
      verify(tracked, times(mode.equals("io") ? 1 : 3)).copyWithLatestSchema();
      verify(tracked, never()).copy(anyMap());
      verify(tracked, never()).newBatchWriteBuilder();
      assertEquals(snapshot, current.latestSnapshot().get().id());
      assertEquals(2, files(current).size());
    }
  }

  @Test
  void thirdRefreshCanRecoverWithoutChangingTaskIdentity(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(true);
      opts.put("num-sorted-run.compaction-trigger", "2");
      FileStoreTable old = table(catalog, "schema", opts);
      append(old, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyCompactionInput planned = input(old, OptimizingType.MINOR, 99L);
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Collections.singletonList(SchemaChange.addColumn("new_col", DataTypes.INT())),
          false);
      FileStoreTable current = (FileStoreTable) catalog.getTable(Identifier.create("db", "schema"));
      append(current, GenericRow.of(1, 11, 21, 2L, 12345));
      FileStoreTable tracked = spy(old);
      doReturn(tracked, tracked, spy(old.copyWithLatestSchema()))
          .when(tracked)
          .copyWithLatestSchema();
      PaimonPrimaryKeyCompactionInput in = withTable(planned, tracked);
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(in).execute();
      verify(tracked, times(3)).copyWithLatestSchema();
      assertEquals(2, output.getCompactedFileCount());
      commit(current, in, output);
      assertEquals(
          Collections.singletonList("1|11|2|12345"),
          TestPaimonPrimaryKeySchemaEvolution.read(current));
      assertEquals(planned.getCommitIdentifier(), in.getCommitIdentifier());
      assertEquals(planned.getTargetSnapshotId(), in.getTargetSnapshotId());
    }
  }

  @Test
  void changedAggregationOptionsRejectOldTaskAndFreshTaskPreservesSum(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(false);
      opts.put("num-sorted-run.compaction-trigger", "2");
      opts.put("merge-engine", "aggregation");
      opts.put("fields.a.aggregate-function", "sum");
      opts.put("fields.b.aggregate-function", "sum");
      opts.put("fields.seq.aggregate-function", "max");
      FileStoreTable old = table(catalog, "schema", opts);
      append(old, GenericRow.of(1, 10, 20, 1L));
      append(old, GenericRow.of(1, 11, 21, 2L));
      PaimonPrimaryKeyCompactionInput planned = input(old, OptimizingType.MINOR, 99L);
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Arrays.asList(
              SchemaChange.addColumn("new_col", DataTypes.INT()),
              SchemaChange.setOption("fields.new_col.aggregate-function", "sum")),
          false);
      FileStoreTable current = (FileStoreTable) catalog.getTable(Identifier.create("db", "schema"));
      append(current, GenericRow.of(1, 12, 22, 3L, 10));
      append(current, GenericRow.of(1, 13, 23, 4L, 20));
      long snapshot = current.latestSnapshot().get().id();
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () -> new PaimonPrimaryKeyCompactionExecutor(planned).execute());
      assertTrue(error.getCause().getMessage().contains("Persisted table options changed"));
      assertEquals(snapshot, current.latestSnapshot().get().id());
      assertEquals(4, files(current).size());
      PaimonPrimaryKeyCompactionInput fresh = withTable(planned, current);
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(fresh).execute();
      assertEquals(4, output.getCompactedFileCount());
      commit(current, fresh, output);
      assertEquals(
          Collections.singletonList("1|46|4|30"),
          TestPaimonPrimaryKeySchemaEvolution.read(current));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"drop", "rename", "type", "options"})
  void incompatibleDdlFailsBeforeWriter(String mode, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable old = table(catalog, "schema", options(true));
      append(old, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyCompactionInput planned = input(old, OptimizingType.MINOR, 99L);
      SchemaChange change;
      GenericRow row;
      switch (mode) {
        case "drop":
          change = SchemaChange.dropColumn("a");
          row = GenericRow.of(1, 21, 2L, 12345);
          break;
        case "rename":
          change = SchemaChange.renameColumn("a", "renamed");
          row = GenericRow.of(1, 11, 21, 2L, 12345);
          break;
        case "type":
          change = SchemaChange.updateColumnType("a", DataTypes.BIGINT());
          row = GenericRow.of(1, 11L, 21, 2L, 12345);
          break;
        default:
          change = SchemaChange.setOption("target-file-size", "64 mb");
          row = GenericRow.of(1, 11, 21, 2L, 12345);
      }
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Arrays.asList(change, SchemaChange.addColumn("new_col", DataTypes.INT())),
          false);
      FileStoreTable current = (FileStoreTable) catalog.getTable(Identifier.create("db", "schema"));
      append(current, row);
      FileStoreTable tracked = spy(old);
      PaimonPrimaryKeyCompactionInput in = withTable(planned, tracked);
      assertThrows(
          IllegalStateException.class, () -> new PaimonPrimaryKeyCompactionExecutor(in).execute());
      verify(tracked, never()).copy(anyMap());
      verify(tracked, never()).newBatchWriteBuilder();
      assertEquals(2, current.latestSnapshot().get().id());
    }
  }

  @ParameterizedTest
  @CsvSource({"drop,false", "rename,false", "drop,true", "rename,true"})
  void intermediateAddedThenDroppedOrRenamedColumnRejectsOldTaskAndHealsOnReplan(
      String mode, boolean writeLatest, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(true);
      opts.put("num-sorted-run.compaction-trigger", "2");
      FileStoreTable schema0 = table(catalog, "pk_intermediate", opts);
      append(schema0, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyCompactionInput plannedAt0 = input(schema0, OptimizingType.MINOR, 101L);

      Identifier id = Identifier.create("db", "pk_intermediate");
      catalog.alterTable(
          id,
          Collections.singletonList(SchemaChange.addColumn("temp_col", DataTypes.INT())),
          false);
      FileStoreTable schema1 = (FileStoreTable) catalog.getTable(id);
      append(schema1, GenericRow.of(1, 11, 21, 2L, 777));

      SchemaChange step2 =
          mode.equals("drop")
              ? SchemaChange.dropColumn("temp_col")
              : SchemaChange.renameColumn("temp_col", "renamed_col");
      catalog.alterTable(id, Collections.singletonList(step2), false);

      FileStoreTable schema2 = (FileStoreTable) catalog.getTable(id);
      if (writeLatest) {
        append(
            schema2,
            mode.equals("drop") ? GenericRow.of(1, 12, 22, 3L) : GenericRow.of(1, 12, 22, 3L, 888));
      }

      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () -> new PaimonPrimaryKeyCompactionExecutor(plannedAt0).execute());
      assertTrue(error.getMessage().contains("Unsafe Paimon compaction schema"));

      // Replanning at schema 2 self-heals and compacts the historical schema 1 file cleanly.
      PaimonPrimaryKeyCompactionInput freshAt2 = input(schema2, OptimizingType.MINOR, 102L);
      PaimonPrimaryKeyCompactionOutput freshOutput =
          new PaimonPrimaryKeyCompactionExecutor(freshAt2).execute();
      assertEquals(writeLatest ? 3 : 2, freshOutput.getCompactedFileCount());
      commit(schema2, freshAt2, freshOutput);
    }
  }

  static PaimonPrimaryKeyCompactionInput withTable(
      PaimonPrimaryKeyCompactionInput planned, FileStoreTable table) {
    return new PaimonPrimaryKeyCompactionInput(
        wrap(table),
        planned.getUnits(),
        planned.getOptimizingType(),
        planned.isFullCompaction(),
        planned.getTargetSnapshotId(),
        planned.getCommitUser(),
        planned.getCommitIdentifier());
  }
}
