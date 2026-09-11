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

import static org.apache.amoro.formats.paimon.optimizing.TestPaimonAppendSchemaEvolution.append;
import static org.apache.amoro.formats.paimon.optimizing.TestPaimonAppendSchemaEvolution.plan;
import static org.apache.amoro.formats.paimon.optimizing.TestPaimonAppendSchemaEvolution.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestPaimonAppendSchemaFailure {
  @ParameterizedTest
  @ValueSource(strings = {"behind", "io", "options"})
  void rejectsUnsafeSchemaBeforeCreatingAppendWriter(String mode, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      Identifier id = Identifier.create("db", "append_schema");
      catalog.createTable(
          id,
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .column("a", DataTypes.INT())
              .option("bucket", "-1")
              .option("write-only", "true")
              .option("self-optimizing.enabled", "true")
              .option("compaction.min.file-num", "2")
              .build(),
          false);
      AppendOnlyFileStoreTable old = (AppendOnlyFileStoreTable) catalog.getTable(id);
      append(old, GenericRow.of(1, 10));
      append(old, GenericRow.of(2, 20));
      List<SchemaChange> changes = new ArrayList<>();
      changes.add(SchemaChange.addColumn("new_col", DataTypes.INT()));
      if (mode.equals("options")) {
        changes.add(SchemaChange.setOption("target-file-size", "64 mb"));
      }
      catalog.alterTable(id, changes, false);
      AppendOnlyFileStoreTable current = (AppendOnlyFileStoreTable) catalog.getTable(id);
      append(current, GenericRow.of(3, 30, 12345));
      PaimonCompactionInput original = plan(old).get(0).getInput();
      AppendOnlyFileStoreTable tracked = spy(old);
      AppendOnlyFileStore store = spy(old.store());
      doReturn(store).when(tracked).store();
      if (mode.equals("behind")) {
        doReturn(tracked).when(tracked).copyWithLatestSchema();
      } else if (mode.equals("io")) {
        doThrow(new IllegalStateException("schema IO unavailable"))
            .when(tracked)
            .copyWithLatestSchema();
      } else {
        doReturn(spy(old.copyWithLatestSchema())).when(tracked).copyWithLatestSchema();
      }
      PaimonCompactionInput input =
          new PaimonCompactionInput(
              wrap(tracked),
              original.getTaskBytes(),
              original.getSerializerVersion(),
              original.getCommitUser(),
              original.getPartitionPath(),
              original.getTargetSnapshotId(),
              original.getCommitIdentifier());
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class, () -> new PaimonCompactionExecutor(input).execute());
      assertTrue(error.getMessage().contains("Unsafe Paimon compaction schema"));
      if (mode.equals("options")) {
        assertTrue(error.getCause().getMessage().contains("Persisted table options changed"));
      }
      verify(tracked, times(mode.equals("behind") ? 3 : 1)).copyWithLatestSchema();
      verify(store, never()).newWrite(anyString());
      assertEquals(3, current.latestSnapshot().get().id());
    }
  }
}
