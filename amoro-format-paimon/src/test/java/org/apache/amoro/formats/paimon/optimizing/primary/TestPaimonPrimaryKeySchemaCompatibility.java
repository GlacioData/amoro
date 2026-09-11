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

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionSchema;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

public class TestPaimonPrimaryKeySchemaCompatibility {
  @Test
  void coveredSchemaAvoidsMetadataIo(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable original = spy(table(catalog, "schema", options(true)));
      assertSame(original, PaimonCompactionSchema.refresh(original, original.schema().id(), 1));
      verify(original, never()).copyWithLatestSchema();
      verify(original, never()).schemaManager();
    }
  }

  @Test
  void additionAndReorderRetainRuntimeOverrides(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable original =
          table(catalog, "schema", options(true))
              .copy(Collections.singletonMap("target-file-size", "64 mb"));
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Arrays.asList(
              SchemaChange.addColumn("new_col", DataTypes.INT()),
              SchemaChange.updateColumnPosition(SchemaChange.Move.last("a"))),
          false);
      FileStoreTable refreshed = PaimonCompactionSchema.refresh(original, 1, 1);
      assertEquals(
          Arrays.asList("id", "b", "seq", "new_col", "a"), refreshed.schema().fieldNames());
      assertEquals(original.options(), refreshed.options());
      assertEquals(0, original.schema().id());
      assertEquals(1, refreshed.schema().id());
    }
  }

  @Test
  void defaultChangeIsRejected(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable original = table(catalog, "schema", options(true));
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Arrays.asList(
              SchemaChange.addColumn("new_col", DataTypes.INT()),
              SchemaChange.updateColumnDefaultValue(new String[] {"a"}, "7")),
          false);
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class, () -> PaimonCompactionSchema.refresh(original, 1, 1));
      assertTrue(error.getCause().getMessage().contains("Incompatible existing field"));
    }
  }

  @Test
  void missingHistoricalOptionsIsNotTreatedAsCompatible(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable original = table(catalog, "schema", options(true));
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Collections.singletonList(SchemaChange.addColumn("new_col", DataTypes.INT())),
          false);
      FileStoreTable tracked = spy(original);
      doReturn(spy(original.copyWithLatestSchema())).when(tracked).copyWithLatestSchema();
      SchemaManager manager = spy(original.schemaManager());
      doThrow(new IllegalStateException("historical schema unavailable"))
          .when(manager)
          .schema(original.schema().id());
      doReturn(manager).when(tracked).schemaManager();
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class, () -> PaimonCompactionSchema.refresh(tracked, 1, 1));
      assertEquals("historical schema unavailable", error.getCause().getMessage());
    }
  }
}
