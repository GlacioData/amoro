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

package org.apache.amoro.server.process.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.PaimonTableCreationTime;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.table.DefaultTableRuntimeStore;
import org.apache.amoro.server.table.DerbyPersistence;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.hadoop.conf.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

class TestPaimonTableCreationTime {
  @TempDir Path warehouse;

  @Test
  void schemaEvolutionKeepsSchemaZeroTimeAndRetainedFallbackWorks() throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", false);
      Identifier id = Identifier.create("db", "test");
      catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
      FileStoreTable original = (FileStoreTable) catalog.getTable(id);
      long firstTime = original.schema().timeMillis();
      catalog.alterTable(id, SchemaChange.addColumn("name", DataTypes.STRING()), false);
      FileStoreTable evolved = (FileStoreTable) catalog.getTable(id);
      assertEquals(1, evolved.schema().id());
      PaimonTableCreationTime first = PaimonTableCreationTime.read(evolved);
      assertEquals(firstTime, first.getCreateTime());
      assertEquals(0, first.getSchemaId());
      assertEquals("SCHEMA_TIME", first.getSource());
      evolved.fileIO().delete(evolved.schemaManager().toSchemaPath(0), false);
      PaimonTableCreationTime retained = PaimonTableCreationTime.read(evolved);
      assertEquals(1, retained.getSchemaId());
      assertEquals(evolved.schema().timeMillis(), retained.getCreateTime());
    }
  }

  @Test
  void legacyTimestampUsesPaimonFileTimeAndOrdersSchemasById() {
    FileStoreTable table = mock(FileStoreTable.class);
    SchemaManager manager = mock(SchemaManager.class);
    TableSchema schema = mock(TableSchema.class);
    when(table.schemaManager()).thenReturn(manager);
    when(manager.listAllIds()).thenReturn(Arrays.asList(8L, 3L, 5L));
    when(manager.schema(3)).thenReturn(schema);
    when(manager.earliestCreationTime()).thenReturn(1700000000123L);
    PaimonTableCreationTime result = PaimonTableCreationTime.read(table);
    assertEquals(3, result.getSchemaId());
    assertEquals(1700000000123L, result.getCreateTime());
    assertEquals("SCHEMA_FILE_MTIME", result.getSource());
    verify(manager, times(1)).schema(3);
    when(manager.listAllIds()).thenReturn(Collections.emptyList());
    assertThrows(IllegalStateException.class, () -> PaimonTableCreationTime.read(table));
  }

  @Test
  void memoryProjectionHasNoTableLoadAndUnknownTimeStaysEmpty() {
    TableRuntime runtime = mock(TableRuntime.class);
    ServerTableMeta response = new ServerTableMeta();
    PaimonTableCreationTime.project(null, response);
    when(runtime.getState(PaimonTableCreationTime.KEY)).thenReturn(new PaimonTableCreationTime());
    PaimonTableCreationTime.project(runtime, response);
    assertEquals(0, response.getCreateTime());
    PaimonTableCreationTime known = new PaimonTableCreationTime();
    known.setCreateTime(1700000000000L);
    when(runtime.getState(PaimonTableCreationTime.KEY)).thenReturn(known);
    PaimonTableCreationTime.project(runtime, response);
    assertEquals(known.getCreateTime(), response.getCreateTime());
    verify(runtime, times(2)).getState(PaimonTableCreationTime.KEY);
    verifyNoMoreInteractions(runtime);
  }

  @Test
  void backgroundSyncPersistsTimeRestoresItAndPreservesItOnFailure() throws Exception {
    new DerbyPersistence();
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", false);
      Identifier id = Identifier.create("db", "test");
      catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
      FileStoreTable table = (FileStoreTable) catalog.getTable(id);
      ServerTableIdentifier identifier =
          ServerTableIdentifier.of("creation-test", "db", "test", TableFormat.PAIMON);
      TableRuntimeMeta meta = new TableRuntimeMeta();
      meta.setGroupName("default");
      meta.setStatusCode(700);
      meta.setBucketId("0000");
      meta.setTableConfig(Collections.emptyMap());
      try (SqlSession session = SqlSessionFactoryProvider.getInstance().get().openSession(true)) {
        session.getMapper(TableMetaMapper.class).insertTable(identifier);
        meta.setTableId(identifier.getId());
        session.getMapper(TableRuntimeMapper.class).insertRuntime(meta);
      }
      DefaultTableRuntimeStore store =
          new DefaultTableRuntimeStore(
              identifier,
              meta,
              Collections.singletonList(PaimonTableCreationTime.KEY),
              Collections.emptyList());
      TableRuntime runtime = mock(TableRuntime.class);
      when(runtime.getTableIdentifier()).thenReturn(identifier);
      doReturn(new PaimonTable(TableIdentifier.of("creation-test", "db", "test"), table))
          .when(runtime)
          .loadTable();
      doAnswer(
              call -> {
                Function<PaimonTableCreationTime, PaimonTableCreationTime> updater =
                    call.getArgument(1);
                store.begin().updateState(PaimonTableCreationTime.KEY, updater).commit();
                return null;
              })
          .when(runtime)
          .updateState(eq(PaimonTableCreationTime.KEY), any());
      new PaimonTableMetaSyncProcess(runtime).run();
      long expected = table.schema().timeMillis();
      assertEquals(expected, store.getState(PaimonTableCreationTime.KEY).getCreateTime());
      try (SqlSession session = SqlSessionFactoryProvider.getInstance().get().openSession(true)) {
        org.apache.amoro.server.persistence.TableRuntimeState state =
            session
                .getMapper(TableRuntimeMapper.class)
                .getState(identifier.getId(), PaimonTableCreationTime.KEY.getKey());
        DefaultTableRuntimeStore restored =
            new DefaultTableRuntimeStore(
                identifier,
                meta,
                Collections.singletonList(PaimonTableCreationTime.KEY),
                Collections.singletonList(state));
        assertEquals(expected, restored.getState(PaimonTableCreationTime.KEY).getCreateTime());
      }
      // No schemas remain: ancillary read failure must preserve the last successful state.
      table.fileIO().delete(table.schemaManager().toSchemaPath(0), false);
      new PaimonTableMetaSyncProcess(runtime).run();
      assertEquals(expected, store.getState(PaimonTableCreationTime.KEY).getCreateTime());
    }
  }
}
