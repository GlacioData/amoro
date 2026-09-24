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

package org.apache.amoro.server.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.javalin.Javalin;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.PaimonTableCreationTime;
import org.apache.amoro.formats.paimon.PaimonTableDescriptor;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.controller.TableController;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.DefaultTableRuntimeStore;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.server.table.TableService;
import org.apache.amoro.table.TableIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;

/**
 * Real local Paimon metadata and HTTP serialization; catalog/runtime service boundaries are mocked.
 */
public class TestPaimonDetailHttp {
  @TempDir Path warehouse;

  @Test
  void detailHttpReturnsBucketModeAndCachedCreationTime() throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      Identifier id = Identifier.create("db", "demo");
      catalog.createTable(
          id,
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .primaryKey("id")
              .option("bucket", "2")
              .build(),
          true);
      FileStoreTable table = (FileStoreTable) catalog.getTable(id);
      long created = table.schemaManager().schema(0).timeMillis();
      if (table.schema().id() == 0) {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
            BatchTableCommit commit = builder.newCommit()) {
          write.write(GenericRow.of(1));
          commit.commit(write.prepareCommit());
        }
        catalog.alterTable(id, SchemaChange.addColumn("name", DataTypes.STRING()), false);
      }

      CatalogManager catalogs = mock(CatalogManager.class);
      when(catalogs.catalogExist("local")).thenReturn(true);
      TableManager tables = mock(TableManager.class);
      ServerTableIdentifier identifier =
          ServerTableIdentifier.of(1L, "local", "db", "demo", TableFormat.PAIMON);
      when(tables.getServerTableIdentifier(any(org.apache.amoro.api.TableIdentifier.class)))
          .thenReturn(identifier);
      TableRuntimeMeta runtimeMeta = new TableRuntimeMeta();
      runtimeMeta.setStatusCode(700);
      when(tables.getTableRuntimeMata(identifier)).thenReturn(runtimeMeta);
      DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
      when(runtime.getRuntimeHealthSnapshot()).thenReturn(Optional.empty());
      PaimonTableCreationTime creationTime = new PaimonTableCreationTime();
      creationTime.setCreateTime(created);
      TableRuntimeState persisted = new TableRuntimeState();
      persisted.setStateKey(PaimonTableCreationTime.KEY.getKey());
      persisted.setStateValue(PaimonTableCreationTime.KEY.serialize(creationTime));
      DefaultTableRuntimeStore store =
          new DefaultTableRuntimeStore(
              identifier,
              runtimeMeta,
              Collections.singletonList(PaimonTableCreationTime.KEY),
              Collections.singletonList(persisted));
      when(runtime.getState(PaimonTableCreationTime.KEY))
          .thenAnswer(call -> store.getState(PaimonTableCreationTime.KEY));
      TableService service = mock(TableService.class);
      when(service.getRuntime(1L)).thenReturn(runtime);
      ServerTableDescriptor descriptor = mock(ServerTableDescriptor.class);
      PaimonTableDescriptor nativeDescriptor = new PaimonTableDescriptor();
      doAnswer(
              call ->
                  nativeDescriptor.getTableDetail(
                      new PaimonTable(
                          TableIdentifier.of("local", "db", "demo"), catalog.getTable(id))))
          .when(descriptor)
          .getTableDetail(any());
      TableController controller =
          new TableController(catalogs, tables, descriptor, new Configurations(), service);
      Javalin app =
          Javalin.create()
              .get(
                  "/catalogs/{catalog}/dbs/{db}/tables/{table}/details",
                  controller::getTableDetail);
      try {
        app.start(0);
        URL url =
            new URL(
                "http://127.0.0.1:" + app.port() + "/catalogs/local/dbs/db/tables/demo/details");
        String json = request(url);
        assertTrue(json.contains("\"bucketMode\":\"HASH_FIXED\""), json);
        assertTrue(
            json.contains("\"tableModel\":{\"kind\":\"BUCKET_MODE\",\"code\":\"HASH_FIXED\"}"),
            json);
        assertTrue(json.contains("\"createTime\":" + created), json);
      } finally {
        app.stop();
      }
    }
  }

  private static String request(URL url) throws Exception {
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    assertEquals(200, connection.getResponseCode());
    try (java.io.InputStream in = connection.getInputStream();
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
      byte[] buffer = new byte[4096];
      int count;
      while ((count = in.read(buffer)) != -1) {
        out.write(buffer, 0, count);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }
}
