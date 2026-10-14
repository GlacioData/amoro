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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.javalin.Javalin;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.formats.paimon.PaimonCatalog;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.server.dashboard.controller.TableController;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
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
import java.util.Collections;

/**
 * Real Paimon + Javalin HTTP contracts. Authorization mapping is covered separately; the local AMS
 * run also checks anonymous access.
 */
class TestTableTagHttp {
  @TempDir java.nio.file.Path warehouse;

  @Test
  void createCopyDeleteAndTypedTagDetailUseExistingEnvelope() throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      catalog.createTable(
          Identifier.create("db", "test"),
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .primaryKey("id")
              .option("bucket", "1")
              .option("write-only", "true")
              .option("self-optimizing.enabled", "true")
              .build(),
          false);
      PaimonCatalog adapter = new PaimonCatalog(catalog, "local", Collections.emptyMap());
      PaimonTable table = (PaimonTable) adapter.loadTable("db", "test");
      FileStoreTable nativeTable = (FileStoreTable) table.originalTable();
      BatchWriteBuilder builder = nativeTable.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(1));
        commit.commit(write.prepareCommit());
      }
      CatalogManager catalogs = mock(CatalogManager.class);
      ServerCatalog server = mock(ServerCatalog.class);
      when(catalogs.getServerCatalog("local")).thenReturn(server);
      when(server.loadTable("db", "test")).thenAnswer(call -> adapter.loadTable("db", "test"));
      TableManager tables = mock(TableManager.class);
      ServerTableDescriptor descriptor =
          new ServerTableDescriptor(catalogs, tables, new Configurations());
      TableController controller =
          new TableController(catalogs, tables, descriptor, new Configurations());
      String base = "/api/ams/v1/tables/catalogs/{catalog}/dbs/{db}/tables/{table}";
      Javalin app =
          Javalin.create(config -> config.jsonMapper(JavalinJsonMapper.createDefaultJsonMapper()))
              .get(base + "/tags", controller::getTableTags)
              .post(base + "/tags", controller::createTableTag)
              .post(base + "/tags/{sourceName}/copy", controller::copyTableTag)
              .post(base + "/tags/{name}/retention", controller::updateTableTagRetention)
              .delete(base + "/tags/{name}", controller::deleteTableTag)
              .get(base + "/snapshots/{snapshotId}/detail", controller::getSnapshotDetail)
              .error(
                  404,
                  context ->
                      DashboardServer.handleHttpError(
                          context, io.javalin.http.HttpCode.NOT_FOUND, "page not found!"))
              .error(
                  500,
                  context ->
                      DashboardServer.handleHttpError(
                          context,
                          io.javalin.http.HttpCode.INTERNAL_SERVER_ERROR,
                          "internal error!"));
      try {
        app.start(0);
        String url =
            "http://127.0.0.1:"
                + app.port()
                + "/api/ams/v1/tables/catalogs/local/dbs/db/tables/test";
        String body =
            "{\"name\":\"release\",\"target\":{\"id\":\"1\",\"branch\":\"main\"},\"scope\":{\"kind\":\"BRANCH\",\"branch\":\"main\"},\"retention\":{\"mode\":\"NO_AGE_LIMIT\"},\"expectedRetentionModel\":\"TAG_CREATION_AGE\"}";
        request("POST", url + "/tags", body.replace("\"1\"", "1"), null, 400);
        request("POST", url + "/tags", body.replace("\"1\"", "\"01\""), null, 400);
        request(
            "POST",
            url + "/tags",
            body.replace("NO_AGE_LIMIT", "AGE_LIMIT")
                .replace("\"mode\":\"AGE_LIMIT\"", "\"mode\":\"AGE_LIMIT\",\"maxAgeMillis\":1.5"),
            null,
            400);
        JsonNode created = request("POST", url + "/tags", body, null, 200).get("result");
        assertEquals("1", created.path("target").path("id").asText());
        assertEquals("tag", created.path("type").asText());
        assertEquals(
            "TAG_ALREADY_EXISTS",
            request("POST", url + "/tags", body, null, 409).path("reason").asText());
        JsonNode list =
            request("GET", url + "/tags?page=1&pageSize=20", null, null, 200).path("result");
        assertEquals(1, list.path("total").asInt());
        assertEquals("release", list.path("list").get(0).path("name").asText());
        String retention =
            "{\"scope\":{\"kind\":\"BRANCH\",\"branch\":\"main\"},\"retention\":{\"mode\":\"AGE_LIMIT\",\"maxAgeMillis\":3600000},\"expectedRetentionModel\":\"TAG_CREATION_AGE\",\"expectedUpdateClock\":\"RESET_ON_UPDATE\"}";
        String retentionUrl = url + "/tags/release/retention";
        String tagToken = created.path("observationToken").asText();
        if (table.tagOperations().get().capabilities().getActions().contains("UPDATE_RETENTION")) {
          request("POST", retentionUrl, retention, null, 428);
          requestRaw("POST", retentionUrl, retention, "*", 400);
          request("POST", retentionUrl, retention, "stale", 412);
          for (String forbidden : new String[] {"target", "newName", "name", "expectedToken"}) {
            request(
                "POST",
                retentionUrl,
                retention.replaceFirst("\\{", "{\"" + forbidden + "\":\"injected\","),
                tagToken,
                400);
          }
          request("POST", retentionUrl, retention.replace("3600000", "1.5"), tagToken, 400);
          request(
              "POST",
              retentionUrl,
              retention.replace("RESET_ON_UPDATE", "PRESERVE_ANCHOR"),
              tagToken,
              412);
          JsonNode updated = request("POST", retentionUrl, retention, tagToken, 200).path("result");
          assertEquals("release", updated.path("name").asText());
          assertEquals("1", updated.path("target").path("id").asText());
          assertEquals(3600000, updated.path("retention").path("maxAgeMillis").asLong());
          request("POST", retentionUrl, retention, tagToken, 412);
          created = updated;
        } else {
          request("POST", retentionUrl, retention, tagToken, 422);
        }
        String copy =
            "{\"newName\":\"copied\",\"expectedSourceToken\":\""
                + created.path("observationToken").asText()
                + "\",\"retention\":{\"mode\":\"NO_AGE_LIMIT\"},\"expectedRetentionModel\":\"TAG_CREATION_AGE\"}";
        JsonNode copied =
            request("POST", url + "/tags/release/copy", copy, null, 200).path("result");
        assertEquals("1", copied.path("target").path("id").asText());
        JsonNode detail =
            request(
                    "GET",
                    url
                        + "/snapshots/1/detail?refType=TAG&ref=release&scopeKind=BRANCH&branch=main",
                    null,
                    null,
                    200)
                .path("result");
        assertTrue(detail.path("list").isArray());
        assertTrue(detail.has("nextCursor"));
        requestRaw("DELETE", url + "/tags/copied", null, "*", 400);
        requestRaw("DELETE", url + "/tags/copied", null, "unquoted", 400);
        requestRaw("DELETE", url + "/tags/copied", null, "W/\"weak\"", 400);
        requestRaw("DELETE", url + "/tags/copied", null, "\"one\",\"two\"", 400);
        request("DELETE", url + "/tags/copied", null, null, 428);
        request("DELETE", url + "/tags/copied", null, "stale", 412);
        assertEquals(
            "ABSENT",
            request(
                    "DELETE",
                    url + "/tags/copied",
                    null,
                    copied.path("observationToken").asText(),
                    200)
                .path("result")
                .path("referenceState")
                .asText());
        request(
            "DELETE", url + "/tags/copied", null, copied.path("observationToken").asText(), 404);
        request("GET", url + "/tags?page=0", null, null, 400);
        assertEquals(
            "TAG_NOT_FOUND",
            request("DELETE", url + "/tags/missing", null, "token", 404).path("reason").asText());
        assertEquals(
            "Snapshot tag no longer exists",
            request("GET", url + "/snapshots/1/detail?refType=TAG&ref=missing", null, null, 404)
                .path("message")
                .asText());
        org.apache.amoro.table.tag.TableTagOperations failing =
            mock(org.apache.amoro.table.tag.TableTagOperations.class);
        org.apache.amoro.table.tag.TagWriteCapabilities capabilities =
            new org.apache.amoro.table.tag.TagWriteCapabilities();
        capabilities.setActions(Collections.singleton("CREATE"));
        when(failing.capabilities()).thenReturn(capabilities);
        when(failing.createTag(org.mockito.ArgumentMatchers.any()))
            .thenThrow(
                new org.apache.amoro.table.tag.TagOperationException(
                    500,
                    "PARTIAL_FAILURE",
                    "APPLIED_WITH_ERRORS",
                    "Callback failed after metadata publication"));
        PaimonTable failedTable = mock(PaimonTable.class);
        when(failedTable.tagOperations()).thenReturn(java.util.Optional.of(failing));
        org.mockito.Mockito.doReturn(failedTable).when(server).loadTable("db", "test");
        JsonNode partial = request("POST", url + "/tags", body, null, 500);
        assertEquals("PARTIAL_FAILURE", partial.path("reason").asText());
        assertEquals("APPLIED_WITH_ERRORS", partial.path("outcome").asText());
      } finally {
        app.stop();
      }
    }
  }

  @Test
  void optionalCapabilityFailurePreservesRealPaimonDetailAndTags() throws Exception {
    try (Catalog nativeCatalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      nativeCatalog.createDatabase("db", true);
      nativeCatalog.createTable(
          Identifier.create("db", "test"),
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .primaryKey("id")
              .option("bucket", "1")
              .option("write-only", "true")
              .option("self-optimizing.enabled", "true")
              .option("tag.default-time-retained", "0 h")
              .build(),
          false);
      PaimonCatalog adapter = new PaimonCatalog(nativeCatalog, "local", Collections.emptyMap());
      PaimonTable loaded = (PaimonTable) adapter.loadTable("db", "test");
      FileStoreTable nativeTable = (FileStoreTable) loaded.originalTable();
      BatchWriteBuilder builder = nativeTable.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(1));
        commit.commit(write.prepareCommit());
      }
      nativeTable.createTag("zero", 1L, java.time.Duration.ZERO);
      CatalogManager catalogs = mock(CatalogManager.class);
      ServerCatalog server = mock(ServerCatalog.class);
      when(catalogs.getServerCatalog("local")).thenReturn(server);
      org.mockito.Mockito.doReturn(loaded).when(server).loadTable("db", "test");
      ServerTableDescriptor descriptor =
          new ServerTableDescriptor(catalogs, mock(TableManager.class), new Configurations());
      org.apache.amoro.api.TableIdentifier id =
          org.apache.amoro.table.TableIdentifier.of("local", "db", "test").buildTableIdentifier();
      org.apache.amoro.table.descriptor.ServerTableMeta detail = descriptor.getTableDetail(id);
      assertEquals("PAIMON", detail.getTableType());
      assertEquals(
          "INVALID_NATIVE_RETENTION", detail.getTagWriteCapabilities().getUnavailableReason());
      assertTrue(detail.getTagWriteCapabilities().getActions().isEmpty());
      assertEquals(
          1L, descriptor.getTableTags(id, new org.apache.amoro.table.tag.TagQuery()).getTotal());
      // A real initial load succeeds; only the subsequent Catalog refresh in discovery fails.
      Catalog unavailableCatalog = org.mockito.Mockito.spy(nativeCatalog);
      org.mockito.Mockito.doThrow(new IllegalStateException("catalog refresh unavailable"))
          .when(unavailableCatalog)
          .getTable(Identifier.create("db", "test"));
      java.lang.reflect.Method context =
          PaimonTable.class.getDeclaredMethod(
              "withTagContext", java.util.function.Supplier.class, boolean.class);
      context.setAccessible(true);
      context.invoke(
          loaded,
          (java.util.function.Supplier<org.apache.paimon.table.Table>)
              () -> {
                try {
                  return unavailableCatalog.getTable(Identifier.create("db", "test"));
                } catch (Catalog.TableNotExistException failure) {
                  throw new IllegalStateException(failure);
                }
              },
          false);
      detail = descriptor.getTableDetail(id);
      assertEquals("PAIMON", detail.getTableType());
      assertTrue(detail.getTagWriteCapabilities().getActions().isEmpty());
      assertEquals(
          "CAPABILITY_READ_FAILED", detail.getTagWriteCapabilities().getUnavailableReason());
      assertEquals(
          1L, descriptor.getTableTags(id, new org.apache.amoro.table.tag.TagQuery()).getTotal());
      org.mockito.Mockito.verify(unavailableCatalog).getTable(Identifier.create("db", "test"));
    }
  }

  @Test
  void existingAuthorizationRulesAllowTagReadsAndRejectViewerWrites() {
    Configurations configuration = new Configurations();
    configuration.set(org.apache.amoro.server.AmoroManagementConf.AUTHORIZATION_ENABLED, true);
    org.apache.amoro.server.authorization.CasbinAuthorizationManager authorization =
        new org.apache.amoro.server.authorization.CasbinAuthorizationManager(configuration);
    org.apache.amoro.server.authorization.DashboardPrivilegeMapper mapping =
        new org.apache.amoro.server.authorization.DashboardPrivilegeMapper();
    for (String method : new String[] {"GET", "POST", "DELETE"}) {
      io.javalin.http.Context context = mock(io.javalin.http.Context.class);
      when(context.method()).thenReturn(method);
      when(context.path())
          .thenReturn(
              "/api/ams/v1/tables/catalogs/local/dbs/db/tables/test/tags/release/retention");
      org.apache.amoro.server.authorization.AuthorizationRequest request =
          mapping.resolve(context).get();
      assertEquals(
          "GET".equals(method)
              ? org.apache.amoro.server.authorization.Privilege.VIEW_TABLE
              : org.apache.amoro.server.authorization.Privilege.MANAGE_TABLE,
          request.getPrivilege());
      assertEquals(
          "GET".equals(method),
          authorization.authorize(
              Collections.singleton(org.apache.amoro.server.authorization.Role.VIEWER), request));
      assertTrue(
          authorization.authorize(
              Collections.singleton(org.apache.amoro.server.authorization.Role.SERVICE_ADMIN),
              request));
    }
  }

  @Test
  void genericServiceRoutesOpaqueVersionIdsAndKeepsUnimplementedFormatsReadOnly() {
    CatalogManager catalogs = mock(CatalogManager.class);
    ServerCatalog catalog = mock(ServerCatalog.class);
    org.apache.amoro.AmoroTable<?> table = mock(org.apache.amoro.AmoroTable.class);
    org.apache.amoro.table.tag.TableTagOperations operations =
        mock(org.apache.amoro.table.tag.TableTagOperations.class);
    when(catalogs.getServerCatalog("future")).thenReturn(catalog);
    org.mockito.Mockito.doReturn(table).when(catalog).loadTable("db", "table");
    org.mockito.Mockito.doReturn(java.util.Optional.of(operations)).when(table).tagOperations();
    org.apache.amoro.table.tag.TagWriteCapabilities capabilities =
        new org.apache.amoro.table.tag.TagWriteCapabilities();
    capabilities.setActions(Collections.singleton("CREATE"));
    when(operations.capabilities()).thenReturn(capabilities);
    org.apache.amoro.table.tag.CreateTagRequest request =
        new org.apache.amoro.table.tag.CreateTagRequest();
    request.setName("release");
    request.setTarget(
        org.apache.amoro.table.tag.TableVersionRef.of(
            "bda791ee-daa7-4f9e-90c4-591fa91359e1", null));
    request.getRetention().setMode("NO_AGE_LIMIT");
    org.apache.amoro.table.tag.TableTagInfo expected =
        new org.apache.amoro.table.tag.TableTagInfo();
    expected.setTarget(request.getTarget());
    when(operations.createTag(request)).thenReturn(expected);
    TableTagService service = new TableTagService(catalogs);
    org.apache.amoro.table.TableIdentifier id =
        org.apache.amoro.table.TableIdentifier.of("future", "db", "table");
    assertEquals(request.getTarget().getId(), service.create(id, request).getTarget().getId());
    org.apache.amoro.table.tag.UpdateTagRetentionRequest update =
        new org.apache.amoro.table.tag.UpdateTagRetentionRequest();
    update.setName("release");
    update.setRetention(new org.apache.amoro.table.tag.TagRetentionRequest());
    update.getRetention().setMode("NO_AGE_LIMIT");
    update.setExpectedRetentionModel("TARGET_COMMIT_AGE");
    update.setExpectedUpdateClock("PRESERVE_ANCHOR");
    update.setExpectedToken("opaque-observation");
    assertEquals(
        422,
        org.junit.jupiter.api.Assertions.assertThrows(
                org.apache.amoro.table.tag.TagOperationException.class,
                () -> service.updateRetention(id, update))
            .getStatus());
    capabilities.setActions(Collections.singleton("UPDATE_RETENTION"));
    when(operations.updateTagRetention(update)).thenReturn(expected);
    assertEquals(
        request.getTarget().getId(), service.updateRetention(id, update).getTarget().getId());
    org.mockito.Mockito.doReturn(java.util.Optional.empty()).when(table).tagOperations();
    assertEquals(
        422,
        org.junit.jupiter.api.Assertions.assertThrows(
                org.apache.amoro.table.tag.TagOperationException.class,
                () -> service.create(id, request))
            .getStatus());
  }

  private JsonNode request(String method, String url, String body, String match, int status)
      throws Exception {
    return requestRaw(method, url, body, match == null ? null : "\"" + match + "\"", status);
  }

  private JsonNode requestRaw(String method, String url, String body, String match, int status)
      throws Exception {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod(method);
    connection.setReadTimeout(10000);
    if (match != null) {
      connection.setRequestProperty("If-Match", match);
    }
    if (body != null) {
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      try (java.io.OutputStream output = connection.getOutputStream()) {
        output.write(body.getBytes(StandardCharsets.UTF_8));
      }
    }
    int observed = connection.getResponseCode();
    try (java.io.InputStream stream =
        observed < 400 ? connection.getInputStream() : connection.getErrorStream()) {
      JsonNode response = new ObjectMapper().readTree(stream);
      assertEquals(status, observed, response.toString());
      return response;
    } finally {
      connection.disconnect();
    }
  }
}
