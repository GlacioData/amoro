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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.Javalin;
import io.javalin.http.Context;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.exception.ObjectNotExistsException;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.AmoroServiceContainer;
import org.apache.amoro.server.PlainFailures;
import org.apache.amoro.server.RestCatalogService;
import org.apache.amoro.server.authorization.CasbinAuthorizationManager;
import org.apache.amoro.server.authorization.Role;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.controller.LoginController;
import org.apache.amoro.server.resource.OptimizerManager;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.server.table.internal.InternalTableManager;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.utils.JacksonUtil;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Error responses tell the client what went wrong with its request, but not what went wrong inside
 * the server. Validation messages stay; infrastructure failures come back as a generic text.
 */
class TestErrorMessages {

  private static final String INTERNAL = "Internal server error";

  static Stream<Arguments> internalFailures() {
    return Stream.of(
        Arguments.of(
            new PersistenceException(
                "### Error querying database. Cause: jdbc:mysql://db-host:3306/ams: Access denied"
                    + " for user 'ams'\n### The error may exist in catalog_storage\n"
                    + "### SQL: select * from catalog_storage")),
        Arguments.of(
            new UncheckedIOException(new ConnectException("Connection refused to hms-host:9083"))),
        Arguments.of(new NullPointerException()),
        Arguments.of(
            new RuntimeException(
                new IOException("java.net.ConnectException: nn-host:8020 Connection refused"))),
        Arguments.of(new ClassCastException("CatalogMeta cannot be cast to ServerCatalog")),
        // Types that usually carry validation, but here with infrastructure text.
        Arguments.of(new IllegalArgumentException("java.net.UnknownHostException: hdfs-nn")),
        Arguments.of(
            new IllegalStateException("Wrong FS: hdfs://nn-host:8020/a, expected: file:///")),
        Arguments.of(
            new NullPointerException(
                "Cannot invoke \"java.util.Map.get(Object)\" because \"properties\" is null")),
        // A plain RuntimeException raised below the dashboard layer.
        Arguments.of(PlainFailures.plain("table is not managed by AMS")));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("internalFailures")
  void internalFailuresAreAnsweredWithAGenericMessage(Exception failure) throws Exception {
    CatalogManager catalogs = mock(CatalogManager.class);
    // thenThrow() would refill the stack trace at the stubbed call, inside the dashboard
    // layer; the answer keeps the failure's own origin.
    when(catalogs.listCatalogMetas())
        .thenAnswer(
            invocation -> {
              throw failure;
            });
    Javalin app = app(catalogs);
    try {
      JsonNode response = json(call(app, "GET", "/api/ams/v1/catalogs", null, true));
      String body = response.toString();
      assertEquals(500, response.path("code").asInt(), body);
      assertEquals(INTERNAL, response.path("message").asText(), String.valueOf(failure));
      assertFalse(
          body.contains("db-host") || body.contains("hms-host") || body.contains("nn-host"), body);
    } finally {
      app.stop();
    }
  }

  @Test
  void validationMessagesAreStillReturned() throws Exception {
    CatalogManager catalogs = mock(CatalogManager.class);
    when(catalogs.listCatalogMetas()).thenReturn(Collections.emptyList());
    when(catalogs.catalogExist("demo")).thenReturn(true);
    when(catalogs.getCatalogMeta("missing"))
        .thenThrow(new ObjectNotExistsException("Catalog missing"));
    Javalin app = app(catalogs);
    try {
      // Guava Preconditions.checkArgument in TableController
      assertEquals(
          "page[0] must > 0",
          message(
              call(
                  app,
                  "GET",
                  "/api/ams/v1/tables/catalogs/c/dbs/d/tables/t/optimizing-processes?page=0",
                  null,
                  true)));
      // The project's own not-found exception (its message ends with "not exists")
      assertEquals(
          "Catalog missing not exists",
          message(call(app, "GET", "/api/ams/v1/catalogs/missing", null, true)));
      // BadRequestException raised by a controller
      assertEquals(
          "Page offset is too large",
          message(
              call(
                  app,
                  "GET",
                  "/api/ams/v1/optimize/optimizerGroups/g/tables?page=2147483647&pageSize=2147483647",
                  null,
                  true)));
      // A plain RuntimeException raised in the dashboard layer itself
      assertEquals(
          "Duplicate catalog name!",
          message(
              call(
                  app,
                  "POST",
                  "/api/ams/v1/catalogs",
                  "{\"name\":\"demo\",\"type\":\"hadoop\",\"optimizerGroup\":\"default\","
                      + "\"tableFormatList\":[\"ICEBERG\"],"
                      + "\"storageConfig\":{\"storage.type\":\"Hadoop\"},"
                      + "\"authConfig\":{\"auth.type\":\"simple\"},"
                      + "\"properties\":{\"warehouse\":\"/tmp/warehouse\"},"
                      + "\"tableProperties\":{}}",
                  true)));
      // Guava Preconditions.checkState
      assertEquals(
          "invalid catalog!",
          message(
              call(
                  app,
                  "GET",
                  "/api/ams/v1/tables/catalogs/none/dbs/d/tables/t/details",
                  null,
                  true)));
    } finally {
      app.stop();
    }
  }

  @Test
  void loginFailureDoesNotExposeTheAuthenticationCause() throws Exception {
    Javalin app = app(mock(CatalogManager.class));
    try {
      JsonNode response =
          json(
              call(
                  app,
                  "POST",
                  "/api/ams/v1/login",
                  "{\"user\":\"admin\",\"password\":\"wrong-password\"}",
                  false));
      assertEquals(500, response.path("code").asInt());
      assertEquals("invalid user admin or password!", response.path("message").asText());
    } finally {
      app.stop();
    }
  }

  @Test
  void aRequestBodyThatCannotBeParsedIsNotEchoed() throws Exception {
    // Any client can send this: the JSON mapper wraps its parse error together with the input.
    CatalogManager catalogs = mock(CatalogManager.class);
    Javalin app = app(catalogs);
    try {
      JsonNode response = json(call(app, "POST", "/api/ams/v1/catalogs", "{\"name\":", true));
      assertEquals(500, response.path("code").asInt(), response.toString());
      assertEquals(INTERNAL, response.path("message").asText(), response.toString());
    } finally {
      app.stop();
    }
  }

  static Stream<Arguments> restCatalogFailures() {
    return Stream.of(
        Arguments.of(
            new PersistenceException(
                "jdbc:mysql://db-host:3306/ams: Access denied for user 'ams' (using password: YES)"),
            500,
            "InternalServerError",
            INTERNAL),
        Arguments.of(
            new RuntimeException(new IOException("java.net.ConnectException: nn-host:8020")),
            500,
            "InternalServerError",
            INTERNAL),
        // Preconditions.checkNotNull / checkArgument of the REST catalog handlers
        Arguments.of(
            new NullPointerException("lack required params: warehouse"),
            500,
            "NullPointerException",
            "lack required params: warehouse"),
        Arguments.of(
            new IllegalArgumentException("The catalog is not an iceberg rest catalog"),
            500,
            "IllegalArgumentException",
            "The catalog is not an iceberg rest catalog"),
        // The typed errors of the REST catalog
        Arguments.of(
            new UnsupportedOperationException("rename is not supported now."),
            406,
            "UnsupportedOperationException",
            "rename is not supported now."),
        Arguments.of(
            new NoSuchNamespaceException("Database: db doesn't exists"),
            404,
            "NoSuchNamespaceException",
            "Database: db doesn't exists"));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("restCatalogFailures")
  void restCatalogErrorsExposeOnlyClientFacingDetails(
      Exception failure, int code, String type, String message) {
    Context context = mock(Context.class, RETURNS_SELF);
    HttpServletResponse response = mock(HttpServletResponse.class);
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(context.res()).thenReturn(response);
    when(context.req()).thenReturn(request);
    when(request.getMethod()).thenReturn("GET");
    when(request.getRequestURI()).thenReturn("/api/iceberg/rest/v1/config");
    RestCatalogService service =
        new RestCatalogService(mock(CatalogManager.class), mock(InternalTableManager.class));

    service.handleException(failure, context);

    ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
    verify(context).result(body.capture());
    JsonNode error = JacksonUtil.parseObject(body.getValue(), JsonNode.class).path("error");
    assertEquals(code, error.path("code").asInt(), String.valueOf(failure));
    assertEquals(type, error.path("type").asText(), String.valueOf(failure));
    assertEquals(message, error.path("message").asText(), String.valueOf(failure));
  }

  /** The response as JSON, or a failure that quotes the status, headers and raw body. */
  private static JsonNode json(HttpURLConnection connection) throws Exception {
    int status = connection.getResponseCode();
    String body = body(connection);
    try {
      return JacksonUtil.parseObject(body, JsonNode.class);
    } catch (RuntimeException unparsable) {
      throw new AssertionError(
          "HTTP "
              + status
              + " with headers "
              + connection.getHeaderFields()
              + " is not a JSON response; body: "
              + body,
          unparsable);
    }
  }

  private static String message(HttpURLConnection connection) throws Exception {
    return json(connection).path("message").asText();
  }

  private static Javalin app(CatalogManager catalogs) {
    Configurations conf = new Configurations();
    conf.set(AmoroManagementConf.AUTHORIZATION_ENABLED, true);
    DashboardServer server =
        new DashboardServer(
            conf,
            catalogs,
            mock(TableManager.class),
            mock(OptimizerManager.class),
            mock(AmoroServiceContainer.class));
    CasbinAuthorizationManager authorizationManager = new CasbinAuthorizationManager(conf);
    // Register the actual AMS endpoints and filters, not an empty test-only server.
    // https://javadoc.io/static/io.javalin/javalin/5.6.5/io/javalin/Javalin.html
    Javalin app = Javalin.create(c -> c.jsonMapper(JavalinJsonMapper.createDefaultJsonMapper()));
    app.routes(server.endpoints());
    app.before(
        ctx -> {
          if (ctx.header("X-Test-Role") != null) {
            Set<String> roles = Collections.singleton(Role.SERVICE_ADMIN);
            ctx.sessionAttribute(
                "user",
                new LoginController.SessionInfo(
                    "fixture", "0", roles, authorizationManager.resolvePrivileges(roles)));
          }
        });
    app.before(server::preHandleRequest);
    app.exception(Exception.class, server::handleException);
    return app.start(0);
  }

  private static HttpURLConnection call(
      Javalin app, String method, String path, String jsonBody, boolean signedIn) throws Exception {
    byte[] body = jsonBody == null ? null : jsonBody.getBytes(StandardCharsets.UTF_8);
    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://127.0.0.1:" + app.port() + path).openConnection();
    connection.setRequestMethod(method);
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(15000);
    // The whole suite shares this JVM and its process-wide HTTP client caches. Ports are reused,
    // so a kept-alive connection may belong to a server that has already been stopped.
    connection.setUseCaches(false);
    connection.setRequestProperty("Connection", "close");
    if (signedIn) {
      // The dashboard marks its requests; the test role then maps to an admin session.
      connection.setRequestProperty("X-Request-Source", "Web");
      connection.setRequestProperty("X-Test-Role", Role.SERVICE_ADMIN);
    }
    if (body != null) {
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      try (OutputStream out = connection.getOutputStream()) {
        out.write(body);
      }
    }
    return connection;
  }

  private static String body(HttpURLConnection connection) throws Exception {
    InputStream stream =
        connection.getResponseCode() >= 400
            ? connection.getErrorStream()
            : connection.getInputStream();
    try (InputStream in = stream) {
      return IOUtils.toString(in, StandardCharsets.UTF_8);
    }
  }
}
