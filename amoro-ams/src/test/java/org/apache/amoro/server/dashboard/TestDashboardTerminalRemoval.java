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
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.AmoroServiceContainer;
import org.apache.amoro.server.authorization.CasbinAuthorizationManager;
import org.apache.amoro.server.authorization.Role;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.controller.LoginController;
import org.apache.amoro.server.resource.OptimizerManager;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.utils.JacksonUtil;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

class TestDashboardTerminalRemoval {
  private static final String[][] REMOVED_ROUTES = {
    {"GET", "/examples"},
    {"GET", "/examples/CreateTable"},
    {"POST", "/catalogs/fixture/execute"},
    {"GET", "/fixture/logs"},
    {"GET", "/fixture/result"},
    {"PUT", "/fixture/stop"},
    {"GET", "/latestInfos"}
  };

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void removedRoutesCannotExecuteAndCatalogAccessIsRetained(boolean authorization)
      throws Exception {
    Javalin app = app(authorization);
    try {
      for (String role : new String[] {Role.SERVICE_ADMIN, Role.VIEWER}) {
        for (String[] route : REMOVED_ROUTES) {
          for (String suffix : new String[] {"", "/"}) {
            HttpURLConnection connection =
                request(app, route[0], "/api/ams/v1/terminal" + route[1] + suffix, role);
            try {
              int status = connection.getResponseCode();
              String body = body(connection);
              // Existing RBAC denial uses the business code; do not alter global HTTP semantics.
              assertTrue(
                  status == 404
                      || (authorization
                          && status == 200
                          && JacksonUtil.parseObject(body, JsonNode.class).path("code").asInt()
                              == 403),
                  route[1] + suffix + " " + role + " status=" + status + " body=" + body);
            } finally {
              connection.disconnect();
            }
          }
        }
        HttpURLConnection catalog = request(app, "GET", "/api/ams/v1/catalogs", role);
        try {
          assertEquals(200, catalog.getResponseCode());
          JsonNode response = JacksonUtil.parseObject(body(catalog), JsonNode.class);
          assertEquals(200, response.path("code").asInt());
          assertTrue(response.path("result").isArray());
          assertEquals(0, response.path("result").size());
        } finally {
          catalog.disconnect();
        }
      }
      HttpURLConnection anonymous = request(app, "GET", "/api/ams/v1/catalogs", null);
      try {
        assertEquals(
            403, JacksonUtil.parseObject(body(anonymous), JsonNode.class).path("code").asInt());
      } finally {
        anonymous.disconnect();
      }
    } finally {
      app.stop();
    }
  }

  private static Javalin app(boolean authorization) throws Exception {
    Configurations conf = new Configurations();
    conf.set(AmoroManagementConf.AUTHORIZATION_ENABLED, authorization);
    CatalogManager catalogs = mock(CatalogManager.class);
    when(catalogs.listCatalogMetas()).thenReturn(Collections.emptyList());
    DashboardServer server =
        new DashboardServer(
            conf,
            catalogs,
            mock(TableManager.class),
            mock(OptimizerManager.class),
            mock(AmoroServiceContainer.class));
    CasbinAuthorizationManager authorizationManager = new CasbinAuthorizationManager(conf);
    // Register the actual AMS endpoints and filters, not an empty test-only server.
    // https://javadoc.io/static/io.javalin/javalin/4.6.8/io/javalin/Javalin.html
    Javalin app = Javalin.create(c -> c.jsonMapper(JavalinJsonMapper.createDefaultJsonMapper()));
    app.routes(server.endpoints());
    app.before(
        ctx -> {
          String role = ctx.header("X-Test-Role");
          if (role != null) {
            Set<String> roles = Collections.singleton(role);
            Set<String> privileges =
                new LinkedHashSet<>(authorizationManager.resolvePrivileges(roles));
            privileges.add("EXECUTE_SQL");
            LoginController.SessionInfo oldSession =
                new LoginController.SessionInfo("fixture", "0", roles, privileges);
            // Persisted privileges are strings: old sessions survive enum removal.
            // https://docs.oracle.com/javase/8/docs/api/java/io/ObjectInputStream.html
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
              out.writeObject(oldSession);
            }
            try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
              LoginController.SessionInfo restored = (LoginController.SessionInfo) in.readObject();
              assertTrue(restored.getPrivileges().contains("EXECUTE_SQL"));
              assertEquals(roles, restored.getRoles());
              ctx.sessionAttribute("user", restored);
            }
          }
        });
    app.before(server::preHandleRequest);
    app.exception(Exception.class, server::handleException);
    return app.start(0);
  }

  private static HttpURLConnection request(Javalin app, String method, String path, String role)
      throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://127.0.0.1:" + app.port() + path).openConnection();
    connection.setRequestMethod(method);
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    connection.setRequestProperty("X-Request-Source", "Web");
    if (role != null) {
      connection.setRequestProperty("X-Test-Role", role);
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
