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
import static org.mockito.Mockito.mock;

import io.javalin.Javalin;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.config.shade.utils.ConfigShadeUtils;
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

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code GET /api/ams/v1/settings/system} returns the whole service configuration to the settings
 * page. Every setting that holds a credential must come back masked, while ordinary settings stay
 * readable.
 */
class TestSystemSettingMasking {

  private static final String CREDENTIAL_MARK = "credential-value";
  private static final String MASK = "******";

  /** Declared by the operator through shade.sensitive-keywords only; its name gives no hint. */
  private static final String OPERATOR_DECLARED_SETTING = "operator.declared.credential";

  /** Settings whose value is a credential. All of them must be masked. */
  private static final List<String> CREDENTIAL_SETTINGS =
      Arrays.asList(
          AmoroManagementConf.ADMIN_PASSWORD.key(),
          AmoroManagementConf.DB_PASSWORD.key(),
          AmoroManagementConf.AUTHORIZATION_LDAP_ROLE_MAPPING_BIND_PASSWORD.key(),
          "plugin.jdbc.password",
          "custom.secret-key",
          "ldap.sasl.token",
          "storage.access-key",
          "rest.api-key",
          "aws.secretAccessKey",
          OPERATOR_DECLARED_SETTING);

  /** Ordinary settings that the settings page must keep showing in clear. */
  private static final Map<String, String> VISIBLE_SETTINGS =
      map(
          AmoroManagementConf.ADMIN_USERNAME.key(),
          "admin",
          AmoroManagementConf.DB_USER_NAME.key(),
          "ams",
          AmoroManagementConf.AUTHORIZATION_LDAP_ROLE_MAPPING_BIND_DN.key(),
          "cn=ams,dc=example,dc=com",
          AmoroManagementConf.HA_ZOOKEEPER_AUTH_KEYTAB.key(),
          "/etc/security/keytabs/ams.keytab",
          AmoroManagementConf.HTTP_SERVER_REST_AUTH_TYPE.key(),
          "token",
          AmoroManagementConf.HTTP_SERVER_PORT.key(),
          "19090",
          "plugin.jdbc.url",
          "jdbc:mysql://db:3306/ams",
          "shade.identifier",
          "default");

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void systemSettingsMaskEveryCredential(boolean authorization) throws Exception {
    Configurations conf = new Configurations();
    conf.set(AmoroManagementConf.AUTHORIZATION_ENABLED, authorization);
    for (String key : CREDENTIAL_SETTINGS) {
      conf.setString(key, CREDENTIAL_MARK);
    }
    VISIBLE_SETTINGS.forEach(conf::setString);
    conf.set(
        ConfigShadeUtils.SHADE_SENSITIVE_KEYWORDS,
        Arrays.asList(
            AmoroManagementConf.ADMIN_PASSWORD.key(),
            AmoroManagementConf.DB_PASSWORD.key(),
            OPERATOR_DECLARED_SETTING));

    Javalin app = app(conf);
    try {
      for (String role : new String[] {Role.SERVICE_ADMIN, Role.VIEWER}) {
        HttpURLConnection connection = request(app, role);
        try {
          String body = body(connection);
          assertFalse(body.contains(CREDENTIAL_MARK), "credential leaked for role " + role);
          JsonNode response = JacksonUtil.parseObject(body, JsonNode.class);
          if (authorization && Role.VIEWER.equals(role)) {
            // Reading settings needs MANAGE_PLATFORM; the viewer is still turned away.
            assertEquals(403, response.path("code").asInt());
            continue;
          }
          assertEquals(200, connection.getResponseCode());
          assertEquals(200, response.path("code").asInt());
          JsonNode result = response.path("result");
          for (String key : CREDENTIAL_SETTINGS) {
            assertEquals(MASK, result.path(key).asText(), "setting " + key + " is not masked");
          }
          VISIBLE_SETTINGS.forEach(
              (key, value) -> assertEquals(value, result.path(key).asText(), "setting " + key));
        } finally {
          connection.disconnect();
        }
      }
    } finally {
      app.stop();
    }
  }

  private static Map<String, String> map(String... keyValues) {
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    return map;
  }

  private static Javalin app(Configurations conf) {
    DashboardServer server =
        new DashboardServer(
            conf,
            mock(CatalogManager.class),
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

  private static HttpURLConnection request(Javalin app, String role) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection)
            new URL("http://127.0.0.1:" + app.port() + "/api/ams/v1/settings/system")
                .openConnection();
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    connection.setRequestProperty("X-Request-Source", "Web");
    connection.setRequestProperty("X-Test-Role", role);
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
