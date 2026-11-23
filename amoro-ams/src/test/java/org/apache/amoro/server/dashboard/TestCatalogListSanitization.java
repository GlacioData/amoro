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
import static org.mockito.Mockito.when;

import io.javalin.Javalin;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.properties.CatalogMetaProperties;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code GET /api/ams/v1/catalogs} feeds the catalog menu. It must describe the catalogs without
 * handing out the credentials that are stored inside the catalog metadata.
 */
class TestCatalogListSanitization {

  /** Every credential value in the fixtures carries this marker, so one scan covers them all. */
  private static final String CREDENTIAL_MARK = "credential-value";

  private static final String SITE_XML =
      "<configuration><property><name>fs.s3a.secret.key</name><value>"
          + CREDENTIAL_MARK
          + "</value></property></configuration>";
  private static final String SITE_XML_B64 = base64(SITE_XML);
  private static final String KEYTAB_B64 = base64("keytab-" + CREDENTIAL_MARK);

  /** Property names that carry a credential and must never be listed. */
  private static final List<String> CREDENTIAL_NAMED_PROPERTIES =
      Arrays.asList(
          "s3.access-key-id",
          "s3.secret-key",
          "s3.session-token",
          "client.access-key",
          "fs.s3a.access.key",
          "fs.s3a.secret.key",
          "fs.s3a.session.token",
          "aliyun.oss.access-key-id",
          "aliyun.oss.access-key-secret",
          "fs.oss.accessKeyId",
          "fs.oss.accessKeySecret",
          "fs.oss.securityToken",
          "fs.azure.account.key.storage.dfs.core.windows.net",
          "fs.cos.service.secret.key",
          "glue.access-key-id",
          "glue.secret-access-key",
          "rest.token",
          "credential",
          "javax.jdo.option.ConnectionPassword",
          "keytab",
          "krb_keytab",
          "api-key",
          "apikey",
          "api_key",
          "private-key",
          "private_key",
          "account-key",
          "session-key",
          "AWS_SECRET_ACCESS_KEY",
          "aws.secretAccessKey",
          "password",
          "passwd");

  /** Ordinary catalog options that the listing must keep returning. */
  private static final List<String> ORDINARY_PROPERTIES =
      Arrays.asList(
          "warehouse",
          "uri",
          "catalog.impl",
          "table-formats",
          "s3.endpoint",
          "s3.path-style-access",
          "client.region",
          "s3.sts.endpoint",
          "client.assume-role.arn",
          "s3.role-session-name",
          "aliyun.oss.endpoint",
          "fs.oss.endpoint",
          "fs.defaultFS",
          "hadoop.security.authentication",
          "rest.page-size",
          "glue.skip-name-validation",
          "table-properties.self-optimizing.enabled",
          "table-properties.self-optimizing.group");

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void catalogListOmitsCredentialsForEveryRole(boolean authorization) throws Exception {
    List<CatalogMeta> stored =
        new ArrayList<>(Arrays.asList(akSkCatalog(), kerberosCatalog(), simpleCatalog()));
    List<CatalogMeta> storedBefore = new ArrayList<>();
    for (CatalogMeta catalogMeta : stored) {
      storedBefore.add(catalogMeta.deepCopy());
    }
    CatalogManager catalogs = mock(CatalogManager.class);
    when(catalogs.listCatalogMetas()).thenReturn(stored);
    Javalin app = app(authorization, catalogs);
    try {
      for (String role : new String[] {Role.SERVICE_ADMIN, Role.VIEWER}) {
        HttpURLConnection connection = request(app, role);
        try {
          assertEquals(200, connection.getResponseCode());
          String body = body(connection);
          assertFalse(body.contains(CREDENTIAL_MARK), "credential value leaked for role " + role);
          assertFalse(body.contains(SITE_XML_B64), "site config content leaked for role " + role);
          assertFalse(body.contains(KEYTAB_B64), "keytab content leaked for role " + role);

          JsonNode result = JacksonUtil.parseObject(body, JsonNode.class).path("result");
          assertEquals(3, result.size());

          JsonNode akSk = result.get(0);
          assertEquals("ak_sk_catalog", akSk.path("catalogName").asText());
          assertEquals("hadoop", akSk.path("catalogType").asText());
          assertObject(
              akSk.path("authConfigs"), map(CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE, "ak/sk"));
          assertObject(
              akSk.path("storageConfigs"),
              map(CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE, "Hadoop"));
          assertObject(
              akSk.path("catalogProperties"),
              map("warehouse", "s3://bucket/warehouse", "table-formats", "ICEBERG"));

          JsonNode kerberos = result.get(1);
          assertEquals("kerberos_catalog", kerberos.path("catalogName").asText());
          assertEquals("hive", kerberos.path("catalogType").asText());
          assertObject(
              kerberos.path("authConfigs"),
              map(
                  CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE,
                  "kerberos",
                  CatalogMetaProperties.AUTH_CONFIGS_KEY_PRINCIPAL,
                  "ams/_HOST@REALM"));
          assertObject(
              kerberos.path("storageConfigs"),
              map(CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE, "Hadoop"));
          assertObject(
              kerberos.path("catalogProperties"),
              map("warehouse", "hdfs://nameservice/warehouse", "table-formats", "MIXED_ICEBERG"));

          JsonNode simple = result.get(2);
          assertEquals("simple_catalog", simple.path("catalogName").asText());
          assertEquals("glue", simple.path("catalogType").asText());
          assertObject(
              simple.path("authConfigs"),
              map(
                  CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE,
                  "simple",
                  CatalogMetaProperties.AUTH_CONFIGS_KEY_HADOOP_USERNAME,
                  "hdfs"));
          assertObject(
              simple.path("storageConfigs"),
              map(CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE, "S3"));
          assertObject(
              simple.path("catalogProperties"),
              map(
                  "warehouse",
                  "s3://bucket/warehouse-2",
                  "catalog.impl",
                  "org.example.CustomCatalog",
                  "table-properties.self-optimizing.group",
                  "default"));
        } finally {
          connection.disconnect();
        }
      }

      // The listing must not edit the metadata instances that CatalogManager keeps.
      for (int i = 0; i < stored.size(); i++) {
        assertEquals(storedBefore.get(i), stored.get(i), "stored catalog " + i + " was modified");
      }
      assertEquals(
          SITE_XML_B64,
          stored
              .get(0)
              .getStorageConfigs()
              .get(CatalogMetaProperties.STORAGE_CONFIGS_KEY_HIVE_SITE),
          "stored catalog 0 lost its config file content");
      assertEquals(
          KEYTAB_B64,
          stored.get(1).getAuthConfigs().get(CatalogMetaProperties.AUTH_CONFIGS_KEY_KEYTAB),
          "stored catalog 1 lost its keytab");
    } finally {
      app.stop();
    }
  }

  /**
   * Catalog properties are open-ended, so credential names are matched by pattern. This pins the
   * boundary: credential-like names are dropped, ordinary connection and tuning options stay.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void credentialNamedPropertiesAreDroppedAndOthersKept(boolean authorization) throws Exception {
    Map<String, String> properties = new LinkedHashMap<>();
    for (String name : CREDENTIAL_NAMED_PROPERTIES) {
      properties.put(name, CREDENTIAL_MARK);
    }
    Map<String, String> ordinaryProperties = new LinkedHashMap<>();
    for (String name : ORDINARY_PROPERTIES) {
      ordinaryProperties.put(name, "value-of-" + name);
      properties.put(name, "value-of-" + name);
    }
    CatalogMeta catalogMeta = new CatalogMeta();
    catalogMeta.setCatalogName("pattern_catalog");
    catalogMeta.setCatalogType("custom");
    catalogMeta.setAuthConfigs(map(CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE, "simple"));
    catalogMeta.setStorageConfigs(map(CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE, "S3"));
    catalogMeta.setCatalogProperties(properties);

    CatalogManager catalogs = mock(CatalogManager.class);
    when(catalogs.listCatalogMetas())
        .thenReturn(new ArrayList<>(Collections.singletonList(catalogMeta)));
    Javalin app = app(authorization, catalogs);
    try {
      HttpURLConnection connection = request(app, Role.VIEWER);
      try {
        assertEquals(200, connection.getResponseCode());
        String body = body(connection);
        assertFalse(body.contains(CREDENTIAL_MARK), "credential-named property leaked");
        JsonNode listed = JacksonUtil.parseObject(body, JsonNode.class).path("result").get(0);
        assertEquals("pattern_catalog", listed.path("catalogName").asText());
        assertObject(listed.path("catalogProperties"), ordinaryProperties);
      } finally {
        connection.disconnect();
      }
    } finally {
      app.stop();
    }
  }

  private static CatalogMeta akSkCatalog() {
    CatalogMeta catalogMeta = new CatalogMeta();
    catalogMeta.setCatalogName("ak_sk_catalog");
    catalogMeta.setCatalogType("hadoop");
    catalogMeta.setAuthConfigs(
        map(
            CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE,
            "ak/sk",
            CatalogMetaProperties.AUTH_CONFIGS_KEY_ACCESS_KEY,
            "ak-" + CREDENTIAL_MARK,
            CatalogMetaProperties.AUTH_CONFIGS_KEY_SECRET_KEY,
            "sk-" + CREDENTIAL_MARK));
    catalogMeta.setStorageConfigs(storageConfigs());
    catalogMeta.setCatalogProperties(
        map(
            "warehouse",
            "s3://bucket/warehouse",
            "table-formats",
            "ICEBERG",
            "s3.access-key-id",
            CREDENTIAL_MARK,
            "s3.secret-key",
            CREDENTIAL_MARK,
            "s3.session-token",
            CREDENTIAL_MARK,
            "aliyun.oss.access-key-secret",
            CREDENTIAL_MARK,
            "fs.oss.accessKeySecret",
            CREDENTIAL_MARK,
            "fs.s3a.secret.key",
            CREDENTIAL_MARK,
            "fs.azure.account.key.account.dfs.core.windows.net",
            CREDENTIAL_MARK,
            "rest.token",
            CREDENTIAL_MARK,
            "credential",
            CREDENTIAL_MARK,
            "storage.password",
            CREDENTIAL_MARK));
    return catalogMeta;
  }

  private static CatalogMeta kerberosCatalog() {
    CatalogMeta catalogMeta = new CatalogMeta();
    catalogMeta.setCatalogName("kerberos_catalog");
    catalogMeta.setCatalogType("hive");
    catalogMeta.setAuthConfigs(
        map(
            CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE,
            "kerberos",
            CatalogMetaProperties.AUTH_CONFIGS_KEY_PRINCIPAL,
            "ams/_HOST@REALM",
            CatalogMetaProperties.AUTH_CONFIGS_KEY_KEYTAB,
            KEYTAB_B64,
            CatalogMetaProperties.AUTH_CONFIGS_KEY_KRB5,
            base64("[libdefaults]")));
    catalogMeta.setStorageConfigs(storageConfigs());
    catalogMeta.setCatalogProperties(
        map("warehouse", "hdfs://nameservice/warehouse", "table-formats", "MIXED_ICEBERG"));
    return catalogMeta;
  }

  private static CatalogMeta simpleCatalog() {
    CatalogMeta catalogMeta = new CatalogMeta();
    catalogMeta.setCatalogName("simple_catalog");
    catalogMeta.setCatalogType("glue");
    catalogMeta.setAuthConfigs(
        map(
            CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE,
            "simple",
            CatalogMetaProperties.AUTH_CONFIGS_KEY_HADOOP_USERNAME,
            "hdfs"));
    catalogMeta.setStorageConfigs(map(CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE, "S3"));
    catalogMeta.setCatalogProperties(
        map(
            "warehouse",
            "s3://bucket/warehouse-2",
            "catalog.impl",
            "org.example.CustomCatalog",
            "table-properties.self-optimizing.group",
            "default"));
    return catalogMeta;
  }

  private static Map<String, String> storageConfigs() {
    Map<String, String> storageConfigs =
        map(
            CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE,
            "Hadoop",
            CatalogMetaProperties.STORAGE_CONFIGS_KEY_HIVE_SITE,
            SITE_XML_B64);
    storageConfigs.put(CatalogMetaProperties.STORAGE_CONFIGS_KEY_CORE_SITE, SITE_XML_B64);
    storageConfigs.put(CatalogMetaProperties.STORAGE_CONFIGS_KEY_HDFS_SITE, SITE_XML_B64);
    return storageConfigs;
  }

  /** Asserts that {@code node} is an object holding exactly {@code expected}. */
  private static void assertObject(JsonNode node, Map<String, String> expected) {
    Set<String> names = new HashSet<>();
    node.fieldNames().forEachRemaining(names::add);
    assertEquals(expected.keySet(), names, "fields of " + node);
    expected.forEach(
        (name, value) ->
            assertEquals(
                value, node.path(name).asText(), "value of field " + name + " in " + node));
  }

  private static Map<String, String> map(String... keyValues) {
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    return map;
  }

  private static String base64(String content) {
    return Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
  }

  private static Javalin app(boolean authorization, CatalogManager catalogs) {
    Configurations conf = new Configurations();
    conf.set(AmoroManagementConf.AUTHORIZATION_ENABLED, authorization);
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
            new URL("http://127.0.0.1:" + app.port() + "/api/ams/v1/catalogs").openConnection();
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
