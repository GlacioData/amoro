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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * {@code GET /api/ams/v1/catalogs/{catalog}/config/{type}/{key}} serves the config files that the
 * catalog page shows. It must hand out only those files, never the credentials stored next to them:
 * the keytab is the server's own Kerberos credential, and the secret key is masked everywhere else
 * in the API.
 */
class TestCatalogConfigFileDownload {

  private static final String KERBEROS_CATALOG = "kerberos_catalog";
  private static final String AK_SK_CATALOG = "ak_sk_catalog";
  private static final String CONFIG_PATH = "/api/ams/v1/catalogs/%s/config/%s/%s";

  private static final byte[] KEYTAB =
      "keytab-bytes-of-the-server".getBytes(StandardCharsets.UTF_8);
  private static final byte[] KRB5 =
      "[libdefaults]\ndefault_realm = EXAMPLE.COM\n".getBytes(StandardCharsets.UTF_8);
  private static final byte[] CORE_SITE = siteXml("fs.defaultFS", "hdfs://nn:8020");
  private static final byte[] HDFS_SITE = siteXml("dfs.replication", "3");
  private static final byte[] HIVE_SITE = siteXml("hive.metastore.uris", "thrift://hms:9083");

  /**
   * Shape of a real access key pair: both values decode as base64, so the previous lookup turned
   * the masked secret key into its raw bytes.
   */
  private static final String ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE";

  private static final String SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

  private static byte[] siteXml(String name, String value) {
    return ("<configuration><property><name>"
            + name
            + "</name><value>"
            + value
            + "</value></property></configuration>")
        .getBytes(StandardCharsets.UTF_8);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void keytabAndAccessKeysAreNotDownloadable(boolean authorization) throws Exception {
    Javalin app = app(authorization);
    try {
      for (String role : new String[] {Role.SERVICE_ADMIN, Role.VIEWER}) {
        // The secret key is masked in the catalog detail; it must not be readable here either.
        String decodedSecret =
            new String(Base64.getDecoder().decode(SECRET_KEY), StandardCharsets.ISO_8859_1);
        assertRefused(
            app,
            path(AK_SK_CATALOG, "auth-config", "auth.ak_sk.secret_key"),
            role,
            SECRET_KEY,
            decodedSecret);
        assertRefused(
            app, path(AK_SK_CATALOG, "auth-config", "auth-ak_sk-secret_key"), role, SECRET_KEY);
        assertRefused(
            app, path(AK_SK_CATALOG, "auth-config", "auth.ak_sk.access_key"), role, ACCESS_KEY);
        assertRefused(app, path(AK_SK_CATALOG, "auth-config", "auth.simple.hadoop_username"), role);

        String keytabText = new String(KEYTAB, StandardCharsets.ISO_8859_1);
        assertRefused(
            app,
            path(KERBEROS_CATALOG, "auth-config", "auth-kerberos-keytab"),
            role,
            keytabText,
            base64(KEYTAB));
      }
    } finally {
      app.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void configFilesRemainDownloadableAndOtherKeysAreRefused(boolean authorization) throws Exception {
    Javalin app = app(authorization);
    try {
      for (String role : new String[] {Role.SERVICE_ADMIN, Role.VIEWER}) {
        assertArrayEquals(
            CORE_SITE,
            download(app, path(KERBEROS_CATALOG, "storage-config", "hadoop-core-site"), role));
        assertArrayEquals(
            HDFS_SITE,
            download(app, path(KERBEROS_CATALOG, "storage-config", "hadoop-hdfs-site"), role));
        assertArrayEquals(
            HIVE_SITE, download(app, path(KERBEROS_CATALOG, "storage-config", "hive-site"), role));
        assertArrayEquals(
            KRB5, download(app, path(KERBEROS_CATALOG, "auth-config", "auth-kerberos-krb5"), role));
        assertArrayEquals(
            HDFS_SITE,
            download(app, path(KERBEROS_CATALOG, "storage-config", "hadoop.hdfs.site"), role));

        // Entries that are not config files, and anything unknown, are not served.
        assertRefused(app, path(KERBEROS_CATALOG, "storage-config", "storage-type"), role);
        assertRefused(app, path(KERBEROS_CATALOG, "storage-config", "not-a-config"), role);
        assertRefused(app, path(AK_SK_CATALOG, "auth-config", "auth-kerberos-keytab"), role);
      }
    } finally {
      app.stop();
    }
  }

  private static String path(String catalog, String type, String key) {
    return String.format(CONFIG_PATH, catalog, type, key);
  }

  private static void assertRefused(Javalin app, String path, String role, String... mustNotAppear)
      throws Exception {
    HttpURLConnection connection = request(app, path, role);
    try {
      assertEquals(404, connection.getResponseCode(), path + " for role " + role);
      String body = new String(bytes(connection), StandardCharsets.ISO_8859_1);
      for (String credential : mustNotAppear) {
        assertFalse(body.contains(credential), credential + " leaked through " + path);
      }
    } finally {
      connection.disconnect();
    }
  }

  private static byte[] download(Javalin app, String path, String role) throws Exception {
    HttpURLConnection connection = request(app, path, role);
    try {
      assertEquals(200, connection.getResponseCode(), path + " for role " + role);
      return bytes(connection);
    } finally {
      connection.disconnect();
    }
  }

  private static HttpURLConnection request(Javalin app, String path, String role) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://127.0.0.1:" + app.port() + path).openConnection();
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    connection.setRequestProperty("X-Request-Source", "Web");
    connection.setRequestProperty("X-Test-Role", role);
    return connection;
  }

  private static byte[] bytes(HttpURLConnection connection) throws Exception {
    InputStream stream =
        connection.getResponseCode() >= 400
            ? connection.getErrorStream()
            : connection.getInputStream();
    try (InputStream in = stream) {
      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
      IOUtils.copy(in, out);
      return out.toByteArray();
    }
  }

  private static String base64(byte[] content) {
    return Base64.getEncoder().encodeToString(content);
  }

  private static CatalogMeta kerberosCatalog() {
    CatalogMeta catalogMeta = new CatalogMeta();
    catalogMeta.setCatalogName(KERBEROS_CATALOG);
    catalogMeta.setCatalogType("hive");
    catalogMeta.setAuthConfigs(
        map(
            CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE,
            "kerberos",
            CatalogMetaProperties.AUTH_CONFIGS_KEY_PRINCIPAL,
            "ams/_HOST@EXAMPLE.COM",
            CatalogMetaProperties.AUTH_CONFIGS_KEY_KEYTAB,
            base64(KEYTAB),
            CatalogMetaProperties.AUTH_CONFIGS_KEY_KRB5,
            base64(KRB5)));
    Map<String, String> storageConfigs =
        map(
            CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE,
            "Hadoop",
            CatalogMetaProperties.STORAGE_CONFIGS_KEY_HIVE_SITE,
            base64(HIVE_SITE));
    storageConfigs.put(CatalogMetaProperties.STORAGE_CONFIGS_KEY_CORE_SITE, base64(CORE_SITE));
    storageConfigs.put(CatalogMetaProperties.STORAGE_CONFIGS_KEY_HDFS_SITE, base64(HDFS_SITE));
    catalogMeta.setStorageConfigs(storageConfigs);
    catalogMeta.setCatalogProperties(map("warehouse", "hdfs://nn/warehouse"));
    return catalogMeta;
  }

  private static CatalogMeta akSkCatalog() {
    CatalogMeta catalogMeta = new CatalogMeta();
    catalogMeta.setCatalogName(AK_SK_CATALOG);
    catalogMeta.setCatalogType("hadoop");
    catalogMeta.setAuthConfigs(
        map(
            CatalogMetaProperties.AUTH_CONFIGS_KEY_TYPE,
            "ak/sk",
            CatalogMetaProperties.AUTH_CONFIGS_KEY_ACCESS_KEY,
            ACCESS_KEY,
            CatalogMetaProperties.AUTH_CONFIGS_KEY_SECRET_KEY,
            SECRET_KEY));
    catalogMeta.setStorageConfigs(map(CatalogMetaProperties.STORAGE_CONFIGS_KEY_TYPE, "S3"));
    catalogMeta.setCatalogProperties(map("warehouse", "s3://bucket/warehouse"));
    return catalogMeta;
  }

  private static Map<String, String> map(String... keyValues) {
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    return map;
  }

  private static Javalin app(boolean authorization) {
    Configurations conf = new Configurations();
    conf.set(AmoroManagementConf.AUTHORIZATION_ENABLED, authorization);
    CatalogManager catalogs = mock(CatalogManager.class);
    when(catalogs.getCatalogMeta(KERBEROS_CATALOG)).thenReturn(kerberosCatalog());
    when(catalogs.getCatalogMeta(AK_SK_CATALOG)).thenReturn(akSkCatalog());
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
}
