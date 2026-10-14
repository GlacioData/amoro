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

package org.apache.amoro.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import io.javalin.Javalin;
import org.apache.amoro.config.ConfigOptions;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.server.manager.EventsManager;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.persistence.DataSourceFactory;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.utils.JacksonUtil;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;

class TestTerminalRemovalLifecycle {
  @TempDir Path home;
  private Object previousSqlFactory;
  private Object previousEvents;
  private Object previousMetrics;
  private String previousHome;
  private BasicDataSource dataSource;
  private AmoroServiceContainer service;
  private Configurations conf;

  @BeforeEach
  void initializeRestDependencies() throws Exception {
    previousHome = System.getProperty(Environments.AMORO_HOME);
    System.setProperty(Environments.AMORO_HOME, home.toString());
    previousSqlFactory =
        replace(
            SqlSessionFactoryProvider.class,
            SqlSessionFactoryProvider.getInstance(),
            "sqlSessionFactory",
            null);
    previousEvents = replace(EventsManager.class, null, "INSTANCE", null);
    previousMetrics = replace(MetricManager.class, null, "INSTANCE", null);
    conf = new Configurations();
    conf.set(AmoroManagementConf.HTTP_SERVER_PORT, 0);
    conf.set(AmoroManagementConf.HTTP_SERVER_REST_AUTH_TYPE, "basic");
    conf.set(AmoroManagementConf.SERVER_EXPOSE_HOST, "127.0.0.1");
    conf.set(AmoroManagementConf.DB_TYPE, AmoroManagementConf.DB_TYPE_DERBY);
    conf.set(AmoroManagementConf.DB_DRIVER_CLASS_NAME, "org.apache.derby.jdbc.EmbeddedDriver");
    conf.set(
        AmoroManagementConf.DB_CONNECTION_URL,
        "jdbc:derby:" + home.resolve("derby") + ";create=true");
    conf.set(AmoroManagementConf.DB_AUTO_CREATE_TABLES, true);
    conf.set(ConfigOptions.key("terminal.backend").stringType().noDefaultValue(), "custom");
    conf.set(
        ConfigOptions.key("terminal.factory").stringType().noDefaultValue(),
        "fixture.removed.TerminalFactoryMustNeverLoad");
    conf.set(
        ConfigOptions.key("terminal.kyuubi.jdbc.url").stringType().noDefaultValue(),
        "jdbc:hive2://127.0.0.1:1/");
    dataSource = (BasicDataSource) DataSourceFactory.createDataSource(conf);
    SqlSessionFactoryProvider.getInstance().init(dataSource);
    // Isolate constructor-only HA election/environment loading. All REST methods and managers
    // below execute real production code, including private HTTP initialization and shutdown.
    // https://javadoc.io/static/org.mockito/mockito-core/4.11.0/org/mockito/Mockito.html#CALLS_REAL_METHODS
    service = mock(AmoroServiceContainer.class, CALLS_REAL_METHODS);
    replace(AmoroServiceContainer.class, service, "serviceConfig", conf);
    replace(AmoroServiceContainer.class, service, "dataSource", dataSource);
    replace(
        AmoroServiceContainer.class, service, "haState", AmoroServiceContainer.HAState.FOLLOWER);
  }

  @AfterEach
  void restoreDependencies() throws Exception {
    try {
      if (service != null) {
        service.disposeRestService();
      }
      if (dataSource != null) {
        dataSource.close();
      }
    } finally {
      replace(
          SqlSessionFactoryProvider.class,
          SqlSessionFactoryProvider.getInstance(),
          "sqlSessionFactory",
          previousSqlFactory);
      replace(EventsManager.class, null, "INSTANCE", previousEvents);
      replace(MetricManager.class, null, "INSTANCE", previousMetrics);
      if (previousHome == null) {
        System.clearProperty(Environments.AMORO_HOME);
      } else {
        System.setProperty(Environments.AMORO_HOME, previousHome);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"absent", "local", "kyuubi", "custom", "invalid_terminal_backend"})
  void restStartsAndStopsWithoutLoadingLegacyTerminalFactory(String backend) throws Exception {
    if ("absent".equals(backend)) {
      for (String key :
          new String[] {"terminal.backend", "terminal.factory", "terminal.kyuubi.jdbc.url"}) {
        conf.removeConfig(ConfigOptions.key(key).stringType().noDefaultValue());
      }
    } else {
      conf.set(ConfigOptions.key("terminal.backend").stringType().noDefaultValue(), backend);
    }
    AmoroManagementConfValidator.validateConfig(conf);
    service.startRestServices();
    Javalin app = httpServer();
    int port = app.port();
    assertManagementResponse(port, "/api/ams/v1/catalogs");
    assertManagementResponse(port, "/api/ams/v1/health/status");
    assertFalse(
        Thread.getAllStackTraces().keySet().stream()
            .anyMatch(t -> t.isAlive() && t.getName().startsWith("terminal-")));
    MetricRegistry registry = MetricManager.getInstance().getGlobalRegistry();
    assertFalse(registry.getMetrics().isEmpty());
    service.disposeRestService();
    assertTrue(app.jettyServer().server().isStopped());
    assertTrue(registry.getMetrics().isEmpty());
    try (ServerSocket releasedPort = new ServerSocket(port)) {
      assertEquals(port, releasedPort.getLocalPort());
    }
  }

  @Test
  void failedHttpBindCanBeDisposedWithoutTerminalResources() throws Exception {
    try (ServerSocket occupied = new ServerSocket(0)) {
      conf.set(AmoroManagementConf.HTTP_SERVER_PORT, occupied.getLocalPort());
      assertThrows(Exception.class, service::startRestServices);
      service.disposeRestService();
      assertTrue(httpServer().jettyServer().server().isStopped());
    }
  }

  private Javalin httpServer() throws Exception {
    Field field = AmoroServiceContainer.class.getDeclaredField("httpServer");
    field.setAccessible(true);
    return (Javalin) field.get(service);
  }

  private static Object replace(Class<?> type, Object target, String name, Object value)
      throws Exception {
    Field field = type.getDeclaredField(name);
    field.setAccessible(true);
    Object previous = field.get(target);
    field.set(target, value);
    return previous;
  }

  private static void assertManagementResponse(int port, String path) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    connection.setRequestProperty(
        "Authorization",
        "Basic "
            + Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8)));
    try {
      assertEquals(200, connection.getResponseCode());
      try (InputStream in = connection.getInputStream()) {
        JsonNode response =
            JacksonUtil.parseObject(IOUtils.toString(in, StandardCharsets.UTF_8), JsonNode.class);
        assertEquals(200, response.path("code").asInt());
      }
    } finally {
      connection.disconnect();
    }
  }
}
