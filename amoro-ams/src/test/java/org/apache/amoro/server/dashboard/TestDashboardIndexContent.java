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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import io.javalin.Javalin;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

class TestDashboardIndexContent {
  @Test
  void httpKeepsUtf8WhitespaceAndInlineScriptBoundaries() throws Exception {
    DashboardServer server = mock(DashboardServer.class, CALLS_REAL_METHODS);
    String expected;
    try (InputStream in = getClass().getClassLoader().getResourceAsStream("static/index.html")) {
      expected = IOUtils.toString(in, StandardCharsets.UTF_8);
    }
    Javalin app = Javalin.create().get("/", ctx -> ctx.html(server.getIndexFileContent())).start(0);
    try {
      try (InputStream in = new URL("http://127.0.0.1:" + app.port() + "/").openStream()) {
        assertEquals(expected, IOUtils.toString(in, StandardCharsets.UTF_8));
      }
      assertEquals(expected, server.getIndexFileContent());
    } finally {
      app.stop();
    }
  }

  @Test
  void webjarScriptsKeepTheirMediaTypeAndCachePolicy() throws Exception {
    Properties pom = new Properties();
    try (InputStream input =
        getClass().getResourceAsStream("/META-INF/maven/org.webjars/swagger-ui/pom.properties")) {
      pom.load(input);
    }
    String script = "/swagger-ui/" + pom.getProperty("version") + "/swagger-initializer.js";
    Javalin app =
        Javalin.create(config -> DashboardServer.configureStaticFiles(config.staticFiles)).start(0);
    try {
      assertScriptHeaders(app.port(), script, "max-age=0");
      assertScriptHeaders(app.port(), "/webjars" + script, "max-age=31622400");
    } finally {
      app.stop();
    }
  }

  private static void assertScriptHeaders(int port, String path, String cacheControl)
      throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
    try {
      assertEquals(200, connection.getResponseCode());
      assertEquals("application/javascript", connection.getContentType());
      assertEquals(cacheControl, connection.getHeaderField("Cache-Control"));
    } finally {
      connection.disconnect();
    }
  }
}
