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

package org.apache.amoro.server.dashboard.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import io.javalin.Javalin;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.dashboard.JavalinJsonMapper;
import org.apache.amoro.server.dashboard.ServerTableDescriptor;
import org.apache.amoro.server.dashboard.response.PageResult;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.table.descriptor.SnapshotDetailPage;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.stream.Collectors;

class TestSnapshotHttpErrors {
  @Test
  void realHttpStatusesSurviveUgiAndWrappedCause() throws Exception {
    ServerTableDescriptor descriptor = mock(ServerTableDescriptor.class);
    TableController controller =
        new TableController(
            mock(CatalogManager.class), mock(TableManager.class), descriptor, new Configurations());
    String route = "/catalogs/{catalog}/dbs/{db}/tables/{table}/snapshots/{snapshotId}/detail";
    Javalin app =
        Javalin.create(c -> c.jsonMapper(JavalinJsonMapper.createDefaultJsonMapper()))
            .get(route, controller::getSnapshotDetail)
            .start(0);
    try {
      SnapshotReadException.Reason[] reasons = SnapshotReadException.Reason.values();
      int[] statuses = {400, 404, 429, 503, 504, 500};
      for (int i = 0; i < reasons.length; i++) {
        SnapshotReadException.Reason reason = reasons[i];
        doAnswer(
                call ->
                    UserGroupInformation.createRemoteUser("snapshot-test")
                        .doAs(
                            (PrivilegedExceptionAction<Object>)
                                () -> {
                                  throw new RuntimeException(
                                      "Run with ugi request failed.",
                                      new SnapshotReadException(reason, "typed snapshot failure"));
                                }))
            .when(descriptor)
            .getSnapshotDetailResponse(any(), any(), any(), any(), any(), any(), anyBoolean());
        HttpURLConnection connection = request(app.port(), "?pageSize=25");
        assertEquals(statuses[i], connection.getResponseCode());
        try (BufferedReader body =
            new BufferedReader(
                new InputStreamReader(connection.getErrorStream(), StandardCharsets.UTF_8))) {
          assertTrue(body.lines().collect(Collectors.joining()).contains("typed snapshot failure"));
        } finally {
          connection.disconnect();
        }
      }
      HttpURLConnection bad = request(app.port(), "?pageSize=bad");
      assertEquals(400, bad.getResponseCode());
      bad.disconnect();
    } finally {
      app.stop();
    }
  }

  @Test
  void jsonKeepsLongTotalAndDoesNotInventDetailTotal() {
    JavalinJsonMapper mapper = JavalinJsonMapper.createDefaultJsonMapper();
    assertTrue(
        mapper
            .toJsonString(PageResult.of(Collections.emptyList(), 3_000_000_000L))
            .contains("3000000000"));
    String page = mapper.toJsonString(new SnapshotDetailPage(Collections.emptyList(), null));
    assertTrue(page.contains("nextCursor"));
    assertTrue(!page.contains("total"));
  }

  private static HttpURLConnection request(int port, String query) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection)
            new URL(
                    "http://localhost:"
                        + port
                        + "/catalogs/c/dbs/d/tables/t/snapshots/1/detail"
                        + query)
                .openConnection();
    connection.setConnectTimeout(5000);
    connection.setReadTimeout(5000);
    return connection;
  }
}
