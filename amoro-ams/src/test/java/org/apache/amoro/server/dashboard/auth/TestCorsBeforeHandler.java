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

package org.apache.amoro.server.dashboard.auth;

import static io.javalin.http.Header.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static io.javalin.http.Header.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.javalin.http.Header.ACCESS_CONTROL_ALLOW_METHODS;
import static io.javalin.http.Header.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.javalin.http.Header.ACCESS_CONTROL_MAX_AGE;
import static io.javalin.http.Header.ORIGIN;
import static io.javalin.http.Header.VARY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.http.Context;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestCorsBeforeHandler {

  private static final String ALLOWED_ORIGIN = "https://dashboard.example.com";

  private final HttpServletRequest req = mock(HttpServletRequest.class);
  private final HttpServletResponse res = mock(HttpServletResponse.class);
  private final Context ctx = mock(Context.class, org.mockito.Mockito.CALLS_REAL_METHODS);
  private final Map<String, String> responseHeaders = new HashMap<>();
  private final Map<String, String> requestHeaders = new HashMap<>();

  @BeforeEach
  public void beforeEach() {
    when(ctx.req()).thenReturn(req);
    when(ctx.res()).thenReturn(res);
    responseHeaders.clear();
    requestHeaders.clear();
    when(req.getHeader(anyString()))
        .thenAnswer(invocation -> requestHeaders.get(invocation.getArgument(0)));
    doAnswer(
            invocation -> {
              responseHeaders.put(invocation.getArgument(0), invocation.getArgument(1));
              return null;
            })
        .when(res)
        .setHeader(anyString(), anyString());
    doAnswer(
            invocation -> {
              responseHeaders.merge(
                  invocation.getArgument(0), invocation.getArgument(1), (a, b) -> a + ", " + b);
              return null;
            })
        .when(res)
        .addHeader(anyString(), anyString());
    when(res.getHeaders(VARY))
        .thenAnswer(
            invocation ->
                responseHeaders.containsKey(VARY)
                    ? Collections.singletonList(responseHeaders.get(VARY))
                    : Collections.emptyList());
    when(req.getMethod()).thenReturn("GET");
    requestHeaders.put(ORIGIN, ALLOWED_ORIGIN);
  }

  @Test
  public void testNoHeadersWhenUnconfigured() {
    new CorsBeforeHandler(Collections.emptyList()).handle(ctx);
    assertTrue(responseHeaders.isEmpty(), "empty allowlist must not emit any CORS header");
    verify(res, never()).setHeader(anyString(), anyString());
  }

  @Test
  public void testAllowedOriginReflected() {
    new CorsBeforeHandler(Collections.singletonList(ALLOWED_ORIGIN)).handle(ctx);
    assertEquals(ALLOWED_ORIGIN, responseHeaders.get(ACCESS_CONTROL_ALLOW_ORIGIN));
    assertEquals("true", responseHeaders.get(ACCESS_CONTROL_ALLOW_CREDENTIALS));
    assertEquals(ORIGIN, responseHeaders.get(VARY));
  }

  @Test
  public void testDisallowedOriginGetsVaryWithoutCorsPermission() {
    requestHeaders.put(ORIGIN, "https://evil.example.com");
    new CorsBeforeHandler(Collections.singletonList(ALLOWED_ORIGIN)).handle(ctx);
    assertEquals(Collections.singletonMap(VARY, ORIGIN), responseHeaders);
  }

  @Test
  public void testPreflightUsesFixedHeaderSet() {
    when(req.getMethod()).thenReturn("OPTIONS");
    new CorsBeforeHandler(Collections.singletonList(ALLOWED_ORIGIN)).handle(ctx);
    assertEquals(
        CorsBeforeHandler.ALLOWED_METHODS, responseHeaders.get(ACCESS_CONTROL_ALLOW_METHODS));
    assertEquals(
        CorsBeforeHandler.ALLOWED_HEADERS, responseHeaders.get(ACCESS_CONTROL_ALLOW_HEADERS));
    assertEquals(
        CorsBeforeHandler.PREFLIGHT_MAX_AGE_SECONDS, responseHeaders.get(ACCESS_CONTROL_MAX_AGE));
    // Must never read, let alone echo, the client-declared preflight headers.
    verify(req, never()).getHeader("Access-Control-Request-Headers");
    verify(req, never()).getHeader("Access-Control-Request-Method");
  }

  @Test
  public void testNonPreflightRequestHasNoPreflightHeaders() {
    new CorsBeforeHandler(Collections.singletonList(ALLOWED_ORIGIN)).handle(ctx);
    assertNull(responseHeaders.get(ACCESS_CONTROL_ALLOW_METHODS));
    assertNull(responseHeaders.get(ACCESS_CONTROL_ALLOW_HEADERS));
    assertNull(responseHeaders.get(ACCESS_CONTROL_MAX_AGE));
  }

  @Test
  public void testVaryIsAppendedNotOverwritten() {
    responseHeaders.put(VARY, "Accept-Encoding");
    new CorsBeforeHandler(Collections.singletonList(ALLOWED_ORIGIN)).handle(ctx);
    assertEquals("Accept-Encoding, " + ORIGIN, responseHeaders.get(VARY));
  }

  @Test
  public void testVaryNotDuplicatedWhenAlreadyPresent() {
    responseHeaders.put(VARY, ORIGIN);
    new CorsBeforeHandler(Collections.singletonList(ALLOWED_ORIGIN)).handle(ctx);
    assertEquals(ORIGIN, responseHeaders.get(VARY));
  }

  @Test
  public void testOriginMatchingIsNormalized() {
    // Configured with uppercase host and trailing slash; request arrives lowercase.
    CorsBeforeHandler handler =
        new CorsBeforeHandler(Collections.singletonList("https://Dashboard.Example.com/"));
    handler.handle(ctx);
    assertEquals(ALLOWED_ORIGIN, responseHeaders.get(ACCESS_CONTROL_ALLOW_ORIGIN));
  }

  @Test
  public void testExplicitDefaultPortMatchesBrowserOrigin() {
    new CorsBeforeHandler(Collections.singletonList("https://dashboard.example.com:443"))
        .handle(ctx);
    assertEquals(ALLOWED_ORIGIN, responseHeaders.get(ACCESS_CONTROL_ALLOW_ORIGIN));
  }

  @Test
  public void testNonDefaultPortIsNotImplicitlyAllowed() {
    requestHeaders.put(ORIGIN, "https://dashboard.example.com:8443");
    new CorsBeforeHandler(Collections.singletonList(ALLOWED_ORIGIN)).handle(ctx);
    assertNull(responseHeaders.get(ACCESS_CONTROL_ALLOW_ORIGIN));
  }

  @Test
  public void testNoOriginGetsVaryWithoutCorsPermission() {
    requestHeaders.remove(ORIGIN);
    new CorsBeforeHandler(Collections.singletonList(ALLOWED_ORIGIN)).handle(ctx);
    assertEquals(Collections.singletonMap(VARY, ORIGIN), responseHeaders);
  }
}
