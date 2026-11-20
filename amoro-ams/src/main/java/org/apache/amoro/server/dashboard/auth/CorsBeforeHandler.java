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

import static io.javalin.core.util.Header.ACCESS_CONTROL_ALLOW_CREDENTIALS;
import static io.javalin.core.util.Header.ACCESS_CONTROL_ALLOW_HEADERS;
import static io.javalin.core.util.Header.ACCESS_CONTROL_ALLOW_METHODS;
import static io.javalin.core.util.Header.ACCESS_CONTROL_ALLOW_ORIGIN;
import static io.javalin.core.util.Header.ACCESS_CONTROL_MAX_AGE;
import static io.javalin.core.util.Header.ORIGIN;
import static io.javalin.core.util.Header.VARY;

import io.javalin.http.Context;
import io.javalin.http.Handler;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Allowlist-based CORS handler.
 *
 * <p>Replaces Javalin's {@code enableCorsForAllOrigins()}, which reflects any request Origin with
 * {@code Access-Control-Allow-Credentials: true} and echoes arbitrary requested headers on
 * preflight (verified from {@code io/javalin/http/util/CorsUtil.kt} in javalin-4.6.8-sources.jar) -
 * any web page could then issue credentialed requests with a victim's session.
 *
 * <p>Behavior contract: specs/003-credential-security-hardening/contracts/api-contracts.md (C1).
 *
 * <p>Sources:
 *
 * <ul>
 *   <li>MDN CORS guide - credentialed responses must name an explicit origin (no wildcard) and
 *       servers returning a single origin from an allowlist should send {@code Vary: Origin};
 *       preflight responses advertise the allowed methods/headers as explicit lists:
 *       https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS
 * </ul>
 */
public class CorsBeforeHandler implements Handler {

  /** Fixed set: covers the dashboard frontend and API clients; never reflects request input. */
  static final String ALLOWED_HEADERS =
      "Content-Type, Authorization, X-Request-Source, X-Requested-With";

  static final String ALLOWED_METHODS = "GET, POST, PUT, DELETE, OPTIONS";

  static final String PREFLIGHT_MAX_AGE_SECONDS = "600";

  private final Set<String> allowedOrigins;

  public CorsBeforeHandler(List<String> configuredOrigins) {
    this.allowedOrigins =
        configuredOrigins == null
            ? new LinkedHashSet<>()
            : configuredOrigins.stream()
                .map(CorsBeforeHandler::normalize)
                .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  @Override
  public void handle(Context ctx) {
    String requestOrigin = ctx.header(ORIGIN);
    if (requestOrigin == null || allowedOrigins.isEmpty()) {
      return;
    }
    if (!allowedOrigins.contains(normalize(requestOrigin))) {
      return;
    }
    // Explicit origin, never the request value reflected from an allowlist miss; credentialed
    // responses must not use the wildcard (MDN: Requests with credentials).
    ctx.header(ACCESS_CONTROL_ALLOW_ORIGIN, requestOrigin);
    ctx.header(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
    appendVary(ctx);
    if ("OPTIONS".equalsIgnoreCase(ctx.method())) {
      // Fixed lists only - do not echo Access-Control-Request-Headers from the request.
      ctx.header(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
      ctx.header(ACCESS_CONTROL_ALLOW_HEADERS, ALLOWED_HEADERS);
      ctx.header(ACCESS_CONTROL_MAX_AGE, PREFLIGHT_MAX_AGE_SECONDS);
    }
  }

  private static String normalize(String origin) {
    String trimmed = origin.trim();
    if (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    // Scheme and host are case-insensitive URL components; port is numeric.
    return trimmed.toLowerCase(Locale.ROOT);
  }

  /**
   * Append {@code Origin} to the response Vary header instead of overwriting it. Note the
   * single-argument {@code ctx.header(name)} reads request headers (Javalin 4.6.8 Context.kt
   * delegates to {@code req.getHeader}), so an existing response value is read via {@code
   * ctx.res.getHeader}.
   */
  private static void appendVary(Context ctx) {
    String existing = ctx.res.getHeader(VARY);
    if (existing == null || existing.isEmpty()) {
      ctx.header(VARY, ORIGIN);
      return;
    }
    boolean alreadyListed =
        Arrays.stream(existing.split(","))
            .map(value -> value.trim().toLowerCase(Locale.ROOT))
            .anyMatch(ORIGIN.toLowerCase(Locale.ROOT)::equals);
    if (!alreadyListed) {
      ctx.header(VARY, existing + ", " + ORIGIN);
    }
  }
}
