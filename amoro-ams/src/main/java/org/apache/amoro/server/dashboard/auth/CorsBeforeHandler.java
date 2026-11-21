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

import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HandlerType;
import org.apache.amoro.server.AmoroManagementConf;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Handles CORS for explicitly configured HTTP and HTTPS origins. */
public class CorsBeforeHandler implements Handler {

  /** Fixed set: covers the dashboard frontend and API clients; never reflects request input. */
  static final String ALLOWED_HEADERS =
      "Content-Type, Authorization, X-Request-Source, X-Requested-With";

  static final String ALLOWED_METHODS = "GET, POST, PUT, DELETE, OPTIONS";

  static final String PREFLIGHT_MAX_AGE_SECONDS = "600";

  private final Set<String> allowedOrigins;

  public CorsBeforeHandler(List<String> configuredOrigins) {
    this.allowedOrigins = parseAllowedOrigins(configuredOrigins);
  }

  public static void validateAllowedOrigins(List<String> configuredOrigins) {
    parseAllowedOrigins(configuredOrigins);
  }

  private static Set<String> parseAllowedOrigins(List<String> configuredOrigins) {
    if (configuredOrigins == null || configuredOrigins.isEmpty()) {
      return Collections.emptySet();
    }

    Set<String> origins = new LinkedHashSet<>();
    for (String configuredOrigin : configuredOrigins) {
      String origin = normalizeOrigin(configuredOrigin);
      if (origin == null) {
        throw new IllegalArgumentException(
            "configuration "
                + AmoroManagementConf.HTTP_SERVER_CORS_ALLOWED_ORIGINS.key()
                + " must contain only explicit HTTP or HTTPS origins (scheme, host and optional port)");
      }
      origins.add(origin);
    }
    return origins;
  }

  @Override
  public void handle(Context ctx) {
    if (allowedOrigins.isEmpty()) {
      return;
    }

    // A cached response without CORS headers must also vary by Origin.
    appendVary(ctx);
    String requestOrigin = ctx.header(ORIGIN);
    if (!allowedOrigins.contains(normalizeOrigin(requestOrigin))) {
      return;
    }
    // Explicit origin, never the request value reflected from an allowlist miss; credentialed
    // responses must not use the wildcard (MDN: Requests with credentials).
    ctx.header(ACCESS_CONTROL_ALLOW_ORIGIN, requestOrigin);
    ctx.header(ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
    if (ctx.method() == HandlerType.OPTIONS) {
      // Fixed lists only - do not echo Access-Control-Request-Headers from the request.
      ctx.header(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
      ctx.header(ACCESS_CONTROL_ALLOW_HEADERS, ALLOWED_HEADERS);
      ctx.header(ACCESS_CONTROL_MAX_AGE, PREFLIGHT_MAX_AGE_SECONDS);
    }
  }

  private static String normalizeOrigin(String value) {
    if (value == null) {
      return null;
    }

    URI origin;
    try {
      origin = new URI(value.trim());
    } catch (URISyntaxException e) {
      return null;
    }

    String scheme = origin.getScheme();
    boolean https = "https".equalsIgnoreCase(scheme);
    String host = origin.getHost();
    int port = origin.getPort();
    if ((!https && !"http".equalsIgnoreCase(scheme))
        || host == null
        || host.contains("%")
        || origin.getRawAuthority().endsWith(":")
        || port > 65535
        || origin.getRawUserInfo() != null
        || origin.getRawQuery() != null
        || origin.getRawFragment() != null
        || (!origin.getRawPath().isEmpty() && !"/".equals(origin.getRawPath()))) {
      return null;
    }

    String normalized = scheme.toLowerCase(Locale.ROOT) + "://" + host.toLowerCase(Locale.ROOT);
    int defaultPort = https ? 443 : 80;
    return port == -1 || port == defaultPort ? normalized : normalized + ":" + port;
  }

  private static void appendVary(Context ctx) {
    boolean alreadyVariesByOrigin =
        ctx.res().getHeaders(VARY).stream()
            .flatMap(header -> Arrays.stream(header.split(",")))
            .map(String::trim)
            .anyMatch(value -> ORIGIN.equalsIgnoreCase(value) || "*".equals(value));
    if (!alreadyVariesByOrigin) {
      ctx.res().addHeader(VARY, ORIGIN);
    }
  }
}
