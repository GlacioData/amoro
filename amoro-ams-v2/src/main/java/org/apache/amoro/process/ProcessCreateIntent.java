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

package org.apache.amoro.process;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Canonical, already-authorized creation intent shared by manual and scheduled entry points. */
public final class ProcessCreateIntent {

  private final ProcessResource.TableRef table;
  private final String action;
  private final String executionEngine;
  private final String triggerSource;
  private final String idempotencyKeyHash;
  private final String requestHash;
  private final Map<String, Object> parameters;

  public ProcessCreateIntent(
      ProcessResource.TableRef table,
      String action,
      String executionEngine,
      String triggerSource,
      String idempotencyKeyHash,
      String requestHash,
      Map<String, Object> parameters) {
    this.table = Objects.requireNonNull(table, "table");
    this.action = Objects.requireNonNull(action, "action");
    this.executionEngine = Objects.requireNonNull(executionEngine, "executionEngine");
    this.triggerSource = Objects.requireNonNull(triggerSource, "triggerSource");
    this.idempotencyKeyHash =
        Objects.requireNonNull(idempotencyKeyHash, "idempotencyKeyHash");
    this.requestHash = Objects.requireNonNull(requestHash, "requestHash");
    this.parameters =
        Collections.unmodifiableMap(
            new LinkedHashMap<>(Objects.requireNonNull(parameters, "parameters")));
  }

  /**
   * Resolves a caller-visible idempotency key into the canonical identity shared by REST and
   * scheduled creation. The raw key is never stored in the Process resource.
   */
  public static ProcessCreateIntent resolve(
      ProcessResource.TableRef table,
      String action,
      String executionEngine,
      String triggerSource,
      String rawIdempotencyKey,
      Map<String, Object> parameters) {
    Objects.requireNonNull(rawIdempotencyKey, "rawIdempotencyKey");
    Map<String, Object> frozen =
        parameters == null ? Collections.emptyMap() : new LinkedHashMap<>(parameters);
    String requestHash =
        sha256(
            table.catalog()
                + "|"
                + table.database()
                + "|"
                + table.table()
                + "|"
                + action
                + "|"
                + executionEngine
                + "|"
                + canonical(frozen));
    return new ProcessCreateIntent(
        table,
        action,
        executionEngine,
        triggerSource,
        sha256(rawIdempotencyKey),
        requestHash,
        frozen);
  }

  public ProcessResource.TableRef table() {
    return table;
  }

  public String action() {
    return action;
  }

  public String executionEngine() {
    return executionEngine;
  }

  public String triggerSource() {
    return triggerSource;
  }

  public String idempotencyKeyHash() {
    return idempotencyKeyHash;
  }

  public String requestHash() {
    return requestHash;
  }

  public Map<String, Object> parameters() {
    return parameters;
  }

  String admissionScope() {
    return table.tableId() + "|" + action;
  }

  private static String canonical(Object value) {
    StringBuilder builder = new StringBuilder();
    appendCanonical(value, builder);
    return builder.toString();
  }

  private static void appendCanonical(Object value, StringBuilder builder) {
    if (value == null) {
      builder.append("null");
    } else if (value instanceof String) {
      builder
          .append('"')
          .append(((String) value).replace("\\", "\\\\").replace("\"", "\\\""))
          .append('"');
    } else if (value instanceof Number || value instanceof Boolean) {
      builder.append(value);
    } else if (value instanceof Map) {
      Map<String, Object> sorted = new TreeMap<>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        sorted.put(String.valueOf(entry.getKey()), entry.getValue());
      }
      builder.append('{');
      boolean first = true;
      for (Map.Entry<String, Object> entry : sorted.entrySet()) {
        if (!first) {
          builder.append(',');
        }
        first = false;
        appendCanonical(entry.getKey(), builder);
        builder.append(':');
        appendCanonical(entry.getValue(), builder);
      }
      builder.append('}');
    } else if (value instanceof List) {
      builder.append('[');
      boolean first = true;
      for (Object item : (List<?>) value) {
        if (!first) {
          builder.append(',');
        }
        first = false;
        appendCanonical(item, builder);
      }
      builder.append(']');
    } else {
      appendCanonical(String.valueOf(value), builder);
    }
  }

  private static String sha256(String input) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }
}
