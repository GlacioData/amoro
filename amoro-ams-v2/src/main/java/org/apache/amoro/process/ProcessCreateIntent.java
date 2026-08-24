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

import lombok.Getter;
import lombok.NonNull;
import org.apache.amoro.resources.ProcessResource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Canonical, already-authorized creation intent shared by manual and scheduled entry points. */
@Getter
public final class ProcessCreateIntent {

  public static final int MAX_PARAMETERS_BYTES = 16 * 1024;
  private static final int MAX_JSON_DEPTH = 64;

  private final ProcessResource.TableRef table;
  private final String action;
  private final String executionEngine;
  private final String triggerSource;
  private final String idempotencyKeyHash;
  private final String requestHash;
  private final Map<String, Object> parameters;

  public ProcessCreateIntent(
      @NonNull ProcessResource.TableRef table,
      @NonNull String action,
      @NonNull String executionEngine,
      @NonNull String triggerSource,
      @NonNull String idempotencyKeyHash,
      @NonNull String requestHash,
      @NonNull Map<String, Object> parameters) {
    this.table = table;
    this.action = action;
    this.executionEngine = executionEngine;
    this.triggerSource = triggerSource;
    this.idempotencyKeyHash = idempotencyKeyHash;
    this.requestHash = requestHash;
    this.parameters = freezeParameters(parameters);
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
        parameters == null ? Collections.emptyMap() : freezeParameters(parameters);
    String requestHash =
        sha256(
            table.catalog()
                + "|"
                + table.database()
                + "|"
                + table.table()
                + "|"
                + table.tableFormat()
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

  String admissionScope() {
    return table.tableId() + "|" + action;
  }

  /**
   * Validates a format-neutral JSON object, rejects cycles/non-finite numbers and returns a deep
   * immutable copy. The cap is measured on the deterministic canonical UTF-8 representation.
   */
  public static Map<String, Object> freezeParameters(Map<String, Object> parameters) {
    Objects.requireNonNull(parameters, "parameters");
    @SuppressWarnings("unchecked")
    Map<String, Object> frozen =
        (Map<String, Object>) freezeJson(parameters, new IdentityHashMap<Object, Boolean>(), 0);
    int encodedBytes = canonical(frozen).getBytes(StandardCharsets.UTF_8).length;
    if (encodedBytes > MAX_PARAMETERS_BYTES) {
      throw new IllegalArgumentException(
          "canonical parameters exceed " + MAX_PARAMETERS_BYTES + " UTF-8 bytes");
    }
    return frozen;
  }

  /** Deterministic JSON encoding used by dummy submission builders and request hashing. */
  public static String canonicalParameters(Map<String, Object> parameters) {
    return canonical(freezeParameters(parameters));
  }

  private static Object freezeJson(
      Object value, IdentityHashMap<Object, Boolean> ancestors, int depth) {
    if (depth > MAX_JSON_DEPTH) {
      throw new IllegalArgumentException(
          "parameters exceed the maximum JSON depth of " + MAX_JSON_DEPTH);
    }
    if (value == null || value instanceof String || value instanceof Boolean) {
      return value;
    }
    if (value instanceof Double) {
      if (!Double.isFinite((Double) value)) {
        throw new IllegalArgumentException("parameters contain a non-finite number");
      }
      return value;
    }
    if (value instanceof Float) {
      if (!Float.isFinite((Float) value)) {
        throw new IllegalArgumentException("parameters contain a non-finite number");
      }
      return value;
    }
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long
        || value instanceof java.math.BigInteger
        || value instanceof java.math.BigDecimal) {
      return value;
    }
    if (value instanceof Map) {
      enter(value, ancestors);
      try {
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          if (!(entry.getKey() instanceof String)) {
            throw new IllegalArgumentException("parameter object keys must be strings");
          }
          copy.put((String) entry.getKey(), freezeJson(entry.getValue(), ancestors, depth + 1));
        }
        return Collections.unmodifiableMap(copy);
      } finally {
        ancestors.remove(value);
      }
    }
    if (value instanceof List) {
      enter(value, ancestors);
      try {
        java.util.ArrayList<Object> copy = new java.util.ArrayList<>();
        for (Object item : (List<?>) value) {
          copy.add(freezeJson(item, ancestors, depth + 1));
        }
        return Collections.unmodifiableList(copy);
      } finally {
        ancestors.remove(value);
      }
    }
    throw new IllegalArgumentException(
        "parameters contain a non-JSON value of type " + value.getClass().getName());
  }

  private static void enter(Object value, IdentityHashMap<Object, Boolean> ancestors) {
    if (ancestors.put(value, Boolean.TRUE) != null) {
      throw new IllegalArgumentException("parameters contain a cyclic JSON structure");
    }
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
      appendJsonString((String) value, builder);
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

  private static void appendJsonString(String value, StringBuilder builder) {
    builder.append('"');
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      switch (character) {
        case '"':
          builder.append("\\\"");
          break;
        case '\\':
          builder.append("\\\\");
          break;
        case '\b':
          builder.append("\\b");
          break;
        case '\f':
          builder.append("\\f");
          break;
        case '\n':
          builder.append("\\n");
          break;
        case '\r':
          builder.append("\\r");
          break;
        case '\t':
          builder.append("\\t");
          break;
        default:
          if (character < 0x20) {
            builder.append(String.format("\\u%04x", (int) character));
          } else {
            builder.append(character);
          }
      }
    }
    builder.append('"');
  }

  private static String sha256(String input) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return "sha256:"
          + HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }
}
