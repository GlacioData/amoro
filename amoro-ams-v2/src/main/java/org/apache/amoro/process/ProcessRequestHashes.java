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
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Canonical SHA-256 identity for a frozen action-attempt command. */
public final class ProcessRequestHashes {

  private ProcessRequestHashes() {}

  public static String actionAttempt(
      String processName, int retryNumber, ProcessResource.ProcessSpec spec) {
    return sha256(
        processName
            + "|"
            + retryNumber
            + "|"
            + spec.action()
            + "|"
            + spec.executionEngine()
            + "|"
            + canonical(spec.parameters()));
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

  private static String sha256(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return "sha256:"
          + HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception unavailable) {
      throw new IllegalStateException("SHA-256 is unavailable", unavailable);
    }
  }
}
