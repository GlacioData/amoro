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

package org.apache.amoro.process.engine;

import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.EngineCapabilities;
import org.apache.amoro.process.engine.EngineTypes.EngineFailure;
import org.apache.amoro.process.engine.EngineTypes.EngineObservation;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates and sanitizes untrusted adapter responses before any Process mutation sees them. */
final class EngineBoundaryValidator {

  private static final int MAX_ID_BYTES = 512;
  private static final int MAX_REASON_BYTES = 1024;
  private static final int MAX_SUMMARY_BYTES = 8192;
  private static final com.fasterxml.jackson.databind.ObjectMapper SUMMARY_MAPPER =
      new com.fasterxml.jackson.databind.ObjectMapper();
  private static final Set<String> PHASES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "SUBMITTED", "RUNNING", "SUCCESS", "FAILED", "CANCELED", "KILLED", "CLOSED")));
  private static final Set<String> TERMINAL_PHASES =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("SUCCESS", "FAILED", "CANCELED", "KILLED", "CLOSED")));

  private EngineBoundaryValidator() {}

  static EngineCapabilities capabilities(EngineCapabilities capabilities) {
    if (capabilities == null || !bounded(capabilities.capabilityVersion(), 1, 128)) {
      throw new IllegalArgumentException("invalid engine capability snapshot");
    }
    return capabilities;
  }

  static SubmissionOutcome submission(SubmissionOutcome outcome) {
    if (outcome == null) {
      return SubmissionOutcome.unknown();
    }
    switch (outcome.kind()) {
      case ACKNOWLEDGED:
        return bounded(outcome.externalId(), 1, MAX_ID_BYTES)
            ? outcome
            : SubmissionOutcome.unknown();
      case REJECTED:
        return nullableBounded(outcome.reason(), MAX_REASON_BYTES)
            ? outcome
            : SubmissionOutcome.unknown();
      case UNKNOWN:
      case CONFLICT:
      case UNAVAILABLE:
        return outcome.externalId() == null ? outcome : SubmissionOutcome.unknown();
      default:
        return SubmissionOutcome.unknown();
    }
  }

  static SubmissionResolution resolution(SubmissionResolution resolution) {
    if (resolution == null) {
      return SubmissionResolution.unavailable();
    }
    switch (resolution.kind()) {
      case ACKNOWLEDGED:
        return bounded(resolution.externalId(), 1, MAX_ID_BYTES)
            ? resolution
            : SubmissionResolution.unavailable();
      case LOST:
        return nullableBounded(resolution.reason(), MAX_REASON_BYTES)
            ? resolution
            : SubmissionResolution.unavailable();
      case NOT_FOUND:
      case UNAVAILABLE:
      case UNSUPPORTED:
      case CONFLICT:
        return resolution.externalId() == null ? resolution : SubmissionResolution.unavailable();
      default:
        return SubmissionResolution.unavailable();
    }
  }

  static ProcessObservation observation(ProcessObservation observation) {
    if (observation == null) {
      return ProcessObservation.unavailable();
    }
    if (observation.kind() == ProcessObservation.Kind.KNOWN) {
      EngineObservation sanitized = sanitizeObservation(observation.observation(), false);
      return sanitized == null
          ? ProcessObservation.unavailable()
          : ProcessObservation.known(sanitized);
    }
    if (observation.kind() == ProcessObservation.Kind.LOST
        && !nullableBounded(observation.reason(), MAX_REASON_BYTES)) {
      return ProcessObservation.unavailable();
    }
    return observation;
  }

  static CancellationOutcome cancellation(CancellationOutcome cancellation) {
    if (cancellation == null) {
      return CancellationOutcome.unavailable();
    }
    if (cancellation.kind() == CancellationOutcome.Kind.ALREADY_TERMINAL) {
      EngineObservation sanitized = sanitizeObservation(cancellation.terminalObservation(), true);
      return sanitized == null
          ? CancellationOutcome.unavailable()
          : CancellationOutcome.alreadyTerminal(sanitized);
    }
    return cancellation;
  }

  private static EngineObservation sanitizeObservation(
      EngineObservation observation, boolean requireTerminal) {
    if (observation == null
        || !PHASES.contains(observation.remotePhase())
        || (requireTerminal && !TERMINAL_PHASES.contains(observation.remotePhase()))) {
      return null;
    }
    EngineFailure failure = observation.failure();
    if ("FAILED".equals(observation.remotePhase())) {
      if (failure == null
          || !bounded(failure.code(), 1, MAX_REASON_BYTES)
          || !bounded(failure.message(), 1, MAX_REASON_BYTES)) {
        return null;
      }
    } else if (failure != null) {
      return null;
    }
    Map<String, Object> summary = freezeSummary(observation.summaryDelta());
    if (summary == null) {
      return null;
    }
    String trackUri = validTrackUri(observation.trackUri()) ? observation.trackUri() : null;
    return new EngineObservation(
        observation.remotePhase(), trackUri, summary, observation.failure());
  }

  private static boolean validTrackUri(String value) {
    if (value == null) {
      return true;
    }
    for (int i = 0; i < value.length(); i++) {
      if (Character.isISOControl(value.charAt(i))) {
        return false;
      }
    }
    try {
      URI uri = URI.create(value);
      return uri.isAbsolute()
          && uri.getUserInfo() == null
          && uri.getHost() != null
          && ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()));
    } catch (IllegalArgumentException malformed) {
      return false;
    }
  }

  private static Map<String, Object> freezeSummary(Map<String, Object> summary) {
    if (summary == null || summary.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      SummaryBudget budget = new SummaryBudget(MAX_SUMMARY_BYTES);
      @SuppressWarnings("unchecked")
      Map<String, Object> frozen =
          (Map<String, Object>)
              freezeJson(summary, new IdentityHashMap<Object, Boolean>(), budget, 0);
      return SUMMARY_MAPPER.writeValueAsBytes(frozen).length <= MAX_SUMMARY_BYTES ? frozen : null;
    } catch (IllegalArgumentException
        | com.fasterxml.jackson.core.JsonProcessingException invalid) {
      return null;
    }
  }

  private static Object freezeJson(
      Object value, IdentityHashMap<Object, Boolean> ancestors, SummaryBudget budget, int depth) {
    if (depth > 32) {
      throw new IllegalArgumentException("summary nesting is too deep");
    }
    if (value == null) {
      budget.add(4);
      return null;
    }
    if (value instanceof String) {
      budget.add(((String) value).getBytes(StandardCharsets.UTF_8).length + 2);
      return value;
    }
    if (value instanceof Boolean
        || value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long
        || value instanceof BigInteger
        || value instanceof BigDecimal) {
      budget.add(String.valueOf(value).getBytes(StandardCharsets.UTF_8).length);
      return value;
    }
    if (value instanceof Float || value instanceof Double) {
      double number = ((Number) value).doubleValue();
      if (!Double.isFinite(number)) {
        throw new IllegalArgumentException("summary number must be finite");
      }
      budget.add(String.valueOf(value).getBytes(StandardCharsets.UTF_8).length);
      return value;
    }
    if (!(value instanceof Map) && !(value instanceof List)) {
      throw new IllegalArgumentException("summary contains a non-JSON value");
    }
    if (ancestors.put(value, Boolean.TRUE) != null) {
      throw new IllegalArgumentException("summary contains a cycle");
    }
    try {
      if (value instanceof Map) {
        budget.add(2);
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          if (!(entry.getKey() instanceof String)) {
            throw new IllegalArgumentException("summary object keys must be strings");
          }
          String key = (String) entry.getKey();
          budget.add(key.getBytes(StandardCharsets.UTF_8).length + 4);
          copy.put(key, freezeJson(entry.getValue(), ancestors, budget, depth + 1));
        }
        return Collections.unmodifiableMap(copy);
      }
      budget.add(2);
      List<Object> copy = new ArrayList<>();
      for (Object item : (List<?>) value) {
        budget.add(1);
        copy.add(freezeJson(item, ancestors, budget, depth + 1));
      }
      return Collections.unmodifiableList(copy);
    } finally {
      ancestors.remove(value);
    }
  }

  private static final class SummaryBudget {
    private final int maximum;
    private int used;

    private SummaryBudget(int maximum) {
      this.maximum = maximum;
    }

    private void add(int bytes) {
      used = Math.addExact(used, bytes);
      if (used > maximum) {
        throw new IllegalArgumentException("summary is too large");
      }
    }
  }

  private static boolean nullableBounded(String value, int maxBytes) {
    return value == null || bounded(value, 0, maxBytes);
  }

  private static boolean bounded(String value, int minBytes, int maxBytes) {
    if (value == null) {
      return false;
    }
    int bytes = value.getBytes(StandardCharsets.UTF_8).length;
    return bytes >= minBytes && bytes <= maxBytes;
  }
}
