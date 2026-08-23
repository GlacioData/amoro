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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Validates and sanitizes untrusted adapter responses before any Process mutation sees them. */
final class EngineBoundaryValidator {

  private static final int MAX_ID_BYTES = 512;
  private static final int MAX_REASON_BYTES = 1024;
  private static final int MAX_SUMMARY_BYTES = 8192;
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
    if (capabilities == null
        || !bounded(capabilities.capabilityVersion(), 1, 128)) {
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
        return resolution.externalId() == null
            ? resolution
            : SubmissionResolution.unavailable();
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
    if (estimatedSummaryBytes(observation.summaryDelta()) > MAX_SUMMARY_BYTES) {
      return null;
    }
    String trackUri = validTrackUri(observation.trackUri()) ? observation.trackUri() : null;
    return new EngineObservation(
        observation.remotePhase(), trackUri, observation.summaryDelta(), observation.failure());
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

  private static int estimatedSummaryBytes(Map<String, Object> summary) {
    return String.valueOf(summary).getBytes(StandardCharsets.UTF_8).length;
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
