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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The v2 engine result vocabulary (process spec §6.1): five-way submission, six-way submission
 * resolution, four-way observation and cancellation outcomes, the engine observation payload and
 * the immutable capability snapshot. Every outcome type is final with equals/hashCode so
 * transitions and tests compare by value.
 */
public final class EngineTypes {

  private EngineTypes() {}

  /** submit(): what the adapter could prove about the submission attempt. */
  public static final class SubmissionOutcome {
    public enum Kind {
      ACKNOWLEDGED, // accepted; externalId known
      REJECTED, // authoritative refusal; consumes retry budget
      UNKNOWN, // side effects undetermined; never blind-resubmit
      CONFLICT, // the submissionKey collided with a different request
      UNAVAILABLE // provably never sent (e.g. connection never established)
    }

    private final Kind kind;
    private final String externalId; // non-null only for ACKNOWLEDGED
    private final String reason; // optional diagnostics

    private SubmissionOutcome(Kind kind, String externalId, String reason) {
      this.kind = kind;
      this.externalId = externalId;
      this.reason = reason;
    }

    public static SubmissionOutcome acknowledged(String externalId) {
      return new SubmissionOutcome(
          Kind.ACKNOWLEDGED, Objects.requireNonNull(externalId, "externalId"), null);
    }

    public static SubmissionOutcome rejected(String reason) {
      return new SubmissionOutcome(Kind.REJECTED, null, reason);
    }

    public static SubmissionOutcome unknown() {
      return new SubmissionOutcome(Kind.UNKNOWN, null, null);
    }

    public static SubmissionOutcome conflict() {
      return new SubmissionOutcome(Kind.CONFLICT, null, null);
    }

    public static SubmissionOutcome unavailable() {
      return new SubmissionOutcome(Kind.UNAVAILABLE, null, null);
    }

    public Kind kind() {
      return kind;
    }

    public String externalId() {
      return externalId;
    }

    public String reason() {
      return reason;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubmissionOutcome that = (SubmissionOutcome) o;
      return kind == that.kind
          && Objects.equals(externalId, that.externalId)
          && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, externalId, reason);
    }
  }

  /** resolveSubmission(): authoritative disposition of a submitted key. */
  public static final class SubmissionResolution {
    public enum Kind {
      ACKNOWLEDGED,
      NOT_FOUND,
      UNAVAILABLE,
      UNSUPPORTED,
      CONFLICT,
      LOST
    }

    private final Kind kind;
    private final String externalId; // for ACKNOWLEDGED
    private final String reason; // for LOST

    private SubmissionResolution(Kind kind, String externalId, String reason) {
      this.kind = kind;
      this.externalId = externalId;
      this.reason = reason;
    }

    public static SubmissionResolution acknowledged(String externalId) {
      return new SubmissionResolution(
          Kind.ACKNOWLEDGED, Objects.requireNonNull(externalId, "externalId"), null);
    }

    public static SubmissionResolution notFound() {
      return new SubmissionResolution(Kind.NOT_FOUND, null, null);
    }

    public static SubmissionResolution unavailable() {
      return new SubmissionResolution(Kind.UNAVAILABLE, null, null);
    }

    public static SubmissionResolution unsupported() {
      return new SubmissionResolution(Kind.UNSUPPORTED, null, null);
    }

    public static SubmissionResolution conflict() {
      return new SubmissionResolution(Kind.CONFLICT, null, null);
    }

    public static SubmissionResolution lost(String reason) {
      return new SubmissionResolution(Kind.LOST, null, reason);
    }

    public Kind kind() {
      return kind;
    }

    public String externalId() {
      return externalId;
    }

    public String reason() {
      return reason;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubmissionResolution that = (SubmissionResolution) o;
      return kind == that.kind
          && Objects.equals(externalId, that.externalId)
          && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, externalId, reason);
    }
  }

  /** observe(): the engine's view of one execution. */
  public static final class ProcessObservation {
    public enum Kind {
      KNOWN,
      NOT_FOUND,
      UNAVAILABLE,
      LOST
    }

    private final Kind kind;
    private final EngineObservation observation; // for KNOWN
    private final String reason; // for LOST

    private ProcessObservation(Kind kind, EngineObservation observation, String reason) {
      this.kind = kind;
      this.observation = observation;
      this.reason = reason;
    }

    public static ProcessObservation known(EngineObservation observation) {
      return new ProcessObservation(
          Kind.KNOWN, Objects.requireNonNull(observation, "observation"), null);
    }

    public static ProcessObservation notFound() {
      return new ProcessObservation(Kind.NOT_FOUND, null, null);
    }

    public static ProcessObservation unavailable() {
      return new ProcessObservation(Kind.UNAVAILABLE, null, null);
    }

    public static ProcessObservation lost(String reason) {
      return new ProcessObservation(Kind.LOST, null, reason);
    }

    public Kind kind() {
      return kind;
    }

    public EngineObservation observation() {
      return observation;
    }

    public String reason() {
      return reason;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProcessObservation that = (ProcessObservation) o;
      return kind == that.kind
          && Objects.equals(observation, that.observation)
          && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, observation, reason);
    }
  }

  /** cancel(): what the engine could do about the cancellation. */
  public static final class CancellationOutcome {
    public enum Kind {
      ACCEPTED,
      ALREADY_TERMINAL,
      NOT_FOUND,
      UNAVAILABLE,
      UNSUPPORTED
    }

    private final Kind kind;
    private final EngineObservation terminalObservation; // for ALREADY_TERMINAL

    private CancellationOutcome(Kind kind, EngineObservation terminalObservation) {
      this.kind = kind;
      this.terminalObservation = terminalObservation;
    }

    public static CancellationOutcome accepted() {
      return new CancellationOutcome(Kind.ACCEPTED, null);
    }

    public static CancellationOutcome alreadyTerminal(EngineObservation observation) {
      return new CancellationOutcome(
          Kind.ALREADY_TERMINAL, Objects.requireNonNull(observation, "observation"));
    }

    public static CancellationOutcome notFound() {
      return new CancellationOutcome(Kind.NOT_FOUND, null);
    }

    public static CancellationOutcome unavailable() {
      return new CancellationOutcome(Kind.UNAVAILABLE, null);
    }

    public static CancellationOutcome unsupported() {
      return new CancellationOutcome(Kind.UNSUPPORTED, null);
    }

    public Kind kind() {
      return kind;
    }

    public EngineObservation terminalObservation() {
      return terminalObservation;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CancellationOutcome that = (CancellationOutcome) o;
      return kind == that.kind && Objects.equals(terminalObservation, that.terminalObservation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, terminalObservation);
    }
  }

  /** Structured engine failure: FAILED observations must carry one, others must not. */
  public static final class EngineFailure {
    private final String code;
    private final String message;
    private final boolean retryable;

    public EngineFailure(String code, String message, boolean retryable) {
      this.code = Objects.requireNonNull(code, "code");
      this.message = Objects.requireNonNull(message, "message");
      this.retryable = retryable;
    }

    public String code() {
      return code;
    }

    public String message() {
      return message;
    }

    public boolean retryable() {
      return retryable;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EngineFailure that = (EngineFailure) o;
      return retryable == that.retryable && code.equals(that.code) && message.equals(that.message);
    }

    @Override
    public int hashCode() {
      return Objects.hash(code, message, retryable);
    }
  }

  /** What observe()/cancel(terminal) report about one execution. */
  public static final class EngineObservation {
    private final String remotePhase; // SUBMITTED..CLOSED
    private final String trackUri; // validated absolute http(s) or null
    private final Map<String, Object> summaryDelta; // bounded action output
    private final EngineFailure failure; // only for FAILED

    public EngineObservation(
        String remotePhase,
        String trackUri,
        Map<String, Object> summaryDelta,
        EngineFailure failure) {
      this.remotePhase = Objects.requireNonNull(remotePhase, "remotePhase");
      this.trackUri = trackUri;
      this.summaryDelta =
          summaryDelta == null
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new LinkedHashMap<>(summaryDelta));
      this.failure = failure;
    }

    public String remotePhase() {
      return remotePhase;
    }

    public String trackUri() {
      return trackUri;
    }

    public Map<String, Object> summaryDelta() {
      return summaryDelta;
    }

    public EngineFailure failure() {
      return failure;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EngineObservation that = (EngineObservation) o;
      return remotePhase.equals(that.remotePhase)
          && Objects.equals(trackUri, that.trackUri)
          && summaryDelta.equals(that.summaryDelta)
          && Objects.equals(failure, that.failure);
    }

    @Override
    public int hashCode() {
      return Objects.hash(remotePhase, trackUri, summaryDelta, failure);
    }
  }

  /** Immutable adapter capability snapshot; capabilities() must not perform I/O. */
  public static final class EngineCapabilities {
    private final boolean supportsSubmissionResolution;
    private final boolean supportsCancellation;
    private final String capabilityVersion;

    public EngineCapabilities(
        boolean supportsSubmissionResolution,
        boolean supportsCancellation,
        String capabilityVersion) {
      this.supportsSubmissionResolution = supportsSubmissionResolution;
      this.supportsCancellation = supportsCancellation;
      this.capabilityVersion = Objects.requireNonNull(capabilityVersion, "capabilityVersion");
    }

    public boolean supportsSubmissionResolution() {
      return supportsSubmissionResolution;
    }

    public boolean supportsCancellation() {
      return supportsCancellation;
    }

    public String capabilityVersion() {
      return capabilityVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EngineCapabilities that = (EngineCapabilities) o;
      return supportsSubmissionResolution == that.supportsSubmissionResolution
          && supportsCancellation == that.supportsCancellation
          && capabilityVersion.equals(that.capabilityVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hash(supportsSubmissionResolution, supportsCancellation, capabilityVersion);
    }
  }
}
