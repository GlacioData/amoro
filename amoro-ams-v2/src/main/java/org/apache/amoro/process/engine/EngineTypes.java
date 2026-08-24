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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The v2 engine result vocabulary (process spec §6.1): five-way submission, six-way submission
 * resolution, four-way observation and cancellation outcomes, the engine observation payload and
 * the immutable capability snapshot. Every outcome type is final with equals/hashCode so
 * transitions and tests compare by value.
 */
public final class EngineTypes {

  private EngineTypes() {}

  /** submit(): what the adapter could prove about the submission attempt. */
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
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

    public static SubmissionOutcome acknowledged(@NonNull String externalId) {
      return new SubmissionOutcome(Kind.ACKNOWLEDGED, externalId, null);
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
  }

  /** resolveSubmission(): authoritative disposition of a submitted key. */
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
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

    public static SubmissionResolution acknowledged(@NonNull String externalId) {
      return new SubmissionResolution(Kind.ACKNOWLEDGED, externalId, null);
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
  }

  /** observe(): the engine's view of one execution. */
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
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

    public static ProcessObservation known(@NonNull EngineObservation observation) {
      return new ProcessObservation(Kind.KNOWN, observation, null);
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
  }

  /** cancel(): what the engine could do about the cancellation. */
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
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

    public static CancellationOutcome accepted() {
      return new CancellationOutcome(Kind.ACCEPTED, null);
    }

    public static CancellationOutcome alreadyTerminal(@NonNull EngineObservation observation) {
      return new CancellationOutcome(Kind.ALREADY_TERMINAL, observation);
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
  }

  /** Structured engine failure: FAILED observations must carry one, others must not. */
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor
  public static final class EngineFailure {
    @NonNull private final String code;
    @NonNull private final String message;
    private final boolean retryable;
  }

  /** What observe()/cancel(terminal) report about one execution. */
  @Getter
  @EqualsAndHashCode
  public static final class EngineObservation {
    private final String remotePhase; // SUBMITTED..CLOSED
    private final String trackUri; // validated absolute http(s) or null
    private final Map<String, Object> summaryDelta; // bounded action output
    private final EngineFailure failure; // only for FAILED

    public EngineObservation(
        @NonNull String remotePhase,
        String trackUri,
        Map<String, Object> summaryDelta,
        EngineFailure failure) {
      this.remotePhase = remotePhase;
      this.trackUri = trackUri;
      this.summaryDelta =
          summaryDelta == null
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new LinkedHashMap<>(summaryDelta));
      this.failure = failure;
    }
  }

  /** Immutable adapter capability snapshot; capabilities() must not perform I/O. */
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor
  public static final class EngineCapabilities {
    private final boolean supportsSubmissionResolution;
    private final boolean supportsCancellation;
    @NonNull private final String capabilityVersion;
  }
}
