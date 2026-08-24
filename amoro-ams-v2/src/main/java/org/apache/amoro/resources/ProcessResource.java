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

package org.apache.amoro.resources;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.With;
import org.apache.amoro.persistence.ControlledResource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The v2 Process resource (process spec §3.1), persisted as Base64(YAML) in the dedicated {@code
 * amoro_process} table. Deeply immutable by contract: every mutation returns a new instance; the
 * framework's serde round-trip additionally isolates untrusted aliases.
 *
 * <p>Field ownership follows process spec §3.2: spec is frozen at creation except {@code
 * desiredState} (RUN→CANCEL only); status fields are written exclusively by the Transitions through
 * version-CAS modifies; histories are bounded (attempts ≤ maxRetries, submission generations ≤
 * maxSubmissionRetries) so the max-legal shape stays under the framework's 64KiB document bound.
 */
@JsonAutoDetect(
    fieldVisibility = JsonAutoDetect.Visibility.ANY,
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    isGetterVisibility = JsonAutoDetect.Visibility.NONE)
@Getter
@EqualsAndHashCode
public final class ProcessResource implements ControlledResource {

  public static final String API_VERSION = "process/v1";
  public static final String COLLECTION = "process";

  private final String apiVersion;
  private final String name;
  private final String collection;
  @With private final long resourceVersion;
  @With private final ProcessSpec spec;
  @With private final ProcessStatus status;

  public ProcessResource(String name, ProcessSpec spec, ProcessStatus status) {
    this(API_VERSION, name, COLLECTION, 0L, spec, status);
  }

  @JsonCreator
  public ProcessResource(
      @NonNull String apiVersion,
      @NonNull String name,
      @NonNull String collection,
      long resourceVersion,
      @NonNull ProcessSpec spec,
      @NonNull ProcessStatus status) {
    this.apiVersion = apiVersion;
    this.name = name;
    this.collection = collection;
    this.resourceVersion = resourceVersion;
    this.spec = spec;
    this.status = status;
  }

  @Override
  public String toString() {
    return "ProcessResource{"
        + name
        + ", v"
        + resourceVersion
        + ", phase="
        + status.phase()
        + ", desired="
        + spec.desiredState()
        + "}";
  }

  // ------------------------------------------------------------------ spec section

  /** The frozen creation intent plus the monotonic desired state (process spec §3.1). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class ProcessSpec {

    private final TableRef table;
    private final String action; // lower-kebab wire value, e.g. dummy-maintenance
    private final String executionEngine; // remote-spark | local
    private final String triggerSource; // MANUAL | SCHEDULED
    private final String createdAt; // RFC 3339 UTC
    @With private final String desiredState; // RUN | CANCEL, RUN->CANCEL only
    private final RequestIdentity request;
    private final java.util.Map<String, Object> parameters; // frozen at creation
    private final RetryPolicy retryPolicy;

    @JsonCreator
    public ProcessSpec(
        @NonNull TableRef table,
        @NonNull String action,
        @NonNull String executionEngine,
        @NonNull String triggerSource,
        @NonNull String createdAt,
        @NonNull String desiredState,
        @NonNull RequestIdentity request,
        @NonNull java.util.Map<String, Object> parameters,
        @NonNull RetryPolicy retryPolicy) {
      this.table = table;
      this.action = action;
      this.executionEngine = executionEngine;
      this.triggerSource = triggerSource;
      this.createdAt = createdAt;
      this.desiredState = desiredState;
      this.request = request;
      this.parameters = immutableJsonMap(parameters);
      this.retryPolicy = retryPolicy;
    }
  }

  /** Canonical table coordinates (strings end to end; tableId never a number). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class TableRef {
    private final String catalog;
    private final String database;
    private final String table;
    private final String tableId;
    private final String tableFormat;

    @JsonCreator
    public TableRef(
        @NonNull String catalog,
        @NonNull String database,
        @NonNull String table,
        @NonNull String tableId,
        @NonNull String tableFormat) {
      this.catalog = catalog;
      this.database = database;
      this.table = table;
      this.tableId = tableId;
      this.tableFormat = tableFormat;
    }
  }

  /** Creation-intent identity hashes; raw keys are never stored. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class RequestIdentity {
    private final String idempotencyKeyHash;
    private final String requestHash;

    @JsonCreator
    public RequestIdentity(@NonNull String idempotencyKeyHash, @NonNull String requestHash) {
      this.idempotencyKeyHash = idempotencyKeyHash;
      this.requestHash = requestHash;
    }
  }

  /** Server-frozen retry budgets (first version fixed: 3/2/30s, process spec §8.3). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor(onConstructor_ = @JsonCreator)
  public static final class RetryPolicy {
    private final int maxRetries;
    private final int maxSubmissionRetries;
    private final int retryDelaySeconds;
  }

  // ------------------------------------------------------------------ status section

  /** The mutable execution view, written only through version-CAS Transitions. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class ProcessStatus {
    @With private final String phase; // v1 ten-state names
    private final int retryNumber;
    private final ProcessAttempt attempt; // current action attempt (latest, complete)
    private final List<AttemptSummary> attemptHistory; // bounded by maxRetries
    private final String lastObservedAt;
    private final String lastCancelAttemptAt;
    private final String nextReconcileAt; // persisted business gating
    private final EngineBackoff engineBackoffAttempts; // per-operation, restart-persistent
    private final List<Condition> conditions; // ≤ 8, unique by type
    private final Summary summary; // bounded action result
    private final String failure; // structured final failure message
    private final String submittedAt;
    private final String startedAt;
    private final String finishedAt;

    @JsonCreator
    public ProcessStatus(
        @NonNull String phase,
        int retryNumber,
        ProcessAttempt attempt,
        List<AttemptSummary> attemptHistory,
        String lastObservedAt,
        String lastCancelAttemptAt,
        String nextReconcileAt,
        EngineBackoff engineBackoffAttempts,
        List<Condition> conditions,
        Summary summary,
        String failure,
        String submittedAt,
        String startedAt,
        String finishedAt) {
      this.phase = phase;
      this.retryNumber = retryNumber;
      this.attempt = attempt;
      this.attemptHistory =
          attemptHistory == null
              ? Collections.emptyList()
              : Collections.unmodifiableList(new ArrayList<>(attemptHistory));
      this.lastObservedAt = lastObservedAt;
      this.lastCancelAttemptAt = lastCancelAttemptAt;
      this.nextReconcileAt = nextReconcileAt;
      this.engineBackoffAttempts =
          engineBackoffAttempts == null ? new EngineBackoff(0, 0, 0, 0) : engineBackoffAttempts;
      this.conditions =
          conditions == null
              ? Collections.emptyList()
              : Collections.unmodifiableList(new ArrayList<>(conditions));
      this.summary = summary;
      this.failure = failure;
      this.submittedAt = submittedAt;
      this.startedAt = startedAt;
      this.finishedAt = finishedAt;
    }

    /** Backward-compatible construction shape used by existing callers and imported resources. */
    public ProcessStatus(
        String phase,
        int retryNumber,
        ProcessAttempt attempt,
        List<AttemptSummary> attemptHistory,
        String lastObservedAt,
        String nextReconcileAt,
        EngineBackoff engineBackoffAttempts,
        List<Condition> conditions,
        Summary summary,
        String failure,
        String submittedAt,
        String startedAt,
        String finishedAt) {
      this(
          phase,
          retryNumber,
          attempt,
          attemptHistory,
          lastObservedAt,
          null,
          nextReconcileAt,
          engineBackoffAttempts,
          conditions,
          summary,
          failure,
          submittedAt,
          startedAt,
          finishedAt);
    }
  }

  /** The current action attempt: latest submission generation plus audit slots. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class ProcessAttempt {
    private final int dispatchGeneration;
    private final String submissionKey;
    private final String requestHash;
    private final String submitState; // CREATED..UNAVAILABLE
    private final String externalId;
    private final String dispatchedAt;
    private final String lastError;
    private final String retryDisposition; // AUTO | ALLOW | FINAL
    private final String finishedAt; // attempt-scoped terminal time
    private final List<SubmissionSummary> submissionHistory; // bounded generations
    private final ManualResolutions manualResolutions;

    @JsonCreator
    public ProcessAttempt(
        int dispatchGeneration,
        String submissionKey,
        String requestHash,
        String submitState,
        String externalId,
        String dispatchedAt,
        String lastError,
        String retryDisposition,
        String finishedAt,
        List<SubmissionSummary> submissionHistory,
        ManualResolutions manualResolutions) {
      this.dispatchGeneration = dispatchGeneration;
      this.submissionKey = submissionKey;
      this.requestHash = requestHash;
      this.submitState = submitState;
      this.externalId = externalId;
      this.dispatchedAt = dispatchedAt;
      this.lastError = lastError;
      this.retryDisposition = retryDisposition;
      this.finishedAt = finishedAt;
      this.submissionHistory =
          submissionHistory == null
              ? Collections.emptyList()
              : Collections.unmodifiableList(new ArrayList<>(submissionHistory));
      this.manualResolutions = manualResolutions;
    }

    /** Backward-compatible shape; new transition code writes the structured lastError slot. */
    public ProcessAttempt(
        int dispatchGeneration,
        String submissionKey,
        String requestHash,
        String submitState,
        String externalId,
        String dispatchedAt,
        String retryDisposition,
        String finishedAt,
        List<SubmissionSummary> submissionHistory,
        ManualResolutions manualResolutions) {
      this(
          dispatchGeneration,
          submissionKey,
          requestHash,
          submitState,
          externalId,
          dispatchedAt,
          null,
          retryDisposition,
          finishedAt,
          submissionHistory,
          manualResolutions);
    }
  }

  /** Archived ended submission generation (audit). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor(onConstructor_ = @JsonCreator)
  public static final class SubmissionSummary {
    private final int dispatchGeneration;
    private final String submissionKey;
    private final String requestHash;
    private final String outcome;
    private final ManualResolution manualResolution;
    private final String finishedAt;
  }

  /** A bounded, attempt-bound manual conclusion audit. Raw idempotency keys are never stored. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class ManualResolution {
    private final String idempotencyKeyHash;
    private final String commandHash;
    private final String submissionKey;
    private final String requestHash;
    private final String outcome;
    private final String externalId;
    private final Boolean retryAllowed;
    private final String reason;
    private final String operatorContext;
    private final String resolvedAt;

    @JsonCreator
    public ManualResolution(
        @NonNull String idempotencyKeyHash,
        @NonNull String commandHash,
        @NonNull String submissionKey,
        @NonNull String requestHash,
        @NonNull String outcome,
        String externalId,
        Boolean retryAllowed,
        @NonNull String reason,
        @NonNull String operatorContext,
        @NonNull String resolvedAt) {
      this.idempotencyKeyHash = idempotencyKeyHash;
      this.commandHash = commandHash;
      this.submissionKey = submissionKey;
      this.requestHash = requestHash;
      this.outcome = outcome;
      this.externalId = externalId;
      this.retryAllowed = retryAllowed;
      this.reason = reason;
      this.operatorContext = operatorContext;
      this.resolvedAt = resolvedAt;
    }
  }

  /** Attempt-bound manual resolution audit slots (null when no conclusion exists). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor(onConstructor_ = @JsonCreator)
  public static final class ManualResolutions {
    private final ManualResolution submission;
    private final ManualResolution execution;
  }

  /** Per-operation engine backoff counters, persisted across restarts (0..7 saturated). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  @AllArgsConstructor(onConstructor_ = @JsonCreator)
  public static final class EngineBackoff {
    private final int submit;
    private final int resolve;
    private final int observe;
    private final int cancel;
  }

  /** A status condition; type-unique, ≤ 8 entries (process spec §3.4). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class Condition {
    private final String type;
    private final String status; // "True" | "False"
    private final String reason;
    private final String message;
    private final String lastTransitionTime;
    private final String lastUpdateTime;
    private final String observedCapabilityVersion;

    @JsonCreator
    public Condition(
        @NonNull String type,
        @NonNull String status,
        String reason,
        String message,
        String lastTransitionTime,
        String lastUpdateTime,
        String observedCapabilityVersion) {
      this.type = type;
      this.status = status;
      this.reason = reason;
      this.message = message;
      this.lastTransitionTime = lastTransitionTime;
      this.lastUpdateTime = lastUpdateTime;
      this.observedCapabilityVersion = observedCapabilityVersion;
    }

    public Condition(
        String type,
        String status,
        String reason,
        String message,
        String lastTransitionTime,
        String lastUpdateTime) {
      this(type, status, reason, message, lastTransitionTime, lastUpdateTime, null);
    }
  }

  /** Bounded action result summary (trackUri validated at the adapter boundary). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class Summary {
    private final String trackUri;
    private final java.util.Map<String, Object> result;

    @JsonCreator
    public Summary(String trackUri, java.util.Map<String, Object> result) {
      this.trackUri = trackUri;
      this.result = result == null ? Collections.emptyMap() : immutableJsonMap(result);
    }
  }

  /** Archived action attempt (retry history), bounded by maxRetries. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  @Getter
  @EqualsAndHashCode
  public static final class AttemptSummary {
    private final int retryNumber;
    private final int dispatchGeneration;
    private final String submissionKey;
    private final String requestHash;
    private final String outcome;
    private final String externalId; // kept for idempotent handle release
    private final String retryDisposition;
    private final List<SubmissionSummary> submissionHistory;
    private final ManualResolutions manualResolutions;
    private final String finishedAt;
    private final String reason;

    @JsonCreator
    public AttemptSummary(
        int retryNumber,
        int dispatchGeneration,
        String submissionKey,
        String requestHash,
        String outcome,
        String externalId,
        String retryDisposition,
        List<SubmissionSummary> submissionHistory,
        ManualResolutions manualResolutions,
        String finishedAt,
        String reason) {
      this.retryNumber = retryNumber;
      this.dispatchGeneration = dispatchGeneration;
      this.submissionKey = submissionKey;
      this.requestHash = requestHash;
      this.outcome = outcome;
      this.externalId = externalId;
      this.retryDisposition = retryDisposition;
      this.submissionHistory =
          submissionHistory == null
              ? Collections.emptyList()
              : Collections.unmodifiableList(new ArrayList<>(submissionHistory));
      this.manualResolutions = manualResolutions;
      this.finishedAt = finishedAt;
      this.reason = reason;
    }
  }

  static java.util.Map<String, Object> immutableJsonMap(java.util.Map<String, Object> source) {
    java.util.Map<String, Object> copy = new java.util.LinkedHashMap<>();
    for (java.util.Map.Entry<String, Object> entry : source.entrySet()) {
      if (entry.getKey() == null) {
        throw new IllegalArgumentException("JSON object keys must not be null");
      }
      copy.put(entry.getKey(), immutableJsonValue(entry.getValue()));
    }
    return Collections.unmodifiableMap(copy);
  }

  private static Object immutableJsonValue(Object value) {
    if (value == null
        || value instanceof String
        || value instanceof Number
        || value instanceof Boolean) {
      return value;
    }
    if (value instanceof java.util.Map) {
      java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
      for (java.util.Map.Entry<?, ?> entry : ((java.util.Map<?, ?>) value).entrySet()) {
        if (!(entry.getKey() instanceof String)) {
          throw new IllegalArgumentException("JSON object keys must be strings");
        }
        map.put((String) entry.getKey(), immutableJsonValue(entry.getValue()));
      }
      return Collections.unmodifiableMap(map);
    }
    if (value instanceof java.util.List) {
      java.util.List<Object> list = new ArrayList<>();
      for (Object item : (java.util.List<?>) value) {
        list.add(immutableJsonValue(item));
      }
      return Collections.unmodifiableList(list);
    }
    throw new IllegalArgumentException("unsupported JSON value type " + value.getClass().getName());
  }
}
