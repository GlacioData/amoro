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
import org.apache.amoro.persistence.ControlledResource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
public final class ProcessResource implements ControlledResource {

  public static final String API_VERSION = "process/v1";
  public static final String COLLECTION = "process";

  private final String apiVersion;
  private final String name;
  private final String collection;
  private final long resourceVersion;

  private final ProcessSpec spec;
  private final ProcessStatus status;

  public ProcessResource(String name, ProcessSpec spec, ProcessStatus status) {
    this(API_VERSION, name, COLLECTION, 0L, spec, status);
  }

  @JsonCreator
  public ProcessResource(
      String apiVersion,
      String name,
      String collection,
      long resourceVersion,
      ProcessSpec spec,
      ProcessStatus status) {
    this.apiVersion = Objects.requireNonNull(apiVersion, "apiVersion");
    this.name = Objects.requireNonNull(name, "name");
    this.collection = Objects.requireNonNull(collection, "collection");
    this.resourceVersion = resourceVersion;
    this.spec = Objects.requireNonNull(spec, "spec");
    this.status = Objects.requireNonNull(status, "status");
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String collection() {
    return collection;
  }

  @Override
  public long resourceVersion() {
    return resourceVersion;
  }

  @Override
  public ControlledResource withResourceVersion(long newResourceVersion) {
    return new ProcessResource(apiVersion, name, collection, newResourceVersion, spec, status);
  }

  public String apiVersion() {
    return apiVersion;
  }

  public ProcessSpec spec() {
    return spec;
  }

  public ProcessStatus status() {
    return status;
  }

  public ProcessResource withSpec(ProcessSpec newSpec) {
    return new ProcessResource(apiVersion, name, collection, resourceVersion, newSpec, status);
  }

  public ProcessResource withStatus(ProcessStatus newStatus) {
    return new ProcessResource(apiVersion, name, collection, resourceVersion, spec, newStatus);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProcessResource that = (ProcessResource) o;
    return resourceVersion == that.resourceVersion
        && apiVersion.equals(that.apiVersion)
        && name.equals(that.name)
        && collection.equals(that.collection)
        && spec.equals(that.spec)
        && status.equals(that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apiVersion, name, collection, resourceVersion, spec, status);
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
  public static final class ProcessSpec {

    private final TableRef table;
    private final String action; // lower-kebab wire value, e.g. dummy-maintenance
    private final String executionEngine; // remote-spark | local
    private final String triggerSource; // MANUAL | SCHEDULED
    private final String createdAt; // RFC 3339 UTC
    private final String desiredState; // RUN | CANCEL, RUN->CANCEL only
    private final RequestIdentity request;
    private final java.util.Map<String, Object> parameters; // frozen at creation
    private final RetryPolicy retryPolicy;

    @JsonCreator
    public ProcessSpec(
        TableRef table,
        String action,
        String executionEngine,
        String triggerSource,
        String createdAt,
        String desiredState,
        RequestIdentity request,
        java.util.Map<String, Object> parameters,
        RetryPolicy retryPolicy) {
      this.table = Objects.requireNonNull(table, "table");
      this.action = Objects.requireNonNull(action, "action");
      this.executionEngine = Objects.requireNonNull(executionEngine, "executionEngine");
      this.triggerSource = Objects.requireNonNull(triggerSource, "triggerSource");
      this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
      this.desiredState = Objects.requireNonNull(desiredState, "desiredState");
      this.request = Objects.requireNonNull(request, "request");
      this.parameters = immutableJsonMap(Objects.requireNonNull(parameters, "parameters"));
      this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    }

    public TableRef table() {
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

    public String createdAt() {
      return createdAt;
    }

    public String desiredState() {
      return desiredState;
    }

    public ProcessSpec withDesiredState(String newDesiredState) {
      return new ProcessSpec(
          table,
          action,
          executionEngine,
          triggerSource,
          createdAt,
          newDesiredState,
          request,
          parameters,
          retryPolicy);
    }

    public RequestIdentity request() {
      return request;
    }

    public java.util.Map<String, Object> parameters() {
      return parameters;
    }

    public RetryPolicy retryPolicy() {
      return retryPolicy;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProcessSpec that = (ProcessSpec) o;
      return table.equals(that.table)
          && action.equals(that.action)
          && executionEngine.equals(that.executionEngine)
          && triggerSource.equals(that.triggerSource)
          && createdAt.equals(that.createdAt)
          && desiredState.equals(that.desiredState)
          && request.equals(that.request)
          && parameters.equals(that.parameters)
          && retryPolicy.equals(that.retryPolicy);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          table,
          action,
          executionEngine,
          triggerSource,
          createdAt,
          desiredState,
          request,
          parameters,
          retryPolicy);
    }
  }

  /** Canonical table coordinates (strings end to end; tableId never a number). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  public static final class TableRef {
    private final String catalog;
    private final String database;
    private final String table;
    private final String tableId;
    private final String tableFormat;

    @JsonCreator
    public TableRef(
        String catalog, String database, String table, String tableId, String tableFormat) {
      this.catalog = Objects.requireNonNull(catalog, "catalog");
      this.database = Objects.requireNonNull(database, "database");
      this.table = Objects.requireNonNull(table, "table");
      this.tableId = Objects.requireNonNull(tableId, "tableId");
      this.tableFormat = Objects.requireNonNull(tableFormat, "tableFormat");
    }

    public String catalog() {
      return catalog;
    }

    public String database() {
      return database;
    }

    public String table() {
      return table;
    }

    public String tableId() {
      return tableId;
    }

    public String tableFormat() {
      return tableFormat;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TableRef that = (TableRef) o;
      return catalog.equals(that.catalog)
          && database.equals(that.database)
          && table.equals(that.table)
          && tableId.equals(that.tableId)
          && tableFormat.equals(that.tableFormat);
    }

    @Override
    public int hashCode() {
      return Objects.hash(catalog, database, table, tableId, tableFormat);
    }
  }

  /** Creation-intent identity hashes; raw keys are never stored. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  public static final class RequestIdentity {
    private final String idempotencyKeyHash;
    private final String requestHash;

    @JsonCreator
    public RequestIdentity(String idempotencyKeyHash, String requestHash) {
      this.idempotencyKeyHash = Objects.requireNonNull(idempotencyKeyHash, "idempotencyKeyHash");
      this.requestHash = Objects.requireNonNull(requestHash, "requestHash");
    }

    public String idempotencyKeyHash() {
      return idempotencyKeyHash;
    }

    public String requestHash() {
      return requestHash;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RequestIdentity that = (RequestIdentity) o;
      return idempotencyKeyHash.equals(that.idempotencyKeyHash)
          && requestHash.equals(that.requestHash);
    }

    @Override
    public int hashCode() {
      return Objects.hash(idempotencyKeyHash, requestHash);
    }
  }

  /** Server-frozen retry budgets (first version fixed: 3/2/30s, process spec §8.3). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  public static final class RetryPolicy {
    private final int maxRetries;
    private final int maxSubmissionRetries;
    private final int retryDelaySeconds;

    @JsonCreator
    public RetryPolicy(int maxRetries, int maxSubmissionRetries, int retryDelaySeconds) {
      this.maxRetries = maxRetries;
      this.maxSubmissionRetries = maxSubmissionRetries;
      this.retryDelaySeconds = retryDelaySeconds;
    }

    public int maxRetries() {
      return maxRetries;
    }

    public int maxSubmissionRetries() {
      return maxSubmissionRetries;
    }

    public int retryDelaySeconds() {
      return retryDelaySeconds;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RetryPolicy that = (RetryPolicy) o;
      return maxRetries == that.maxRetries
          && maxSubmissionRetries == that.maxSubmissionRetries
          && retryDelaySeconds == that.retryDelaySeconds;
    }

    @Override
    public int hashCode() {
      return Objects.hash(maxRetries, maxSubmissionRetries, retryDelaySeconds);
    }
  }

  // ------------------------------------------------------------------ status section

  /** The mutable execution view, written only through version-CAS Transitions. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  public static final class ProcessStatus {
    private final String phase; // v1 ten-state names
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
        String phase,
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
      this.phase = Objects.requireNonNull(phase, "phase");
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

    public String phase() {
      return phase;
    }

    public int retryNumber() {
      return retryNumber;
    }

    public ProcessAttempt attempt() {
      return attempt;
    }

    public List<AttemptSummary> attemptHistory() {
      return attemptHistory;
    }

    public String lastObservedAt() {
      return lastObservedAt;
    }

    public String lastCancelAttemptAt() {
      return lastCancelAttemptAt;
    }

    public String nextReconcileAt() {
      return nextReconcileAt;
    }

    public EngineBackoff engineBackoffAttempts() {
      return engineBackoffAttempts;
    }

    public List<Condition> conditions() {
      return conditions;
    }

    public Summary summary() {
      return summary;
    }

    public String failure() {
      return failure;
    }

    public String submittedAt() {
      return submittedAt;
    }

    public String startedAt() {
      return startedAt;
    }

    public String finishedAt() {
      return finishedAt;
    }

    public ProcessStatus withPhase(String newPhase) {
      return new ProcessStatus(
          newPhase,
          retryNumber,
          attempt,
          attemptHistory,
          lastObservedAt,
          lastCancelAttemptAt,
          nextReconcileAt,
          engineBackoffAttempts,
          conditions,
          summary,
          failure,
          submittedAt,
          startedAt,
          finishedAt);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProcessStatus that = (ProcessStatus) o;
      return retryNumber == that.retryNumber
          && phase.equals(that.phase)
          && Objects.equals(attempt, that.attempt)
          && attemptHistory.equals(that.attemptHistory)
          && Objects.equals(lastObservedAt, that.lastObservedAt)
          && Objects.equals(lastCancelAttemptAt, that.lastCancelAttemptAt)
          && Objects.equals(nextReconcileAt, that.nextReconcileAt)
          && engineBackoffAttempts.equals(that.engineBackoffAttempts)
          && conditions.equals(that.conditions)
          && Objects.equals(summary, that.summary)
          && Objects.equals(failure, that.failure)
          && Objects.equals(submittedAt, that.submittedAt)
          && Objects.equals(startedAt, that.startedAt)
          && Objects.equals(finishedAt, that.finishedAt);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          phase,
          retryNumber,
          attempt,
          attemptHistory,
          lastObservedAt,
          lastCancelAttemptAt,
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

    public int dispatchGeneration() {
      return dispatchGeneration;
    }

    public String submissionKey() {
      return submissionKey;
    }

    public String requestHash() {
      return requestHash;
    }

    public String submitState() {
      return submitState;
    }

    public String externalId() {
      return externalId;
    }

    public String dispatchedAt() {
      return dispatchedAt;
    }

    public String lastError() {
      return lastError;
    }

    public String retryDisposition() {
      return retryDisposition;
    }

    public String finishedAt() {
      return finishedAt;
    }

    public List<SubmissionSummary> submissionHistory() {
      return submissionHistory;
    }

    public ManualResolutions manualResolutions() {
      return manualResolutions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProcessAttempt that = (ProcessAttempt) o;
      return dispatchGeneration == that.dispatchGeneration
          && Objects.equals(submissionKey, that.submissionKey)
          && Objects.equals(requestHash, that.requestHash)
          && Objects.equals(submitState, that.submitState)
          && Objects.equals(externalId, that.externalId)
          && Objects.equals(dispatchedAt, that.dispatchedAt)
          && Objects.equals(lastError, that.lastError)
          && Objects.equals(retryDisposition, that.retryDisposition)
          && Objects.equals(finishedAt, that.finishedAt)
          && submissionHistory.equals(that.submissionHistory)
          && Objects.equals(manualResolutions, that.manualResolutions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          dispatchGeneration,
          submissionKey,
          requestHash,
          submitState,
          externalId,
          dispatchedAt,
          lastError,
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
  public static final class SubmissionSummary {
    private final int dispatchGeneration;
    private final String submissionKey;
    private final String requestHash;
    private final String outcome;
    private final ManualResolution manualResolution;
    private final String finishedAt;

    @JsonCreator
    public SubmissionSummary(
        int dispatchGeneration,
        String submissionKey,
        String requestHash,
        String outcome,
        ManualResolution manualResolution,
        String finishedAt) {
      this.dispatchGeneration = dispatchGeneration;
      this.submissionKey = submissionKey;
      this.requestHash = requestHash;
      this.outcome = outcome;
      this.manualResolution = manualResolution;
      this.finishedAt = finishedAt;
    }

    public int dispatchGeneration() {
      return dispatchGeneration;
    }

    public String submissionKey() {
      return submissionKey;
    }

    public String requestHash() {
      return requestHash;
    }

    public String outcome() {
      return outcome;
    }

    public ManualResolution manualResolution() {
      return manualResolution;
    }

    public String finishedAt() {
      return finishedAt;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubmissionSummary that = (SubmissionSummary) o;
      return dispatchGeneration == that.dispatchGeneration
          && Objects.equals(submissionKey, that.submissionKey)
          && Objects.equals(requestHash, that.requestHash)
          && Objects.equals(outcome, that.outcome)
          && Objects.equals(manualResolution, that.manualResolution)
          && Objects.equals(finishedAt, that.finishedAt);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          dispatchGeneration, submissionKey, requestHash, outcome, manualResolution, finishedAt);
    }
  }

  /** A bounded, attempt-bound manual conclusion audit. Raw idempotency keys are never stored. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
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
        String idempotencyKeyHash,
        String commandHash,
        String submissionKey,
        String requestHash,
        String outcome,
        String externalId,
        Boolean retryAllowed,
        String reason,
        String operatorContext,
        String resolvedAt) {
      this.idempotencyKeyHash = Objects.requireNonNull(idempotencyKeyHash, "idempotencyKeyHash");
      this.commandHash = Objects.requireNonNull(commandHash, "commandHash");
      this.submissionKey = Objects.requireNonNull(submissionKey, "submissionKey");
      this.requestHash = Objects.requireNonNull(requestHash, "requestHash");
      this.outcome = Objects.requireNonNull(outcome, "outcome");
      this.externalId = externalId;
      this.retryAllowed = retryAllowed;
      this.reason = Objects.requireNonNull(reason, "reason");
      this.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext");
      this.resolvedAt = Objects.requireNonNull(resolvedAt, "resolvedAt");
    }

    public String idempotencyKeyHash() {
      return idempotencyKeyHash;
    }

    public String commandHash() {
      return commandHash;
    }

    public String submissionKey() {
      return submissionKey;
    }

    public String requestHash() {
      return requestHash;
    }

    public String outcome() {
      return outcome;
    }

    public String externalId() {
      return externalId;
    }

    public Boolean retryAllowed() {
      return retryAllowed;
    }

    public String reason() {
      return reason;
    }

    public String operatorContext() {
      return operatorContext;
    }

    public String resolvedAt() {
      return resolvedAt;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ManualResolution that = (ManualResolution) o;
      return idempotencyKeyHash.equals(that.idempotencyKeyHash)
          && commandHash.equals(that.commandHash)
          && submissionKey.equals(that.submissionKey)
          && requestHash.equals(that.requestHash)
          && outcome.equals(that.outcome)
          && Objects.equals(externalId, that.externalId)
          && Objects.equals(retryAllowed, that.retryAllowed)
          && reason.equals(that.reason)
          && operatorContext.equals(that.operatorContext)
          && resolvedAt.equals(that.resolvedAt);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          idempotencyKeyHash,
          commandHash,
          submissionKey,
          requestHash,
          outcome,
          externalId,
          retryAllowed,
          reason,
          operatorContext,
          resolvedAt);
    }
  }

  /** Attempt-bound manual resolution audit slots (null when no conclusion exists). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  public static final class ManualResolutions {
    private final ManualResolution submission;
    private final ManualResolution execution;

    @JsonCreator
    public ManualResolutions(ManualResolution submission, ManualResolution execution) {
      this.submission = submission;
      this.execution = execution;
    }

    public ManualResolution submission() {
      return submission;
    }

    public ManualResolution execution() {
      return execution;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ManualResolutions that = (ManualResolutions) o;
      return Objects.equals(submission, that.submission)
          && Objects.equals(execution, that.execution);
    }

    @Override
    public int hashCode() {
      return Objects.hash(submission, execution);
    }
  }

  /** Per-operation engine backoff counters, persisted across restarts (0..7 saturated). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  public static final class EngineBackoff {
    private final int submit;
    private final int resolve;
    private final int observe;
    private final int cancel;

    @JsonCreator
    public EngineBackoff(int submit, int resolve, int observe, int cancel) {
      this.submit = submit;
      this.resolve = resolve;
      this.observe = observe;
      this.cancel = cancel;
    }

    public int submit() {
      return submit;
    }

    public int resolve() {
      return resolve;
    }

    public int observe() {
      return observe;
    }

    public int cancel() {
      return cancel;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EngineBackoff that = (EngineBackoff) o;
      return submit == that.submit
          && resolve == that.resolve
          && observe == that.observe
          && cancel == that.cancel;
    }

    @Override
    public int hashCode() {
      return Objects.hash(submit, resolve, observe, cancel);
    }
  }

  /** A status condition; type-unique, ≤ 8 entries (process spec §3.4). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
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
        String type,
        String status,
        String reason,
        String message,
        String lastTransitionTime,
        String lastUpdateTime,
        String observedCapabilityVersion) {
      this.type = Objects.requireNonNull(type, "type");
      this.status = Objects.requireNonNull(status, "status");
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

    public String type() {
      return type;
    }

    public String status() {
      return status;
    }

    public String reason() {
      return reason;
    }

    public String message() {
      return message;
    }

    public String lastTransitionTime() {
      return lastTransitionTime;
    }

    public String lastUpdateTime() {
      return lastUpdateTime;
    }

    public String observedCapabilityVersion() {
      return observedCapabilityVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Condition that = (Condition) o;
      return type.equals(that.type)
          && status.equals(that.status)
          && Objects.equals(reason, that.reason)
          && Objects.equals(message, that.message)
          && Objects.equals(lastTransitionTime, that.lastTransitionTime)
          && Objects.equals(lastUpdateTime, that.lastUpdateTime)
          && Objects.equals(observedCapabilityVersion, that.observedCapabilityVersion);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          type,
          status,
          reason,
          message,
          lastTransitionTime,
          lastUpdateTime,
          observedCapabilityVersion);
    }
  }

  /** Bounded action result summary (trackUri validated at the adapter boundary). */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
  public static final class Summary {
    private final String trackUri;
    private final java.util.Map<String, Object> result;

    @JsonCreator
    public Summary(String trackUri, java.util.Map<String, Object> result) {
      this.trackUri = trackUri;
      this.result = result == null ? Collections.emptyMap() : immutableJsonMap(result);
    }

    public String trackUri() {
      return trackUri;
    }

    public java.util.Map<String, Object> result() {
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Summary that = (Summary) o;
      return Objects.equals(trackUri, that.trackUri) && result.equals(that.result);
    }

    @Override
    public int hashCode() {
      return Objects.hash(trackUri, result);
    }
  }

  /** Archived action attempt (retry history), bounded by maxRetries. */
  @JsonAutoDetect(
      fieldVisibility = JsonAutoDetect.Visibility.ANY,
      getterVisibility = JsonAutoDetect.Visibility.NONE,
      isGetterVisibility = JsonAutoDetect.Visibility.NONE)
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

    public int retryNumber() {
      return retryNumber;
    }

    public int dispatchGeneration() {
      return dispatchGeneration;
    }

    public String submissionKey() {
      return submissionKey;
    }

    public String requestHash() {
      return requestHash;
    }

    public String outcome() {
      return outcome;
    }

    public String externalId() {
      return externalId;
    }

    public String retryDisposition() {
      return retryDisposition;
    }

    public List<SubmissionSummary> submissionHistory() {
      return submissionHistory;
    }

    public ManualResolutions manualResolutions() {
      return manualResolutions;
    }

    public String finishedAt() {
      return finishedAt;
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
      AttemptSummary that = (AttemptSummary) o;
      return retryNumber == that.retryNumber
          && dispatchGeneration == that.dispatchGeneration
          && Objects.equals(submissionKey, that.submissionKey)
          && Objects.equals(requestHash, that.requestHash)
          && Objects.equals(outcome, that.outcome)
          && Objects.equals(externalId, that.externalId)
          && Objects.equals(retryDisposition, that.retryDisposition)
          && submissionHistory.equals(that.submissionHistory)
          && Objects.equals(manualResolutions, that.manualResolutions)
          && Objects.equals(finishedAt, that.finishedAt)
          && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          retryNumber,
          dispatchGeneration,
          submissionKey,
          requestHash,
          outcome,
          externalId,
          retryDisposition,
          submissionHistory,
          manualResolutions,
          finishedAt,
          reason);
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
