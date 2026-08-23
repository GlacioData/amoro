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

import org.apache.amoro.persistence.exception.PreconditionFailedException;
import org.apache.amoro.persistence.exception.ResourceDoesNotExist;
import org.apache.amoro.persistence.facade.RepositoryFacade;
import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.EngineObservation;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The sole asynchronous engine-result writer. Every callback re-reads the latest resource, verifies
 * its exact attempt identity and retries semantic CAS conflicts without reverting a concurrent
 * desired RUN→CANCEL update.
 */
public final class ProcessResultApplier {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessResultApplier.class);
  private static final int MAX_SUMMARY_BYTES = 8 * 1024;
  private static final com.fasterxml.jackson.databind.ObjectMapper SUMMARY_MAPPER =
      new com.fasterxml.jackson.databind.ObjectMapper();
  private static final long[] ENGINE_BACKOFF_SECONDS = {3, 3, 5, 8, 13, 21, 34, 55};

  private final RepositoryFacade<ProcessResource> repository;
  private final Supplier<String> now;
  private final int maxCasAttempts;
  private final long pollIntervalMillis;
  private final long submissionUnresolvedMillis;
  private final long executionUnresolvedMillis;

  public ProcessResultApplier(
      RepositoryFacade<ProcessResource> repository,
      Supplier<String> now,
      int maxCasAttempts,
      long pollIntervalMillis,
      long submissionUnresolvedMillis,
      long executionUnresolvedMillis) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.now = Objects.requireNonNull(now, "now");
    if (maxCasAttempts <= 0) {
      throw new IllegalArgumentException("maxCasAttempts must be > 0");
    }
    this.maxCasAttempts = maxCasAttempts;
    this.pollIntervalMillis = positive(pollIntervalMillis, "pollIntervalMillis");
    this.submissionUnresolvedMillis =
        positive(submissionUnresolvedMillis, "submissionUnresolvedMillis");
    this.executionUnresolvedMillis =
        positive(executionUnresolvedMillis, "executionUnresolvedMillis");
  }

  public boolean applySubmit(
      String name,
      String submissionKey,
      String requestHash,
      SubmissionOutcome outcome,
      Throwable error) {
    SubmissionOutcome classified = error == null ? outcome : SubmissionOutcome.unknown();
    return apply(
        name,
        current ->
            submitResultApplicable(current, submissionKey, requestHash)
                ? submitTransition(current, classified)
                : null);
  }

  public boolean applyResolution(
      String name,
      String submissionKey,
      String requestHash,
      SubmissionResolution resolution,
      Throwable error,
      String capabilityVersion) {
    SubmissionResolution classified =
        error == null ? resolution : SubmissionResolution.unavailable();
    return apply(
        name,
        current ->
            resolutionResultApplicable(current, submissionKey, requestHash)
                ? resolutionTransition(current, classified, capabilityVersion)
                : null);
  }

  public boolean applyObservation(
      String name,
      String submissionKey,
      String requestHash,
      String externalId,
      ProcessObservation observation,
      Throwable error) {
    ProcessObservation classified = error == null ? observation : ProcessObservation.unavailable();
    return apply(
        name,
        current ->
            observationResultApplicable(current, submissionKey, requestHash, externalId)
                ? observationTransition(current, classified)
                : null);
  }

  public boolean applyCancellation(
      String name,
      String submissionKey,
      String requestHash,
      String externalId,
      CancellationOutcome cancellation,
      Throwable error,
      String capabilityVersion) {
    CancellationOutcome classified =
        error == null ? cancellation : CancellationOutcome.unavailable();
    return apply(
        name,
        current ->
            cancellationResultApplicable(current, submissionKey, requestHash, externalId)
                ? cancellationTransition(current, classified, capabilityVersion)
                : null);
  }

  private boolean apply(String name, Function<ProcessResource, ProcessResource.ProcessStatus> fn) {
    for (int attempt = 0; attempt < maxCasAttempts; attempt++) {
      ProcessResource current;
      try {
        current = repository.get(name);
      } catch (ResourceDoesNotExist deleted) {
        return true;
      } catch (RuntimeException unavailable) {
        LOG.warn(
            "Engine result for Process {} could not read durable state; retry is required.",
            name,
            unavailable);
        return false;
      }
      ProcessResource.ProcessStatus next = fn.apply(current);
      if (next == null || next.equals(current.status())) {
        return true;
      }
      try {
        repository.modify(name, current.resourceVersion(), resource -> resource.withStatus(next));
        return true;
      } catch (PreconditionFailedException raced) {
        // Re-read and re-derive from the winner, preserving desiredState and newer audit fields.
      } catch (RuntimeException unavailable) {
        LOG.warn(
            "Engine result for Process {} could not be persisted; durable state will recover it.",
            name,
            unavailable);
        return false;
      }
    }
    LOG.warn(
        "Engine result CAS retries exhausted for Process {}; next reconciliation will recover.",
        name);
    return false;
  }

  private ProcessResource.ProcessStatus submitTransition(
      ProcessResource current, SubmissionOutcome outcome) {
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    String timestamp = now.get();
    switch (outcome.kind()) {
      case ACKNOWLEDGED:
        return acknowledged(current, outcome.externalId(), Operation.SUBMIT, timestamp);
      case REJECTED:
        return terminal(
            current,
            "FAILED",
            "REJECTED: " + safe(outcome.reason()),
            "ALLOW",
            timestamp,
            current.status().summary(),
            "REJECTED");
      case UNKNOWN:
      case CONFLICT:
        return status(
            status,
            status.phase(),
            attempt(
                attempt,
                outcome.kind().name(),
                null,
                attempt.dispatchedAt(),
                outcome.kind().name(),
                attempt.retryDisposition(),
                null),
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            plusMillis(timestamp, submissionUnresolvedMillis),
            reset(status.engineBackoffAttempts(), Operation.SUBMIT),
            clearEngineCondition(
                ProcessConditions.set(
                    status.conditions(),
                    ProcessConditions.SUBMISSION_UNRESOLVED,
                    outcome.kind().name(),
                    "submission side effects are unresolved",
                    timestamp,
                    null),
                reset(status.engineBackoffAttempts(), Operation.SUBMIT)),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            null);
      case UNAVAILABLE:
      default:
        return unavailable(current, Operation.SUBMIT, "engine unavailable during submit");
    }
  }

  private ProcessResource.ProcessStatus resolutionTransition(
      ProcessResource current, SubmissionResolution resolution, String capabilityVersion) {
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    String timestamp = now.get();
    switch (resolution.kind()) {
      case ACKNOWLEDGED:
        return acknowledged(current, resolution.externalId(), Operation.RESOLVE, timestamp);
      case NOT_FOUND:
        if ("CANCEL".equals(current.spec().desiredState())) {
          return terminal(current, "CANCELED", null, "FINAL", timestamp, null);
        }
        if (attempt.dispatchGeneration() < current.spec().retryPolicy().maxSubmissionRetries()) {
          List<ProcessResource.SubmissionSummary> history =
              new ArrayList<>(attempt.submissionHistory());
          history.add(
              new ProcessResource.SubmissionSummary(
                  attempt.dispatchGeneration(),
                  attempt.submissionKey(),
                  attempt.requestHash(),
                  "NOT_FOUND",
                  null,
                  timestamp));
          int nextGeneration = attempt.dispatchGeneration() + 1;
          ProcessResource.ProcessAttempt nextAttempt =
              new ProcessResource.ProcessAttempt(
                  nextGeneration,
                  current.name() + ":" + status.retryNumber() + ":" + nextGeneration,
                  attempt.requestHash(),
                  "CREATED",
                  null,
                  null,
                  null,
                  "AUTO",
                  null,
                  history,
                  attempt.manualResolutions());
          return status(
              status,
              "PENDING",
              nextAttempt,
              status.attemptHistory(),
              status.lastObservedAt(),
              status.lastCancelAttemptAt(),
              timestamp,
              reset(status.engineBackoffAttempts(), Operation.RESOLVE),
              clearEngineCondition(
                  ProcessConditions.remove(
                      status.conditions(), ProcessConditions.SUBMISSION_UNRESOLVED),
                  reset(status.engineBackoffAttempts(), Operation.RESOLVE)),
              status.summary(),
              null,
              status.submittedAt(),
              status.startedAt(),
              null);
        }
        return failed(current, "SUBMISSION_NOT_ACCEPTED", "ALLOW", timestamp);
      case LOST:
        return executionUnresolved(
            current,
            "SUBMISSION_LEDGER_LOST",
            safe(resolution.reason()),
            Operation.RESOLVE,
            timestamp);
      case UNSUPPORTED:
        return status(
            status,
            status.phase(),
            attempt,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            plusMillis(timestamp, submissionUnresolvedMillis),
            reset(status.engineBackoffAttempts(), Operation.RESOLVE),
            clearEngineCondition(
                ProcessConditions.set(
                    status.conditions(),
                    ProcessConditions.SUBMISSION_UNRESOLVED,
                    "ResolutionUnsupported",
                    "submission resolution is unsupported by this engine capability",
                    timestamp,
                    capabilityVersion),
                reset(status.engineBackoffAttempts(), Operation.RESOLVE)),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt());
      case CONFLICT:
        return status(
            status,
            status.phase(),
            attempt(
                attempt,
                "CONFLICT",
                null,
                attempt.dispatchedAt(),
                "RESOLUTION_CONFLICT",
                attempt.retryDisposition(),
                null),
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            plusMillis(timestamp, submissionUnresolvedMillis),
            reset(status.engineBackoffAttempts(), Operation.RESOLVE),
            clearEngineCondition(
                ProcessConditions.set(
                    status.conditions(),
                    ProcessConditions.SUBMISSION_UNRESOLVED,
                    "CONFLICT",
                    "submission resolution conflicted with the request identity",
                    timestamp,
                    null),
                reset(status.engineBackoffAttempts(), Operation.RESOLVE)),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt());
      case UNAVAILABLE:
      default:
        return unavailable(current, Operation.RESOLVE, "engine unavailable during resolution");
    }
  }

  private ProcessResource.ProcessStatus acknowledged(
      ProcessResource current, String externalId, Operation operation, String timestamp) {
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    ProcessResource.EngineBackoff backoff = reset(status.engineBackoffAttempts(), operation);
    return status(
        status,
        "CANCEL".equals(current.spec().desiredState()) ? "CANCELING" : "SUBMITTED",
        attempt(
            attempt,
            "ACKNOWLEDGED",
            externalId,
            attempt.dispatchedAt() == null ? timestamp : attempt.dispatchedAt(),
            null,
            attempt.retryDisposition(),
            attempt.finishedAt()),
        status.attemptHistory(),
        status.lastObservedAt(),
        status.lastCancelAttemptAt(),
        plusMillis(timestamp, pollIntervalMillis),
        backoff,
        clearEngineCondition(
            ProcessConditions.remove(status.conditions(), ProcessConditions.SUBMISSION_UNRESOLVED),
            backoff),
        status.summary(),
        status.failure(),
        status.submittedAt() == null ? timestamp : status.submittedAt(),
        status.startedAt(),
        status.finishedAt());
  }

  private ProcessResource.ProcessStatus observationTransition(
      ProcessResource current, ProcessObservation observation) {
    String timestamp = now.get();
    switch (observation.kind()) {
      case KNOWN:
        return observed(current, observation.observation(), timestamp);
      case NOT_FOUND:
        return executionUnresolved(
            current,
            "EXECUTION_NOT_FOUND",
            "the acknowledged execution is not authoritatively observable",
            Operation.OBSERVE,
            timestamp);
      case LOST:
        return executionUnresolved(
            current, "EXECUTION_LOST", safe(observation.reason()), Operation.OBSERVE, timestamp);
      case UNAVAILABLE:
      default:
        return unavailable(current, Operation.OBSERVE, "engine unavailable during observation");
    }
  }

  private ProcessResource.ProcessStatus cancellationTransition(
      ProcessResource current, CancellationOutcome cancellation, String capabilityVersion) {
    String timestamp = now.get();
    switch (cancellation.kind()) {
      case ACCEPTED:
        return status(
            current.status(),
            "CANCELING",
            current.status().attempt(),
            current.status().attemptHistory(),
            current.status().lastObservedAt(),
            current.status().lastCancelAttemptAt(),
            plusMillis(timestamp, pollIntervalMillis),
            reset(current.status().engineBackoffAttempts(), Operation.CANCEL),
            clearEngineCondition(
                current.status().conditions(),
                reset(current.status().engineBackoffAttempts(), Operation.CANCEL)),
            current.status().summary(),
            current.status().failure(),
            current.status().submittedAt(),
            current.status().startedAt(),
            current.status().finishedAt());
      case ALREADY_TERMINAL:
        return observed(current, cancellation.terminalObservation(), timestamp);
      case NOT_FOUND:
        return executionUnresolved(
            current,
            "EXECUTION_NOT_FOUND",
            "cancel could not authoritatively locate the acknowledged execution",
            Operation.CANCEL,
            timestamp);
      case UNSUPPORTED:
        return status(
            current.status(),
            "CANCELING",
            current.status().attempt(),
            current.status().attemptHistory(),
            current.status().lastObservedAt(),
            current.status().lastCancelAttemptAt(),
            timestamp,
            reset(current.status().engineBackoffAttempts(), Operation.CANCEL),
            clearEngineCondition(
                ProcessConditions.set(
                    current.status().conditions(),
                    ProcessConditions.CANCELLATION_UNSUPPORTED,
                    "CancellationUnsupported",
                    "engine cancellation is unsupported; observation remains active",
                    timestamp,
                    capabilityVersion),
                reset(current.status().engineBackoffAttempts(), Operation.CANCEL)),
            current.status().summary(),
            current.status().failure(),
            current.status().submittedAt(),
            current.status().startedAt(),
            current.status().finishedAt());
      case UNAVAILABLE:
      default:
        return unavailable(current, Operation.CANCEL, "engine unavailable during cancellation");
    }
  }

  private ProcessResource.ProcessStatus observed(
      ProcessResource current, EngineObservation observation, String timestamp) {
    String phase = observation.remotePhase();
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.EngineBackoff backoff =
        reset(status.engineBackoffAttempts(), Operation.OBSERVE);
    List<ProcessResource.Condition> conditions = clearEngineCondition(status.conditions(), backoff);
    ProcessResource.Summary summary = mergeSummary(status.summary(), observation);
    if ("SUBMITTED".equals(phase) || "RUNNING".equals(phase)) {
      String persistedPhase =
          "CANCEL".equals(current.spec().desiredState())
              ? "CANCELING"
              : "RUNNING".equals(status.phase()) && "SUBMITTED".equals(phase) ? "RUNNING" : phase;
      return status(
          status,
          persistedPhase,
          status.attempt(),
          status.attemptHistory(),
          timestamp,
          status.lastCancelAttemptAt(),
          plusMillis(timestamp, pollIntervalMillis),
          backoff,
          conditions,
          summary,
          null,
          status.submittedAt(),
          "RUNNING".equals(phase) && status.startedAt() == null ? timestamp : status.startedAt(),
          null);
    }
    String failure = observation.failure() == null ? null : safe(observation.failure().message());
    String disposition =
        observation.failure() != null && !observation.failure().retryable() ? "FINAL" : "ALLOW";
    return terminal(current, phase, failure, disposition, timestamp, summary);
  }

  private ProcessResource.ProcessStatus failed(
      ProcessResource current, String failure, String disposition, String timestamp) {
    return terminal(current, "FAILED", failure, disposition, timestamp, current.status().summary());
  }

  private ProcessResource.ProcessStatus terminal(
      ProcessResource current,
      String phase,
      String failure,
      String disposition,
      String timestamp,
      ProcessResource.Summary summary) {
    return terminal(
        current,
        phase,
        failure,
        disposition,
        timestamp,
        summary,
        current.status().attempt().submitState());
  }

  private ProcessResource.ProcessStatus terminal(
      ProcessResource current,
      String phase,
      String failure,
      String disposition,
      String timestamp,
      ProcessResource.Summary summary,
      String submitState) {
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt closed =
        attempt(
            status.attempt(),
            submitState,
            status.attempt().externalId(),
            status.attempt().dispatchedAt(),
            failure,
            disposition,
            timestamp);
    boolean finalResult =
        ProcessFinality.isFixedTerminal(phase)
            || "CANCEL".equals(current.spec().desiredState())
            || status.retryNumber() >= current.spec().retryPolicy().maxRetries()
            || "FINAL".equals(disposition);
    return status(
        status,
        phase,
        closed,
        status.attemptHistory(),
        timestamp,
        status.lastCancelAttemptAt(),
        finalResult
            ? null
            : plusMillis(timestamp, current.spec().retryPolicy().retryDelaySeconds() * 1000L),
        new ProcessResource.EngineBackoff(0, 0, 0, 0),
        ProcessConditions.remove(
            status.conditions(),
            ProcessConditions.SUBMISSION_UNRESOLVED,
            ProcessConditions.EXECUTION_UNRESOLVED,
            ProcessConditions.ENGINE_UNREACHABLE,
            ProcessConditions.CANCELLATION_UNSUPPORTED),
        summary,
        finalResult && "FAILED".equals(phase) ? failure : null,
        status.submittedAt(),
        status.startedAt(),
        finalResult ? timestamp : null);
  }

  private ProcessResource.ProcessStatus executionUnresolved(
      ProcessResource current,
      String reason,
      String message,
      Operation operation,
      String timestamp) {
    ProcessResource.EngineBackoff reset =
        reset(current.status().engineBackoffAttempts(), operation);
    return status(
        current.status(),
        current.status().phase(),
        current.status().attempt(),
        current.status().attemptHistory(),
        current.status().lastObservedAt(),
        current.status().lastCancelAttemptAt(),
        plusMillis(timestamp, executionUnresolvedMillis),
        reset,
        clearEngineCondition(
            ProcessConditions.set(
                ProcessConditions.remove(
                    current.status().conditions(), ProcessConditions.SUBMISSION_UNRESOLVED),
                ProcessConditions.EXECUTION_UNRESOLVED,
                reason,
                message,
                timestamp,
                null),
            reset),
        current.status().summary(),
        current.status().failure(),
        current.status().submittedAt(),
        current.status().startedAt(),
        current.status().finishedAt());
  }

  private ProcessResource.ProcessStatus unavailable(
      ProcessResource current, Operation operation, String message) {
    ProcessResource.ProcessStatus status = current.status();
    String timestamp = now.get();
    int before = operation.count(status.engineBackoffAttempts());
    ProcessResource.EngineBackoff incremented =
        increment(status.engineBackoffAttempts(), operation);
    long delay =
        ENGINE_BACKOFF_SECONDS[Math.min(before, ENGINE_BACKOFF_SECONDS.length - 1)] * 1000L
            + ThreadLocalRandom.current().nextLong(250L);
    ProcessResource.ProcessAttempt attempt = status.attempt();
    if (operation == Operation.SUBMIT) {
      attempt =
          attempt(
              attempt,
              "UNAVAILABLE",
              null,
              attempt.dispatchedAt(),
              message,
              attempt.retryDisposition(),
              null);
    }
    return status(
        status,
        status.phase(),
        attempt,
        status.attemptHistory(),
        status.lastObservedAt(),
        status.lastCancelAttemptAt(),
        plusMillis(timestamp, delay),
        incremented,
        ProcessConditions.set(
            status.conditions(),
            ProcessConditions.ENGINE_UNREACHABLE,
            operation.name(),
            message,
            timestamp,
            null),
        status.summary(),
        status.failure(),
        status.submittedAt(),
        status.startedAt(),
        status.finishedAt());
  }

  private static boolean identityMatches(
      ProcessResource resource, String submissionKey, String requestHash, String externalId) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    return attempt != null
        && submissionKey.equals(attempt.submissionKey())
        && requestHash.equals(attempt.requestHash())
        && (externalId == null || externalId.equals(attempt.externalId()));
  }

  private static boolean submitResultApplicable(
      ProcessResource resource, String submissionKey, String requestHash) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    return resultMayMutate(resource, attempt)
        && identityMatches(resource, submissionKey, requestHash, null)
        && "DISPATCHING".equals(attempt.submitState())
        && submissionAudit(attempt) == null;
  }

  private static boolean resolutionResultApplicable(
      ProcessResource resource, String submissionKey, String requestHash) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    if (!resultMayMutate(resource, attempt)
        || !identityMatches(resource, submissionKey, requestHash, null)
        || submissionAudit(attempt) != null) {
      return false;
    }
    String state = attempt.submitState();
    return "DISPATCHING".equals(state) || "UNKNOWN".equals(state) || "CONFLICT".equals(state);
  }

  private static boolean observationResultApplicable(
      ProcessResource resource, String submissionKey, String requestHash, String externalId) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    String phase = resource.status().phase();
    return resultMayMutate(resource, attempt)
        && identityMatches(resource, submissionKey, requestHash, externalId)
        && executionAudit(attempt) == null
        && "ACKNOWLEDGED".equals(attempt.submitState())
        && ("SUBMITTED".equals(phase) || "RUNNING".equals(phase) || "CANCELING".equals(phase));
  }

  private static boolean cancellationResultApplicable(
      ProcessResource resource, String submissionKey, String requestHash, String externalId) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    return resultMayMutate(resource, attempt)
        && identityMatches(resource, submissionKey, requestHash, externalId)
        && executionAudit(attempt) == null
        && "CANCEL".equals(resource.spec().desiredState())
        && "CANCELING".equals(resource.status().phase());
  }

  private static boolean resultMayMutate(
      ProcessResource resource, ProcessResource.ProcessAttempt attempt) {
    return attempt != null && attempt.finishedAt() == null && !ProcessFinality.isFinal(resource);
  }

  private static ProcessResource.ManualResolution submissionAudit(
      ProcessResource.ProcessAttempt attempt) {
    return attempt.manualResolutions() == null ? null : attempt.manualResolutions().submission();
  }

  private static ProcessResource.ManualResolution executionAudit(
      ProcessResource.ProcessAttempt attempt) {
    return attempt.manualResolutions() == null ? null : attempt.manualResolutions().execution();
  }

  private static ProcessResource.ProcessAttempt attempt(
      ProcessResource.ProcessAttempt source,
      String submitState,
      String externalId,
      String dispatchedAt,
      String lastError,
      String retryDisposition,
      String finishedAt) {
    return new ProcessResource.ProcessAttempt(
        source.dispatchGeneration(),
        source.submissionKey(),
        source.requestHash(),
        submitState,
        externalId,
        dispatchedAt,
        lastError,
        retryDisposition,
        finishedAt,
        source.submissionHistory(),
        source.manualResolutions());
  }

  private static ProcessResource.ProcessStatus status(
      ProcessResource.ProcessStatus source,
      String phase,
      ProcessResource.ProcessAttempt attempt,
      List<ProcessResource.AttemptSummary> attemptHistory,
      String lastObservedAt,
      String lastCancelAttemptAt,
      String nextReconcileAt,
      ProcessResource.EngineBackoff backoff,
      List<ProcessResource.Condition> conditions,
      ProcessResource.Summary summary,
      String failure,
      String submittedAt,
      String startedAt,
      String finishedAt) {
    return new ProcessResource.ProcessStatus(
        phase,
        source.retryNumber(),
        attempt,
        attemptHistory,
        lastObservedAt,
        lastCancelAttemptAt,
        nextReconcileAt,
        backoff,
        conditions,
        summary,
        failure,
        submittedAt,
        startedAt,
        finishedAt);
  }

  private static ProcessResource.Summary mergeSummary(
      ProcessResource.Summary current, EngineObservation observation) {
    java.util.Map<String, Object> merged = new java.util.LinkedHashMap<>();
    String trackUri = null;
    if (current != null) {
      trackUri = current.trackUri();
      merged.putAll(current.result());
    }
    if (observation.trackUri() != null) {
      trackUri = observation.trackUri();
    }
    if (observation.summaryDelta() != null) {
      merged.putAll(observation.summaryDelta());
    }
    try {
      if (SUMMARY_MAPPER.writeValueAsBytes(merged).length <= MAX_SUMMARY_BYTES) {
        return new ProcessResource.Summary(trackUri, merged);
      }
    } catch (com.fasterxml.jackson.core.JsonProcessingException invalidSummary) {
      LOG.warn("Engine summary delta was not JSON serializable; preserving the prior summary.");
      return new ProcessResource.Summary(
          trackUri, current == null ? java.util.Collections.emptyMap() : current.result());
    }
    LOG.warn("Engine summary delta exceeded 8 KiB after merge; preserving the prior result map.");
    return new ProcessResource.Summary(
        trackUri, current == null ? java.util.Collections.emptyMap() : current.result());
  }

  private static List<ProcessResource.Condition> clearEngineCondition(
      List<ProcessResource.Condition> conditions, ProcessResource.EngineBackoff backoff) {
    return backoff.submit() == 0
            && backoff.resolve() == 0
            && backoff.observe() == 0
            && backoff.cancel() == 0
        ? ProcessConditions.remove(conditions, ProcessConditions.ENGINE_UNREACHABLE)
        : conditions;
  }

  private static ProcessResource.EngineBackoff reset(
      ProcessResource.EngineBackoff current, Operation operation) {
    return operation.with(current, 0);
  }

  private static ProcessResource.EngineBackoff increment(
      ProcessResource.EngineBackoff current, Operation operation) {
    return operation.with(current, Math.min(7, operation.count(current) + 1));
  }

  private static String plusMillis(String timestamp, long millis) {
    return Instant.parse(timestamp).plusMillis(millis).toString();
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  private static long positive(long value, String label) {
    if (value <= 0) {
      throw new IllegalArgumentException(label + " must be > 0");
    }
    return value;
  }

  private enum Operation {
    SUBMIT {
      @Override
      int count(ProcessResource.EngineBackoff backoff) {
        return backoff.submit();
      }

      @Override
      ProcessResource.EngineBackoff with(ProcessResource.EngineBackoff b, int value) {
        return new ProcessResource.EngineBackoff(value, b.resolve(), b.observe(), b.cancel());
      }
    },
    RESOLVE {
      @Override
      int count(ProcessResource.EngineBackoff backoff) {
        return backoff.resolve();
      }

      @Override
      ProcessResource.EngineBackoff with(ProcessResource.EngineBackoff b, int value) {
        return new ProcessResource.EngineBackoff(b.submit(), value, b.observe(), b.cancel());
      }
    },
    OBSERVE {
      @Override
      int count(ProcessResource.EngineBackoff backoff) {
        return backoff.observe();
      }

      @Override
      ProcessResource.EngineBackoff with(ProcessResource.EngineBackoff b, int value) {
        return new ProcessResource.EngineBackoff(b.submit(), b.resolve(), value, b.cancel());
      }
    },
    CANCEL {
      @Override
      int count(ProcessResource.EngineBackoff backoff) {
        return backoff.cancel();
      }

      @Override
      ProcessResource.EngineBackoff with(ProcessResource.EngineBackoff b, int value) {
        return new ProcessResource.EngineBackoff(b.submit(), b.resolve(), b.observe(), value);
      }
    };

    abstract int count(ProcessResource.EngineBackoff backoff);

    abstract ProcessResource.EngineBackoff with(ProcessResource.EngineBackoff backoff, int value);
  }
}
