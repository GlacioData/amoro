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

import org.apache.amoro.control.Controller;
import org.apache.amoro.control.ControllerKey;
import org.apache.amoro.control.Scheduler;
import org.apache.amoro.control.TerminalState;
import org.apache.amoro.persistence.exception.PreconditionFailedException;
import org.apache.amoro.persistence.facade.RepositoryFacade;
import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight;
import org.apache.amoro.process.engine.ProcessEngineRegistry;
import org.apache.amoro.resources.ProcessResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Level-triggered Process controller. A round first honors persisted conditions/deadlines, then
 * performs at most one state-machine operation. A fresh submission is persisted as DISPATCHING
 * before the adapter call; any DISPATCHING observed on a later invocation is resolved and is never
 * blindly submitted.
 */
public final class ProcessReconciler implements Controller {

  public enum Step {
    DONE,
    WAIT,
    DISPATCHED
  }

  private static final Logger LOG = LoggerFactory.getLogger(ProcessReconciler.class);
  private static final long DEFAULT_SUBMISSION_UNRESOLVED_MILLIS = 60_000L;
  private static final long DEFAULT_CANCEL_RETRY_MILLIS = 10_000L;
  private static final long DEFAULT_COMMAND_IN_FLIGHT_MILLIS = 250L;
  private static final long DEFAULT_EXECUTION_UNRESOLVED_MILLIS = 300_000L;

  private final String processName;
  private final RepositoryFacade<ProcessResource> repository;
  private final ProcessEngineRegistry engines;
  private final Scheduler scheduler;
  private final Clock clock;
  private final long pollIntervalMillis;
  private final long submissionUnresolvedMillis;
  private final long cancelRetryMillis;
  private final long commandInFlightMillis;
  private final long executionUnresolvedMillis;
  private final ProcessResultApplier resultApplier;
  private final ProcessResultPersistenceRetryer resultRetryer;
  private final ProcessSubmissionBuilder submissionBuilder;

  /** UTC wall clock persisted as RFC 3339. */
  public interface Clock {
    String now();

    static Clock systemUtc() {
      return () -> Instant.now().toString();
    }
  }

  public ProcessReconciler(
      String processName,
      RepositoryFacade<ProcessResource> repository,
      ProcessEngineDispatcher engine,
      Scheduler scheduler,
      Clock clock,
      long pollIntervalMillis) {
    this(
        processName,
        repository,
        ProcessEngineRegistry.single("local", engine),
        scheduler,
        clock,
        pollIntervalMillis,
        new org.apache.amoro.process.engine.ExecutionHandleRegistry(),
        null);
  }

  public ProcessReconciler(
      String processName,
      RepositoryFacade<ProcessResource> repository,
      ProcessEngineRegistry engines,
      Scheduler scheduler,
      Clock clock,
      long pollIntervalMillis,
      org.apache.amoro.process.engine.ExecutionHandleRegistry ignoredLegacyHandleRegistry) {
    this(
        processName,
        repository,
        engines,
        scheduler,
        clock,
        pollIntervalMillis,
        ignoredLegacyHandleRegistry,
        null);
  }

  public ProcessReconciler(
      String processName,
      RepositoryFacade<ProcessResource> repository,
      ProcessEngineRegistry engines,
      Scheduler scheduler,
      Clock clock,
      long pollIntervalMillis,
      org.apache.amoro.process.engine.ExecutionHandleRegistry ignoredLegacyHandleRegistry,
      ProcessResultPersistenceRetryer resultRetryer) {
    this(
        processName,
        repository,
        engines,
        scheduler,
        clock,
        pollIntervalMillis,
        DEFAULT_SUBMISSION_UNRESOLVED_MILLIS,
        DEFAULT_CANCEL_RETRY_MILLIS,
        DEFAULT_COMMAND_IN_FLIGHT_MILLIS,
        DEFAULT_EXECUTION_UNRESOLVED_MILLIS,
        resultRetryer);
  }

  public ProcessReconciler(
      String processName,
      RepositoryFacade<ProcessResource> repository,
      ProcessEngineRegistry engines,
      Scheduler scheduler,
      Clock clock,
      long pollIntervalMillis,
      long submissionUnresolvedMillis,
      long cancelRetryMillis,
      long commandInFlightMillis,
      long executionUnresolvedMillis,
      ProcessResultPersistenceRetryer resultRetryer) {
    this(
        processName,
        repository,
        engines,
        scheduler,
        clock,
        pollIntervalMillis,
        submissionUnresolvedMillis,
        cancelRetryMillis,
        commandInFlightMillis,
        executionUnresolvedMillis,
        resultRetryer,
        ProcessSubmissionBuilder.deterministic());
  }

  public ProcessReconciler(
      String processName,
      RepositoryFacade<ProcessResource> repository,
      ProcessEngineRegistry engines,
      Scheduler scheduler,
      Clock clock,
      long pollIntervalMillis,
      long submissionUnresolvedMillis,
      long cancelRetryMillis,
      long commandInFlightMillis,
      long executionUnresolvedMillis,
      ProcessResultPersistenceRetryer resultRetryer,
      ProcessSubmissionBuilder submissionBuilder) {
    this.processName = Objects.requireNonNull(processName, "processName");
    this.repository = Objects.requireNonNull(repository, "repository");
    this.engines = Objects.requireNonNull(engines, "engines");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.clock = Objects.requireNonNull(clock, "clock");
    if (pollIntervalMillis <= 0
        || submissionUnresolvedMillis <= 0
        || cancelRetryMillis <= 0
        || commandInFlightMillis <= 0
        || executionUnresolvedMillis <= 0) {
      throw new IllegalArgumentException("all Process reconcile intervals must be > 0");
    }
    this.pollIntervalMillis = pollIntervalMillis;
    this.submissionUnresolvedMillis = submissionUnresolvedMillis;
    this.cancelRetryMillis = cancelRetryMillis;
    this.commandInFlightMillis = commandInFlightMillis;
    this.executionUnresolvedMillis = executionUnresolvedMillis;
    this.resultRetryer = resultRetryer;
    this.submissionBuilder = Objects.requireNonNull(submissionBuilder, "submissionBuilder");
    this.resultApplier =
        new ProcessResultApplier(
            repository,
            clock::now,
            4,
            pollIntervalMillis,
            submissionUnresolvedMillis,
            executionUnresolvedMillis);
  }

  @Override
  public ControllerKey key() {
    return ControllerKey.of("process", processName);
  }

  @Override
  public void invoke() {
    ProcessResource resource = repository.get(processName);
    if (repairFinalityIfNeeded(resource)) {
      return;
    }
    if (ProcessFinality.isFinal(resource)) {
      throw TerminalState.INSTANCE;
    }
    Duration remaining = remaining(resource.status().nextReconcileAt());
    if (!remaining.isZero()) {
      scheduler.schedule(this, remaining);
      return;
    }
    if (ProcessConditions.isTrue(
        resource.status().conditions(), ProcessConditions.EXECUTION_UNRESOLVED)) {
      refreshExecutionUnresolvedReminder(resource);
      scheduler.schedule(this, Duration.ofMillis(executionUnresolvedMillis));
      return;
    }
    try {
      Step step =
          "CANCEL".equals(resource.spec().desiredState())
              ? cancelStep(resource)
              : runStep(resource);
      if (step == Step.WAIT) {
        scheduler.schedule(this, Duration.ofMillis(pollIntervalMillis));
      }
    } catch (ProcessEngineDispatcher.CommandInFlightException inFlight) {
      scheduler.schedule(this, Duration.ofMillis(commandInFlightMillis));
    }
  }

  private Step runStep(ProcessResource resource) {
    String phase = resource.status().phase();
    if ("PENDING".equals(phase) || "UNKNOWN".equals(phase)) {
      return submissionStep(resource, false);
    }
    if ("SUBMITTED".equals(phase) || "RUNNING".equals(phase)) {
      return observe(resource);
    }
    if ("CANCELING".equals(phase)) {
      return canceling(resource);
    }
    if ("FAILED".equals(phase)) {
      return retry(resource);
    }
    throw new IllegalStateException("unexpected RUN phase " + phase);
  }

  private Step cancelStep(ProcessResource resource) {
    String phase = resource.status().phase();
    if ("FAILED".equals(phase)) {
      throw TerminalState.INSTANCE;
    }
    if ("PENDING".equals(phase) || "UNKNOWN".equals(phase)) {
      ProcessResource.ProcessAttempt attempt = resource.status().attempt();
      if (attempt == null
          || "CREATED".equals(attempt.submitState())
          || "UNAVAILABLE".equals(attempt.submitState())) {
        directCancel(resource);
        return Step.DONE;
      }
      if (attempt.externalId() != null) {
        transitionPhase(resource, "CANCELING", clock.now());
        return Step.DONE;
      }
      return resolve(resource, true);
    }
    if ("SUBMITTED".equals(phase) || "RUNNING".equals(phase)) {
      transitionPhase(resource, "CANCELING", clock.now());
      return Step.DONE;
    }
    if ("CANCELING".equals(phase)) {
      return canceling(resource);
    }
    throw new IllegalStateException("unexpected CANCEL phase " + phase);
  }

  private Step submissionStep(ProcessResource resource, boolean cancelDesired) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    if (attempt == null) {
      persistInitialAttempt(resource);
      return Step.DONE;
    }
    if (attempt.externalId() != null || "ACKNOWLEDGED".equals(attempt.submitState())) {
      transitionPhase(resource, cancelDesired ? "CANCELING" : "SUBMITTED", clock.now());
      return Step.DONE;
    }
    switch (attempt.submitState()) {
      case "CREATED":
      case "UNAVAILABLE":
        if (cancelDesired) {
          directCancel(resource);
          return Step.DONE;
        }
        return stageAndSubmit(resource);
      case "DISPATCHING":
      case "UNKNOWN":
      case "CONFLICT":
        return resolve(resource, cancelDesired);
      case "REJECTED":
        return Step.WAIT;
      default:
        throw new IllegalStateException("unexpected submitState " + attempt.submitState());
    }
  }

  private Step stageAndSubmit(ProcessResource resource) {
    ProcessEngineDispatcher engine = engineOf(resource);
    if (engine == null) {
      return Step.WAIT;
    }
    ProcessResultPersistenceRetryer.Lease resultLease = reserveResultSlot();
    if (resultRetryer != null && resultLease == null) {
      return Step.WAIT;
    }
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt current = status.attempt();
    String timestamp = clock.now();
    ProcessResource.ProcessAttempt dispatching =
        copyAttempt(
            current, "DISPATCHING", null, timestamp, null, current.retryDisposition(), null);
    ProcessResource.ProcessStatus staged =
        copyStatus(
            status,
            status.phase(),
            status.retryNumber(),
            dispatching,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            plusMillis(timestamp, submissionUnresolvedMillis),
            status.engineBackoffAttempts(),
            status.conditions(),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            null);
    ProcessResource durable;
    try {
      durable =
          repository.modify(
              resource.name(),
              resource.resourceVersion(),
              currentResource -> currentResource.withStatus(staged));
    } catch (PreconditionFailedException raced) {
      closeLease(resultLease);
      return Step.DONE;
    } catch (RuntimeException failure) {
      closeLease(resultLease);
      throw failure;
    }
    return submit(durable, engine, resultLease);
  }

  private Step submit(
      ProcessResource durableDispatching,
      ProcessEngineDispatcher engine,
      ProcessResultPersistenceRetryer.Lease resultLease) {
    ProcessResource.ProcessAttempt attempt = durableDispatching.status().attempt();
    final CommandFlight<SubmissionOutcome> flight;
    try {
      flight =
          engine.submit(
              processName,
              durableDispatching.spec().action(),
              attempt.submissionKey(),
              attempt.requestHash(),
              payloadOf(durableDispatching));
    } catch (RuntimeException dispatchFailure) {
      closeLease(resultLease);
      throw dispatchFailure;
    }
    flight.whenComplete(
        (outcome, error) -> {
          persistResult(
              "submit|" + attempt.submissionKey() + "|" + attempt.requestHash(),
              flight,
              () ->
                  resultApplier.applySubmit(
                      processName, attempt.submissionKey(), attempt.requestHash(), outcome, error),
              resultLease);
        });
    return Step.DISPATCHED;
  }

  private Step resolve(ProcessResource resource, boolean cancelDesired) {
    ProcessEngineDispatcher engine = engineOf(resource);
    if (engine == null) {
      return Step.WAIT;
    }
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    String capabilityVersion = engine.capabilities().capabilityVersion();
    java.util.Optional<ProcessResource.Condition> unresolved =
        ProcessConditions.find(
            resource.status().conditions(), ProcessConditions.SUBMISSION_UNRESOLVED);
    boolean sameUnsupportedCapability =
        unresolved.isPresent()
            && "ResolutionUnsupported".equals(unresolved.get().reason())
            && capabilityVersion.equals(unresolved.get().observedCapabilityVersion());
    if (!engine.capabilities().supportsSubmissionResolution() || sameUnsupportedCapability) {
      persistUnsupportedResolution(resource, capabilityVersion);
      return Step.WAIT;
    }
    ProcessResultPersistenceRetryer.Lease resultLease = reserveResultSlot();
    if (resultRetryer != null && resultLease == null) {
      return Step.WAIT;
    }
    final CommandFlight<SubmissionResolution> flight;
    try {
      flight =
          engine.resolveSubmission(processName, attempt.submissionKey(), attempt.requestHash());
    } catch (RuntimeException dispatchFailure) {
      closeLease(resultLease);
      throw dispatchFailure;
    }
    flight.whenComplete(
        (resolution, error) -> {
          persistResult(
              "resolve|" + attempt.submissionKey() + "|" + attempt.requestHash(),
              flight,
              () ->
                  resultApplier.applyResolution(
                      processName,
                      attempt.submissionKey(),
                      attempt.requestHash(),
                      resolution,
                      error,
                      capabilityVersion),
              resultLease);
        });
    return Step.DISPATCHED;
  }

  private Step observe(ProcessResource resource) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    if (attempt == null || attempt.externalId() == null) {
      return submissionStep(resource, "CANCEL".equals(resource.spec().desiredState()));
    }
    ProcessEngineDispatcher engine = engineOf(resource);
    if (engine == null) {
      return Step.WAIT;
    }
    ProcessResultPersistenceRetryer.Lease resultLease = reserveResultSlot();
    if (resultRetryer != null && resultLease == null) {
      return Step.WAIT;
    }
    final CommandFlight<ProcessObservation> flight;
    try {
      flight = engine.observe(processName, attempt.externalId());
    } catch (RuntimeException dispatchFailure) {
      closeLease(resultLease);
      throw dispatchFailure;
    }
    flight.whenComplete(
        (observation, error) -> {
          persistResult(
              "observe|" + attempt.externalId(),
              flight,
              () ->
                  resultApplier.applyObservation(
                      processName,
                      attempt.submissionKey(),
                      attempt.requestHash(),
                      attempt.externalId(),
                      observation,
                      error),
              resultLease);
        });
    return Step.DISPATCHED;
  }

  private Step canceling(ProcessResource resource) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    if (attempt == null || attempt.externalId() == null) {
      return resolve(resource, true);
    }
    ProcessEngineDispatcher engine = engineOf(resource);
    if (engine == null) {
      return Step.WAIT;
    }
    String capabilityVersion = engine.capabilities().capabilityVersion();
    java.util.Optional<ProcessResource.Condition> unsupported =
        ProcessConditions.find(
            resource.status().conditions(), ProcessConditions.CANCELLATION_UNSUPPORTED);
    if (!engine.capabilities().supportsCancellation()
        || (unsupported.isPresent()
            && capabilityVersion.equals(unsupported.get().observedCapabilityVersion()))) {
      if (unsupported.isEmpty()
          || !capabilityVersion.equals(unsupported.get().observedCapabilityVersion())) {
        persistCancellationUnsupported(resource, capabilityVersion);
        return Step.DONE;
      }
      return observe(resource);
    }
    if (unsupported.isPresent()) {
      removeCondition(resource, ProcessConditions.CANCELLATION_UNSUPPORTED);
      return Step.DONE;
    }
    if (cancelDue(resource.status().lastCancelAttemptAt())) {
      ProcessResultPersistenceRetryer.Lease resultLease = reserveResultSlot();
      if (resultRetryer != null && resultLease == null) {
        return Step.WAIT;
      }
      ProcessResource staged = stageCancelAttempt(resource);
      if (staged == null) {
        closeLease(resultLease);
        return Step.DONE;
      }
      return dispatchCancel(staged, engine, capabilityVersion, resultLease);
    }
    return observe(resource);
  }

  private Step dispatchCancel(
      ProcessResource resource,
      ProcessEngineDispatcher engine,
      String capabilityVersion,
      ProcessResultPersistenceRetryer.Lease resultLease) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    final CommandFlight<CancellationOutcome> flight;
    try {
      flight = engine.cancel(processName, attempt.externalId());
    } catch (RuntimeException dispatchFailure) {
      closeLease(resultLease);
      throw dispatchFailure;
    }
    flight.whenComplete(
        (outcome, error) -> {
          persistResult(
              "cancel|" + attempt.externalId(),
              flight,
              () ->
                  resultApplier.applyCancellation(
                      processName,
                      attempt.submissionKey(),
                      attempt.requestHash(),
                      attempt.externalId(),
                      outcome,
                      error,
                      capabilityVersion),
              resultLease);
        });
    return Step.DISPATCHED;
  }

  private Step retry(ProcessResource resource) {
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    List<ProcessResource.AttemptSummary> history = new ArrayList<>(status.attemptHistory());
    history.add(
        new ProcessResource.AttemptSummary(
            status.retryNumber(),
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            "FAILED",
            attempt.externalId(),
            attempt.retryDisposition(),
            attempt.submissionHistory(),
            attempt.manualResolutions(),
            attempt.finishedAt(),
            attempt.lastError()));
    int retryNumber = status.retryNumber() + 1;
    ProcessResource.ProcessAttempt next =
        new ProcessResource.ProcessAttempt(
            0,
            processName + ":" + retryNumber + ":0",
            ProcessRequestHashes.actionAttempt(processName, retryNumber, resource.spec()),
            "CREATED",
            null,
            null,
            null,
            "AUTO",
            null,
            new ArrayList<>(),
            new ProcessResource.ManualResolutions(null, null));
    String timestamp = clock.now();
    ProcessResource.ProcessStatus opened =
        copyStatus(
            status,
            "PENDING",
            retryNumber,
            next,
            history,
            status.lastObservedAt(),
            null,
            timestamp,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            ProcessConditions.remove(
                status.conditions(),
                ProcessConditions.SUBMISSION_UNRESOLVED,
                ProcessConditions.EXECUTION_UNRESOLVED,
                ProcessConditions.ENGINE_UNREACHABLE,
                ProcessConditions.CANCELLATION_UNSUPPORTED),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            null);
    casWrite(resource, opened);
    return Step.WAIT;
  }

  private void persistInitialAttempt(ProcessResource resource) {
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            processName + ":" + status.retryNumber() + ":0",
            ProcessRequestHashes.actionAttempt(processName, status.retryNumber(), resource.spec()),
            "CREATED",
            null,
            null,
            null,
            "AUTO",
            null,
            new ArrayList<>(),
            new ProcessResource.ManualResolutions(null, null));
    casWrite(
        resource,
        copyStatus(
            status,
            status.phase(),
            status.retryNumber(),
            attempt,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            clock.now(),
            status.engineBackoffAttempts(),
            status.conditions(),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            null));
  }

  private void directCancel(ProcessResource resource) {
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt source = status.attempt();
    if (source == null) {
      source =
          new ProcessResource.ProcessAttempt(
              0,
              processName + ":" + status.retryNumber() + ":0",
              ProcessRequestHashes.actionAttempt(
                  processName, status.retryNumber(), resource.spec()),
              "CREATED",
              null,
              null,
              null,
              "AUTO",
              null,
              new ArrayList<>(),
              new ProcessResource.ManualResolutions(null, null));
    }
    String timestamp = clock.now();
    ProcessResource.ProcessAttempt closed =
        copyAttempt(
            source, source.submitState(), null, source.dispatchedAt(), null, "FINAL", timestamp);
    casWrite(
        resource,
        copyStatus(
            status,
            "CANCELED",
            status.retryNumber(),
            closed,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            ProcessConditions.remove(
                status.conditions(),
                ProcessConditions.SUBMISSION_UNRESOLVED,
                ProcessConditions.EXECUTION_UNRESOLVED,
                ProcessConditions.ENGINE_UNREACHABLE,
                ProcessConditions.CANCELLATION_UNSUPPORTED),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            timestamp));
  }

  private ProcessResource stageCancelAttempt(ProcessResource resource) {
    ProcessResource.ProcessStatus status = resource.status();
    String timestamp = clock.now();
    ProcessResource.ProcessStatus staged =
        copyStatus(
            status,
            "CANCELING",
            status.retryNumber(),
            status.attempt(),
            status.attemptHistory(),
            status.lastObservedAt(),
            timestamp,
            plusMillis(timestamp, pollIntervalMillis),
            status.engineBackoffAttempts(),
            status.conditions(),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt());
    try {
      return repository.modify(
          resource.name(), resource.resourceVersion(), current -> current.withStatus(staged));
    } catch (PreconditionFailedException raced) {
      return null;
    }
  }

  private void transitionPhase(ProcessResource resource, String phase, String nextReconcileAt) {
    ProcessResource.ProcessStatus status = resource.status();
    casWrite(
        resource,
        copyStatus(
            status,
            phase,
            status.retryNumber(),
            status.attempt(),
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            nextReconcileAt,
            status.engineBackoffAttempts(),
            status.conditions(),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt()));
  }

  private void persistUnsupportedResolution(ProcessResource resource, String capabilityVersion) {
    ProcessResource.ProcessStatus status = resource.status();
    String timestamp = clock.now();
    ProcessResource.EngineBackoff backoff =
        new ProcessResource.EngineBackoff(
            status.engineBackoffAttempts().submit(),
            0,
            status.engineBackoffAttempts().observe(),
            status.engineBackoffAttempts().cancel());
    List<ProcessResource.Condition> conditions =
        clearEngineCondition(
            ProcessConditions.set(
                status.conditions(),
                ProcessConditions.SUBMISSION_UNRESOLVED,
                "ResolutionUnsupported",
                "submission resolution is not supported",
                timestamp,
                capabilityVersion),
            backoff);
    casWrite(
        resource,
        copyStatus(
            status,
            status.phase(),
            status.retryNumber(),
            status.attempt(),
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            plusMillis(timestamp, submissionUnresolvedMillis),
            backoff,
            conditions,
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt()));
  }

  private void persistCancellationUnsupported(ProcessResource resource, String capabilityVersion) {
    ProcessResource.ProcessStatus status = resource.status();
    String timestamp = clock.now();
    ProcessResource.EngineBackoff backoff =
        new ProcessResource.EngineBackoff(
            status.engineBackoffAttempts().submit(),
            status.engineBackoffAttempts().resolve(),
            status.engineBackoffAttempts().observe(),
            0);
    List<ProcessResource.Condition> conditions =
        clearEngineCondition(
            ProcessConditions.set(
                status.conditions(),
                ProcessConditions.CANCELLATION_UNSUPPORTED,
                "CancellationUnsupported",
                "engine cancellation is not supported; observe only",
                timestamp,
                capabilityVersion),
            backoff);
    casWrite(
        resource,
        copyStatus(
            status,
            "CANCELING",
            status.retryNumber(),
            status.attempt(),
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            timestamp,
            backoff,
            conditions,
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt()));
  }

  private void removeCondition(ProcessResource resource, String type) {
    ProcessResource.ProcessStatus status = resource.status();
    casWrite(
        resource,
        copyStatus(
            status,
            status.phase(),
            status.retryNumber(),
            status.attempt(),
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            clock.now(),
            status.engineBackoffAttempts(),
            ProcessConditions.remove(status.conditions(), type),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt()));
  }

  private void refreshExecutionUnresolvedReminder(ProcessResource resource) {
    ProcessResource.ProcessStatus status = resource.status();
    casWrite(
        resource,
        copyStatus(
            status,
            status.phase(),
            status.retryNumber(),
            status.attempt(),
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            plusMillis(clock.now(), executionUnresolvedMillis),
            status.engineBackoffAttempts(),
            status.conditions(),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt()));
  }

  private boolean repairFinalityIfNeeded(ProcessResource resource) {
    if (!ProcessFinality.isFinal(resource)) {
      return false;
    }
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    boolean needsRepair =
        attempt == null
            || attempt.finishedAt() == null
            || status.finishedAt() == null
            || ("FAILED".equals(status.phase())
                && (status.failure() == null || attempt.lastError() == null));
    if (!needsRepair) {
      return false;
    }
    String timestamp =
        status.finishedAt() != null
            ? status.finishedAt()
            : attempt != null && attempt.finishedAt() != null ? attempt.finishedAt() : clock.now();
    String attemptFinishedAt =
        attempt != null && attempt.finishedAt() != null
            ? attempt.finishedAt()
            : status.finishedAt() != null ? status.finishedAt() : timestamp;
    String statusFinishedAt = status.finishedAt() != null ? status.finishedAt() : attemptFinishedAt;
    if (attempt == null) {
      attempt =
          new ProcessResource.ProcessAttempt(
              0,
              processName + ":" + status.retryNumber() + ":0",
              ProcessRequestHashes.actionAttempt(
                  processName, status.retryNumber(), resource.spec()),
              "CREATED",
              null,
              null,
              "FAILED".equals(status.phase()) ? "FAILED" : null,
              "FINAL",
              attemptFinishedAt,
              new ArrayList<>(),
              new ProcessResource.ManualResolutions(null, null));
    } else {
      attempt =
          copyAttempt(
              attempt,
              attempt.submitState(),
              attempt.externalId(),
              attempt.dispatchedAt(),
              "FAILED".equals(status.phase()) && attempt.lastError() == null
                  ? status.failure() != null ? status.failure() : "FAILED"
                  : attempt.lastError(),
              attempt.retryDisposition(),
              attemptFinishedAt);
    }
    List<ProcessResource.Condition> repaired =
        ProcessConditions.set(
            status.conditions(),
            ProcessConditions.DATA_REPAIRED,
            "FinalityMarkersRepaired",
            "missing finality markers were reconstructed",
            timestamp,
            null);
    casWrite(
        resource,
        copyStatus(
            status,
            status.phase(),
            status.retryNumber(),
            attempt,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            null,
            status.engineBackoffAttempts(),
            repaired,
            status.summary(),
            "FAILED".equals(status.phase())
                ? (status.failure() != null
                    ? status.failure()
                    : attempt.lastError() != null ? attempt.lastError() : "FAILED")
                : null,
            status.submittedAt(),
            status.startedAt(),
            statusFinishedAt));
    return true;
  }

  private ProcessEngineDispatcher engineOf(ProcessResource resource) {
    return engines.dispatcherFor(resource.spec().executionEngine()).orElse(null);
  }

  private Duration remaining(String deadline) {
    if (deadline == null) {
      return Duration.ZERO;
    }
    try {
      Duration duration = Duration.between(Instant.parse(clock.now()), Instant.parse(deadline));
      return duration.isNegative() || duration.isZero() ? Duration.ZERO : duration;
    } catch (RuntimeException malformedImportedDeadline) {
      LOG.warn(
          "Process {} has malformed nextReconcileAt {}; treating it as due.",
          processName,
          deadline);
      return Duration.ZERO;
    }
  }

  private boolean cancelDue(String lastCancelAttemptAt) {
    if (lastCancelAttemptAt == null) {
      return true;
    }
    return !Instant.parse(clock.now())
        .isBefore(Instant.parse(lastCancelAttemptAt).plusMillis(cancelRetryMillis));
  }

  private void wake() {
    try {
      scheduler.schedule(this);
    } catch (java.util.concurrent.RejectedExecutionException shuttingDown) {
      LOG.debug("Scheduler stopped before Process {} callback wake-up.", processName);
    }
  }

  private void persistResult(
      String operationIdentity,
      CommandFlight<?> flight,
      java.util.function.BooleanSupplier durableApply,
      ProcessResultPersistenceRetryer.Lease resultLease) {
    String identity = processName + "|" + operationIdentity;
    if (resultRetryer != null) {
      resultRetryer.handle(identity, flight, durableApply, this::wake, resultLease);
      return;
    }
    // Lightweight/test assembly has no background lifecycle. It still releases only a result that
    // was durably handled; a failed write deliberately holds the flight until restart.
    try {
      if (durableApply.getAsBoolean()) {
        flight.markDurablyHandled();
        wake();
      }
    } catch (RuntimeException unavailable) {
      LOG.warn(
          "Engine result {} could not be applied; retaining its command flight.",
          identity,
          unavailable);
    }
  }

  private ProcessResultPersistenceRetryer.Lease reserveResultSlot() {
    return resultRetryer == null ? null : resultRetryer.tryReserve();
  }

  private static void closeLease(ProcessResultPersistenceRetryer.Lease lease) {
    if (lease != null) {
      lease.close();
    }
  }

  private boolean casWrite(ProcessResource current, ProcessResource.ProcessStatus next) {
    try {
      repository.modify(
          current.name(), current.resourceVersion(), resource -> resource.withStatus(next));
      return true;
    } catch (PreconditionFailedException raced) {
      return false;
    }
  }

  private byte[] payloadOf(ProcessResource resource) {
    return submissionBuilder.build(resource);
  }

  private static String plusMillis(String timestamp, long millis) {
    return Instant.parse(timestamp).plusMillis(millis).toString();
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

  private static ProcessResource.ProcessAttempt copyAttempt(
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

  private static ProcessResource.ProcessStatus copyStatus(
      ProcessResource.ProcessStatus source,
      String phase,
      int retryNumber,
      ProcessResource.ProcessAttempt attempt,
      List<ProcessResource.AttemptSummary> history,
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
        retryNumber,
        attempt,
        history,
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
}
