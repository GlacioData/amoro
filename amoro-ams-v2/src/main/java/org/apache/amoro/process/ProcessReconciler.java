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
import org.apache.amoro.persistence.facade.RepositoryFacade;
import org.apache.amoro.process.engine.EngineTypes.EngineObservation;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * The level-triggered Process controller (process spec §7): each invoke reads the latest durable
 * resource, performs at most one logical step — a version-CAS write or ONE async engine command —
 * and relies on the scheduler period for the next round. Engine callbacks apply results through
 * {@link ProcessResultApplier} with the same CAS discipline. Terminal phases throw {@link
 * TerminalState}.
 */
public final class ProcessReconciler implements Controller {

  /** One reconcile round's outcome for the scheduler. */
  public enum Step {
    DONE, // a durable write happened; reschedule at the period
    WAIT, // business gating: reschedule after the configured delay
    DISPATCHED // an async engine command is in flight; its callback will reschedule
  }

  private static final Logger LOG = LoggerFactory.getLogger(ProcessReconciler.class);

  private final String processName;
  private final RepositoryFacade<ProcessResource> repository;
  private final org.apache.amoro.process.engine.ProcessEngineRegistry engines;
  private final Scheduler scheduler;
  private final Clock clock;
  private final long retryDelayMillis;
  private final org.apache.amoro.process.engine.ExecutionHandleRegistry handleRegistry;

  /** Wall-clock abstraction (UTC instants as RFC 3339 strings). */
  public interface Clock {
    String now();

    static Clock systemUtc() {
      return () -> java.time.Instant.now().toString();
    }
  }

  public ProcessReconciler(
      String processName,
      RepositoryFacade<ProcessResource> repository,
      ProcessEngineDispatcher engine,
      Scheduler scheduler,
      Clock clock,
      long retryDelayMillis) {
    this(
        processName,
        repository,
        org.apache.amoro.process.engine.ProcessEngineRegistry.single("local", engine),
        scheduler,
        clock,
        retryDelayMillis,
        new org.apache.amoro.process.engine.ExecutionHandleRegistry());
  }

  public ProcessReconciler(
      String processName,
      RepositoryFacade<ProcessResource> repository,
      org.apache.amoro.process.engine.ProcessEngineRegistry engines,
      Scheduler scheduler,
      Clock clock,
      long retryDelayMillis,
      org.apache.amoro.process.engine.ExecutionHandleRegistry handleRegistry) {
    this.processName = processName;
    this.repository = repository;
    this.engines = engines;
    this.scheduler = scheduler;
    this.clock = clock;
    this.retryDelayMillis = retryDelayMillis;
    this.handleRegistry = handleRegistry;
  }

  @Override
  public ControllerKey key() {
    return ControllerKey.of("process", processName);
  }

  @Override
  public void invoke() {
    ProcessResource resource = repository.get(processName);
    Step step =
        "CANCEL".equals(resource.spec().desiredState()) ? cancelStep(resource) : runStep(resource);
    switch (step) {
      case DONE:
        return; // the framework period reschedules us
      case WAIT:
        scheduler.schedule(this, Duration.ofMillis(retryDelayMillis));
        return;
      case DISPATCHED:
        return; // the async callback completes the round
      default:
        throw new AssertionError(step);
    }
  }

  // ------------------------------------------------------------------ desired = RUN

  private Step runStep(ProcessResource resource) {
    if (ProcessFinality.isFinal(resource)) {
      throw TerminalState.INSTANCE;
    }
    String phase = resource.status().phase();
    if ("PENDING".equals(phase) || "UNKNOWN".equals(phase)) {
      return submitStep(resource, false);
    }
    if ("SUBMITTED".equals(phase) || "RUNNING".equals(phase) || "CANCELING".equals(phase)) {
      return observeStep(resource, true);
    }
    if ("FAILED".equals(phase)) {
      return retryStep(resource);
    }
    throw new IllegalStateException("unexpected phase " + phase);
  }

  // ------------------------------------------------------------------ desired = CANCEL

  private Step cancelStep(ProcessResource resource) {
    if (ProcessFinality.isFinal(resource)) {
      throw TerminalState.INSTANCE;
    }
    String phase = resource.status().phase();
    if ("PENDING".equals(phase) || "UNKNOWN".equals(phase)) {
      String submitState =
          resource.status().attempt() != null ? resource.status().attempt().submitState() : null;
      if ("CREATED".equals(submitState) || submitState == null) {
        // provably never dispatched (spec §7.4): cancel directly, no engine work at all
        return casWrite(resource, directCanceled(resource)) ? Step.DONE : Step.DONE;
      }
      // DISPATCHING/UNKNOWN/CONFLICT: dispatch evidence exists but no external id — the
      // submission-resolution path owns it; conservatively CANCELING and observe rounds
      // plus the manual submission resolution converge it
      return casTransition(resource, "CANCELING");
    }
    if ("SUBMITTED".equals(phase) || "RUNNING".equals(phase)) {
      return casTransition(resource, "CANCELING");
    }
    if ("CANCELING".equals(phase)) {
      return cancelingStep(resource);
    }
    if ("FAILED".equals(phase)) {
      throw TerminalState.INSTANCE; // desired=CANCEL makes FAILED final
    }
    throw new IllegalStateException("unexpected phase " + phase);
  }

  // ------------------------------------------------------------------ steps

  private Step submitStep(ProcessResource resource, boolean cancelling) {
    ProcessResource.ProcessAttempt attempt = ensureAttempt(resource);
    if (cancelling) {
      throw new IllegalStateException("cancel path never dispatches new submissions");
    }
    if (resource.status().attempt() == null) {
      // persist the attempt first (process spec §7.3: the attempt exists before dispatch)
      ProcessResource.ProcessStatus status = resource.status();
      ProcessResource.ProcessStatus withAttempt =
          new ProcessResource.ProcessStatus(
              status.phase(),
              status.retryNumber(),
              attempt,
              status.attemptHistory(),
              status.lastObservedAt(),
              null,
              status.engineBackoffAttempts(),
              status.conditions(),
              status.summary(),
              status.failure(),
              status.submittedAt(),
              status.startedAt(),
              status.finishedAt());
      casWrite(resource, withAttempt);
      return Step.DONE; // the next round stages DISPATCHING
    }
    String state = attempt.submitState();
    if ("UNKNOWN".equals(state) || "CONFLICT".equals(state)) {
      // unresolved submission: never blind-resubmit the same key (spec §7.3); the manual
      // submission-resolution endpoint or a future resolve round owns this attempt
      LOG.info(
          "Process {} attempt {} is {}; awaiting manual submission resolution.",
          processName,
          attempt.submissionKey(),
          state);
      return Step.WAIT;
    }
    if (!"DISPATCHING".equals(state)) {
      // durable DISPATCHING first (spec §7.3): a crash after this write restarts into
      // resolution instead of a duplicate submit of the same key
      ProcessResource.ProcessStatus status = resource.status();
      ProcessResource.ProcessAttempt dispatching =
          new ProcessResource.ProcessAttempt(
              attempt.dispatchGeneration(),
              attempt.submissionKey(),
              attempt.requestHash(),
              "DISPATCHING",
              attempt.externalId(),
              attempt.dispatchedAt(),
              attempt.retryDisposition(),
              attempt.finishedAt(),
              attempt.submissionHistory(),
              attempt.manualResolutions());
      casWrite(
          resource,
          new ProcessResource.ProcessStatus(
              status.phase(),
              status.retryNumber(),
              dispatching,
              status.attemptHistory(),
              status.lastObservedAt(),
              null,
              status.engineBackoffAttempts(),
              status.conditions(),
              status.summary(),
              status.failure(),
              status.submittedAt(),
              status.startedAt(),
              status.finishedAt()));
      return Step.DONE; // the next round performs the actual dispatch
    }
    ProcessEngineDispatcher engine = engineOf(resource);
    if (engine == null) {
      LOG.info(
          "Engine '{}' for {} is not deployed; waiting (resource stays durable).",
          resource.spec().executionEngine(),
          processName);
      return Step.WAIT;
    }
    String submissionKey = attempt.submissionKey();
    org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight<SubmissionOutcome>
        flight =
            engine.submit(
                processName, submissionKey, attempt.requestHash(), payloadOf(resource));
    flight.whenComplete(
        (outcome, error) -> {
          try {
            applySubmitOutcome(
                processName, submissionKey, attempt.requestHash(), outcome, error, false);
          } finally {
            flight.markDurablyHandled();
          }
        });
    return Step.DISPATCHED;
  }

  private Step observeStep(ProcessResource resource, boolean runDesired) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    if (attempt == null || attempt.externalId() == null) {
      // no external identity: the submit side owns this resource for now
      return submitStep(resource, false);
    }
    ProcessEngineDispatcher engine = engineOf(resource);
    if (engine == null) {
      LOG.info(
          "Engine '{}' for {} is not deployed; waiting (resource stays durable).",
          resource.spec().executionEngine(),
          processName);
      return Step.WAIT;
    }
    String submissionKey = attempt.submissionKey();
    org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight<ProcessObservation>
        flight = engine.observe(processName, attempt.externalId());
    flight.whenComplete(
        (observation, error) -> {
          try {
            applyObservation(processName, submissionKey, observation, error);
          } finally {
            flight.markDurablyHandled();
          }
        });
    return Step.DISPATCHED;
  }

  private Step cancelingStep(ProcessResource resource) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    if (attempt == null || attempt.externalId() == null) {
      return observeStep(resource, false);
    }
    if ("CANCEL_REQUESTED".equals(attempt.submitState())) {
      // cancellation already accepted: observe rounds record the terminal phase
      return observeStep(resource, false);
    }
    ProcessEngineDispatcher engine = engineOf(resource);
    if (engine == null) {
      LOG.info(
          "Engine '{}' for {} is not deployed; waiting (resource stays durable).",
          resource.spec().executionEngine(),
          processName);
      return Step.WAIT;
    }
    String submissionKey = attempt.submissionKey();
    org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight<
            org.apache.amoro.process.engine.EngineTypes.CancellationOutcome>
        flight = engine.cancel(processName, attempt.externalId());
    flight.whenComplete(
        (outcome, error) -> {
          try {
            applyCancelOutcome(processName, submissionKey, outcome, error);
          } finally {
            flight.markDurablyHandled();
          }
        });
    return Step.DISPATCHED;
  }

  void applyCancelOutcome(
      String name,
      String submissionKey,
      org.apache.amoro.process.engine.EngineTypes.CancellationOutcome outcome,
      Throwable error) {
    try {
      ProcessResource current = repository.get(name);
      if (error != null) {
        LOG.warn("Cancel of {} failed before classification.", name, error);
        return;
      }
      if (outcome.kind()
          == org.apache.amoro.process.engine.EngineTypes.CancellationOutcome.Kind.ACCEPTED) {
        markCancelRequested(current); // later rounds observe instead of re-cancelling
        return;
      }
      if (outcome.kind()
          == org.apache.amoro.process.engine.EngineTypes.CancellationOutcome.Kind
              .ALREADY_TERMINAL) {
        EngineObservation observation = outcome.terminalObservation();
        casWrite(
            current,
            terminal(
                observation.remotePhase(),
                current,
                observation.failure() != null ? observation.failure().message() : null));
      }
      // NOT_FOUND/UNAVAILABLE/UNSUPPORTED: observe rounds and alerts carry it (first version)
    } catch (RuntimeException e) {
      LOG.warn("Applying cancel outcome of {} failed.", name, e);
    }
  }

  private void markCancelRequested(ProcessResource current) {
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    if (attempt == null) {
      return;
    }
    ProcessResource.ProcessAttempt marked =
        new ProcessResource.ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            "CANCEL_REQUESTED",
            attempt.externalId(),
            attempt.dispatchedAt(),
            attempt.retryDisposition(),
            attempt.finishedAt(),
            attempt.submissionHistory(),
            attempt.manualResolutions());
    casWrite(
        current,
        new ProcessResource.ProcessStatus(
            status.phase(),
            status.retryNumber(),
            marked,
            status.attemptHistory(),
            status.lastObservedAt(),
            null,
            status.engineBackoffAttempts(),
            status.conditions(),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt()));
  }

  private Step retryStep(ProcessResource resource) {
    // archive the failed attempt and open a fresh one after the retry delay
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    List<ProcessResource.AttemptSummary> history =
        new ArrayList<ProcessResource.AttemptSummary>(status.attemptHistory());
    if (attempt != null) {
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
              attempt.manualResolutions() != null ? attempt.manualResolutions().execution() : null,
              attempt.finishedAt() != null ? attempt.finishedAt() : clock.now(),
              "retry"));
    }
    ProcessResource.ProcessStatus next =
        new ProcessResource.ProcessStatus(
            "PENDING",
            status.retryNumber() + 1,
            null,
            history,
            status.lastObservedAt(),
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            new ArrayList<ProcessResource.Condition>(),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            status.startedAt(),
            status.finishedAt());
    casWrite(resource, next);
    return Step.WAIT; // retryDelay gates the next attempt
  }

  // ------------------------------------------------------------------ result application

  void applySubmitOutcome(
      String name,
      String submissionKey,
      String requestHash,
      SubmissionOutcome outcome,
      Throwable error,
      boolean cancelling) {
    try {
      ProcessResource current = repository.get(name);
      ProcessResource.ProcessAttempt attempt = current.status().attempt();
      if (attempt == null || !submissionKey.equals(attempt.submissionKey())) {
        LOG.debug("Late submit result for {} ignored: attempt rotated.", name);
        return;
      }
      if (error != null) {
        LOG.warn("Submit of {} failed before classification.", name, error);
        return; // the next reconcile round retries with backoff semantics
      }
      switch (outcome.kind()) {
        case ACKNOWLEDGED:
          if (cancelling) {
            casTransition(current, "CANCELING", outcome.externalId());
          } else {
            casTransition(current, "SUBMITTED", outcome.externalId());
          }
          return;
        case REJECTED:
          casWrite(current, terminal("FAILED", current, "REJECTED: " + outcome.reason()));
          return;
        case UNKNOWN:
        case CONFLICT:
          // persist the unresolved state: later rounds never blind-resubmit this key and
          // the manual submission-resolution endpoint (or a resolve round) owns it
          casWrite(
              current,
              withSubmitState(
                  current,
                  outcome.kind() == SubmissionOutcome.Kind.UNKNOWN ? "UNKNOWN" : "CONFLICT"));
          return;
        case UNAVAILABLE:
        default:
          // provably never sent: the same key is retried on the next round
          LOG.info(
              "Submit of {} classified {}; retrying same key next round.", name, outcome.kind());
      }
    } catch (RuntimeException e) {
      LOG.warn("Applying submit result of {} failed.", name, e);
    }
  }

  void applyObservation(
      String name, String submissionKey, ProcessObservation observation, Throwable error) {
    try {
      ProcessResource current = repository.get(name);
      if (error != null) {
        LOG.warn("Observe of {} failed before classification.", name, error);
        return;
      }
      if (observation.kind() != ProcessObservation.Kind.KNOWN) {
        LOG.info("Observe of {} returned {}; awaiting next round.", name, observation.kind());
        return;
      }
      EngineObservation engineObservation = observation.observation();
      String phase = engineObservation.remotePhase();
      if ("SUBMITTED".equals(phase) || "RUNNING".equals(phase)) {
        casTransition(
            current,
            "RUNNING".equals(phase) ? "RUNNING" : current.status().phase(),
            current.status().attempt() != null ? current.status().attempt().externalId() : null);
        return;
      }
      String finishedAt = clock.now();
      ProcessResource.ProcessStatus status = current.status();
      ProcessResource.ProcessAttempt attempt = status.attempt();
      ProcessResource.ProcessAttempt closedAttempt =
          attempt == null
              ? null
              : new ProcessResource.ProcessAttempt(
                  attempt.dispatchGeneration(),
                  attempt.submissionKey(),
                  attempt.requestHash(),
                  attempt.submitState(),
                  attempt.externalId(),
                  attempt.dispatchedAt(),
                  engineObservation.failure() != null && !engineObservation.failure().retryable()
                      ? "FINAL"
                      : "ALLOW",
                  finishedAt,
                  attempt.submissionHistory(),
                  attempt.manualResolutions());
      ProcessResource.ProcessStatus next =
          new ProcessResource.ProcessStatus(
              phase,
              status.retryNumber(),
              closedAttempt,
              status.attemptHistory(),
              status.lastObservedAt(),
              null,
              status.engineBackoffAttempts(),
              status.conditions(),
              new ProcessResource.Summary(
                  engineObservation.trackUri(), engineObservation.summaryDelta()),
              "FAILED".equals(phase) && engineObservation.failure() != null
                  ? engineObservation.failure().message()
                  : null,
              status.submittedAt(),
              status.startedAt(),
              finishedAt);
      boolean written = casWrite(current, next);
      if (written) {
        // the terminal result is durable: the engine handle may be cleaned up now
        String externalId = attempt != null ? attempt.externalId() : null;
        ProcessEngineDispatcher engine = engineOf(current);
        if (externalId != null && engine != null) {
          handleRegistry.track(name, externalId);
          org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight<Void>
              releaseFlight =
                  engine.release(current.spec().executionEngine(), externalId);
          releaseFlight.whenComplete(
              (ignored, releaseError) -> {
                try {
                  if (releaseError == null) {
                    handleRegistry.release(name);
                  }
                } finally {
                  releaseFlight.markDurablyHandled();
                }
              });
        }
      }
    } catch (RuntimeException e) {
      LOG.warn("Applying observation of {} failed.", name, e);
    }
  }

  // ------------------------------------------------------------------ helpers

  /** direct CANCELED for a provably-never-dispatched attempt (spec §7.4 first row). */
  private ProcessResource.ProcessStatus directCanceled(ProcessResource resource) {
    ProcessResource.ProcessStatus status = resource.status();
    String now = clock.now();
    return new ProcessResource.ProcessStatus(
        "CANCELED",
        status.retryNumber(),
        closeForCancel(status.attempt(), now),
        status.attemptHistory(),
        status.lastObservedAt(),
        now,
        status.engineBackoffAttempts(),
        status.conditions(),
        status.summary(),
        null,
        status.submittedAt(),
        status.startedAt(),
        now);
  }

  private ProcessResource.ProcessAttempt closeForCancel(
      ProcessResource.ProcessAttempt attempt, String now) {
    if (attempt == null) {
      return null;
    }
    return new ProcessResource.ProcessAttempt(
        attempt.dispatchGeneration(),
        attempt.submissionKey(),
        attempt.requestHash(),
        attempt.submitState(),
        attempt.externalId(),
        attempt.dispatchedAt(),
        "FINAL",
        now,
        attempt.submissionHistory(),
        attempt.manualResolutions());
  }

  private ProcessResource.ProcessStatus withSubmitState(ProcessResource current, String state) {
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    if (attempt == null) {
      return status;
    }
    ProcessResource.ProcessAttempt marked =
        new ProcessResource.ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            state,
            attempt.externalId(),
            attempt.dispatchedAt(),
            attempt.retryDisposition(),
            attempt.finishedAt(),
            attempt.submissionHistory(),
            attempt.manualResolutions());
    return new ProcessResource.ProcessStatus(
        status.phase(),
        status.retryNumber(),
        marked,
        status.attemptHistory(),
        status.lastObservedAt(),
        null,
        status.engineBackoffAttempts(),
        status.conditions(),
        status.summary(),
        status.failure(),
        status.submittedAt(),
        status.startedAt(),
        status.finishedAt());
  }

  /**
   * The dispatcher serving this process's {@code spec.executionEngine}; empty when the engine is
   * not deployed in this installation (e.g. remote Spark not yet wired). The caller waits — the
   * resource stays durable and the next round retries the lookup.
   */
  private org.apache.amoro.process.engine.ProcessEngineDispatcher engineOf(
      ProcessResource resource) {
    return engines.dispatcherFor(resource.spec().executionEngine()).orElse(null);
  }

  private ProcessResource.ProcessAttempt ensureAttempt(ProcessResource resource) {
    ProcessResource.ProcessAttempt attempt = resource.status().attempt();
    if (attempt != null) {
      return attempt;
    }
    int retryNumber = resource.status().retryNumber();
    String submissionKey = processName + ":" + retryNumber + ":0";
    return new ProcessResource.ProcessAttempt(
        0,
        submissionKey,
        "sha256:" + resource.spec().request().requestHash(),
        "CREATED",
        null,
        null,
        "AUTO",
        null,
        new ArrayList<ProcessResource.SubmissionSummary>(),
        new ProcessResource.ManualResolutions(null, null));
  }

  private byte[] payloadOf(ProcessResource resource) {
    // the frozen spec IS the submission payload for the fake/remote adapter contract
    return (resource.name() + "|" + resource.spec().action() + "|" + resource.spec().parameters())
        .getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private ProcessResource.ProcessStatus terminal(
      String phase, ProcessResource current, String failure) {
    ProcessResource.ProcessStatus status = current.status();
    String finishedAt = clock.now();
    return new ProcessResource.ProcessStatus(
        phase,
        status.retryNumber(),
        status.attempt(),
        status.attemptHistory(),
        status.lastObservedAt(),
        null,
        status.engineBackoffAttempts(),
        status.conditions(),
        status.summary(),
        failure != null ? failure : status.failure(),
        status.submittedAt(),
        status.startedAt(),
        finishedAt);
  }

  private Step casTransition(ProcessResource resource, String newPhase) {
    return casTransition(resource, newPhase, null);
  }

  private Step casTransition(ProcessResource resource, String newPhase, String externalId) {
    ProcessResource.ProcessStatus status = resource.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    ProcessResource.ProcessAttempt withId =
        attempt == null
            ? null
            : externalId == null || externalId.equals(attempt.externalId())
                ? attempt
                : new ProcessResource.ProcessAttempt(
                    attempt.dispatchGeneration(),
                    attempt.submissionKey(),
                    attempt.requestHash(),
                    "ACKNOWLEDGED",
                    externalId,
                    clock.now(),
                    attempt.retryDisposition(),
                    attempt.finishedAt(),
                    attempt.submissionHistory(),
                    attempt.manualResolutions());
    ProcessResource.ProcessStatus next =
        new ProcessResource.ProcessStatus(
            newPhase,
            status.retryNumber(),
            withId != null ? withId : attempt,
            status.attemptHistory(),
            clock.now(),
            null,
            status.engineBackoffAttempts(),
            status.conditions(),
            status.summary(),
            status.failure(),
            status.submittedAt(),
            "RUNNING".equals(newPhase) && status.startedAt() == null
                ? clock.now()
                : status.startedAt(),
            status.finishedAt());
    return casWrite(resource, next) ? Step.DONE : Step.DONE;
  }

  private boolean casWrite(ProcessResource current, ProcessResource.ProcessStatus next) {
    try {
      repository.modify(current.name(), current.resourceVersion(), r -> r.withStatus(next));
      return true;
    } catch (org.apache.amoro.persistence.exception.PreconditionFailedException raced) {
      // another writer won; the next reconcile round reads the fresh state (level-triggered)
      LOG.debug("CAS for {} lost a race; next round converges.", current.name());
      return false;
    }
  }
}
