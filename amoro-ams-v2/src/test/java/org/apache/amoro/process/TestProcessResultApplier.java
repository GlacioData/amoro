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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.process.engine.EngineTypes.EngineFailure;
import org.apache.amoro.process.engine.EngineTypes.EngineObservation;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

/** Identity-aware callback persistence, finality and desired-state race behavior. */
public class TestProcessResultApplier {

  private DefaultScheduler scheduler;
  private ProcessDomainAssembly assembly;
  private ProcessResultApplier applier;

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(1, 1000L);
    scheduler.start();
    assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            scheduler,
            128,
            10_000L,
            65536);
    applier =
        new ProcessResultApplier(
            assembly.repository(), () -> "2026-08-24T01:00:00Z", 4, 1000L, 60_000L, 300_000L);
  }

  @AfterEach
  public void tearDown() {
    assembly.persistence().shutdown(Duration.ofSeconds(5));
    scheduler.shutdown(Duration.ofSeconds(5));
  }

  @Test
  public void lateAckPreservesConcurrentCancelAndRecordsExternalIdentity() {
    create("cancel-race", "DISPATCHING", null, "RUN");
    ProcessResource current = assembly.repository().get("cancel-race");
    assembly
        .repository()
        .modify(
            current.name(),
            current.resourceVersion(),
            resource -> resource.withSpec(resource.spec().withDesiredState("CANCEL")));

    assertTrue(
        applier.applySubmit(
            "cancel-race",
            "cancel-race:0:0",
            "sha256:req",
            SubmissionOutcome.acknowledged("remote-1"),
            null));

    ProcessResource result = assembly.repository().get("cancel-race");
    assertEquals("CANCEL", result.spec().desiredState());
    assertEquals("CANCELING", result.status().phase());
    assertEquals("remote-1", result.status().attempt().externalId());
  }

  @Test
  public void runningObservationCannotRegressCancelingPhase() {
    create("cancel-observation", "ACKNOWLEDGED", "remote-1", "CANCEL");
    ProcessResource current = assembly.repository().get("cancel-observation");
    assembly
        .repository()
        .modify(
            current.name(),
            current.resourceVersion(),
            resource ->
                resource.withStatus(
                    new ProcessResource.ProcessStatus(
                        "CANCELING",
                        resource.status().retryNumber(),
                        resource.status().attempt(),
                        resource.status().attemptHistory(),
                        resource.status().lastObservedAt(),
                        resource.status().lastCancelAttemptAt(),
                        resource.status().nextReconcileAt(),
                        resource.status().engineBackoffAttempts(),
                        resource.status().conditions(),
                        resource.status().summary(),
                        resource.status().failure(),
                        resource.status().submittedAt(),
                        resource.status().startedAt(),
                        resource.status().finishedAt())));

    assertTrue(
        applier.applyObservation(
            "cancel-observation",
            "cancel-observation:0:0",
            "sha256:req",
            "remote-1",
            ProcessObservation.known(
                new EngineObservation(
                    "RUNNING", null, Collections.singletonMap("simulated", true), null)),
            null));

    ProcessResource result = assembly.repository().get("cancel-observation");
    assertEquals("CANCELING", result.status().phase());
    assertEquals("2026-08-24T01:00:00Z", result.status().lastObservedAt());
    assertEquals("2026-08-24T01:00:00Z", result.status().startedAt());
  }

  @Test
  public void staleSubmittedObservationCannotRegressRunningPhase() {
    create("running-observation", "ACKNOWLEDGED", "remote-1", "RUN");
    ProcessResource current = assembly.repository().get("running-observation");
    assembly
        .repository()
        .modify(
            current.name(),
            current.resourceVersion(),
            resource -> {
              ProcessResource.ProcessStatus status = resource.status();
              return resource.withStatus(
                  new ProcessResource.ProcessStatus(
                      "RUNNING",
                      status.retryNumber(),
                      status.attempt(),
                      status.attemptHistory(),
                      status.lastObservedAt(),
                      status.lastCancelAttemptAt(),
                      status.nextReconcileAt(),
                      status.engineBackoffAttempts(),
                      status.conditions(),
                      status.summary(),
                      status.failure(),
                      status.submittedAt(),
                      "2026-08-24T00:59:30Z",
                      status.finishedAt()));
            });

    assertTrue(
        applier.applyObservation(
            "running-observation",
            "running-observation:0:0",
            "sha256:req",
            "remote-1",
            ProcessObservation.known(
                new EngineObservation(
                    "SUBMITTED", null, Collections.singletonMap("simulated", true), null)),
            null));

    ProcessResource result = assembly.repository().get("running-observation");
    assertEquals("RUNNING", result.status().phase());
    assertEquals("2026-08-24T01:00:00Z", result.status().lastObservedAt());
    assertEquals("2026-08-24T00:59:30Z", result.status().startedAt());
  }

  @Test
  public void rejectedSubmissionIsPersistedInTheClosedAttempt() {
    create("rejected", "DISPATCHING", null, "RUN");

    assertTrue(
        applier.applySubmit(
            "rejected",
            "rejected:0:0",
            "sha256:req",
            SubmissionOutcome.rejected("dummy policy rejection"),
            null));

    ProcessResource result = assembly.repository().get("rejected");
    assertEquals("FAILED", result.status().phase());
    assertEquals("REJECTED", result.status().attempt().submitState());
    assertEquals("REJECTED: dummy policy rejection", result.status().attempt().lastError());
    assertEquals("2026-08-24T01:00:00Z", result.status().attempt().finishedAt());
  }

  @Test
  public void staleAttemptCallbackDoesNotOverwriteCurrentAttempt() {
    create("stale", "ACKNOWLEDGED", "remote-current", "RUN");

    assertTrue(
        applier.applyObservation(
            "stale",
            "stale:9:9",
            "sha256:old",
            "remote-old",
            ProcessObservation.known(
                new EngineObservation("SUCCESS", null, Collections.emptyMap(), null)),
            null));

    ProcessResource result = assembly.repository().get("stale");
    assertEquals("SUBMITTED", result.status().phase());
    assertEquals("remote-current", result.status().attempt().externalId());
  }

  @Test
  public void retryableFailureClosesAttemptWithoutTopLevelFinalMarkers() {
    create("retryable", "ACKNOWLEDGED", "remote-1", "RUN");

    applier.applyObservation(
        "retryable",
        "retryable:0:0",
        "sha256:req",
        "remote-1",
        ProcessObservation.known(
            new EngineObservation(
                "FAILED",
                null,
                Collections.singletonMap("simulated", true),
                new EngineFailure("DUMMY_FAILED", "simulated failure", true))),
        null);

    ProcessResource result = assembly.repository().get("retryable");
    assertEquals("FAILED", result.status().phase());
    assertEquals("ALLOW", result.status().attempt().retryDisposition());
    assertEquals("simulated failure", result.status().attempt().lastError());
    assertEquals("2026-08-24T01:00:00Z", result.status().attempt().finishedAt());
    assertNull(result.status().failure());
    assertNull(result.status().finishedAt());
    assertFalse(ProcessFinality.isFinal(result));
  }

  @Test
  public void fixedTerminalClosesAttemptAndProcessInOneWrite() {
    create("success", "ACKNOWLEDGED", "remote-1", "RUN");

    applier.applyObservation(
        "success",
        "success:0:0",
        "sha256:req",
        "remote-1",
        ProcessObservation.known(
            new EngineObservation(
                "SUCCESS", null, Collections.singletonMap("simulated", true), null)),
        null);

    ProcessResource result = assembly.repository().get("success");
    assertEquals("SUCCESS", result.status().phase());
    assertEquals("2026-08-24T01:00:00Z", result.status().attempt().finishedAt());
    assertEquals("2026-08-24T01:00:00Z", result.status().finishedAt());
    assertTrue(ProcessFinality.isFinal(result));
  }

  @Test
  public void resolutionAckResetsOnlyResolveBackoff() {
    create("resolved", "UNKNOWN", null, "RUN");
    ProcessResource current = assembly.repository().get("resolved");
    assembly
        .repository()
        .modify(
            current.name(),
            current.resourceVersion(),
            resource ->
                resource.withStatus(
                    new ProcessResource.ProcessStatus(
                        resource.status().phase(),
                        resource.status().retryNumber(),
                        resource.status().attempt(),
                        resource.status().attemptHistory(),
                        resource.status().lastObservedAt(),
                        resource.status().lastCancelAttemptAt(),
                        resource.status().nextReconcileAt(),
                        new ProcessResource.EngineBackoff(2, 3, 4, 5),
                        ProcessConditions.set(
                            resource.status().conditions(),
                            ProcessConditions.SUBMISSION_UNRESOLVED,
                            "UNKNOWN",
                            "pending resolution",
                            "2026-08-24T00:59:00Z",
                            null),
                        resource.status().summary(),
                        resource.status().failure(),
                        resource.status().submittedAt(),
                        resource.status().startedAt(),
                        resource.status().finishedAt())));

    assertTrue(
        applier.applyResolution(
            "resolved",
            "resolved:0:0",
            "sha256:req",
            SubmissionResolution.acknowledged("remote-2"),
            null,
            "cap-v2"));

    ProcessResource result = assembly.repository().get("resolved");
    assertEquals(
        new ProcessResource.EngineBackoff(2, 0, 4, 5), result.status().engineBackoffAttempts());
    assertFalse(
        ProcessConditions.isTrue(
            result.status().conditions(), ProcessConditions.SUBMISSION_UNRESOLVED));
  }

  @Test
  public void lateObservationCannotOverrideManualExecutionResolution() {
    create("manual-wins", "ACKNOWLEDGED", "remote-1", "RUN");
    ProcessResource current = assembly.repository().get("manual-wins");
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource unresolved =
        assembly
            .repository()
            .modify(
                current.name(),
                current.resourceVersion(),
                resource ->
                    resource.withStatus(
                        new ProcessResource.ProcessStatus(
                            status.phase(),
                            status.retryNumber(),
                            status.attempt(),
                            status.attemptHistory(),
                            status.lastObservedAt(),
                            status.lastCancelAttemptAt(),
                            status.nextReconcileAt(),
                            status.engineBackoffAttempts(),
                            ProcessConditions.set(
                                status.conditions(),
                                ProcessConditions.EXECUTION_UNRESOLVED,
                                "LOST",
                                "dummy execution cannot be observed",
                                "2026-08-24T00:59:30Z",
                                null),
                            status.summary(),
                            status.failure(),
                            status.submittedAt(),
                            status.startedAt(),
                            status.finishedAt())));
    ManualResolutionTransition.Result manual =
        ManualResolutionTransition.apply(
            unresolved,
            new ManualResolutionTransition.Command(
                ManualResolutionTransition.Kind.EXECUTION,
                "manual-key",
                "manual-wins:0:0",
                "sha256:req",
                "FAILED",
                null,
                false,
                "operator verified failure",
                "test-operator"),
            "2026-08-24T00:59:40Z");
    ProcessResource terminal =
        assembly
            .repository()
            .modify(
                unresolved.name(),
                unresolved.resourceVersion(),
                resource -> resource.withStatus(manual.status()));

    assertTrue(
        applier.applyObservation(
            "manual-wins",
            "manual-wins:0:0",
            "sha256:req",
            "remote-1",
            ProcessObservation.known(
                new EngineObservation("SUCCESS", null, Collections.emptyMap(), null)),
            null));

    ProcessResource afterLateCallback = assembly.repository().get("manual-wins");
    assertEquals(terminal.resourceVersion(), afterLateCallback.resourceVersion());
    assertEquals("FAILED", afterLateCallback.status().phase());
    assertEquals("operator verified failure", afterLateCallback.status().attempt().lastError());
  }

  @Test
  public void lateSubmitAckCannotOverrideManualSubmissionAck() {
    create("manual-submission-ack", "DISPATCHING", null, "RUN");
    ProcessResource unresolved = forceSubmissionUnresolved("manual-submission-ack");
    ManualResolutionTransition.Result manual =
        ManualResolutionTransition.apply(
            unresolved,
            new ManualResolutionTransition.Command(
                ManualResolutionTransition.Kind.SUBMISSION,
                "manual-ack-key",
                "manual-submission-ack:0:0",
                "sha256:req",
                "ACKNOWLEDGED",
                "manual-external-id",
                null,
                "operator verified the simulated ledger",
                "test-operator"),
            "2026-08-24T00:59:40Z");
    ProcessResource authoritative =
        assembly
            .repository()
            .modify(
                unresolved.name(),
                unresolved.resourceVersion(),
                resource -> resource.withStatus(manual.status()));

    assertTrue(
        applier.applySubmit(
            authoritative.name(),
            "manual-submission-ack:0:0",
            "sha256:req",
            SubmissionOutcome.acknowledged("late-engine-external-id"),
            null));

    ProcessResource afterLateCallback = assembly.repository().get(authoritative.name());
    assertEquals(authoritative.resourceVersion(), afterLateCallback.resourceVersion());
    assertEquals(authoritative.status(), afterLateCallback.status());
    assertEquals("manual-external-id", afterLateCallback.status().attempt().externalId());
  }

  @Test
  public void lateSubmitAckCannotReopenManuallyRejectedSubmissionGeneration() {
    create("manual-submission-not-found", "DISPATCHING", null, "RUN");
    ProcessResource unresolved = forceSubmissionUnresolved("manual-submission-not-found");
    ManualResolutionTransition.Result manual =
        ManualResolutionTransition.apply(
            unresolved,
            new ManualResolutionTransition.Command(
                ManualResolutionTransition.Kind.SUBMISSION,
                "manual-not-found-key",
                "manual-submission-not-found:0:0",
                "sha256:req",
                "NOT_FOUND",
                null,
                null,
                "operator verified that generation zero was not accepted",
                "test-operator"),
            "2026-08-24T00:59:40Z");
    ProcessResource authoritative =
        assembly
            .repository()
            .modify(
                unresolved.name(),
                unresolved.resourceVersion(),
                resource -> resource.withStatus(manual.status()));

    assertTrue(
        applier.applySubmit(
            authoritative.name(),
            "manual-submission-not-found:0:0",
            "sha256:req",
            SubmissionOutcome.acknowledged("late-engine-external-id"),
            null));

    ProcessResource afterLateCallback = assembly.repository().get(authoritative.name());
    assertEquals(authoritative.resourceVersion(), afterLateCallback.resourceVersion());
    assertEquals(authoritative.status(), afterLateCallback.status());
    assertEquals(1, afterLateCallback.status().attempt().dispatchGeneration());
    assertEquals(
        "NOT_FOUND", afterLateCallback.status().attempt().submissionHistory().get(0).outcome());
  }

  private ProcessResource forceSubmissionUnresolved(String name) {
    ProcessResource current = assembly.repository().get(name);
    ProcessResource.ProcessStatus status = current.status();
    return assembly
        .repository()
        .modify(
            name,
            current.resourceVersion(),
            resource ->
                resource.withStatus(
                    new ProcessResource.ProcessStatus(
                        status.phase(),
                        status.retryNumber(),
                        status.attempt(),
                        status.attemptHistory(),
                        status.lastObservedAt(),
                        status.lastCancelAttemptAt(),
                        status.nextReconcileAt(),
                        status.engineBackoffAttempts(),
                        ProcessConditions.set(
                            status.conditions(),
                            ProcessConditions.SUBMISSION_UNRESOLVED,
                            "UNKNOWN",
                            "simulated submission outcome is unresolved",
                            "2026-08-24T00:59:30Z",
                            null),
                        status.summary(),
                        status.failure(),
                        status.submittedAt(),
                        status.startedAt(),
                        status.finishedAt())));
  }

  private void create(String name, String submitState, String externalId, String desiredState) {
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            name + ":0:0",
            "sha256:req",
            submitState,
            externalId,
            "2026-08-24T00:59:00Z",
            "AUTO",
            null,
            null,
            new ProcessResource.ManualResolutions(null, null));
    assembly
        .repository()
        .create(
            new ProcessResource(
                name,
                new ProcessResource.ProcessSpec(
                    new ProcessResource.TableRef("sim", "db", name, name, "simulated"),
                    "dummy-maintenance",
                    "local",
                    "MANUAL",
                    "2026-08-24T00:00:00Z",
                    desiredState,
                    new ProcessResource.RequestIdentity("sha256:key-" + name, "sha256:req"),
                    Collections.emptyMap(),
                    new ProcessResource.RetryPolicy(3, 2, 1)),
                new ProcessResource.ProcessStatus(
                    "ACKNOWLEDGED".equals(submitState) ? "SUBMITTED" : "PENDING",
                    0,
                    attempt,
                    null,
                    null,
                    null,
                    new ProcessResource.EngineBackoff(0, 0, 0, 0),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null)));
  }
}
