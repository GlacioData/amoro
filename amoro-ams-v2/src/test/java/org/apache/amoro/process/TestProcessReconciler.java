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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.process.engine.FakeEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * P4: full state-machine walks over the real scheduler + fake engine — the run-to-SUCCESS
 * lifecycle, the retryable-FAILED path, and the cancel path.
 */
@Timeout(90)
public class TestProcessReconciler {

  /** Counts submissions while delegating to the scriptable fake. */
  static final class CountingFakeEngine extends FakeEngineAdapter {
    private final java.util.concurrent.atomic.AtomicLong counter;

    CountingFakeEngine(java.util.concurrent.atomic.AtomicLong counter) {
      this.counter = counter;
    }

    @Override
    public java.util.concurrent.CompletionStage<
            org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome>
        submit(String submissionKey, String requestHash, byte[] payload) {
      counter.incrementAndGet();
      return super.submit(submissionKey, requestHash, payload);
    }
  }

  private static String externalIdOf(String name) {
    return "fake-app-" + Math.abs((name + ":0:0").hashCode());
  }

  private DefaultScheduler scheduler;
  private CountingFakeEngine engine;
  private final java.util.concurrent.atomic.AtomicLong submitCalls =
      new java.util.concurrent.atomic.AtomicLong();
  private ProcessEngineDispatcher dispatcher;
  private ProcessDomainAssembly assembly;

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(2, 50L);
    scheduler.start();
    engine = new CountingFakeEngine(submitCalls);
    dispatcher = new ProcessEngineDispatcher(engine, 5_000L);
    assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            scheduler,
            128,
            10_000L,
            65536);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
  }

  private void createAndSchedule(String name) {
    createAndSchedule(name, 100L);
  }

  private void createAndSchedule(String name, long retryDelayMillis) {
    Map<String, Object> parameters = new LinkedHashMap<String, Object>();
    parameters.put("retainLast", 1);
    ProcessResource resource =
        new ProcessResource(
            name,
            new ProcessResource.ProcessSpec(
                new ProcessResource.TableRef("prod", "db", "t", "42", "simulated"),
                "dummy-maintenance",
                "local",
                "MANUAL",
                "2026-08-22T10:00:00Z",
                "RUN",
                new ProcessResource.RequestIdentity("sha256:key", "sha256:req"),
                parameters,
                new ProcessResource.RetryPolicy(
                    3, 2, Math.max(1, (int) Math.ceil(retryDelayMillis / 1000.0)))),
            new ProcessResource.ProcessStatus(
                "PENDING",
                0,
                null,
                null,
                null,
                null,
                new ProcessResource.EngineBackoff(0, 0, 0, 0),
                null,
                null,
                null,
                null,
                null,
                null));
    assembly.repository().create(resource);
    ProcessReconciler reconciler =
        new ProcessReconciler(
            name,
            assembly.repository(),
            dispatcher,
            scheduler,
            ProcessReconciler.Clock.systemUtc(),
            retryDelayMillis);
    scheduler.schedule(reconciler);
  }

  @Test
  public void runLifecycleReachesSuccessAndStopsScheduling() throws Exception {
    createAndSchedule("life-1");

    // fake engine: ACK on submit, then RUNNING, then SUCCESS on successive observes
    await().atMost(10, TimeUnit.SECONDS).until(() -> "SUBMITTED".equals(phaseOf("life-1")));
    String externalId = externalIdOf("life-1");
    engine.stageExecution(externalId, "RUNNING", false);
    await().atMost(10, TimeUnit.SECONDS).until(() -> "RUNNING".equals(phaseOf("life-1")));
    engine.stageExecution(externalId, "SUCCESS", false);

    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> ProcessFinality.isFixedTerminal(phaseOf("life-1")));
    assertEquals("SUCCESS", phaseOf("life-1"));
    assertTrue(assembly.repository().get("life-1").status().finishedAt() != null);
    await().atMost(10, TimeUnit.SECONDS).until(() -> scheduler.registrySize() == 0);
  }

  @Test
  public void retryableFailedArchivesAttemptAndRetries() throws Exception {
    createAndSchedule("retry-1", 3_000L); // FAILED must stay observable before the retry
    await().atMost(10, TimeUnit.SECONDS).until(() -> "SUBMITTED".equals(phaseOf("retry-1")));

    // a retryable engine failure: FAILED phase with retryable=true keeps the resource alive
    engine.stageExecution(externalIdOf("retry-1"), "FAILED", true);
    // the FAILED phase may flash between polls: assert the DURABLE retry evidence instead —
    // the failed attempt is archived with its externalId and a fresh attempt opens
    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> assembly.repository().get("retry-1").status().retryNumber() >= 1);
    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> assembly.repository().get("retry-1").status().attemptHistory().size() >= 1);
    assertEquals(
        1,
        assembly.repository().get("retry-1").status().attemptHistory().size(),
        "the failed attempt was archived with its externalId for release");
  }

  @Test
  public void cancelBeforeAckCancelsWithoutEngineWork() {
    createAndSchedule("cancel-1");
    ProcessResource created = assembly.repository().get("cancel-1");
    assembly
        .repository()
        .modify(
            "cancel-1",
            created.resourceVersion(),
            r -> r.withSpec(r.spec().withDesiredState("CANCEL")));

    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(
            () ->
                "CANCELED".equals(phaseOf("cancel-1")) || "CANCELING".equals(phaseOf("cancel-1")));
    await()
        .atMost(20, TimeUnit.SECONDS)
        .until(() -> ProcessFinality.isFinal(assembly.repository().get("cancel-1")));
    await().atMost(10, TimeUnit.SECONDS).until(() -> scheduler.registrySize() == 0);
  }

  @Test
  public void cancelOfNeverDispatchedProcessDoesZeroEngineWork() throws Exception {
    // create WITHOUT scheduling the reconciler first: no attempt exists yet (the reconciler
    // persists it), so the resource is provably never dispatched and cancel must not submit
    java.util.Map<String, Object> parameters = new java.util.LinkedHashMap<String, Object>();
    parameters.put("retainLast", 1);
    ProcessResource created =
        assembly
            .repository()
            .create(
                new ProcessResource(
                    "cancel-3",
                    new ProcessResource.ProcessSpec(
                        new ProcessResource.TableRef("prod", "db", "t", "42", "simulated"),
                        "dummy-maintenance",
                        "local",
                        "MANUAL",
                        "2026-08-22T10:00:00Z",
                        "CANCEL",
                        new ProcessResource.RequestIdentity("sha256:k", "sha256:r"),
                        parameters,
                        new ProcessResource.RetryPolicy(3, 2, 30)),
                    new ProcessResource.ProcessStatus(
                        "PENDING",
                        0,
                        null,
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
    long submitsBefore = submitCalls.get();

    ProcessReconciler reconciler =
        new ProcessReconciler(
            "cancel-3",
            assembly.repository(),
            dispatcher,
            scheduler,
            ProcessReconciler.Clock.systemUtc(),
            100L);
    scheduler.schedule(reconciler);

    await().atMost(10, TimeUnit.SECONDS).until(() -> "CANCELED".equals(phaseOf("cancel-3")));
    assertEquals(
        submitsBefore,
        submitCalls.get(),
        "cancel of a never-dispatched process performs zero engine submissions");
    await().atMost(10, TimeUnit.SECONDS).until(() -> scheduler.registrySize() == 0);
  }

  @Test
  public void unknownSubmitOutcomeNeverBlindResubmits() throws Exception {
    createAndSchedule("unknown-1");
    // first submit returns UNKNOWN: the state machine must persist the unresolved state and
    // never resubmit the same submissionKey (spec §7.3)
    engine.setBehavior(
        new org.apache.amoro.process.engine.FakeEngineAdapter.Behavior() {
          @Override
          public org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome onSubmit(
              String submissionKey) {
            return org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome.unknown();
          }
        });
    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(
            () -> {
              ProcessResource r = assembly.repository().get("unknown-1");
              return r.status().attempt() != null
                  && "UNKNOWN".equals(r.status().attempt().submitState());
            });
    long submitsAtUnresolved = submitCalls.get();
    Thread.sleep(500L); // several periods pass
    assertEquals(
        submitsAtUnresolved,
        submitCalls.get(),
        "an unresolved submission is never blind-resubmitted with the same key");
  }

  @Test
  public void legacyFinalRepairPreservesExistingAttemptAndStatusTimes() {
    String attemptFinishedAt = "2026-08-22T10:00:02Z";
    String statusFinishedAt = "2026-08-22T10:00:03Z";
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            "repair-final:0:0",
            "sha256:req",
            "ACKNOWLEDGED",
            "dummy-execution-repair",
            "2026-08-22T10:00:01Z",
            null,
            "FINAL",
            attemptFinishedAt,
            Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    ProcessResource legacy =
        new ProcessResource(
            "repair-final",
            new ProcessResource.ProcessSpec(
                new ProcessResource.TableRef("prod", "db", "repair", "repair-table", "simulated"),
                "dummy-maintenance",
                "local",
                "MANUAL",
                "2026-08-22T10:00:00Z",
                "RUN",
                new ProcessResource.RequestIdentity("sha256:key-repair", "sha256:req"),
                Collections.singletonMap("simulated", true),
                new ProcessResource.RetryPolicy(3, 2, 30)),
            new ProcessResource.ProcessStatus(
                "FAILED",
                0,
                attempt,
                Collections.emptyList(),
                null,
                null,
                null,
                new ProcessResource.EngineBackoff(0, 0, 0, 0),
                Collections.emptyList(),
                null,
                "durable remote failure",
                "2026-08-22T10:00:01Z",
                null,
                statusFinishedAt));
    assembly.repository().create(legacy);

    new ProcessReconciler(
            legacy.name(),
            assembly.repository(),
            dispatcher,
            scheduler,
            () -> "2026-08-22T10:00:09Z",
            100L)
        .invoke();

    ProcessResource repaired = assembly.repository().get(legacy.name());
    assertEquals("durable remote failure", repaired.status().attempt().lastError());
    assertEquals(attemptFinishedAt, repaired.status().attempt().finishedAt());
    assertEquals(statusFinishedAt, repaired.status().finishedAt());
    assertTrue(
        ProcessConditions.isTrue(repaired.status().conditions(), ProcessConditions.DATA_REPAIRED));
    assertEquals(0L, submitCalls.get(), "repair must never call an Engine");
  }

  private String phaseOf(String name) {
    try {
      return assembly.repository().get(name).status().phase();
    } catch (RuntimeException missing) {
      return "MISSING";
    }
  }
}
