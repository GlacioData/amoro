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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.Controller;
import org.apache.amoro.control.ControllerKey;
import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.control.Scheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.apache.amoro.process.engine.FakeEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/** Restart-safe DISPATCHING and deadline behavior for the RUN/CANCEL control paths. */
@Timeout(60)
public class TestProcessSubmissionRecovery {

  private DefaultScheduler domainScheduler;
  private ProcessDomainAssembly assembly;
  private RecordingEngine engine;
  private ProcessEngineDispatcher dispatcher;
  private RecordingScheduler wakeups;

  @BeforeEach
  public void setUp() {
    domainScheduler = DefaultScheduler.create(1, 1000L);
    domainScheduler.start();
    assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            domainScheduler,
            128,
            10_000L,
            65536);
    engine = new RecordingEngine();
    dispatcher = new ProcessEngineDispatcher(engine, 5_000L);
    wakeups = new RecordingScheduler();
  }

  @AfterEach
  public void tearDown() {
    dispatcher.close();
    assembly.persistence().shutdown(Duration.ofSeconds(5));
    domainScheduler.shutdown(Duration.ofSeconds(5));
  }

  @Test
  public void freshCreatedAttemptIsDurableBeforeItsSingleSubmit() {
    create("fresh", "RUN", "CREATED", null);

    reconciler("fresh").invoke();

    assertEquals(1, engine.submits.get());
    assertEquals(0, engine.resolves.get());
    assertEquals("SUBMITTED", assembly.repository().get("fresh").status().phase());
    assertEquals(
        "ACKNOWLEDGED", assembly.repository().get("fresh").status().attempt().submitState());
  }

  @Test
  public void restartWithDispatchingOnlyResolvesAndNeverSubmits() {
    create("restart", "RUN", "DISPATCHING", null);
    engine.resolution = SubmissionResolution.acknowledged("remote-existing");

    reconciler("restart").invoke();

    assertEquals(0, engine.submits.get());
    assertEquals(1, engine.resolves.get());
    assertEquals(
        "remote-existing", assembly.repository().get("restart").status().attempt().externalId());
  }

  @Test
  public void cancelWhileDispatchingResolvesNotFoundWithoutSubmitting() {
    create("cancel", "CANCEL", "DISPATCHING", null);
    engine.resolution = SubmissionResolution.notFound();

    reconciler("cancel").invoke();

    assertEquals(0, engine.submits.get());
    assertEquals(1, engine.resolves.get());
    assertEquals("CANCELED", assembly.repository().get("cancel").status().phase());
    assertTrue(ProcessFinality.isFinal(assembly.repository().get("cancel")));
  }

  @Test
  public void unresolvedSubmissionIsAutomaticallyResolved() {
    create("unknown", "RUN", "UNKNOWN", null);
    engine.resolution = SubmissionResolution.acknowledged("resolved-external");

    reconciler("unknown").invoke();

    assertEquals(0, engine.submits.get());
    assertEquals(1, engine.resolves.get());
    assertEquals("SUBMITTED", assembly.repository().get("unknown").status().phase());
  }

  @Test
  public void futureDeadlinePerformsZeroEngineIo() {
    create("waiting", "RUN", "UNKNOWN", "2026-08-24T00:01:00Z");

    reconciler("waiting").invoke();

    assertEquals(0, engine.submits.get());
    assertEquals(0, engine.resolves.get());
    assertEquals(1, wakeups.delayed.get());
  }

  private ProcessReconciler reconciler(String name) {
    return new ProcessReconciler(
        name, assembly.repository(), dispatcher, wakeups, () -> "2026-08-24T00:00:00Z", 100L);
  }

  private void create(String name, String desired, String submitState, String nextReconcileAt) {
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            name + ":0:0",
            "sha256:req",
            submitState,
            null,
            "CREATED".equals(submitState) ? null : "2026-08-23T23:59:00Z",
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
                    "2026-08-23T23:00:00Z",
                    desired,
                    new ProcessResource.RequestIdentity("sha256:key-" + name, "sha256:req"),
                    java.util.Collections.emptyMap(),
                    new ProcessResource.RetryPolicy(3, 2, 30)),
                new ProcessResource.ProcessStatus(
                    "PENDING",
                    0,
                    attempt,
                    null,
                    null,
                    nextReconcileAt,
                    new ProcessResource.EngineBackoff(0, 0, 0, 0),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null)));
  }

  private static final class RecordingEngine extends FakeEngineAdapter {
    private final AtomicInteger submits = new AtomicInteger();
    private final AtomicInteger resolves = new AtomicInteger();
    private volatile SubmissionResolution resolution =
        SubmissionResolution.acknowledged("resolved");

    @Override
    public CompletionStage<SubmissionOutcome> submit(
        String submissionKey, String requestHash, byte[] submissionPayload) {
      submits.incrementAndGet();
      return java.util.concurrent.CompletableFuture.completedFuture(
          SubmissionOutcome.acknowledged("submitted"));
    }

    @Override
    public CompletionStage<SubmissionResolution> resolveSubmission(
        String submissionKey, String requestHash) {
      resolves.incrementAndGet();
      return java.util.concurrent.CompletableFuture.completedFuture(resolution);
    }
  }

  private static final class RecordingScheduler implements Scheduler {
    private final AtomicInteger delayed = new AtomicInteger();

    @Override
    public void schedule(Controller controller) {}

    @Override
    public void schedule(Controller controller, Duration nextDelay) {
      delayed.incrementAndGet();
    }

    @Override
    public void unschedule(ControllerKey key) {}

    @Override
    public void postStart() {}

    @Override
    public void shutdown(Duration timeout) {}
  }
}
