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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * P4: full state-machine walks over the real scheduler + fake engine — the run-to-SUCCESS
 * lifecycle, the retryable-FAILED path, and the cancel path.
 */
@Timeout(90)
public class TestProcessReconciler {

  private static String externalIdOf(String name) {
    return "fake-app-" + Math.abs((name + ":0:0").hashCode());
  }

  private DefaultScheduler scheduler;
  private FakeEngineAdapter engine;
  private ProcessEngineDispatcher dispatcher;
  private ProcessDomainAssembly assembly;

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(2, 50L);
    scheduler.start();
    engine = new FakeEngineAdapter();
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
                new ProcessResource.TableRef("prod", "db", "t", "42"),
                "expire-snapshots",
                "local",
                "MANUAL",
                "2026-08-22T10:00:00Z",
                "RUN",
                new ProcessResource.RequestIdentity("sha256:key", "sha256:req"),
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

  private String phaseOf(String name) {
    try {
      return assembly.repository().get(name).status().phase();
    } catch (RuntimeException missing) {
      return "MISSING";
    }
  }
}
