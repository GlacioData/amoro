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
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.LocalEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * P7: the local engine adapter drives the reconciler end-to-end without any HTTP surface — a
 * scheduled/REST process reaches SUCCESS purely on local threads, and cancel converges.
 */
@Timeout(90)
public class TestLocalEngine {

  private DefaultScheduler scheduler;
  private ProcessDomainAssembly assembly;
  private LocalEngineAdapter engine;
  private ProcessEngineDispatcher dispatcher;

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(2, 50L);
    scheduler.start();
    assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            scheduler,
            128,
            10_000L,
            65536);
    engine = new LocalEngineAdapter(2, 64, LocalEngineAdapter.simulatedAction());
    dispatcher = new ProcessEngineDispatcher(engine, 5_000L);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
    engine.shutdown(5_000L);
  }

  private org.apache.amoro.process.ProcessResource create(String name) {
    java.util.Map<String, Object> parameters = new java.util.LinkedHashMap<>();
    parameters.put("retainLast", 1);
    return assembly
        .repository()
        .create(
            new org.apache.amoro.process.ProcessResource(
                name,
                new org.apache.amoro.process.ProcessResource.ProcessSpec(
                    new org.apache.amoro.process.ProcessResource.TableRef(
                        "prod", "db", "t", "42", "simulated"),
                    "dummy-maintenance",
                    "local",
                    "MANUAL",
                    "2026-08-22T10:00:00Z",
                    "RUN",
                    new org.apache.amoro.process.ProcessResource.RequestIdentity(
                        "sha256:key-" + name, "sha256:req-" + name),
                    parameters,
                    new org.apache.amoro.process.ProcessResource.RetryPolicy(3, 2, 30)),
                new org.apache.amoro.process.ProcessResource.ProcessStatus(
                    "PENDING",
                    0,
                    null,
                    null,
                    null,
                    null,
                    new org.apache.amoro.process.ProcessResource.EngineBackoff(0, 0, 0, 0),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null)));
  }

  @Test
  public void localProcessRunsToSuccessThroughTheReconciler() {
    org.apache.amoro.process.ProcessResource created = create("local-1");
    ProcessReconciler reconciler =
        new ProcessReconciler(
            "local-1",
            assembly.repository(),
            dispatcher,
            scheduler,
            ProcessReconciler.Clock.systemUtc(),
            100L);
    scheduler.schedule(reconciler);

    await()
        .atMost(20, TimeUnit.SECONDS)
        .until(() -> "SUCCESS".equals(assembly.repository().get("local-1").status().phase()));
    assertTrue(
        assembly.repository().get("local-1").status().finishedAt() != null,
        "the terminal write stamps the top-level finishedAt");
    await().atMost(10, TimeUnit.SECONDS).until(() -> scheduler.registrySize() == 0);
  }

  @Test
  public void fullQueueIsAnAuthoritativeRejection() throws Exception {
    // pool 1, queue 1: first occupies the worker, second fills the queue, third is rejected
    // with a provable "nothing ran" — never UNKNOWN (spec §6.1)
    java.util.concurrent.CountDownLatch block = new java.util.concurrent.CountDownLatch(1);
    LocalEngineAdapter tiny =
        new LocalEngineAdapter(1, 1, (payload, summarySink, cancelRequested) -> block.await());
    try {
      org.apache.amoro.process.engine.ProcessEngineDispatcher tinyDispatcher =
          new org.apache.amoro.process.engine.ProcessEngineDispatcher(tiny, 5_000L);
      tinyDispatcher.submit("p", "k1", "h", new byte[] {1});
      tinyDispatcher.submit("p", "k2", "h", new byte[] {1});
      org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome third =
          tinyDispatcher
              .submit("p", "k3", "h", new byte[] {1})
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);
      assertEquals(
          org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome.Kind.REJECTED,
          third.kind());
      org.junit.jupiter.api.Assertions.assertTrue(
          third.reason() != null && third.reason().contains("CAPACITY"));
    } finally {
      block.countDown();
      tiny.shutdown(5_000L);
    }
  }

  @Test
  public void directPortSemanticsAckObserveCancelRelease() throws Exception {
    org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight<SubmissionOutcome>
        submitFlight = dispatcher.submit("p", "p:0:0", "sha256:r", new byte[] {1});
    SubmissionOutcome outcome = submitFlight.toCompletableFuture().get(5, TimeUnit.SECONDS);
    submitFlight.markDurablyHandled();
    assertEquals(SubmissionOutcome.Kind.ACKNOWLEDGED, outcome.kind());
    String externalId = outcome.externalId();

    // the simulated action finishes in ~5ms: observation converges to SUCCESS
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> {
              org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight<
                      ProcessObservation>
                  flight = dispatcher.observe("p", externalId);
              try {
                ProcessObservation observation =
                    flight.toCompletableFuture().get(1, TimeUnit.SECONDS);
                return observation.kind() == ProcessObservation.Kind.KNOWN
                    && "SUCCESS".equals(observation.observation().remotePhase());
              } finally {
                flight.markDurablyHandled();
              }
            });

    // observing a released handle is LOST (side effects may exist) — never NOT_FOUND
    org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight<Void> releaseFlight =
        dispatcher.release("local", externalId);
    releaseFlight.toCompletableFuture().get(5, TimeUnit.SECONDS);
    releaseFlight.markDurablyHandled();
    org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight<ProcessObservation>
        observeFlight = dispatcher.observe("p", externalId);
    ProcessObservation afterRelease = observeFlight.toCompletableFuture().get(5, TimeUnit.SECONDS);
    observeFlight.markDurablyHandled();
    assertEquals(ProcessObservation.Kind.LOST, afterRelease.kind());
  }
}
