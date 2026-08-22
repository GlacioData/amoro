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

package org.apache.amoro.process.engine;

import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.EngineCapabilities;
import org.apache.amoro.process.engine.EngineTypes.EngineFailure;
import org.apache.amoro.process.engine.EngineTypes.EngineObservation;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * The local execution engine (process spec §6.1 local adapter): submissions dispatch to a dedicated
 * bounded action pool and the submit future completes immediately (the observation rounds converge
 * the result). The queue-full rejection is an authoritative "nothing ran"
 * (REJECTED/CAPACITY_EXHAUSTED), never UNKNOWN. Cancellation marks the handle; a running action
 * observes the flag cooperatively. The action body is pluggable — the first version ships a no-op
 * simulator (the real Iceberg/Paimon maintenance calls land with the format adapters).
 */
public final class LocalEngineAdapter implements ProcessEnginePort {

  /** The action body: receives the payload and a cancel-flag; writes its summary. */
  public interface LocalAction {
    void run(byte[] payload, Consumer<Map<String, Object>> summarySink, Runnable cancelled)
        throws Exception;
  }

  private final ExecutorService actionPool;
  private final LocalAction action;
  private final Map<String, String> acknowledgedBySubmissionKey =
      new ConcurrentHashMap<String, String>(); // submissionKey -> externalId
  private final Map<String, LocalExecution> executionsByExternalId =
      new ConcurrentHashMap<String, LocalExecution>();
  private final AtomicInteger externalIdSequence = new AtomicInteger();

  private static final class LocalExecution {
    volatile EngineObservation observation;
    volatile boolean cancelRequested;
  }

  /** A no-op action body for wiring tests and demos: succeeds after a short delay. */
  public static LocalAction simulatedAction() {
    return (payload, summarySink, cancelled) -> {
      Thread.sleep(5L);
      Map<String, Object> summary = new LinkedHashMap<String, Object>();
      summary.put("simulated", true);
      summarySink.accept(summary);
    };
  }

  public LocalEngineAdapter(int poolSize, LocalAction action) {
    this.actionPool =
        Executors.newFixedThreadPool(
            poolSize,
            runnable -> {
              Thread thread =
                  new Thread(
                      runnable, "amoro-process-local-action-" + poolSequence.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    this.action = action;
  }

  private static final AtomicInteger poolSequence = new AtomicInteger();

  @Override
  public EngineCapabilities capabilities() {
    // local registry is authoritative: both resolution and cancellation are supported
    return new EngineCapabilities(true, true, "local-v1");
  }

  @Override
  public CompletionStage<SubmissionOutcome> submit(
      String submissionKey, String requestHash, byte[] submissionPayload) {
    String externalId = "local-" + externalIdSequence.incrementAndGet();
    LocalExecution execution = new LocalExecution();
    execution.observation = new EngineObservation("SUBMITTED", null, null, null);
    try {
      actionPool.execute(() -> runAction(externalId, execution, submissionPayload));
    } catch (RejectedExecutionException capacityExhausted) {
      // provably nothing ran: an authoritative rejection, not UNKNOWN
      return CompletableFuture.completedFuture(SubmissionOutcome.rejected("CAPACITY_EXHAUSTED"));
    }
    acknowledgedBySubmissionKey.put(submissionKey, externalId);
    executionsByExternalId.put(externalId, execution);
    return CompletableFuture.completedFuture(SubmissionOutcome.acknowledged(externalId));
  }

  private void runAction(String externalId, LocalExecution execution, byte[] payload) {
    execution.observation = new EngineObservation("RUNNING", null, null, null);
    try {
      Map<String, Object> summary = new LinkedHashMap<String, Object>();
      action.run(payload, summary::putAll, () -> execution.cancelRequested = true);
      if (execution.cancelRequested) {
        execution.observation = new EngineObservation("CANCELED", null, summary, null);
        return;
      }
      execution.observation = new EngineObservation("SUCCESS", null, summary, null);
    } catch (Exception failure) {
      execution.observation =
          new EngineObservation(
              "FAILED",
              null,
              null,
              new EngineFailure("E_LOCAL", String.valueOf(failure.getMessage()), true));
    }
  }

  @Override
  public CompletionStage<SubmissionResolution> resolveSubmission(
      String submissionKey, String requestHash) {
    String externalId = acknowledgedBySubmissionKey.get(submissionKey);
    return CompletableFuture.completedFuture(
        externalId == null
            ? SubmissionResolution.notFound()
            : SubmissionResolution.acknowledged(externalId));
  }

  @Override
  public CompletionStage<ProcessObservation> observe(String externalId) {
    LocalExecution execution = executionsByExternalId.get(externalId);
    return CompletableFuture.completedFuture(
        execution == null
            ? ProcessObservation.lost("local execution registry lost this handle")
            : ProcessObservation.known(execution.observation));
  }

  @Override
  public CompletionStage<CancellationOutcome> cancel(String externalId) {
    LocalExecution execution = executionsByExternalId.get(externalId);
    if (execution == null) {
      return CompletableFuture.completedFuture(CancellationOutcome.notFound());
    }
    String phase = execution.observation.remotePhase();
    if ("SUCCESS".equals(phase) || "FAILED".equals(phase) || "CANCELED".equals(phase)) {
      return CompletableFuture.completedFuture(
          CancellationOutcome.alreadyTerminal(execution.observation));
    }
    execution.cancelRequested = true;
    return CompletableFuture.completedFuture(CancellationOutcome.accepted());
  }

  @Override
  public CompletionStage<Void> release(String externalId) {
    executionsByExternalId.remove(externalId);
    return CompletableFuture.completedFuture(null);
  }

  /** Bounded shutdown of the action pool. */
  public void shutdown(long timeoutMillis) {
    actionPool.shutdown();
    try {
      if (!actionPool.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
        actionPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      actionPool.shutdownNow();
    }
  }

  @SuppressWarnings("unused")
  private static List<String> keepImport() {
    return null;
  }
}
