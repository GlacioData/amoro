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

import org.apache.amoro.process.BoundedExecutorShutdown;
import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.EngineCapabilities;
import org.apache.amoro.process.engine.EngineTypes.EngineFailure;
import org.apache.amoro.process.engine.EngineTypes.EngineObservation;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.apache.amoro.process.engine.local.LocalActionCommand;
import org.apache.amoro.process.engine.local.LocalActionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * The local execution engine (process spec §6.1 local adapter): submissions dispatch to a dedicated
 * bounded action pool and the submit future completes immediately (the observation rounds converge
 * the result). The queue-full rejection is an authoritative "nothing ran"
 * (REJECTED/CAPACITY_EXHAUSTED), never UNKNOWN. Cancellation marks the handle; a running action
 * observes the flag cooperatively. The action body is pluggable — the first version ships a no-op
 * simulator. Future format Actions require their own reviewed Spec and provider.
 */
public final class LocalEngineAdapter implements ProcessEnginePort, ProcessEngineLifecycle {

  private static final Logger LOG = LoggerFactory.getLogger(LocalEngineAdapter.class);

  /** The action body: receives the payload and a cancel-flag; writes its summary. */
  public interface LocalAction {
    void run(
        byte[] payload,
        Consumer<Map<String, Object>> summarySink,
        java.util.function.BooleanSupplier cancelRequested)
        throws Exception;
  }

  private final java.util.concurrent.ThreadPoolExecutor actionPool;
  private final LocalAction action;
  private final LocalActionRegistry actionRegistry;
  private final long terminalRetentionMillis;
  private final java.util.concurrent.ScheduledThreadPoolExecutor retentionExecutor;
  private final AtomicBoolean closed = new AtomicBoolean();
  private static final int SUBMISSION_LOCK_STRIPES = 256;

  private final LocalSubmissionLedger submissionLedger = new LocalSubmissionLedger();
  private final Map<String, LocalExecution> executionsByExternalId =
      new ConcurrentHashMap<String, LocalExecution>();
  private final Object[] submissionLocks = new Object[SUBMISSION_LOCK_STRIPES];

  private static final class LocalExecution {
    volatile EngineObservation observation;
    volatile boolean cancelRequested;
    volatile long terminalAtMillis;
  }

  /** A no-op action body for wiring tests and demos: succeeds after a short delay. */
  public static LocalAction simulatedAction() {
    return (payload, summarySink, cancelRequested) -> {
      Thread.sleep(5L);
      Map<String, Object> summary = new LinkedHashMap<String, Object>();
      summary.put("simulated", true);
      summarySink.accept(summary);
    };
  }

  /**
   * @param poolSize action worker threads
   * @param queueCapacity bounded dispatch queue: a full queue is an authoritative "nothing ran"
   *     rejection (spec §6.1), unlike the unbounded v1 work queue
   */
  public LocalEngineAdapter(int poolSize, int queueCapacity, LocalAction action) {
    this(poolSize, queueCapacity, Objects.requireNonNull(action, "action"), null);
  }

  /** Action-aware constructor used by SPI-selected Local providers. */
  public LocalEngineAdapter(int poolSize, int queueCapacity, LocalActionRegistry actionRegistry) {
    this(poolSize, queueCapacity, actionRegistry, 7);
  }

  public LocalEngineAdapter(
      int poolSize,
      int queueCapacity,
      LocalActionRegistry actionRegistry,
      int terminalResultRetentionDays) {
    this(
        poolSize,
        queueCapacity,
        null,
        Objects.requireNonNull(actionRegistry, "actionRegistry"),
        terminalResultRetentionDays);
  }

  private LocalEngineAdapter(
      int poolSize, int queueCapacity, LocalAction action, LocalActionRegistry actionRegistry) {
    this(poolSize, queueCapacity, action, actionRegistry, 7);
  }

  private LocalEngineAdapter(
      int poolSize,
      int queueCapacity,
      LocalAction action,
      LocalActionRegistry actionRegistry,
      int terminalResultRetentionDays) {
    if (poolSize <= 0) {
      throw new IllegalArgumentException("poolSize must be > 0");
    }
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("queueCapacity must be > 0");
    }
    if (terminalResultRetentionDays < 1) {
      throw new IllegalArgumentException("terminalResultRetentionDays must be >= 1");
    }
    this.actionPool =
        new java.util.concurrent.ThreadPoolExecutor(
            poolSize,
            poolSize,
            60L,
            TimeUnit.SECONDS,
            new java.util.concurrent.ArrayBlockingQueue<Runnable>(queueCapacity),
            runnable -> {
              Thread thread =
                  new Thread(
                      runnable, "amoro-process-local-action-" + poolSequence.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    this.action = action;
    this.actionRegistry = actionRegistry;
    this.terminalRetentionMillis = TimeUnit.DAYS.toMillis(terminalResultRetentionDays);
    this.retentionExecutor =
        new java.util.concurrent.ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "amoro-process-local-retention");
              thread.setDaemon(true);
              return thread;
            });
    retentionExecutor.setRemoveOnCancelPolicy(true);
    retentionExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    retentionExecutor.scheduleWithFixedDelay(this::safeSweepTerminal, 1L, 1L, TimeUnit.HOURS);
    for (int i = 0; i < submissionLocks.length; i++) {
      submissionLocks[i] = new Object();
    }
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
    if (action == null) {
      return CompletableFuture.completedFuture(SubmissionOutcome.rejected("ACTION_REQUIRED"));
    }
    return submitLegacy(submissionKey, requestHash, submissionPayload, action);
  }

  @Override
  public CompletionStage<SubmissionOutcome> submit(SubmissionCommand command) {
    if (actionRegistry == null) {
      return submit(command.submissionKey(), command.requestHash(), command.payload());
    }
    java.util.Optional<org.apache.amoro.process.engine.local.LocalAction> selected =
        actionRegistry.action(command.action());
    if (selected.isEmpty()) {
      return CompletableFuture.completedFuture(SubmissionOutcome.rejected("ACTION_NOT_DEPLOYED"));
    }
    org.apache.amoro.process.engine.local.LocalAction localAction = selected.get();
    LocalAction bridged =
        (payload, summarySink, cancelRequested) ->
            localAction.execute(
                new LocalActionCommand(
                    command.action(), command.submissionKey(), command.requestHash(), payload),
                new org.apache.amoro.process.engine.local.LocalExecutionContext() {
                  @Override
                  public boolean isCancellationRequested() {
                    return cancelRequested.getAsBoolean();
                  }

                  @Override
                  public void publishSummary(Map<String, Object> summary) {
                    summarySink.accept(summary);
                  }
                });
    return submitLegacy(command.submissionKey(), command.requestHash(), command.payload(), bridged);
  }

  private CompletionStage<SubmissionOutcome> submitLegacy(
      String submissionKey,
      String requestHash,
      byte[] submissionPayload,
      LocalAction selectedAction) {
    Objects.requireNonNull(submissionKey, "submissionKey");
    Objects.requireNonNull(requestHash, "requestHash");
    byte[] frozenPayload =
        java.util.Arrays.copyOf(
            Objects.requireNonNull(submissionPayload, "submissionPayload"),
            submissionPayload.length);
    Object lock =
        submissionLocks[(submissionKey.hashCode() & Integer.MAX_VALUE) % submissionLocks.length];
    synchronized (lock) {
      java.util.Optional<LocalSubmissionLedger.Entry> existing =
          submissionLedger.find(submissionKey);
      if (existing.isPresent()) {
        LocalSubmissionLedger.Entry entry = existing.get();
        return CompletableFuture.completedFuture(
            entry.requestHash().equals(requestHash)
                ? SubmissionOutcome.acknowledged(entry.externalId())
                : SubmissionOutcome.conflict());
      }
      String externalId = "local-" + UUID.randomUUID();
      LocalExecution execution = new LocalExecution();
      execution.observation = new EngineObservation("SUBMITTED", null, null, null);
      try {
        actionPool.execute(() -> runAction(externalId, execution, frozenPayload, selectedAction));
      } catch (RejectedExecutionException capacityExhausted) {
        // provably nothing ran: an authoritative rejection, not UNKNOWN
        return CompletableFuture.completedFuture(SubmissionOutcome.rejected("CAPACITY_EXHAUSTED"));
      }
      submissionLedger.record(submissionKey, requestHash, externalId);
      executionsByExternalId.put(externalId, execution);
      return CompletableFuture.completedFuture(SubmissionOutcome.acknowledged(externalId));
    }
  }

  private void runAction(
      String externalId, LocalExecution execution, byte[] payload, LocalAction selectedAction) {
    execution.observation = new EngineObservation("RUNNING", null, null, null);
    try {
      Map<String, Object> summary = new LinkedHashMap<String, Object>();
      selectedAction.run(payload, summary::putAll, () -> execution.cancelRequested);
      if (execution.cancelRequested) {
        execution.observation = new EngineObservation("CANCELED", null, summary, null);
        execution.terminalAtMillis = System.currentTimeMillis();
        return;
      }
      execution.observation = new EngineObservation("SUCCESS", null, summary, null);
      execution.terminalAtMillis = System.currentTimeMillis();
    } catch (Exception failure) {
      execution.observation =
          new EngineObservation(
              "FAILED",
              null,
              null,
              new EngineFailure("E_LOCAL", String.valueOf(failure.getMessage()), true));
      execution.terminalAtMillis = System.currentTimeMillis();
    }
  }

  @Override
  public CompletionStage<SubmissionResolution> resolveSubmission(
      String submissionKey, String requestHash) {
    java.util.Optional<LocalSubmissionLedger.Entry> entry = submissionLedger.find(submissionKey);
    if (entry.isEmpty()) {
      return CompletableFuture.completedFuture(
          SubmissionResolution.lost(
              "local submission ledger has no restart-safe record for this dispatch"));
    }
    return CompletableFuture.completedFuture(
        entry.get().requestHash().equals(requestHash)
            ? SubmissionResolution.acknowledged(entry.get().externalId())
            : SubmissionResolution.conflict());
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
    // the submission registry must not leak one entry per generation forever
    submissionLedger.removeExternalId(externalId);
    return CompletableFuture.completedFuture(null);
  }

  /** Number of accepted identities retained until terminal release; exposed for diagnostics. */
  public int submissionCount() {
    return submissionLedger.size();
  }

  /** No-arg overload for Spring's inferred destroy method. */
  public void shutdown() {
    shutdown(5_000L);
  }

  @Override
  public void close() {
    shutdown();
  }

  /** Bounded shutdown of the action pool. */
  public void shutdown(long timeoutMillis) {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    BoundedExecutorShutdown.shutdown(
        retentionExecutor, remainingMillis(deadline), "Local Engine retention");
    BoundedExecutorShutdown.shutdown(actionPool, remainingMillis(deadline), "Local Engine actions");
  }

  private static long remainingMillis(long deadlineNanos) {
    return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
  }

  private void safeSweepTerminal() {
    try {
      sweepTerminal(System.currentTimeMillis());
    } catch (RuntimeException failure) {
      LOG.warn("Local terminal-result retention sweep failed.", failure);
    }
  }

  private void sweepTerminal(long nowMillis) {
    for (Map.Entry<String, LocalExecution> entry : executionsByExternalId.entrySet()) {
      LocalExecution execution = entry.getValue();
      long terminalAt = execution.terminalAtMillis;
      if (terminalAt > 0L
          && nowMillis - terminalAt >= terminalRetentionMillis
          && executionsByExternalId.remove(entry.getKey(), execution)) {
        submissionLedger.removeExternalId(entry.getKey());
        LOG.warn(
            "Hard-retention removed unreleased local terminal execution {} after {} ms.",
            entry.getKey(),
            terminalRetentionMillis);
      }
    }
  }
}
