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
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Business-command single-flight over one {@link ProcessEnginePort} (process spec §6.1): {@code
 * SubmissionIdentity(process,submissionKey,requestHash)} merges concurrent submit AND resolve calls
 * into one adapter call until the future and its result application complete; {@code
 * ExecutionIdentity(process,externalId)} merges observe and cancel. Controllers hitting an
 * in-flight identity get an already-completed marker instead of a second adapter call. Every
 * adapter future is force-completed at the configured {@code commandTimeoutMillis}: a submit
 * timeout degrades conservatively to UNKNOWN (side effects undetermined), the other commands to
 * UNAVAILABLE.
 */
public final class ProcessEngineDispatcher {

  /** Marker returned to callers that hit an in-flight identity; no adapter call happens. */
  public static final class CommandInFlightException extends RuntimeException {
    public CommandInFlightException(String identity) {
      super("engine command already in flight: " + identity);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ProcessEngineDispatcher.class);

  private final ProcessEnginePort adapter;
  private final long commandTimeoutMillis;
  private final ConcurrentHashMap<String, CompletableFuture<?>> inFlight =
      new ConcurrentHashMap<String, CompletableFuture<?>>();
  private final ScheduledExecutorService timeoutExecutor;
  private final AtomicInteger commandSequence = new AtomicInteger();

  public ProcessEngineDispatcher(ProcessEnginePort adapter, long commandTimeoutMillis) {
    this.adapter = Objects.requireNonNull(adapter, "adapter");
    if (commandTimeoutMillis <= 0) {
      throw new IllegalArgumentException(
          "commandTimeoutMillis must be > 0, got " + commandTimeoutMillis);
    }
    this.commandTimeoutMillis = commandTimeoutMillis;
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread =
                  new Thread(
                      runnable,
                      "amoro-process-engine-timeout-" + commandSequence.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    this.timeoutExecutor = executor;
  }

  public EngineTypes.EngineCapabilities capabilities() {
    return adapter.capabilities();
  }

  public CompletionStage<SubmissionOutcome> submit(
      String processName, String submissionKey, String requestHash, byte[] payload) {
    return dispatch(
        "submit:" + processName + "/" + submissionKey + "/" + requestHash,
        // submit timeout: side effects undetermined -> UNKNOWN, never blind resubmit
        () ->
            bounded(
                adapter.submit(submissionKey, requestHash, payload), SubmissionOutcome.unknown()),
        SubmissionOutcome.class);
  }

  public CompletionStage<SubmissionResolution> resolveSubmission(
      String processName, String submissionKey, String requestHash) {
    return dispatch(
        "resolve:" + processName + "/" + submissionKey + "/" + requestHash,
        () ->
            bounded(
                adapter.resolveSubmission(submissionKey, requestHash),
                SubmissionResolution.unavailable()),
        SubmissionResolution.class);
  }

  public CompletionStage<ProcessObservation> observe(String processName, String externalId) {
    return dispatch(
        "observe:" + processName + "/" + externalId,
        () -> bounded(adapter.observe(externalId), ProcessObservation.unavailable()),
        ProcessObservation.class);
  }

  public CompletionStage<CancellationOutcome> cancel(String processName, String externalId) {
    return dispatch(
        "cancel:" + processName + "/" + externalId,
        () -> bounded(adapter.cancel(externalId), CancellationOutcome.unavailable()),
        CancellationOutcome.class);
  }

  public CompletionStage<Void> release(String executionEngine, String externalId) {
    return adapter.release(externalId); // release identities are cleanup-only, never gated
  }

  // ------------------------------------------------------------------ internals

  private <T> CompletableFuture<T> bounded(CompletionStage<T> stage, T timeoutFallback) {
    CompletableFuture<T> bounded = new CompletableFuture<T>();
    java.util.concurrent.ScheduledFuture<?> guard =
        timeoutExecutor.schedule(
            () -> {
              if (bounded.complete(timeoutFallback)) {
                LOG.warn(
                    "Engine command timed out after {}ms; degrading to {}.",
                    commandTimeoutMillis,
                    describe(timeoutFallback));
              }
            },
            commandTimeoutMillis,
            TimeUnit.MILLISECONDS);
    stage.whenComplete(
        (result, error) -> {
          guard.cancel(false);
          if (error != null) {
            bounded.completeExceptionally(error);
          } else {
            bounded.complete(result);
          }
        });
    return bounded;
  }

  private static String describe(Object fallback) {
    return fallback.toString();
  }

  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<T> dispatch(
      String identity, Supplier<CompletableFuture<T>> command, Class<T> type) {
    while (true) {
      CompletableFuture<?> existing = inFlight.get(identity);
      if (existing != null && !existing.isDone()) {
        throw new CommandInFlightException(identity);
      }
      CompletableFuture<T> created = new CompletableFuture<T>();
      CompletableFuture<?> raced = inFlight.putIfAbsent(identity, created);
      if (raced != null && !raced.isDone()) {
        throw new CommandInFlightException(identity);
      }
      if (raced != null) {
        continue; // stale completed entry from an earlier command; replace it
      }
      command
          .get()
          .whenComplete(
              (result, error) -> {
                try {
                  if (error != null) {
                    created.completeExceptionally(error);
                  } else {
                    created.complete(result);
                  }
                } finally {
                  inFlight.remove(identity, created);
                }
              });
      return created;
    }
  }

  /** For tests/ops: identities currently holding an adapter call. */
  public int inFlightCount() {
    return (int) inFlight.values().stream().filter(f -> !f.isDone()).count();
  }
}
