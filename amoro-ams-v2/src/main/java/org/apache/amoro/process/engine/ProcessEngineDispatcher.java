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
import org.apache.amoro.process.engine.EngineCommandIdentity.ExecutionIdentity;
import org.apache.amoro.process.engine.EngineCommandIdentity.ReleaseIdentity;
import org.apache.amoro.process.engine.EngineCommandIdentity.SubmissionIdentity;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Typed cross-operation single-flight boundary over one {@link ProcessEnginePort}. Business
 * identities remain claimed after the adapter future completes and are released only when the
 * caller confirms that the classified result was durably handled. Release duplicates merge into the
 * same cleanup flight.
 */
public final class ProcessEngineDispatcher implements AutoCloseable {

  /** Marker returned when a related business command owns the identity. */
  public static final class CommandInFlightException extends RuntimeException {
    public CommandInFlightException(EngineCommandIdentity identity) {
      super("engine command already in flight: " + identity);
    }
  }

  /** Release timeouts are failures; null is never used as a successful timeout sentinel. */
  public static final class EngineCommandTimeoutException extends RuntimeException {
    public EngineCommandTimeoutException(long timeoutMillis, EngineCommandIdentity identity) {
      super("engine command timed out after " + timeoutMillis + "ms: " + identity);
    }
  }

  /** Result plus explicit durable-apply acknowledgement. */
  public static final class CommandFlight<T> {
    private final CompletionStage<T> result;
    private final Runnable durableHandled;
    private final AtomicBoolean handled = new AtomicBoolean();

    private CommandFlight(CompletionStage<T> result, Runnable durableHandled) {
      this.result = result;
      this.durableHandled = durableHandled;
    }

    public CompletionStage<T> result() {
      return result;
    }

    public CompletableFuture<T> toCompletableFuture() {
      return result.toCompletableFuture();
    }

    /** Convenience for callback-based callers. */
    public CompletionStage<T> whenComplete(
        java.util.function.BiConsumer<? super T, ? super Throwable> action) {
      return result.whenComplete(action);
    }

    /** Idempotently releases the identity after result persistence/handling has completed. */
    public void markDurablyHandled() {
      if (handled.compareAndSet(false, true)) {
        durableHandled.run();
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ProcessEngineDispatcher.class);

  private final ProcessEnginePort adapter;
  private final long commandTimeoutMillis;
  private final ConcurrentHashMap<EngineCommandIdentity, CommandFlight<?>> inFlight =
      new ConcurrentHashMap<>();
  private final ScheduledExecutorService timeoutExecutor;
  private final AtomicInteger commandSequence = new AtomicInteger();
  private final AtomicBoolean closed = new AtomicBoolean();

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
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    this.timeoutExecutor = executor;
  }

  public EngineTypes.EngineCapabilities capabilities() {
    return EngineBoundaryValidator.capabilities(adapter.capabilities());
  }

  public CommandFlight<SubmissionOutcome> submit(
      String processName, String submissionKey, String requestHash, byte[] payload) {
    return submit(processName, "legacy-action", submissionKey, requestHash, payload);
  }

  public CommandFlight<SubmissionOutcome> submit(
      String processName, String action, String submissionKey, String requestHash, byte[] payload) {
    ensureOpen();
    SubmissionIdentity identity = new SubmissionIdentity(processName, submissionKey, requestHash);
    SubmissionCommand command = new SubmissionCommand(action, submissionKey, requestHash, payload);
    return dispatchBusiness(
        identity,
        () -> adapter.submit(command),
        SubmissionOutcome.unknown(),
        EngineBoundaryValidator::submission);
  }

  public CommandFlight<SubmissionResolution> resolveSubmission(
      String processName, String submissionKey, String requestHash) {
    ensureOpen();
    SubmissionIdentity identity = new SubmissionIdentity(processName, submissionKey, requestHash);
    return dispatchBusiness(
        identity,
        () -> adapter.resolveSubmission(submissionKey, requestHash),
        SubmissionResolution.unavailable(),
        EngineBoundaryValidator::resolution);
  }

  public CommandFlight<ProcessObservation> observe(String processName, String externalId) {
    ensureOpen();
    ExecutionIdentity identity = new ExecutionIdentity(processName, externalId);
    return dispatchBusiness(
        identity,
        () -> adapter.observe(externalId),
        ProcessObservation.unavailable(),
        EngineBoundaryValidator::observation);
  }

  public CommandFlight<CancellationOutcome> cancel(String processName, String externalId) {
    ensureOpen();
    ExecutionIdentity identity = new ExecutionIdentity(processName, externalId);
    return dispatchBusiness(
        identity,
        () -> adapter.cancel(externalId),
        CancellationOutcome.unavailable(),
        EngineBoundaryValidator::cancellation);
  }

  public CommandFlight<Void> release(String executionEngine, String externalId) {
    ensureOpen();
    ReleaseIdentity identity = new ReleaseIdentity(executionEngine, externalId);
    while (true) {
      CommandFlight<?> existing = inFlight.get(identity);
      if (existing != null) {
        @SuppressWarnings("unchecked")
        CommandFlight<Void> merged = (CommandFlight<Void>) existing;
        return merged;
      }
      CompletableFuture<Void> result = new CompletableFuture<>();
      CommandFlight<Void>[] holder = flightHolder();
      CommandFlight<Void> created =
          new CommandFlight<>(result, () -> inFlight.remove(identity, holder[0]));
      holder[0] = created;
      CommandFlight<?> raced = inFlight.putIfAbsent(identity, created);
      if (raced == null) {
        boundedRelease(identity, () -> adapter.release(externalId))
            .whenComplete(
                (ignored, error) -> {
                  if (error != null) {
                    result.completeExceptionally(error);
                  } else {
                    result.complete(null);
                  }
                });
        return created;
      }
    }
  }

  private <T> CommandFlight<T> dispatchBusiness(
      EngineCommandIdentity identity,
      Supplier<CompletionStage<T>> command,
      T timeoutFallback,
      Function<T, T> validator) {
    if (inFlight.containsKey(identity)) {
      throw new CommandInFlightException(identity);
    }
    CompletableFuture<T> result = new CompletableFuture<>();
    CommandFlight<T>[] holder = flightHolder();
    CommandFlight<T> created =
        new CommandFlight<>(result, () -> inFlight.remove(identity, holder[0]));
    holder[0] = created;
    CommandFlight<?> raced = inFlight.putIfAbsent(identity, created);
    if (raced != null) {
      throw new CommandInFlightException(identity);
    }
    try {
      bounded(identity, command.get(), timeoutFallback, validator)
          .whenComplete(
              (value, error) -> {
                if (error != null) {
                  result.completeExceptionally(error);
                } else {
                  result.complete(value);
                }
              });
    } catch (RuntimeException synchronousFailure) {
      result.completeExceptionally(synchronousFailure);
    }
    return created;
  }

  private <T> CompletableFuture<T> bounded(
      EngineCommandIdentity identity,
      CompletionStage<T> stage,
      T timeoutFallback,
      Function<T, T> validator) {
    Objects.requireNonNull(stage, "adapter command future");
    CompletableFuture<T> bounded = new CompletableFuture<>();
    java.util.concurrent.ScheduledFuture<?> guard =
        timeoutExecutor.schedule(
            () -> {
              if (bounded.complete(timeoutFallback)) {
                LOG.warn(
                    "Engine command {} timed out after {}ms; applying conservative classification.",
                    identity,
                    commandTimeoutMillis);
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
            try {
              bounded.complete(validator.apply(result));
            } catch (RuntimeException invalid) {
              LOG.warn("Engine command {} returned an invalid result.", identity, invalid);
              bounded.complete(timeoutFallback);
            }
          }
        });
    return bounded;
  }

  private CompletableFuture<Void> boundedRelease(
      EngineCommandIdentity identity, Supplier<CompletionStage<Void>> command) {
    CompletableFuture<Void> bounded = new CompletableFuture<>();
    CompletionStage<Void> stage;
    try {
      stage = Objects.requireNonNull(command.get(), "adapter release future");
    } catch (RuntimeException synchronousFailure) {
      bounded.completeExceptionally(synchronousFailure);
      return bounded;
    }
    java.util.concurrent.ScheduledFuture<?> guard =
        timeoutExecutor.schedule(
            () ->
                bounded.completeExceptionally(
                    new EngineCommandTimeoutException(commandTimeoutMillis, identity)),
            commandTimeoutMillis,
            TimeUnit.MILLISECONDS);
    stage.whenComplete(
        (ignored, error) -> {
          guard.cancel(false);
          if (error != null) {
            bounded.completeExceptionally(error);
          } else {
            bounded.complete(null);
          }
        });
    return bounded;
  }

  @SuppressWarnings("unchecked")
  private static <T> CommandFlight<T>[] flightHolder() {
    return (CommandFlight<T>[]) new CommandFlight<?>[1];
  }

  /** Includes completed results that still await durable handling. */
  public int inFlightCount() {
    return inFlight.size();
  }

  @Override
  public void close() {
    shutdown(5_000L);
  }

  /** Stops command guards and the adapter within one caller-supplied lifecycle budget. */
  public void shutdown(long timeoutMillis) {
    if (timeoutMillis <= 0) {
      throw new IllegalArgumentException("timeoutMillis must be > 0");
    }
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    BoundedExecutorShutdown.shutdown(
        timeoutExecutor, remainingMillis(deadline), "Process Engine timeout guards");
    closeAdapter(remainingMillis(deadline));
  }

  private void closeAdapter(long timeoutMillis) {
    if (!(adapter instanceof AutoCloseable)) {
      return;
    }
    ExecutorService closer =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "amoro-process-engine-close");
              thread.setDaemon(true);
              return thread;
            });
    Future<?> closeResult =
        closer.submit(
            () -> {
              try {
                if (adapter instanceof ProcessEngineLifecycle) {
                  ((ProcessEngineLifecycle) adapter).shutdown(timeoutMillis);
                } else {
                  ((AutoCloseable) adapter).close();
                }
              } catch (RuntimeException runtimeFailure) {
                throw runtimeFailure;
              } catch (Exception closeFailure) {
                throw new IllegalStateException(
                    "failed to close Process engine adapter", closeFailure);
              }
            });
    boolean terminated =
        BoundedExecutorShutdown.shutdown(closer, timeoutMillis, "Process Engine adapter close");
    if (terminated && closeResult.isDone() && !closeResult.isCancelled()) {
      try {
        closeResult.get();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException closeFailure) {
        Throwable cause = closeFailure.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        throw new IllegalStateException("failed to close Process engine adapter", cause);
      }
    }
  }

  private void ensureOpen() {
    if (closed.get()) {
      throw new RejectedExecutionException("Process Engine dispatcher is closed");
    }
  }

  private static long remainingMillis(long deadlineNanos) {
    return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
  }
}
