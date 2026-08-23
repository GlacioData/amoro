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

import org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Bounded, lifecycle-owned retry lane for persisting completed engine commands.
 *
 * <p>A command flight is released only after the result is durably applied (or proved stale). If
 * the repository is temporarily unavailable, one bounded entry retains the flight and retries it.
 * Saturation deliberately fails closed: the flight remains claimed, preventing a duplicate
 * side-effecting command, and restart recovery continues from the durable Process state.
 */
public final class ProcessResultPersistenceRetryer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessResultPersistenceRetryer.class);

  private final ConcurrentHashMap<String, PendingResult> pending = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<String> retryOrder = new ConcurrentLinkedQueue<>();
  private final Semaphore capacity;
  private final int batchSize;
  private final ScheduledThreadPoolExecutor executor;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Object lifecycleLock = new Object();

  public ProcessResultPersistenceRetryer(int maxPending, int batchSize, long retryIntervalMillis) {
    if (maxPending <= 0 || batchSize <= 0 || retryIntervalMillis <= 0) {
      throw new IllegalArgumentException(
          "maxPending, batchSize and retryIntervalMillis must all be > 0");
    }
    this.capacity = new Semaphore(maxPending);
    this.batchSize = batchSize;
    this.executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "amoro-process-result-persistence-retry");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    executor.scheduleWithFixedDelay(
        this::drain, retryIntervalMillis, retryIntervalMillis, TimeUnit.MILLISECONDS);
  }

  /** Executes the durable apply immediately and registers at most one bounded retry when needed. */
  public void handle(
      String identity,
      CommandFlight<?> flight,
      BooleanSupplier durableApply,
      Runnable afterHandled) {
    Lease lease = tryReserve();
    if (lease == null) {
      LOG.error(
          "Process result retry capacity was not reserved for {}; the command remains "
              + "fail-closed until restart.",
          identity);
      return;
    }
    handle(identity, flight, durableApply, afterHandled, lease);
  }

  public void handle(
      String identity,
      CommandFlight<?> flight,
      BooleanSupplier durableApply,
      Runnable afterHandled,
      Lease lease) {
    Objects.requireNonNull(identity, "identity");
    PendingResult result =
        new PendingResult(
            Objects.requireNonNull(flight, "flight"),
            Objects.requireNonNull(durableApply, "durableApply"),
            Objects.requireNonNull(afterHandled, "afterHandled"),
            Objects.requireNonNull(lease, "lease"));
    Future<Boolean> initialApply;
    synchronized (lifecycleLock) {
      if (closed.get()) {
        LOG.warn(
            "Process result {} arrived after the retry lane closed; durable state will recover it "
                + "after restart.",
            identity);
        lease.close();
        return;
      }
      try {
        initialApply = executor.submit(() -> tryHandle(identity, result));
      } catch (RejectedExecutionException closing) {
        LOG.warn(
            "Process result {} could not enter the closing retry lane; durable state will "
                + "recover it after restart.",
            identity);
        lease.close();
        return;
      }
    }
    if (awaitInitialApply(identity, initialApply)) {
      lease.close();
      return;
    }
    synchronized (lifecycleLock) {
      if (closed.get()) {
        LOG.warn(
            "Process result {} was not persisted because the retry lane is closed; "
                + "the command flight remains claimed until restart.",
            identity);
        lease.close();
        return;
      }
      PendingResult raced = pending.putIfAbsent(identity, result);
      if (raced != null) {
        lease.close();
      } else {
        retryOrder.offer(identity);
      }
    }
  }

  private boolean awaitInitialApply(String identity, Future<Boolean> initialApply) {
    try {
      return initialApply.get();
    } catch (InterruptedException interrupted) {
      initialApply.cancel(true);
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException applyFailure) {
      LOG.warn(
          "Process engine result {} failed in the owned persistence lane.",
          identity,
          applyFailure.getCause());
      return false;
    }
  }

  /** Reserves bounded recovery capacity before an engine side effect is dispatched. */
  public Lease tryReserve() {
    if (closed.get() || !capacity.tryAcquire()) {
      return null;
    }
    if (closed.get()) {
      capacity.release();
      return null;
    }
    return new Lease(capacity);
  }

  int pendingCount() {
    return pending.size();
  }

  /** Executes one bounded retry round; package-private for deterministic lifecycle tests. */
  void drainOnce() {
    drain();
  }

  private void drain() {
    if (closed.get()) {
      return;
    }
    for (int processed = 0; processed < batchSize; processed++) {
      String identity = retryOrder.poll();
      if (identity == null) {
        return;
      }
      PendingResult result = pending.get(identity);
      if (result == null) {
        continue;
      }
      if (tryHandle(identity, result) && pending.remove(identity, result)) {
        result.lease.close();
      } else if (pending.get(identity) == result) {
        retryOrder.offer(identity);
      }
    }
  }

  private boolean tryHandle(String identity, PendingResult result) {
    final boolean handled;
    try {
      handled = result.durableApply.getAsBoolean();
    } catch (RuntimeException unavailable) {
      LOG.warn("Process engine result {} could not be durably applied yet.", identity, unavailable);
      return false;
    }
    if (!handled) {
      return false;
    }
    result.flight.markDurablyHandled();
    try {
      result.afterHandled.run();
    } catch (RuntimeException wakeFailure) {
      LOG.debug(
          "Process result {} was durable but its scheduler wake-up failed.", identity, wakeFailure);
    }
    return true;
  }

  @Override
  public void close() {
    shutdown(5_000L);
  }

  public void shutdown(long timeoutMillis) {
    int retained;
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    BoundedExecutorShutdown.shutdown(
        executor, timeoutMillis, "Process result-persistence retry lane");
    synchronized (lifecycleLock) {
      retained = pending.size();
      for (PendingResult result : pending.values()) {
        result.lease.close();
      }
      pending.clear();
      retryOrder.clear();
    }
    if (retained > 0) {
      LOG.warn(
          "Closed Process result retry lane with {} fail-closed command flights; durable state "
              + "will recover them after restart.",
          retained);
    }
  }

  private static final class PendingResult {
    private final CommandFlight<?> flight;
    private final BooleanSupplier durableApply;
    private final Runnable afterHandled;
    private final Lease lease;

    private PendingResult(
        CommandFlight<?> flight, BooleanSupplier durableApply, Runnable afterHandled, Lease lease) {
      this.flight = flight;
      this.durableApply = durableApply;
      this.afterHandled = afterHandled;
      this.lease = lease;
    }
  }

  /** One idempotently released reservation. */
  public static final class Lease implements AutoCloseable {
    private final Semaphore capacity;
    private final AtomicBoolean released = new AtomicBoolean();

    private Lease(Semaphore capacity) {
      this.capacity = capacity;
    }

    @Override
    public void close() {
      if (released.compareAndSet(false, true)) {
        capacity.release();
      }
    }
  }
}
