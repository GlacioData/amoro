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
import org.apache.amoro.process.engine.ExecutionHandleReleaseIndex.ReleaseEntry;
import org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The only component allowed to call {@link ProcessEnginePort#release(String)}. It consumes the
 * durable-publish release index in bounded batches; failures re-enter the ordered index and never
 * reverse an already persisted Process terminal result.
 */
public final class ExecutionHandleReaper implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionHandleReaper.class);

  private final ExecutionHandleReleaseIndex index;
  private final ProcessEngineRegistry engines;
  private final int batchSize;
  private final long intervalMillis;
  private final Supplier<Instant> now;
  private final ScheduledThreadPoolExecutor executor;
  private final AtomicBoolean started = new AtomicBoolean();

  public ExecutionHandleReaper(
      ExecutionHandleReleaseIndex index,
      ProcessEngineRegistry engines,
      int batchSize,
      long intervalMillis) {
    this(index, engines, batchSize, intervalMillis, Instant::now);
  }

  ExecutionHandleReaper(
      ExecutionHandleReleaseIndex index,
      ProcessEngineRegistry engines,
      int batchSize,
      long intervalMillis,
      Supplier<Instant> now) {
    this.index = Objects.requireNonNull(index, "index");
    this.engines = Objects.requireNonNull(engines, "engines");
    if (batchSize <= 0 || intervalMillis <= 0) {
      throw new IllegalArgumentException("batchSize and intervalMillis must be > 0");
    }
    this.batchSize = batchSize;
    this.intervalMillis = intervalMillis;
    this.now = Objects.requireNonNull(now, "now");
    this.executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "amoro-process-execution-handle-reaper");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      executor.scheduleWithFixedDelay(this::safeRun, 0L, intervalMillis, TimeUnit.MILLISECONDS);
    }
  }

  /** Claims and dispatches one bounded batch without waiting for engine futures. */
  public int runOnce(Instant instant) {
    List<ReleaseEntry> due = index.claimDue(instant, batchSize);
    for (ReleaseEntry entry : due) {
      ProcessEngineDispatcher engine =
          engines.dispatcherFor(entry.key().executionEngine()).orElse(null);
      if (engine == null) {
        index.releaseFailed(entry, now.get());
        LOG.warn(
            "Execution handle {} cannot be released because engine {} is not deployed.",
            entry.key().externalId(),
            entry.key().executionEngine());
        continue;
      }
      dispatch(entry, engine);
    }
    return due.size();
  }

  private void dispatch(ReleaseEntry entry, ProcessEngineDispatcher engine) {
    final CommandFlight<Void> flight;
    try {
      flight = engine.release(entry.key().executionEngine(), entry.key().externalId());
    } catch (RuntimeException dispatchFailure) {
      index.releaseFailed(entry, now.get());
      LOG.warn(
          "Execution handle release dispatch failed for {}.",
          entry.key().externalId(),
          dispatchFailure);
      return;
    }
    flight.whenComplete(
        (ignored, error) -> {
          if (error == null) {
            index.releaseSucceeded(entry);
          } else {
            index.releaseFailed(entry, now.get());
            LOG.warn("Execution handle release failed for {}.", entry.key().externalId(), error);
          }
          flight.markDurablyHandled();
        });
  }

  private void safeRun() {
    try {
      runOnce(now.get());
    } catch (RuntimeException failure) {
      LOG.warn("Execution handle reaper round failed; the next bounded round will retry.", failure);
    }
  }

  @Override
  public void close() {
    shutdown(5_000L);
  }

  public void shutdown(long timeoutMillis) {
    BoundedExecutorShutdown.shutdown(executor, timeoutMillis, "Process execution-handle reaper");
  }
}
