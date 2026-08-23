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

import org.apache.amoro.control.Controller;
import org.apache.amoro.control.Scheduler;
import org.apache.amoro.process.ProcessIndexSnapshot.ActiveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Periodic, bounded safety net for listener loss. It scans only the ordered active index and
 * schedules controllers under their normal {@code ControllerKey}; the scheduler merges duplicates
 * and preserves the earliest deadline.
 */
public final class ActiveProcessRescheduler implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ActiveProcessRescheduler.class);

  @FunctionalInterface
  public interface ControllerFactory {
    Controller create(String processName);
  }

  private final ProcessIndexProjection projection;
  private final Scheduler scheduler;
  private final ControllerFactory controllers;
  private final int batchSize;
  private final long maxRuntimeNanos;
  private final long intervalMillis;
  private final LongSupplier nanoTime;
  private final ScheduledThreadPoolExecutor executor;
  private final AtomicBoolean started = new AtomicBoolean();
  private volatile ActiveEntry cursor;

  public ActiveProcessRescheduler(
      ProcessIndexProjection projection,
      Scheduler scheduler,
      ControllerFactory controllers,
      int batchSize,
      long maxRuntimeMillis,
      long intervalMillis) {
    this(
        projection,
        scheduler,
        controllers,
        batchSize,
        maxRuntimeMillis,
        intervalMillis,
        System::nanoTime);
  }

  ActiveProcessRescheduler(
      ProcessIndexProjection projection,
      Scheduler scheduler,
      ControllerFactory controllers,
      int batchSize,
      long maxRuntimeMillis,
      long intervalMillis,
      LongSupplier nanoTime) {
    this.projection = Objects.requireNonNull(projection, "projection");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.controllers = Objects.requireNonNull(controllers, "controllers");
    if (batchSize <= 0 || maxRuntimeMillis <= 0 || intervalMillis <= 0) {
      throw new IllegalArgumentException(
          "batchSize, maxRuntimeMillis and intervalMillis must be > 0");
    }
    this.batchSize = batchSize;
    this.maxRuntimeNanos = TimeUnit.MILLISECONDS.toNanos(maxRuntimeMillis);
    this.intervalMillis = intervalMillis;
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    this.executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "amoro-process-active-rescheduler");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      executor.scheduleWithFixedDelay(
          this::safeRun, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }
  }

  /** Returns the number of active entries visited in this bounded round. */
  public int runOnce() {
    long deadline = nanoTime.getAsLong() + maxRuntimeNanos;
    ProcessIndexSnapshot snapshot = projection.current();
    List<ActiveEntry> entries = snapshot.activeAfter(cursor, batchSize);
    if (entries.isEmpty()) {
      cursor = null;
      return 0;
    }
    int visited = 0;
    for (ActiveEntry entry : entries) {
      if (nanoTime.getAsLong() >= deadline) {
        break;
      }
      cursor = entry;
      visited++;
      ProcessResource resource = snapshot.find(entry.name()).orElse(null);
      if (resource != null && !ProcessFinality.isFinal(resource)) {
        scheduler.schedule(controllers.create(entry.name()));
      }
    }
    return visited;
  }

  private void safeRun() {
    try {
      runOnce();
    } catch (RuntimeException failure) {
      LOG.warn(
          "Active Process rescheduler round failed; the next cursor round will retry.", failure);
    }
  }

  @Override
  public void close() {
    shutdown(5_000L);
  }

  public void shutdown(long timeoutMillis) {
    BoundedExecutorShutdown.shutdown(executor, timeoutMillis, "Process active rescheduler");
  }
}
