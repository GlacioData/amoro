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

package org.apache.amoro.process.trigger;

import org.apache.amoro.process.BoundedExecutorShutdown;
import org.apache.amoro.process.ProcessCreationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** One bounded scheduled thread that drives every selected Action through shared admission. */
public final class ProcessTriggerCoordinator implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessTriggerCoordinator.class);

  private final long intervalMillis;
  private final List<ProcessTriggerScanner> scanners;
  private final ScheduledThreadPoolExecutor executor;
  private final AtomicBoolean started = new AtomicBoolean();

  public ProcessTriggerCoordinator(
      ProcessCreationService creationService,
      ManagedTablePort tables,
      ProcessActionRegistry actions,
      long intervalMillis,
      int batchSize) {
    this(creationService, tables, actions, intervalMillis, batchSize, Clock.systemUTC());
  }

  ProcessTriggerCoordinator(
      ProcessCreationService creationService,
      ManagedTablePort tables,
      ProcessActionRegistry actions,
      long intervalMillis,
      int batchSize,
      Clock clock) {
    Objects.requireNonNull(creationService, "creationService");
    Objects.requireNonNull(tables, "tables");
    Objects.requireNonNull(actions, "actions");
    if (intervalMillis <= 0 || batchSize < 1 || batchSize > 1000) {
      throw new IllegalArgumentException("intervalMillis > 0 and batchSize in [1, 1000] required");
    }
    this.intervalMillis = intervalMillis;
    Clock triggerClock = Objects.requireNonNull(clock, "clock");
    this.scanners = new ArrayList<>();
    for (ProcessActionRegistry.Entry entry : actions.entries()) {
      scanners.add(
          new ProcessTriggerScanner(
              creationService,
              tables,
              entry.plugin(),
              "spring-" + entry.action(),
              triggerClock,
              batchSize));
    }
    this.executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "amoro-process-trigger");
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

  public void runOnce() {
    for (ProcessTriggerScanner scanner : scanners) {
      scanner.scanBatchOnce();
    }
  }

  private void safeRun() {
    try {
      runOnce();
    } catch (RuntimeException failure) {
      LOG.warn("Scheduled Process trigger round failed.", failure);
    }
  }

  @Override
  public void close() {
    shutdown(5_000L);
  }

  public void shutdown(long timeoutMillis) {
    BoundedExecutorShutdown.shutdown(executor, timeoutMillis, "Process trigger coordinator");
  }
}
