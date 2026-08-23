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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Lifecycle wrapper that invokes the bounded TTL cleaner periodically. */
public final class ProcessTtlRuntime implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessTtlRuntime.class);

  private final ProcessTtlCleaner cleaner;
  private final int retentionDays;
  private final int batchSize;
  private final long intervalMillis;
  private final ScheduledThreadPoolExecutor executor;
  private final AtomicBoolean started = new AtomicBoolean();

  public ProcessTtlRuntime(
      ProcessTtlCleaner cleaner, int retentionDays, int batchSize, long intervalMillis) {
    this.cleaner = Objects.requireNonNull(cleaner, "cleaner");
    if (retentionDays < 7 || batchSize < 1 || batchSize > 1000 || intervalMillis <= 0) {
      throw new IllegalArgumentException("invalid TTL runtime bounds");
    }
    this.retentionDays = retentionDays;
    this.batchSize = batchSize;
    this.intervalMillis = intervalMillis;
    this.executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "amoro-process-ttl");
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

  private void safeRun() {
    try {
      cleaner.cleanOnce(Instant.now(), retentionDays, batchSize);
    } catch (RuntimeException failure) {
      LOG.warn("Process TTL round failed.", failure);
    }
  }

  @Override
  public void close() {
    shutdown(5_000L);
  }

  public void shutdown(long timeoutMillis) {
    BoundedExecutorShutdown.shutdown(executor, timeoutMillis, "Process TTL runtime");
  }
}
