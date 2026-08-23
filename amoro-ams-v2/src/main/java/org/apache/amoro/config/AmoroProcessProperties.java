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

package org.apache.amoro.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Process runtime configuration. Simulation providers are opt-in and disabled by default. */
@ConfigurationProperties(prefix = "amoro.process")
public class AmoroProcessProperties {

  private final Simulation simulation = new Simulation();
  private final Engine engine = new Engine();
  private final Reconcile reconcile = new Reconcile();
  private final ResultPersistence resultPersistence = new ResultPersistence();
  private final Rescheduler rescheduler = new Rescheduler();
  private final ExecutionReaper executionReaper = new ExecutionReaper();
  private final Ttl ttl = new Ttl();
  private final Local local = new Local();
  private final Trigger trigger = new Trigger();
  private final Creation creation = new Creation();

  public static final class Simulation {
    private boolean enabled;
    private int workerThreads = 2;
    private int queueCapacity = 1024;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public int getWorkerThreads() {
      return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
      this.workerThreads = workerThreads;
    }

    public int getQueueCapacity() {
      return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
      this.queueCapacity = queueCapacity;
    }
  }

  public static final class Engine {
    private long commandTimeoutMs = 30_000L;

    public long getCommandTimeoutMs() {
      return commandTimeoutMs;
    }

    public void setCommandTimeoutMs(long commandTimeoutMs) {
      this.commandTimeoutMs = commandTimeoutMs;
    }
  }

  public static final class Reconcile {
    private long pollIntervalMs = 3_000L;
    private long submissionUnresolvedIntervalMs = 60_000L;
    private long cancelRetryIntervalMs = 10_000L;
    private long commandInFlightDelayMs = 250L;
    private long executionUnresolvedReminderIntervalMs = 300_000L;

    public long getPollIntervalMs() {
      return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
      this.pollIntervalMs = pollIntervalMs;
    }

    public long getSubmissionUnresolvedIntervalMs() {
      return submissionUnresolvedIntervalMs;
    }

    public void setSubmissionUnresolvedIntervalMs(long value) {
      this.submissionUnresolvedIntervalMs = value;
    }

    public long getCancelRetryIntervalMs() {
      return cancelRetryIntervalMs;
    }

    public void setCancelRetryIntervalMs(long cancelRetryIntervalMs) {
      this.cancelRetryIntervalMs = cancelRetryIntervalMs;
    }

    public long getCommandInFlightDelayMs() {
      return commandInFlightDelayMs;
    }

    public void setCommandInFlightDelayMs(long commandInFlightDelayMs) {
      this.commandInFlightDelayMs = commandInFlightDelayMs;
    }

    public long getExecutionUnresolvedReminderIntervalMs() {
      return executionUnresolvedReminderIntervalMs;
    }

    public void setExecutionUnresolvedReminderIntervalMs(long value) {
      this.executionUnresolvedReminderIntervalMs = value;
    }
  }

  public static final class ResultPersistence {
    private int maxPending = 1024;
    private int batchSize = 64;
    private long retryIntervalMs = 250L;

    public int getMaxPending() {
      return maxPending;
    }

    public void setMaxPending(int maxPending) {
      this.maxPending = maxPending;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    public long getRetryIntervalMs() {
      return retryIntervalMs;
    }

    public void setRetryIntervalMs(long retryIntervalMs) {
      this.retryIntervalMs = retryIntervalMs;
    }
  }

  public static final class Rescheduler {
    private int batchSize = 256;
    private long maxRuntimeMs = 1_000L;
    private long intervalMs = 30_000L;

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    public long getMaxRuntimeMs() {
      return maxRuntimeMs;
    }

    public void setMaxRuntimeMs(long maxRuntimeMs) {
      this.maxRuntimeMs = maxRuntimeMs;
    }

    public long getIntervalMs() {
      return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
      this.intervalMs = intervalMs;
    }
  }

  public static final class ExecutionReaper {
    private int batchSize = 100;
    private long intervalMs = 60_000L;

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    public long getIntervalMs() {
      return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
      this.intervalMs = intervalMs;
    }
  }

  public static final class Ttl {
    private int retentionDays = 30;
    private int batchSize = 100;
    private long intervalMs = 60_000L;

    public int getRetentionDays() {
      return retentionDays;
    }

    public void setRetentionDays(int retentionDays) {
      this.retentionDays = retentionDays;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }

    public long getIntervalMs() {
      return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
      this.intervalMs = intervalMs;
    }
  }

  public static final class Local {
    private int terminalResultRetentionDays = 7;

    public int getTerminalResultRetentionDays() {
      return terminalResultRetentionDays;
    }

    public void setTerminalResultRetentionDays(int terminalResultRetentionDays) {
      this.terminalResultRetentionDays = terminalResultRetentionDays;
    }
  }

  public static final class Trigger {
    private long intervalMs = 60_000L;
    private int batchSize = 100;

    public long getIntervalMs() {
      return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
      this.intervalMs = intervalMs;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }
  }

  public static final class Creation {
    private int maxRetries = 3;
    private int maxSubmissionRetries = 2;
    private int retryDelaySeconds = 30;

    public int getMaxRetries() {
      return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    public int getMaxSubmissionRetries() {
      return maxSubmissionRetries;
    }

    public void setMaxSubmissionRetries(int maxSubmissionRetries) {
      this.maxSubmissionRetries = maxSubmissionRetries;
    }

    public int getRetryDelaySeconds() {
      return retryDelaySeconds;
    }

    public void setRetryDelaySeconds(int retryDelaySeconds) {
      this.retryDelaySeconds = retryDelaySeconds;
    }
  }

  public void validate() {
    requirePositive("amoro.process.simulation.worker-threads", simulation.workerThreads);
    requirePositive("amoro.process.simulation.queue-capacity", simulation.queueCapacity);
    requirePositive("amoro.process.engine.command-timeout-ms", engine.commandTimeoutMs);
    requirePositive("amoro.process.reconcile.poll-interval-ms", reconcile.pollIntervalMs);
    requirePositive(
        "amoro.process.reconcile.submission-unresolved-interval-ms",
        reconcile.submissionUnresolvedIntervalMs);
    requirePositive(
        "amoro.process.reconcile.cancel-retry-interval-ms", reconcile.cancelRetryIntervalMs);
    requirePositive(
        "amoro.process.reconcile.command-in-flight-delay-ms", reconcile.commandInFlightDelayMs);
    requirePositive(
        "amoro.process.reconcile.execution-unresolved-reminder-interval-ms",
        reconcile.executionUnresolvedReminderIntervalMs);
    requirePositive("amoro.process.result-persistence.max-pending", resultPersistence.maxPending);
    requireBatch("amoro.process.result-persistence.batch-size", resultPersistence.batchSize);
    requirePositive(
        "amoro.process.result-persistence.retry-interval-ms", resultPersistence.retryIntervalMs);
    requireBatch("amoro.process.rescheduler.batch-size", rescheduler.batchSize);
    requirePositive("amoro.process.rescheduler.max-runtime-ms", rescheduler.maxRuntimeMs);
    requirePositive("amoro.process.rescheduler.interval-ms", rescheduler.intervalMs);
    requireBatch("amoro.process.execution-reaper.batch-size", executionReaper.batchSize);
    requirePositive("amoro.process.execution-reaper.interval-ms", executionReaper.intervalMs);
    if (ttl.retentionDays < 7) {
      throw new IllegalArgumentException(
          "amoro.process.ttl.retention-days must be >= 7, got " + ttl.retentionDays);
    }
    requireBatch("amoro.process.ttl.batch-size", ttl.batchSize);
    requirePositive("amoro.process.ttl.interval-ms", ttl.intervalMs);
    if (local.terminalResultRetentionDays < 1) {
      throw new IllegalArgumentException(
          "amoro.process.local.terminal-result-retention-days must be >= 1, got "
              + local.terminalResultRetentionDays);
    }
    requirePositive("amoro.process.trigger.interval-ms", trigger.intervalMs);
    requireBatch("amoro.process.trigger.batch-size", trigger.batchSize);
    requireRange("amoro.process.creation.max-retries", creation.maxRetries, 0, 3);
    requireRange(
        "amoro.process.creation.max-submission-retries", creation.maxSubmissionRetries, 0, 2);
    requireRange(
        "amoro.process.creation.retry-delay-seconds", creation.retryDelaySeconds, 1, 86_400);
  }

  public Simulation getSimulation() {
    return simulation;
  }

  public Engine getEngine() {
    return engine;
  }

  public Reconcile getReconcile() {
    return reconcile;
  }

  public ResultPersistence getResultPersistence() {
    return resultPersistence;
  }

  public Rescheduler getRescheduler() {
    return rescheduler;
  }

  public ExecutionReaper getExecutionReaper() {
    return executionReaper;
  }

  public Ttl getTtl() {
    return ttl;
  }

  public Local getLocal() {
    return local;
  }

  public Trigger getTrigger() {
    return trigger;
  }

  public Creation getCreation() {
    return creation;
  }

  private static void requireBatch(String key, int value) {
    requireRange(key, value, 1, 1_000);
  }

  private static void requireRange(String key, long value, long min, long max) {
    if (value < min || value > max) {
      throw new IllegalArgumentException(
          key + " must be in [" + min + ", " + max + "], got " + value);
    }
  }

  private static void requirePositive(String key, long value) {
    if (value <= 0) {
      throw new IllegalArgumentException(key + " must be > 0, got " + value);
    }
  }
}
