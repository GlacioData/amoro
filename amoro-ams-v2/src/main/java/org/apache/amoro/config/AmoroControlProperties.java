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

/**
 * {@code amoro.control.*} configuration keys (framework spec §7). Defaults mirror the spec table;
 * illegal values fail Spring context startup instead of degrading silently.
 */
@ConfigurationProperties(prefix = "amoro.control")
public class AmoroControlProperties {

  private final Scheduler scheduler = new Scheduler();
  private final Storage storage = new Storage();
  private final Actor actor = new Actor();
  private final Listener listener = new Listener();
  private final Repository repository = new Repository();
  private final Lifecycle lifecycle = new Lifecycle();

  public static class Scheduler {
    private int workers = 10;
    private long delayMs = 3_000L;

    public int getWorkers() {
      return workers;
    }

    public void setWorkers(int workers) {
      this.workers = workers;
    }

    public long getDelayMs() {
      return delayMs;
    }

    public void setDelayMs(long delayMs) {
      this.delayMs = delayMs;
    }
  }

  public static class Storage {
    private int maxResourceBytes = 65_536;

    public int getMaxResourceBytes() {
      return maxResourceBytes;
    }

    public void setMaxResourceBytes(int maxResourceBytes) {
      this.maxResourceBytes = maxResourceBytes;
    }
  }

  public static class Actor {
    private int queueCapacity = 1024;

    public int getQueueCapacity() {
      return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
      this.queueCapacity = queueCapacity;
    }
  }

  public static class Listener {
    private int workers = 4;
    private int queueCapacity = 1024;
    private int maxRetries = 3;
    private long retryDelayMs = 1_000L;

    public int getWorkers() {
      return workers;
    }

    public void setWorkers(int workers) {
      this.workers = workers;
    }

    public int getQueueCapacity() {
      return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
      this.queueCapacity = queueCapacity;
    }

    public int getMaxRetries() {
      return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    public long getRetryDelayMs() {
      return retryDelayMs;
    }

    public void setRetryDelayMs(long retryDelayMs) {
      this.retryDelayMs = retryDelayMs;
    }
  }

  public static class Repository {
    private long timeoutMs = 10_000L;

    public long getTimeoutMs() {
      return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
      this.timeoutMs = timeoutMs;
    }
  }

  public static class Lifecycle {
    private long shutdownTimeoutMs = 10_000L;

    public long getShutdownTimeoutMs() {
      return shutdownTimeoutMs;
    }

    public void setShutdownTimeoutMs(long shutdownTimeoutMs) {
      this.shutdownTimeoutMs = shutdownTimeoutMs;
    }
  }

  /** Fails fast on illegal values (framework spec §7 validation column). */
  public void validate() {
    requirePositive("amoro.control.scheduler.workers", scheduler.workers);
    requirePositive("amoro.control.scheduler.delay-ms", scheduler.delayMs);
    requirePositive("amoro.control.storage.max-resource-bytes", storage.maxResourceBytes);
    requirePositive("amoro.control.actor.queue-capacity", actor.queueCapacity);
    requirePositive("amoro.control.listener.workers", listener.workers);
    requirePositive("amoro.control.listener.queue-capacity", listener.queueCapacity);
    requirePositive("amoro.control.listener.retry-delay-ms", listener.retryDelayMs);
    if (listener.maxRetries < 0) {
      throw new IllegalArgumentException(
          "amoro.control.listener.max-retries must be >= 0, got " + listener.maxRetries);
    }
    requirePositive("amoro.control.repository.timeout-ms", repository.timeoutMs);
    requirePositive("amoro.control.lifecycle.shutdown-timeout-ms", lifecycle.shutdownTimeoutMs);
  }

  private static void requirePositive(String key, long value) {
    if (value <= 0) {
      throw new IllegalArgumentException(key + " must be > 0, got " + value);
    }
  }

  public Scheduler getScheduler() {
    return scheduler;
  }

  public Storage getStorage() {
    return storage;
  }

  public Actor getActor() {
    return actor;
  }

  public Listener getListener() {
    return listener;
  }

  public Repository getRepository() {
    return repository;
  }

  public Lifecycle getLifecycle() {
    return lifecycle;
  }
}
