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

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "amoro.control")
@Getter
public class AmoroControlProperties {

  private final Scheduler scheduler = new Scheduler();
  private final Storage storage = new Storage();
  private final Actor actor = new Actor();
  private final Listener listener = new Listener();
  private final Repository repository = new Repository();
  private final Lifecycle lifecycle = new Lifecycle();

  @Getter
  @Setter
  public static class Scheduler {
    private int workers = 10;
    private long delayMs = 3_000L;
  }

  @Getter
  @Setter
  public static class Storage {
    private int maxResourceBytes = 65_536;
  }

  @Getter
  @Setter
  public static class Actor {
    private int queueCapacity = 1024;
  }

  @Getter
  @Setter
  public static class Listener {
    private int workers = 4;
    private int queueCapacity = 1024;
    private int maxRetries = 3;
    private long retryDelayMs = 1_000L;
  }

  @Getter
  @Setter
  public static class Repository {
    private long timeoutMs = 10_000L;
  }

  @Getter
  @Setter
  public static class Lifecycle {
    private long shutdownTimeoutMs = 10_000L;
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
}
