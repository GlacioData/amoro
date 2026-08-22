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

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.InMemoryPersistence;
import org.apache.amoro.persistence.ListenerDispatcher;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;

/**
 * Startup/shutdown ordering of the control plane (framework spec §7): workers start with the
 * scheduler lifecycle; shutdown stops the scheduler first (no new reconciliations, in-flight
 * finish), then the listener dispatcher (reject handoffs, bounded drain), then every domain's
 * mutation lane (bounded drain). Each stage consumes the same {@code lifecycle.shutdown-timeout-ms}
 * budget. Stages are plain {@link Runnable}s so the ordering is unit-testable without the real
 * components.
 */
public final class ControlPlaneLifecycle implements SmartLifecycle {

  private final Runnable startActions;
  private final Runnable schedulerStop;
  private final Runnable dispatcherStop;
  private final Runnable lanesStop;

  private volatile boolean running;

  public ControlPlaneLifecycle(
      Runnable schedulerStop, Runnable dispatcherStop, Runnable lanesStop) {
    this(() -> {}, schedulerStop, dispatcherStop, lanesStop);
  }

  public ControlPlaneLifecycle(
      Runnable startActions, Runnable schedulerStop, Runnable dispatcherStop, Runnable lanesStop) {
    this.startActions = Objects.requireNonNull(startActions, "startActions");
    this.schedulerStop = Objects.requireNonNull(schedulerStop, "schedulerStop");
    this.dispatcherStop = Objects.requireNonNull(dispatcherStop, "dispatcherStop");
    this.lanesStop = Objects.requireNonNull(lanesStop, "lanesStop");
  }

  public static ControlPlaneLifecycle from(
      DefaultScheduler scheduler,
      ListenerDispatcher<?> dispatcher,
      Collection<InMemoryPersistence<?>> persistences,
      long shutdownTimeoutMillis) {
    Duration timeout = Duration.ofMillis(shutdownTimeoutMillis);
    return new ControlPlaneLifecycle(
        scheduler::start,
        () -> scheduler.shutdown(timeout),
        () -> dispatcher.shutdown(timeout),
        () -> {
          for (InMemoryPersistence<?> persistence : persistences) {
            persistence.shutdown(timeout);
          }
        });
  }

  @Override
  public void start() {
    // workers boot here: by now every domain has registered its replay via postStart, so the
    // controllers enqueued by listeners find workers waiting
    startActions.run();
    running = true;
  }

  @Override
  public void stop() {
    schedulerStop.run();
    dispatcherStop.run();
    lanesStop.run();
    running = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    // stop before the web server and other late phases
    return Integer.MAX_VALUE - 100;
  }
}
