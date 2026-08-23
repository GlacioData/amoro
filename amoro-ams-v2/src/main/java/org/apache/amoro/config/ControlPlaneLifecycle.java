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
 * scheduler lifecycle; shutdown first drains the trigger/rescheduler/reaper/TTL maintenance loops,
 * then stops the scheduler, engines, listener dispatcher and every domain's mutation lane. Each
 * stage consumes the same {@code lifecycle.shutdown-timeout-ms} budget. Stages are plain {@link
 * Runnable}s so the ordering is unit-testable without the real components.
 */
public final class ControlPlaneLifecycle implements SmartLifecycle {

  private final Runnable startActions;
  private final Runnable startMaintenance;
  private final Runnable maintenanceStop;
  private final Runnable schedulerStop;
  private final Runnable engineStop;
  private final Runnable dispatcherStop;
  private final Runnable lanesStop;

  private volatile boolean running;

  public ControlPlaneLifecycle(
      Runnable schedulerStop, Runnable dispatcherStop, Runnable lanesStop) {
    this(() -> {}, () -> {}, () -> {}, schedulerStop, () -> {}, dispatcherStop, lanesStop);
  }

  public ControlPlaneLifecycle(
      Runnable startActions, Runnable schedulerStop, Runnable dispatcherStop, Runnable lanesStop) {
    this(startActions, () -> {}, () -> {}, schedulerStop, () -> {}, dispatcherStop, lanesStop);
  }

  public ControlPlaneLifecycle(
      Runnable startActions,
      Runnable startMaintenance,
      Runnable maintenanceStop,
      Runnable schedulerStop,
      Runnable engineStop,
      Runnable dispatcherStop,
      Runnable lanesStop) {
    this.startActions = Objects.requireNonNull(startActions, "startActions");
    this.startMaintenance = Objects.requireNonNull(startMaintenance, "startMaintenance");
    this.maintenanceStop = Objects.requireNonNull(maintenanceStop, "maintenanceStop");
    this.schedulerStop = Objects.requireNonNull(schedulerStop, "schedulerStop");
    this.engineStop = Objects.requireNonNull(engineStop, "engineStop");
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
    try {
      startActions.run();
      startMaintenance.run();
      running = true;
    } catch (RuntimeException | Error startFailure) {
      Throwable rollbackFailure = stopAllStages();
      if (rollbackFailure != null) {
        startFailure.addSuppressed(rollbackFailure);
      }
      throw startFailure;
    }
  }

  @Override
  public void stop() {
    Throwable failure = stopAllStages();
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
  }

  private Throwable stopAllStages() {
    Throwable failure = null;
    try {
      maintenanceStop.run();
    } catch (RuntimeException | Error stageFailure) {
      failure = stageFailure;
    }
    failure = runStopStage(schedulerStop, failure);
    failure = runStopStage(engineStop, failure);
    failure = runStopStage(dispatcherStop, failure);
    failure = runStopStage(lanesStop, failure);
    running = false;
    return failure;
  }

  private static Throwable runStopStage(Runnable stage, Throwable previous) {
    try {
      stage.run();
    } catch (RuntimeException | Error stageFailure) {
      if (previous == null) {
        return stageFailure;
      }
      previous.addSuppressed(stageFailure);
    }
    return previous;
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
