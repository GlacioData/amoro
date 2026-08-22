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

package org.apache.amoro.control;

import java.time.Duration;

/**
 * Registration surface for {@link Controller}s. Semantics (framework spec §3/§4):
 *
 * <ul>
 *   <li>single-flight per {@link ControllerKey}: at most one wrapper in flight per key at any time;
 *       repeated schedule calls for the same key merge to the earliest deadline and never delay an
 *       already-registered earlier deadline;
 *   <li>{@link #unschedule(ControllerKey)} terminates both queued and in-flight entries of that key
 *       and is idempotent;
 *   <li>{@link #postStart()} is retained for interface shape compatibility only; workers are
 *       started by the {@code DefaultScheduler} lifecycle, replay entry lives in {@code
 *       PersistenceService#postStart()}.
 * </ul>
 */
public interface Scheduler {

  /**
   * Registers the controller for immediate-period periodic invocation, merging with any existing
   * registration of the same key.
   *
   * @throws java.util.concurrent.RejectedExecutionException if the scheduler has been shut down
   */
  void schedule(Controller controller);

  /**
   * Registers the controller with an explicit initial delay, merging to the earliest deadline with
   * any existing registration of the same key.
   *
   * @throws java.util.concurrent.RejectedExecutionException if the scheduler has been shut down
   */
  void schedule(Controller controller, Duration nextDelay);

  /**
   * Terminates the queued or in-flight entry for the key (a different controller instance
   * registered under the same key is affected as well). Idempotent, including during and after
   * shutdown. An old generation worker must never cancel or requeue an entry recreated under the
   * same key after this call.
   */
  void unschedule(ControllerKey key);

  /** No-op retained for interface shape; see the interface javadoc. */
  void postStart();

  /**
   * Graceful shutdown: stop accepting {@link #schedule(Controller)} (subsequent calls throw), stop
   * picking up new entries, wait at most {@code timeout} for in-flight invocations, then release.
   * Idempotent; {@link #unschedule(ControllerKey)} stays idempotent afterwards.
   */
  void shutdown(Duration timeout);
}
