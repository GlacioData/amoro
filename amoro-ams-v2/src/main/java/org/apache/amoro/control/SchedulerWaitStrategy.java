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
 * Versioned wakeup port consumed by scheduler workers (framework spec §4.1 / fidelity ledger #10).
 * Workers peek the queue head, compute the remaining wait against the injected {@link Clock}, then
 * park through this strategy instead of a bare {@code DelayQueue.take()}: a bare take cannot be
 * woken by virtual-clock tests and a naive timed poll can lose a signal raised between peek and
 * await.
 *
 * <p>Contract: {@link #awaitChange(long)} must return immediately — without entering a wait — when
 * the observed version is already stale; implementations must re-check the version under their lock
 * before parking.
 */
public interface SchedulerWaitStrategy {

  /** Current version; callers capture it before peeking at deadlines. */
  long signalVersion();

  /**
   * Parks until the version differs from {@code observedVersion} (or the thread is interrupted).
   * Must not park at all when the version has already moved past {@code observedVersion}.
   */
  void awaitChange(long observedVersion) throws InterruptedException;

  /**
   * Parks until the version differs from {@code observedVersion} or {@code maximumWait} elapses,
   * whichever happens first. Must not park when the version has already moved.
   */
  void awaitChange(long observedVersion, Duration maximumWait) throws InterruptedException;

  /** Wakes all current and future parkers observing any older version. */
  void signal();
}
