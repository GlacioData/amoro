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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * The queue-facing wrapper of one scheduling registration. Exactly one wrapper exists per {@link
 * ScheduledEntry} generation; it is removed from the queue, updated and reinserted as the same
 * object so queue cardinality never grows with repeated schedule calls.
 *
 * <p>The mutable fields are explicitly volatile: the framework refuses to depend on the implicit
 * happens-before of queue locks (fidelity ledger #4).
 */
final class ScheduledController implements Delayed {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduledController.class);

  enum InvocationResult {
    SUCCESS,
    TERMINAL,
    RETRY
  }

  private final ControllerKey key;
  private volatile Controller controller;
  private final Clock clock;
  private final RandomSupplier random;
  private final BackoffPolicy backoffPolicy;
  private final long schedulingDelayMillis;

  private volatile long nextDesiredTimeMillis;
  private volatile int backOffAttempts;

  ScheduledController(
      Controller controller,
      long initialDelayMillis,
      long schedulingDelayMillis,
      Clock clock,
      RandomSupplier random,
      BackoffPolicy backoffPolicy) {
    this.key = controller.key();
    this.controller = controller;
    this.schedulingDelayMillis = schedulingDelayMillis;
    this.clock = clock;
    this.random = random;
    this.backoffPolicy = backoffPolicy;
    this.nextDesiredTimeMillis = clock.currentTimeMillisPlus(initialDelayMillis);
    this.backOffAttempts = 0;
  }

  ControllerKey key() {
    return key;
  }

  /**
   * The single-flight identity is the {@link ControllerKey}, not the instance: schedule() with a
   * fresh Controller under an existing key takes over the next invocation. Only called inside the
   * entry monitor; volatile so an already-dispatched invokeOnce keeps running the old instance
   * undisturbed.
   */
  void replaceController(Controller replacement) {
    this.controller = replacement;
  }

  long schedulingDelayMillis() {
    return schedulingDelayMillis;
  }

  /** Executes the three-branch invoke protocol (framework spec §4.2). */
  InvocationResult invokeOnce() {
    try {
      controller.invoke();
      return InvocationResult.SUCCESS;
    } catch (TerminalState terminal) {
      LOG.info("Controller {} reached a terminal state and will not be rescheduled.", key);
      return InvocationResult.TERMINAL;
    } catch (Throwable throwable) {
      // the reference implementation drops the controller here; we keep retrying with backoff
      // (fidelity ledger #2)
      LOG.warn(
          "Unexpected exception from controller {}; it will be retried with backoff.",
          key,
          throwable);
      return InvocationResult.RETRY;
    }
  }

  /**
   * Takes the backoff value for the current attempt first, then advances the counter (capped at the
   * last sequence entry). Fixing the reference implementation's pre-increment is what keeps the
   * leading 3s in the emitted sequence.
   */
  long takeBackoffDelayMillis() {
    long delay = backoffPolicy.nextBackoffDelayMillis(backOffAttempts, random);
    int cap = BackoffPolicy.BACKOFF_MILLIS.length - 1;
    backOffAttempts = Math.min(backOffAttempts + 1, cap);
    return delay;
  }

  void resetBackoff() {
    backOffAttempts = 0;
  }

  long nextDesiredTimeMillis() {
    return nextDesiredTimeMillis;
  }

  void updateNextDesiredTimeMillis(long whenMillis) {
    this.nextDesiredTimeMillis = whenMillis;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    long remainingMillis = nextDesiredTimeMillis - clock.currentTimeMillisPlus(0L);
    return unit.convert(remainingMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed that) {
    ScheduledController other = (ScheduledController) that;
    return Long.compare(this.nextDesiredTimeMillis, other.nextDesiredTimeMillis);
  }
}
