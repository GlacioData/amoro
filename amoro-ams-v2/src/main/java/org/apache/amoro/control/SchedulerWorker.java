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

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * The scheduler loop (framework spec §4.1). Workers never call a bare {@code take()} or a real-time
 * timed poll: the {@link java.util.concurrent.DelayQueue} only orders deadlines and is drained with
 * a non-blocking poll once the injected {@link Clock} says the head is due; until then the worker
 * parks on the signal-version {@link SchedulerWaitStrategy}, which is the only thing that can wake
 * it (new/shortened deadlines, unschedule, shutdown) — advancing time alone never does.
 */
final class SchedulerWorker implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerWorker.class);

  private final DelayQueue<ScheduledController> queue;
  private final ConcurrentHashMap<ControllerKey, ScheduledEntry> registry;
  private final Clock clock;
  private final SchedulerWaitStrategy waitStrategy;
  private final BooleanSupplier running;

  SchedulerWorker(
      DelayQueue<ScheduledController> queue,
      ConcurrentHashMap<ControllerKey, ScheduledEntry> registry,
      Clock clock,
      SchedulerWaitStrategy waitStrategy,
      BooleanSupplier running) {
    this.queue = queue;
    this.registry = registry;
    this.clock = clock;
    this.waitStrategy = waitStrategy;
    this.running = running;
  }

  @Override
  public void run() {
    while (running.getAsBoolean()) {
      try {
        long observedVersion = waitStrategy.signalVersion();
        ScheduledController head = queue.peek();
        if (head == null) {
          waitStrategy.awaitChange(observedVersion);
          continue;
        }
        long remainingMillis = head.getDelay(TimeUnit.MILLISECONDS);
        if (remainingMillis > 0L) {
          waitStrategy.awaitChange(observedVersion, Duration.ofMillis(remainingMillis));
          continue;
        }
        ScheduledController polled = queue.poll(); // non-blocking; may lose to another worker
        if (polled == null) {
          continue;
        }
        runOne(polled);
      } catch (InterruptedException e) {
        // an interrupt means stop: restore the flag so callers/finally blocks up the stack see
        // it, and exit the loop. Swallowing it would spin: a lingering interrupt flag makes
        // every subsequent park throw immediately, burning a core until the next deadline.
        Thread.currentThread().interrupt();
        return;
      } catch (Throwable throwable) {
        LOG.warn("Scheduler worker hit an unexpected error; continuing the loop.", throwable);
      }
    }
  }

  private void runOne(ScheduledController polled) {
    ScheduledEntry entry = registry.get(polled.key());
    if (entry == null || entry.wrapper != polled) {
      // stale wrapper of a removed/recreated generation: drop it, never requeue
      return;
    }
    synchronized (entry) {
      if (entry.state == ScheduledEntry.State.TERMINATED) {
        return;
      }
      // defensive identity re-check: the wrapper field is final, so this can only fail if the
      // registry swapped entries between the unlocked get above and this lock — in which case
      // this polled wrapper belongs to a dead generation and must be dropped
      if (entry.wrapper != polled) {
        return;
      }
      // QUEUED is the normal path; CLAIMED here means an updater saw our queue.remove fail
      entry.state = ScheduledEntry.State.CLAIMED;
    }

    ScheduledController.InvocationResult result = polled.invokeOnce();

    synchronized (entry) {
      if (!running.getAsBoolean()) {
        // shutting down: the invocation result is dropped instead of requeued — the level-
        // triggered restart replay reschedules from the durable store
        LOG.debug("Dropping completed invocation of {} during shutdown.", entry.key);
        entry.state = ScheduledEntry.State.TERMINATED;
        registry.remove(entry.key, entry);
        return;
      }
      if (entry.state == ScheduledEntry.State.TERMINATED) {
        // unschedule raced the invocation: this generation dies, identity-aware remove protects
        // any entry recreated under the same key
        registry.remove(entry.key, entry);
        return;
      }
      if (result == ScheduledController.InvocationResult.TERMINAL) {
        entry.state = ScheduledEntry.State.TERMINATED;
        registry.remove(entry.key, entry);
        return;
      }
      long next;
      if (result == ScheduledController.InvocationResult.SUCCESS) {
        polled.resetBackoff();
        next = clock.currentTimeMillisPlus(polled.schedulingDelayMillis());
      } else {
        next = clock.currentTimeMillisPlus(polled.takeBackoffDelayMillis());
      }
      Long requested = entry.rescheduleRequestedMillis;
      if (requested != null && requested < next) {
        next = requested;
      }
      entry.rescheduleRequestedMillis = null;
      entry.state = ScheduledEntry.State.QUEUED;
      polled.updateNextDesiredTimeMillis(next);
      queue.offer(polled);
      waitStrategy.signal(); // a new element may now be the earliest head
    }
  }
}
