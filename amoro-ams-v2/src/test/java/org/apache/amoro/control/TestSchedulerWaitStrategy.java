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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TestSchedulerWaitStrategy {

  @Test
  @Timeout(10)
  public void signalIncrementsVersion() {
    SchedulerWaitStrategy strategy = new ConditionWaitStrategy();
    long initial = strategy.signalVersion();
    strategy.signal();
    assertEquals(initial + 1L, strategy.signalVersion());
  }

  @Test
  @Timeout(10)
  public void awaitChangeReturnsImmediatelyWhenVersionAlreadyChanged() throws InterruptedException {
    // If the version moved between the caller's peek and its await, waiting would lose the
    // wakeup; the strategy must re-check the version under the lock and return immediately
    // (Spec §4.1 / fidelity ledger #10).
    SchedulerWaitStrategy strategy = new ConditionWaitStrategy();
    long observed = strategy.signalVersion();
    strategy.signal();
    long start = System.nanoTime();
    strategy.awaitChange(observed); // would block forever if implemented naively
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(elapsedMillis < 2000L, "awaitChange must not wait on a stale observed version");
  }

  @Test
  @Timeout(10)
  public void unboundedAwaitBlocksUntilSignalled() throws Exception {
    SchedulerWaitStrategy strategy = new ConditionWaitStrategy();
    long observed = strategy.signalVersion();
    CompletableFuture<Void> released = new CompletableFuture<>();
    Thread waiter =
        new Thread(
            () -> {
              try {
                strategy.awaitChange(observed);
                released.complete(null);
              } catch (InterruptedException e) {
                released.completeExceptionally(e);
              }
            });
    waiter.setDaemon(true);
    waiter.start();

    Thread.sleep(100L);
    assertTrue(!released.isDone(), "no signal yet, the waiter must still be parked");
    strategy.signal();
    released.get(5L, TimeUnit.SECONDS);
  }

  @Test
  @Timeout(10)
  public void boundedAwaitReturnsAfterDeadlineWithoutSignal() throws InterruptedException {
    SchedulerWaitStrategy strategy = new ConditionWaitStrategy();
    long observed = strategy.signalVersion();
    long start = System.nanoTime();
    strategy.awaitChange(observed, Duration.ofMillis(80L));
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(elapsedMillis >= 75L, "bounded await honours the deadline");
    assertEquals(observed, strategy.signalVersion(), "no signal happened, version unchanged");
  }

  @Test
  @Timeout(10)
  public void boundedAwaitReturnsImmediatelyOnStaleVersion() throws InterruptedException {
    SchedulerWaitStrategy strategy = new ConditionWaitStrategy();
    long observed = strategy.signalVersion();
    strategy.signal();
    long start = System.nanoTime();
    strategy.awaitChange(observed, Duration.ofSeconds(30L));
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(
        elapsedMillis < 2000L, "bounded await must not consume its deadline on a stale version");
  }

  @Test
  @Timeout(10)
  public void boundedAwaitParkedOnCurrentVersionIsAwokenBySignal() throws Exception {
    // the bounded overload is the scheduler worker's main path; a regression that parks it for
    // the full deadline even after a signal would surface here, not in the unbounded tests
    SchedulerWaitStrategy strategy = new ConditionWaitStrategy();
    long observed = strategy.signalVersion();
    CompletableFuture<Void> released = new CompletableFuture<>();
    Thread waiter =
        new Thread(
            () -> {
              try {
                strategy.awaitChange(observed, Duration.ofSeconds(30L));
                released.complete(null);
              } catch (InterruptedException e) {
                released.completeExceptionally(e);
              }
            });
    waiter.setDaemon(true);
    waiter.start();

    Thread.sleep(100L);
    assertTrue(!released.isDone(), "no signal yet, the bounded waiter must still be parked");
    long start = System.nanoTime();
    strategy.signal();
    released.get(5L, TimeUnit.SECONDS);
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(
        elapsedMillis < 2000L,
        "signal must preempt the remaining bounded wait, took " + elapsedMillis + "ms");
  }

  @Test
  @Timeout(10)
  public void interruptingAwaitPropagatesInterruptedException() throws Exception {
    SchedulerWaitStrategy strategy = new ConditionWaitStrategy();
    long observed = strategy.signalVersion();
    CompletableFuture<Throwable> caught = new CompletableFuture<>();
    Thread waiter =
        new Thread(
            () -> {
              try {
                strategy.awaitChange(observed);
                caught.complete(null);
              } catch (Throwable t) {
                caught.complete(t);
              }
            });
    waiter.setDaemon(true);
    waiter.start();
    Thread.sleep(100L);
    waiter.interrupt();
    Throwable thrown = caught.get(5L, TimeUnit.SECONDS);
    assertTrue(thrown instanceof InterruptedException);
  }
}
