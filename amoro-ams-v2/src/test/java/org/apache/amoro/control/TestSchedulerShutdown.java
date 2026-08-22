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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Timeout(60)
public class TestSchedulerShutdown {

  private static final long PERIOD_MILLIS = 1000L;

  private TestDefaultScheduler.FakeClock clock;
  private TestDefaultScheduler.FakeWaitStrategy waitStrategy;
  private DefaultScheduler scheduler;
  private ExecutorService helperPool;

  @BeforeEach
  public void setUp() {
    clock = new TestDefaultScheduler.FakeClock();
    waitStrategy = new TestDefaultScheduler.FakeWaitStrategy(clock);
    scheduler =
        new DefaultScheduler(
            2,
            PERIOD_MILLIS,
            clock,
            new TestDefaultScheduler.ZeroRandom(),
            new BackoffPolicy(),
            waitStrategy);
    scheduler.start();
    helperPool = Executors.newCachedThreadPool();
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    helperPool.shutdownNow();
  }

  /** Blocks until released; keeps sleeping even when interrupted (outlives shutdown timeouts). */
  static final class UninterruptibleBlock implements Consumer<TestDefaultScheduler.TestController> {
    final CountDownLatch entered = new CountDownLatch(1);
    final AtomicBoolean released = new AtomicBoolean(false);
    final CountDownLatch exited = new CountDownLatch(1);

    @Override
    public void accept(TestDefaultScheduler.TestController c) {
      entered.countDown();
      while (!released.get()) {
        try {
          Thread.sleep(10L);
        } catch (InterruptedException e) {
          // deliberately swallowed: models an invoke that ignores shutdown interrupts
        }
      }
      exited.countDown();
    }

    void release() {
      released.set(true);
    }
  }

  private TestDefaultScheduler.TestController blocked(
      String id, UninterruptibleBlock block, AtomicInteger maxInFlight) {
    return new TestDefaultScheduler.TestController("shutdown", id, maxInFlight, block);
  }

  @Test
  public void shutdownWaitsForInFlightInvocationToComplete() throws Exception {
    UninterruptibleBlock block = new UninterruptibleBlock();
    AtomicInteger maxInFlight = new AtomicInteger();
    TestDefaultScheduler.TestController controller = blocked("completes", block, maxInFlight);

    scheduler.schedule(controller);
    assertTrue(block.entered.await(5, TimeUnit.SECONDS), "invocation is in flight");

    Future<?> shutdown = helperPool.submit(() -> scheduler.shutdown(Duration.ofSeconds(5)));
    // the in-flight invoke is still running: shutdown must not have returned yet
    Thread.sleep(200L);
    assertFalse(shutdown.isDone(), "shutdown must wait for the in-flight invocation");

    block.release();
    shutdown.get(10, TimeUnit.SECONDS); // returns only after the invoke completed
    assertTrue(block.exited.await(1, TimeUnit.SECONDS));
    assertEquals(1, controller.attempts.get());
  }

  @Test
  public void shutdownTimeoutReleasesWithoutTheInvocation() throws Exception {
    UninterruptibleBlock block = new UninterruptibleBlock();
    AtomicInteger maxInFlight = new AtomicInteger();
    TestDefaultScheduler.TestController controller = blocked("hangs", block, maxInFlight);

    scheduler.schedule(controller);
    assertTrue(block.entered.await(5, TimeUnit.SECONDS));

    long start = System.nanoTime();
    scheduler.shutdown(Duration.ofMillis(200)); // invoke never finishes on its own
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertTrue(elapsedMillis < 3000L, "shutdown must give up after the timeout");
    assertEquals(1L, block.exited.getCount(), "the invocation is still running when we return");

    block.release(); // let the worker thread die so the test can clean up
    assertTrue(block.exited.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void repeatedShutdownIsIdempotent() {
    scheduler.shutdown(Duration.ofSeconds(5));
    scheduler.shutdown(Duration.ofSeconds(5));
    scheduler.shutdown(Duration.ZERO);
  }

  @Test
  public void zeroTimeoutFirstCallReleasesImmediatelyWithoutResidue() {
    AtomicInteger maxInFlight = new AtomicInteger();
    TestDefaultScheduler.TestController queued =
        new TestDefaultScheduler.TestController("shutdown", "zero-timeout", maxInFlight, c -> {});
    scheduler.schedule(queued, Duration.ofSeconds(60));

    long start = System.nanoTime();
    scheduler.shutdown(Duration.ZERO); // immediate release on the first call
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertTrue(elapsedMillis < 3000L, "zero timeout must not wait for workers");
    assertEquals(0, scheduler.registrySize());
    assertEquals(0, scheduler.queuedWrappers());
  }

  @Test
  public void startAfterShutdownIsIgnoredAndShutdownWithoutStartIsSafe() {
    DefaultScheduler fresh =
        new DefaultScheduler(
            1,
            PERIOD_MILLIS,
            clock,
            new TestDefaultScheduler.ZeroRandom(),
            new BackoffPolicy(),
            waitStrategy);
    fresh.shutdown(Duration.ofSeconds(1)); // never started: pool == null path
    fresh.start(); // ignored, must not spawn workers
    fresh.shutdown(Duration.ofSeconds(1)); // still idempotent

    assertEquals(0, fresh.registrySize());
    assertEquals(0, fresh.queuedWrappers());
  }

  @Test
  public void concurrentUnscheduleAndShutdownNeverThrowsOrRevives() throws Exception {
    AtomicInteger maxInFlight = new AtomicInteger();
    TestDefaultScheduler.TestController controller =
        new TestDefaultScheduler.TestController("shutdown", "hammer", maxInFlight, c -> {});
    scheduler.schedule(controller, Duration.ofSeconds(60));

    Future<?> shutdown = helperPool.submit(() -> scheduler.shutdown(Duration.ofSeconds(5)));
    for (int i = 0; i < 3; i++) {
      helperPool.submit(
          () -> {
            for (int j = 0; j < 200; j++) {
              scheduler.unschedule(controller.key());
            }
            return null;
          });
    }
    shutdown.get(10, TimeUnit.SECONDS); // neither side may throw
    Thread.sleep(200L);

    assertEquals(0, scheduler.registrySize(), "no entry survives the shutdown drain");
    assertEquals(0, scheduler.queuedWrappers());
    assertTrue(maxInFlight.get() <= 1);
  }

  @Test
  public void unscheduleDuringAndAfterShutdownIsIdempotentAndNeverRevives() {
    AtomicInteger maxInFlight = new AtomicInteger();
    TestDefaultScheduler.TestController controller =
        new TestDefaultScheduler.TestController("shutdown", "revive", maxInFlight, c -> {});
    scheduler.schedule(controller, Duration.ofSeconds(60));

    scheduler.unschedule(controller.key()); // during pre-shutdown life
    scheduler.shutdown(Duration.ofSeconds(5));
    scheduler.unschedule(controller.key()); // after shutdown: must not throw
    scheduler.unschedule(controller.key());

    assertEquals(0, controller.attempts.get(), "nothing may run after unschedule + shutdown");
    assertEquals(0, scheduler.registrySize());
  }

  @Test
  public void shutdownLeavesNoRegistryOrQueueResidue() throws Exception {
    AtomicInteger maxInFlight = new AtomicInteger();
    for (int i = 0; i < 3; i++) {
      TestDefaultScheduler.TestController queued =
          new TestDefaultScheduler.TestController("shutdown", "queued-" + i, maxInFlight, c -> {});
      scheduler.schedule(queued, Duration.ofSeconds(60));
    }
    assertEquals(3, scheduler.registrySize());

    scheduler.shutdown(Duration.ofSeconds(5));

    assertEquals(0, scheduler.registrySize(), "registry must be drained on shutdown");
    assertEquals(0, scheduler.queuedWrappers(), "queue must be drained on shutdown");
  }

  @Test
  public void postInvocationAfterShutdownDoesNotRequeue() throws Exception {
    UninterruptibleBlock block = new UninterruptibleBlock();
    AtomicInteger maxInFlight = new AtomicInteger();
    TestDefaultScheduler.TestController controller = blocked("norequeue", block, maxInFlight);

    scheduler.schedule(controller);
    assertTrue(block.entered.await(5, TimeUnit.SECONDS));

    Future<?> shutdown = helperPool.submit(() -> scheduler.shutdown(Duration.ofSeconds(5)));
    Thread.sleep(100L); // shutdown flag is set while the invocation is still running
    block.release();
    shutdown.get(10, TimeUnit.SECONDS);

    assertEquals(1, controller.attempts.get());
    assertEquals(0, scheduler.registrySize(), "the completed entry must not be requeued");
    assertEquals(0, scheduler.queuedWrappers());
    waitStrategy.advanceClockAndSignal(120_000L);
    Thread.sleep(200L);
    assertEquals(1, controller.attempts.get(), "no further invocations after shutdown");
  }

  @Test
  public void scheduleIsRejectedAfterShutdownEvenWhileWorkersAreDraining() throws Exception {
    UninterruptibleBlock block = new UninterruptibleBlock();
    AtomicInteger maxInFlight = new AtomicInteger();
    TestDefaultScheduler.TestController controller = blocked("draining", block, maxInFlight);

    scheduler.schedule(controller);
    assertTrue(block.entered.await(5, TimeUnit.SECONDS));

    Future<?> shutdown = helperPool.submit(() -> scheduler.shutdown(Duration.ofSeconds(5)));
    Thread.sleep(100L); // shutdown has begun but the invoke (and pool) is still alive
    TestDefaultScheduler.TestController late =
        new TestDefaultScheduler.TestController("shutdown", "late", maxInFlight, c -> {});
    assertThrows(RejectedExecutionException.class, () -> scheduler.schedule(late));

    block.release();
    shutdown.get(10, TimeUnit.SECONDS);
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> scheduler.registrySize() == 0 && scheduler.queuedWrappers() == 0);
  }
}
