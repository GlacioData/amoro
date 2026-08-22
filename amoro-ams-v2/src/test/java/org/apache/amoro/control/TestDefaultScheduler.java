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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Timeout(60)
public class TestDefaultScheduler {

  private static final long PERIOD_MILLIS = 1000L;

  private FakeClock clock;
  private FakeWaitStrategy waitStrategy;
  private DefaultScheduler scheduler;
  private ExecutorService helperPool;

  /** Shared jitter source pinned to zero so backoff values are exactly the base sequence. */
  private static final RandomSupplier ZERO_RANDOM = new ZeroRandom();

  @BeforeEach
  public void setUp() {
    clock = new FakeClock();
    waitStrategy = new FakeWaitStrategy(clock);
    scheduler =
        new DefaultScheduler(
            2, PERIOD_MILLIS, clock, ZERO_RANDOM, new BackoffPolicy(), waitStrategy);
    scheduler.start();
    helperPool = Executors.newCachedThreadPool();
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    helperPool.shutdownNow();
  }

  // ------------------------------------------------------------------ helpers

  static final class ZeroRandom implements RandomSupplier {
    @Override
    public long nextNonNegative(long upperBound) {
      return 0L;
    }
  }

  static final class FakeClock implements Clock {
    private final AtomicLong nowMillis = new AtomicLong(1_000_000L);

    @Override
    public long currentTimeMillisPlus(long delayInMillis) {
      if (delayInMillis < 0L) {
        throw new IllegalArgumentException("delayInMillis must be >= 0");
      }
      return nowMillis.get() + delayInMillis;
    }

    void advance(long millis) {
      nowMillis.addAndGet(millis);
    }
  }

  static final class FakeWaitStrategy implements SchedulerWaitStrategy {
    private final SchedulerWaitStrategy delegate = new ConditionWaitStrategy();
    private final FakeClock clock;

    FakeWaitStrategy(FakeClock clock) {
      this.clock = clock;
    }

    @Override
    public long signalVersion() {
      return delegate.signalVersion();
    }

    @Override
    public void awaitChange(long observedVersion) throws InterruptedException {
      delegate.awaitChange(observedVersion);
    }

    @Override
    public void awaitChange(long observedVersion, Duration maximumWait)
        throws InterruptedException {
      delegate.awaitChange(observedVersion, maximumWait);
    }

    @Override
    public void signal() {
      delegate.signal();
    }

    /**
     * The only sanctioned way to move test time: advancing the clock alone must not wake anyone.
     */
    void advanceClockAndSignal(long millis) {
      clock.advance(millis);
      delegate.signal();
    }
  }

  /** Controller with counting, an optional block-on-latch behavior and failure injection. */
  static final class TestController implements Controller {
    final ControllerKey key;
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger sharedMaxInFlight;
    final Consumer<TestController> behavior;

    TestController(
        String domain,
        String resourceId,
        AtomicInteger sharedMaxInFlight,
        Consumer<TestController> behavior) {
      this.key = ControllerKey.of(domain, resourceId);
      this.sharedMaxInFlight = sharedMaxInFlight;
      this.behavior = behavior;
    }

    @Override
    public ControllerKey key() {
      return key;
    }

    @Override
    public void invoke() {
      int now = inFlight.incrementAndGet();
      sharedMaxInFlight.accumulateAndGet(now, Math::max);
      try {
        behavior.accept(this);
      } finally {
        inFlight.decrementAndGet();
        attempts.incrementAndGet();
      }
    }
  }

  private final AtomicInteger maxInFlight = new AtomicInteger();

  private TestController controller(String domain, String id, Consumer<TestController> behavior) {
    return new TestController(domain, id, maxInFlight, behavior);
  }

  private static final Consumer<TestController> NO_OP = c -> {};

  private static final class BlockUntilReleased implements Consumer<TestController> {
    final CountDownLatch entered = new CountDownLatch(1);
    volatile boolean released = false;

    @Override
    public void accept(TestController c) {
      entered.countDown();
      while (!released && !Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(5L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    void release() {
      released = true;
    }
  }

  private void advance(long millis) {
    waitStrategy.advanceClockAndSignal(millis);
  }

  // ------------------------------------------------------------------ tests

  @Test
  public void sameKeyInvocationsNeverOverlapEvenWhenRescheduledDuringFlight() throws Exception {
    BlockUntilReleased blocker = new BlockUntilReleased();
    TestController first = controller("d", "1", blocker);
    AtomicInteger secondAttempts = new AtomicInteger();
    TestController second = controller("d", "1", c -> secondAttempts.incrementAndGet());

    scheduler.schedule(first);
    assertTrue(blocker.entered.await(5, TimeUnit.SECONDS), "first invocation must start");

    // schedule a second registration of the same key while the first is in flight;
    // single-flight must merge instead of running it concurrently
    scheduler.schedule(second);
    Thread.sleep(150L);
    assertEquals(0, secondAttempts.get(), "in-flight key must not invoke a second controller");
    assertEquals(1, maxInFlight.get(), "no overlap on the same key");

    blocker.release();
    await().atMost(5, TimeUnit.SECONDS).until(() -> secondAttempts.get() >= 1);
    assertEquals(1, maxInFlight.get(), "still no overlap after the merge handoff");
  }

  @Test
  public void crossDomainKeysWithSameResourceIdAreIsolated() {
    TestController a = controller("domain-a", "1", NO_OP);
    TestController b = controller("domain-b", "1", NO_OP);

    scheduler.schedule(a);
    scheduler.schedule(b);

    await().atMost(5, TimeUnit.SECONDS).until(() -> a.attempts.get() >= 1 && b.attempts.get() >= 1);
    assertEquals(2, scheduler.registrySize());
  }

  @Test
  public void queuedDeadlineMayOnlyShorten() throws Exception {
    TestController controller = controller("d", "shorten", NO_OP);
    TestController replacement = controller("d", "shorten", NO_OP);

    scheduler.schedule(controller, Duration.ofSeconds(60));
    Thread.sleep(150L);
    assertEquals(0, controller.attempts.get(), "60s deadline must not fire yet");

    // a later registration with an immediate deadline must shorten the queued one
    scheduler.schedule(replacement);
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> replacement.attempts.get() + controller.attempts.get() >= 1);
  }

  @Test
  public void laterDeadlineMustNotPostponeInFlightKey() throws Exception {
    BlockUntilReleased blocker = new BlockUntilReleased();
    AtomicInteger afterBlock = new AtomicInteger();
    TestController controller =
        controller(
            "d",
            "postpone",
            c -> {
              blocker.accept(c);
              afterBlock.incrementAndGet();
            });

    scheduler.schedule(controller); // immediate first invocation
    assertTrue(blocker.entered.await(5, TimeUnit.SECONDS));

    scheduler.schedule(controller, Duration.ofSeconds(60)); // later deadline during flight
    blocker.release();

    // natural period (1000ms) must win over the 60s request; advance only one period
    advance(PERIOD_MILLIS);
    await().atMost(5, TimeUnit.SECONDS).until(() -> afterBlock.get() >= 1);
  }

  @Test
  public void repeatedScheduleDoesNotGrowQueueOrRegistry() {
    TestController controller = controller("d", "cardinality", NO_OP);

    for (int i = 0; i < 50; i++) {
      scheduler.schedule(controller, Duration.ofSeconds(60));
    }

    assertEquals(1, scheduler.registrySize(), "one registry entry per key");
    assertEquals(1, scheduler.queuedWrappers(), "queue cardinality must not grow with schedules");
  }

  @Test
  public void unscheduleQueuedEntryPreventsInvocation() throws Exception {
    TestController controller = controller("d", "unsched-queued", NO_OP);

    scheduler.schedule(controller, Duration.ofSeconds(60));
    scheduler.unschedule(controller.key());
    advance(60_000L);
    Thread.sleep(200L);

    assertEquals(0, controller.attempts.get(), "unscheduled controller must never run");
    assertEquals(0, scheduler.registrySize());
    assertEquals(0, scheduler.queuedWrappers());
  }

  @Test
  public void unscheduleInFlightEntryDoesNotRequeueAndNewGenerationSurvives() throws Exception {
    BlockUntilReleased blocker = new BlockUntilReleased();
    TestController first = controller("d", "gen", blocker);
    TestController second = controller("d", "gen", NO_OP);

    scheduler.schedule(first);
    assertTrue(blocker.entered.await(5, TimeUnit.SECONDS));

    scheduler.unschedule(first.key()); // in-flight: current invoke finishes, no requeue
    blocker.release();
    Thread.sleep(200L);
    assertEquals(1, first.attempts.get(), "in-flight invoke completes exactly once");
    advance(60_000L);
    Thread.sleep(100L);
    assertEquals(1, first.attempts.get(), "no requeue after unschedule of claimed entry");

    scheduler.schedule(second); // new generation under the same key
    await().atMost(5, TimeUnit.SECONDS).until(() -> second.attempts.get() >= 1);
    assertEquals(1, first.attempts.get(), "old generation must stay dead");
    assertEquals(1, scheduler.registrySize());
  }

  @Test
  public void terminalStateStopsSchedulingAndRemovesRegistryEntry() {
    AtomicInteger attempts = new AtomicInteger();
    TestController controller =
        controller(
            "d",
            "terminal",
            c -> {
              if (attempts.incrementAndGet() >= 2) {
                throw TerminalState.INSTANCE;
              }
            });

    scheduler.schedule(controller);
    await().atMost(5, TimeUnit.SECONDS).until(() -> attempts.get() >= 1);
    advance(PERIOD_MILLIS);
    await().atMost(5, TimeUnit.SECONDS).until(() -> attempts.get() >= 2);

    advance(120_000L);
    assertEquals(2, attempts.get(), "TerminalState must permanently stop the controller");
    assertEquals(0, scheduler.registrySize(), "registry entry must be removed (no leak)");
    assertEquals(0, scheduler.queuedWrappers());
  }

  @Test
  public void exceptionRetriesWithExactBackoffSequenceAndSuccessResets() {
    AtomicInteger attempts = new AtomicInteger();
    TestController controller =
        controller(
            "d",
            "backoff",
            c -> {
              int attempt = attempts.incrementAndGet();
              if (attempt == 1 || attempt == 3) {
                throw new IllegalStateException("deliberate failure " + attempt);
              }
            });

    scheduler.schedule(controller); // attempt 1: fails -> next deadline = +3000ms

    await().atMost(5, TimeUnit.SECONDS).until(() -> attempts.get() >= 1);
    advance(2_999L);
    await()
        .during(300, TimeUnit.MILLISECONDS)
        .atMost(1, TimeUnit.SECONDS)
        .until(() -> attempts.get() == 1);

    advance(1L); // exactly 3000ms since the failure: attempt 2 succeeds -> next = +1000ms
    await().atMost(5, TimeUnit.SECONDS).until(() -> attempts.get() >= 2);

    advance(PERIOD_MILLIS); // attempt 3 fails -> backoff must start from 3000 again
    await().atMost(5, TimeUnit.SECONDS).until(() -> attempts.get() >= 3);

    advance(2_999L);
    await()
        .during(300, TimeUnit.MILLISECONDS)
        .atMost(1, TimeUnit.SECONDS)
        .until(() -> attempts.get() == 3);
    advance(1L);
    await().atMost(5, TimeUnit.SECONDS).until(() -> attempts.get() >= 4);
  }

  @Test
  public void advancingClockAloneDoesNotWakeWorkers() throws Exception {
    TestController controller = controller("d", "no-self-wake", NO_OP);

    scheduler.schedule(controller, Duration.ofSeconds(10));
    clock.advance(9_999L); // no signal: workers stay parked regardless of the deadline
    Thread.sleep(200L);
    assertEquals(0, controller.attempts.get(), "clock advance without signal must not fire");

    advance(1L);
    await().atMost(5, TimeUnit.SECONDS).until(() -> controller.attempts.get() >= 1);
  }

  @Test
  public void insertingEarlierDeadlineOnAnotherKeyWakesParkedWorkers() throws Exception {
    TestController slow = controller("d", "slow-key", NO_OP);
    TestController urgent = controller("d", "urgent-key", NO_OP);

    scheduler.schedule(slow, Duration.ofSeconds(60));
    Thread.sleep(150L); // workers park on the 60s head

    scheduler.schedule(urgent); // insert with immediate deadline and signal
    await().atMost(5, TimeUnit.SECONDS).until(() -> urgent.attempts.get() >= 1);
    assertEquals(0, slow.attempts.get());
  }

  @Test
  public void concurrentSchedulesOnOneKeyStaySingleFlight() throws Exception {
    TestController controller = controller("d", "hammered", NO_OP);
    CyclicBarrier barrier = new CyclicBarrier(4);
    List<Future<?>> futures = new ArrayList<>();

    for (int t = 0; t < 4; t++) {
      final int slot = t;
      futures.add(
          helperPool.submit(
              () -> {
                barrier.await();
                for (int i = 0; i < 25; i++) {
                  scheduler.schedule(controller, Duration.ofMillis(slot == 0 ? 0L : PERIOD_MILLIS));
                }
                return null;
              }));
    }
    for (Future<?> future : futures) {
      future.get(10, TimeUnit.SECONDS); // deterministic: all schedules completed
    }

    await().atMost(5, TimeUnit.SECONDS).until(() -> controller.attempts.get() >= 1);
    assertEquals(1, maxInFlight.get(), "invocations on one key must never overlap");
    assertEquals(1, scheduler.registrySize());
    assertTrue(scheduler.queuedWrappers() <= 1, "at most one wrapper per key in the queue");
  }

  @Test
  public void twoConcurrentEarlierDeadlinesConvergeToTheEarliest() throws Exception {
    TestController controller = controller("d", "earliest", NO_OP);
    CyclicBarrier barrier = new CyclicBarrier(3);

    scheduler.schedule(controller, Duration.ofSeconds(5));

    List<Future<?>> futures = new ArrayList<>();
    futures.add(
        helperPool.submit(
            () -> {
              barrier.await();
              scheduler.schedule(controller, Duration.ofMillis(1000L));
              return null;
            }));
    futures.add(
        helperPool.submit(
            () -> {
              barrier.await();
              scheduler.schedule(controller, Duration.ofMillis(300L));
              return null;
            }));
    barrier.await();
    for (Future<?> future : futures) {
      future.get(10, TimeUnit.SECONDS); // both deadline updaters finished before advancing time
    }

    advance(299L);
    await()
        .during(300, TimeUnit.MILLISECONDS)
        .atMost(1, TimeUnit.SECONDS)
        .until(() -> controller.attempts.get() == 0);
    advance(1L);
    await().atMost(5, TimeUnit.SECONDS).until(() -> controller.attempts.get() >= 1);
  }

  @Test
  public void queuedLaterDeadlineMustNotPostponeADueWrapper() throws Exception {
    // single worker: keep it busy so the victim wrapper stays QUEUED while due
    DefaultScheduler oneWorker =
        new DefaultScheduler(
            1, PERIOD_MILLIS, clock, ZERO_RANDOM, new BackoffPolicy(), waitStrategy);
    try {
      oneWorker.start();
      BlockUntilReleased blocker = new BlockUntilReleased();
      TestController busy = controller("single", "busy", blocker);
      TestController victim = controller("single", "victim", NO_OP);

      oneWorker.schedule(busy);
      assertTrue(blocker.entered.await(5, TimeUnit.SECONDS), "the only worker is now busy");

      oneWorker.schedule(victim); // due immediately, but QUEUED behind the busy worker
      oneWorker.schedule(victim, Duration.ofSeconds(60)); // must NOT postpone the due deadline

      blocker.release();
      await().atMost(5, TimeUnit.SECONDS).until(() -> victim.attempts.get() >= 1);
    } finally {
      oneWorker.shutdown(Duration.ofSeconds(5));
    }
  }

  @Test
  public void unscheduleRacingScheduleConvergesWithoutLeak() throws Exception {
    TestController controller = controller("d", "race", NO_OP);
    CyclicBarrier barrier = new CyclicBarrier(2);

    Future<?> unsched =
        helperPool.submit(
            () -> {
              barrier.await();
              for (int i = 0; i < 300; i++) {
                scheduler.unschedule(controller.key());
              }
              return null;
            });
    Future<?> sched =
        helperPool.submit(
            () -> {
              barrier.await();
              for (int i = 0; i < 300; i++) {
                scheduler.schedule(controller, Duration.ofMillis(PERIOD_MILLIS));
              }
              return null;
            });
    unsched.get(10, TimeUnit.SECONDS);
    sched.get(10, TimeUnit.SECONDS);

    assertTrue(scheduler.registrySize() <= 1, "at most one entry regardless of the winner");
    assertTrue(scheduler.queuedWrappers() <= 1, "no wrapper leak from the TERMINATED re-check");
    assertTrue(maxInFlight.get() <= 1, "the race never allows overlapping invocations");

    if (scheduler.registrySize() == 1) {
      advance(2 * PERIOD_MILLIS);
      await().atMost(5, TimeUnit.SECONDS).until(() -> controller.attempts.get() >= 1);
    }
  }

  @Test
  public void scheduleRejectsNegativeDelay() {
    TestController controller = controller("d", "negative", NO_OP);
    assertThrows(
        IllegalArgumentException.class,
        () -> scheduler.schedule(controller, Duration.ofMillis(-1)));
  }

  @Test
  public void scheduleAfterShutdownIsRejectedButUnscheduleStaysIdempotent() {
    TestController controller = controller("d", "after-shutdown", NO_OP);
    scheduler.shutdown(Duration.ofSeconds(5));

    assertThrows(RejectedExecutionException.class, () -> scheduler.schedule(controller));
    scheduler.unschedule(controller.key()); // must not throw
    scheduler.unschedule(controller.key());
    assertFalse(scheduler.registrySize() > 0);
  }

  // ------------------------------------------------------------------ misc

  /** Ensures worker threads are named per the spec and are daemons. */
  @Test
  public void workerThreadsAreNamedDaemons() throws Exception {
    TestController controller = controller("d", "naming", NO_OP);
    scheduler.schedule(controller, Duration.ofSeconds(600));

    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () ->
                Thread.getAllStackTraces().keySet().stream()
                        .filter(
                            t -> t.isDaemon() && t.getName().startsWith("amoro-control-worker-"))
                        .count()
                    >= 1);
  }
}
