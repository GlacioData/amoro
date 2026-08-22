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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DelayQueue-backed {@link Scheduler} with same-key single-flight (framework spec §4.6): at most
 * one wrapper per {@link ControllerKey} is in the queue or in flight at any time; repeated schedule
 * calls merge to the earliest deadline; an in-flight key only records the earliest requested
 * deadline, which the worker applies after the invocation returns. Registry entries are generation
 * identities removed on unschedule/terminal, so an old worker can never cancel an entry recreated
 * under the same key.
 */
public final class DefaultScheduler implements Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultScheduler.class);

  private static final AtomicInteger WORKER_SEQUENCE = new AtomicInteger();

  private final int workerThreads;
  private final long schedulingDelayMillis;
  private final Clock clock;
  private final RandomSupplier random;
  private final BackoffPolicy backoffPolicy;
  private final SchedulerWaitStrategy waitStrategy;
  private final ConcurrentHashMap<ControllerKey, ScheduledEntry> registry =
      new ConcurrentHashMap<>();
  private final DelayQueue<ScheduledController> queue = new DelayQueue<>();

  private volatile boolean started;
  private volatile boolean shutdown;
  private volatile ExecutorService workerPool;

  /** Production factory: monotonic clock, thread-local randomness, condition waits. */
  public static DefaultScheduler create(int workerThreads, long schedulingDelayMillis) {
    return new DefaultScheduler(
        workerThreads,
        schedulingDelayMillis,
        MonotonicClock.INSTANCE,
        ThreadLocalRandomSupplier.INSTANCE,
        new BackoffPolicy(),
        new ConditionWaitStrategy());
  }

  public DefaultScheduler(
      int workerThreads,
      long schedulingDelayMillis,
      Clock clock,
      RandomSupplier random,
      BackoffPolicy backoffPolicy,
      SchedulerWaitStrategy waitStrategy) {
    if (workerThreads <= 0) {
      throw new IllegalArgumentException("workerThreads must be > 0, got " + workerThreads);
    }
    if (schedulingDelayMillis < 0L) {
      throw new IllegalArgumentException(
          "schedulingDelayMillis must be >= 0, got " + schedulingDelayMillis);
    }
    this.workerThreads = workerThreads;
    this.schedulingDelayMillis = schedulingDelayMillis;
    this.clock = Objects.requireNonNull(clock, "clock");
    this.random = Objects.requireNonNull(random, "random");
    this.backoffPolicy = Objects.requireNonNull(backoffPolicy, "backoffPolicy");
    this.waitStrategy = Objects.requireNonNull(waitStrategy, "waitStrategy");
  }

  /** Starts the daemon worker threads. Idempotent; retained for the Spring lifecycle (T10). */
  public synchronized void start() {
    if (started || shutdown) {
      return;
    }
    ThreadFactory threadFactory =
        runnable -> {
          Thread thread =
              new Thread(runnable, "amoro-control-worker-" + WORKER_SEQUENCE.incrementAndGet());
          thread.setDaemon(true);
          return thread;
        };
    ExecutorService pool = Executors.newFixedThreadPool(workerThreads, threadFactory);
    SchedulerWorker worker =
        new SchedulerWorker(queue, registry, clock, waitStrategy, () -> !shutdown);
    for (int i = 0; i < workerThreads; i++) {
      pool.execute(worker);
    }
    this.workerPool = pool;
    this.started = true;
    LOG.info(
        "DefaultScheduler started with {} workers and a {}ms period.",
        workerThreads,
        schedulingDelayMillis);
  }

  @Override
  public void postStart() {
    // interface-shape no-op (fidelity ledger #11): workers start via start(); replay entry lives
    // in PersistenceService.postStart()
  }

  @Override
  public void schedule(Controller controller) {
    // first invocation as soon as a worker is free; the period applies from completion onwards
    schedule(controller, Duration.ZERO);
  }

  @Override
  public void schedule(Controller controller, Duration nextDelay) {
    Objects.requireNonNull(controller, "controller");
    Objects.requireNonNull(controller.key(), "controller.key()");
    Objects.requireNonNull(nextDelay, "nextDelay");
    long delayMillis = nextDelay.toMillis();
    if (delayMillis < 0L) {
      throw new IllegalArgumentException("nextDelay must not be negative, got " + nextDelay);
    }
    if (shutdown) {
      throw new RejectedExecutionException("scheduler is shut down");
    }
    long deadline = clock.currentTimeMillisPlus(delayMillis);
    ControllerKey key = controller.key();
    while (true) {
      // re-check on every retry: shutdown may have started while we were losing races
      if (shutdown) {
        throw new RejectedExecutionException("scheduler is shut down");
      }
      ScheduledEntry entry = registry.get(key);
      if (entry == null) {
        ScheduledController wrapper =
            new ScheduledController(
                controller, delayMillis, schedulingDelayMillis, clock, random, backoffPolicy);
        ScheduledEntry candidate = new ScheduledEntry(key, wrapper);
        ScheduledEntry existing = registry.putIfAbsent(key, candidate);
        if (existing == null) {
          // offer inside the entry lock: a concurrent unschedule that sees the published entry
          // blocks here until the offer lands, so its queue.remove cannot miss the wrapper and
          // leave a ghost element nobody owns
          synchronized (candidate) {
            if (shutdown) {
              // drain started between publication and the offer: undo and reject, never leave
              // a registered-but-unowned wrapper behind
              registry.remove(key, candidate);
              throw new RejectedExecutionException("scheduler is shut down");
            }
            if (candidate.state != ScheduledEntry.State.TERMINATED) {
              queue.offer(wrapper);
              waitStrategy.signal();
              return;
            }
          }
          // unschedule terminated the candidate between publication and the offer; drop this
          // registration and retry with a fresh lookup
          registry.remove(key, candidate);
          continue;
        }
        entry = existing; // lost the race: fall through and merge with the winner
      }
      synchronized (entry) {
        if (shutdown) {
          // ThreadPoolExecutor-style recheck: the drain may have cleared the registry while we
          // were waiting for this monitor — reject instead of requeueing into a dead scheduler
          throw new RejectedExecutionException("scheduler is shut down");
        }
        if (entry.state == ScheduledEntry.State.TERMINATED) {
          // stale generation: identity-aware cleanup, then retry with a fresh lookup
          registry.remove(key, entry);
          continue;
        }
        if (entry.state == ScheduledEntry.State.QUEUED) {
          ScheduledController wrapper = entry.wrapper;
          wrapper.replaceController(controller);
          if (deadline < wrapper.nextDesiredTimeMillis()) {
            wrapper.updateNextDesiredTimeMillis(deadline);
            boolean stillQueued = queue.remove(wrapper);
            if (stillQueued) {
              queue.offer(wrapper); // reinsert the same object with the shortened deadline
              waitStrategy.signal();
            } else {
              // the wrapper is not in the queue: either a worker already polled it, or the very
              // first offer (which happens outside this lock, right after putIfAbsent) has not
              // landed yet. Both cases converge identically: record the deadline for the
              // post-invoke requeue / initial offer instead of creating a second wrapper.
              entry.state = ScheduledEntry.State.CLAIMED;
              mergeRescheduleRequest(entry, deadline);
            }
          }
          // a later deadline never postpones an existing earlier one
          return;
        }
        // CLAIMED: the wrapper is being invoked right now; the latest registration takes over
        // the next invocation, and merging keeps urgent requests from being postponed by the
        // natural period
        entry.wrapper.replaceController(controller);
        mergeRescheduleRequest(entry, deadline);
        return;
      }
    }
  }

  @Override
  public void unschedule(ControllerKey key) {
    Objects.requireNonNull(key, "key");
    while (true) {
      ScheduledEntry entry = registry.get(key);
      if (entry == null) {
        return; // idempotent: nothing registered under this key
      }
      synchronized (entry) {
        if (entry.state == ScheduledEntry.State.TERMINATED) {
          registry.remove(key, entry);
          return;
        }
        entry.state = ScheduledEntry.State.TERMINATED;
        queue.remove(entry.wrapper); // no-op if a worker is already invoking it
        registry.remove(key, entry); // identity-aware: never removes a recreated entry
        waitStrategy.signal();
        return;
      }
    }
  }

  /**
   * Graceful shutdown (fidelity ledger #1): reject new schedules, stop picking up work, wait at
   * most {@code timeout} for in-flight invocations, then release — level-triggered restart replay
   * converges whatever was dropped. A non-positive {@code timeout} means immediate release.
   * Idempotent: only the first call performs the drain; later calls (and {@link
   * #unschedule(ControllerKey)}) never throw.
   */
  @Override
  public synchronized void shutdown(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (shutdown) {
      return;
    }
    shutdown = true;
    waitStrategy.signal(); // wake parked workers so they observe the stop flag
    ExecutorService pool = workerPool;
    if (pool != null) {
      pool.shutdown();
      try {
        long timeoutMillis = Math.max(0L, timeout.toMillis());
        if (!pool.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
          LOG.warn(
              "Scheduler shutdown timed out after {}; releasing with in-flight invocations.",
              timeout);
          pool.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        pool.shutdownNow();
      }
    }
    // the scheduler is dead: remaining registrations are meaningless (restart replays from the
    // durable store), so drain them instead of leaking entries
    registry.clear();
    queue.clear();
  }

  private void mergeRescheduleRequest(ScheduledEntry entry, long deadline) {
    Long current = entry.rescheduleRequestedMillis;
    if (current == null || deadline < current) {
      entry.rescheduleRequestedMillis = deadline;
    }
  }

  // ---------------------------------------------------------------- test observability

  /** Observability: live single-flight entries (wired to metrics in the Spring assembly). */
  public int registrySize() {
    return registry.size();
  }

  /** Observability: wrappers currently waiting in the delay queue. */
  public int queuedWrappers() {
    return queue.size();
  }
}
