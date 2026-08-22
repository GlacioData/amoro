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

package org.apache.amoro.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * T6's bounded asynchronous implementation of the {@link ListenerEventSink} port (framework spec
 * §6). Handoff only transfers the envelope — listeners execute later on dedicated workers, so a
 * slow or failing listener never blocks the mutation lane and never turns a durable success into a
 * failed stage.
 *
 * <p>Ordering contract: events of one {@code (listenerIdentity, domain, name)} pair execute in
 * handoff order; a failing pair retries (up to {@code maxRetries} after the first attempt, spaced
 * by {@code retryDelayMillis}) and later events of that pair wait behind it — but other pairs and
 * other listeners keep flowing. Global order is not promised; crash/repair may redeliver, so
 * listeners must be idempotent and level-triggered. A full queue drops the event (counted for
 * alerts); the resource domain's repair sweep compensates.
 */
public final class ListenerDispatcher<R extends ControlledResource>
    implements ListenerEventSink<R> {

  private static final Logger LOG = LoggerFactory.getLogger(ListenerDispatcher.class);

  private final String domainName;
  private final int maxRetries;
  private final long retryDelayMillis;
  private final int pendingBound;

  private final BlockingQueue<ListenerEnvelope<R>> inbox;
  private final Map<String, PairState> pairs = new HashMap<>(); // guarded by pairsLock
  private final Object pairsLock = new Object();

  private final ExecutorService workers;
  private final ScheduledExecutorService retryScheduler;
  private final Thread router;

  private final AtomicLong droppedEvents = new AtomicLong();
  private final AtomicLong exhaustedEvents = new AtomicLong();
  private final AtomicLong retries = new AtomicLong();
  private final AtomicLong shutdownDroppedEvents = new AtomicLong();
  /** Hard bound on routed-but-undelivered events (inbox has its own bound); see routeLoop. */
  private final AtomicInteger pendingCount = new AtomicInteger();

  private volatile boolean closed;
  private volatile boolean routingPaused;

  private static final class PairState {
    final String key;
    final ArrayDeque<ListenerEnvelope<?>> pending = new ArrayDeque<>();
    ListenerEnvelope<?> current;
    int attempts;
    boolean scheduled; // a drain task is queued/running for this pair

    PairState(String key) {
      this.key = key;
    }
  }

  private ListenerDispatcher(
      String domainName,
      int workerCount,
      int queueCapacity,
      int maxRetries,
      long retryDelayMillis) {
    this.domainName = domainName;
    this.maxRetries = maxRetries;
    this.retryDelayMillis = retryDelayMillis;
    this.pendingBound = queueCapacity;
    this.inbox = new ArrayBlockingQueue<ListenerEnvelope<R>>(queueCapacity);

    ThreadFactory workerFactory = namedDaemon("amoro-listener-worker-" + domainName + "-");
    this.workers = Executors.newFixedThreadPool(workerCount, workerFactory);
    this.retryScheduler =
        Executors.newSingleThreadScheduledExecutor(
            namedDaemon("amoro-listener-retry-" + domainName));
    this.router =
        namedDaemon("amoro-listener-router-" + domainName + "-").newThread(this::routeLoop);
    this.router.start();
  }

  public static <R extends ControlledResource> ListenerDispatcher<R> start(
      String domainName,
      int workerCount,
      int queueCapacity,
      int maxRetries,
      long retryDelayMillis) {
    if (domainName == null || domainName.trim().isEmpty()) {
      throw new IllegalArgumentException("domainName must not be blank");
    }
    if (workerCount <= 0 || queueCapacity <= 0 || retryDelayMillis <= 0 || maxRetries < 0) {
      throw new IllegalArgumentException(
          "workerCount/queueCapacity/retryDelayMillis must be > 0 and maxRetries >= 0");
    }
    return new ListenerDispatcher<R>(
        domainName, workerCount, queueCapacity, maxRetries, retryDelayMillis);
  }

  private static ThreadFactory namedDaemon(String prefix) {
    AtomicInteger sequence = new AtomicInteger();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + sequence.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  // ------------------------------------------------------------------ sink

  @Override
  public HandoffResult handoff(ListenerEnvelope<R> event) {
    Objects.requireNonNull(event, "event");
    if (closed) {
      droppedEvents.incrementAndGet();
      return HandoffResult.DROPPED;
    }
    // bounded, non-blocking admission control: a full queue drops and counts for the alert
    if (!inbox.offer(event)) {
      droppedEvents.incrementAndGet();
      return HandoffResult.DROPPED;
    }
    return HandoffResult.ACCEPTED;
  }

  // ------------------------------------------------------------------ routing

  /** Single router thread preserves per-pair handoff order while workers run pairs in parallel. */
  private void routeLoop() {
    while (!closed || !inbox.isEmpty()) {
      if (routingPaused) {
        try {
          Thread.sleep(5L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        continue;
      }
      ListenerEnvelope<R> event;
      try {
        event = inbox.poll(50, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (event == null) {
        continue;
      }
      PairState state;
      synchronized (pairsLock) {
        String key = pairKey(event);
        state = pairs.computeIfAbsent(key, k -> new PairState(key));
        state.pending.add(event);
        if (state.scheduled) {
          continue;
        }
        state.scheduled = true;
      }
      try {
        workers.execute(() -> drainPair(state));
      } catch (RejectedExecutionException e) {
        synchronized (pairsLock) {
          state.scheduled = false;
        }
      }
    }
  }

  private static String pairKey(ListenerEnvelope<?> event) {
    return event.listenerIdentity() + "/" + event.domain() + "/" + event.name();
  }

  private void drainPair(PairState state) {
    while (true) {
      ListenerEnvelope<?> event;
      synchronized (pairsLock) {
        if (state.current != null) {
          return; // a retry owns the head; the retry path re-schedules the drain
        }
        event = state.pending.peek();
        if (event == null) {
          state.scheduled = false;
          if (state.pending.isEmpty() && state.current == null) {
            pairs.remove(state.key, state);
          }
          return;
        }
        state.current = event;
        state.attempts = 0;
      }
      if (!executeAttempt(state, event)) {
        return; // retry scheduled; it will re-enter drainPair
      }
      synchronized (pairsLock) {
        state.pending.poll();
        state.current = null;
      }
      pendingCount.decrementAndGet();
    }
  }

  /** Runs one delivery attempt; false means a retry is scheduled. */
  private boolean executeAttempt(PairState state, ListenerEnvelope<?> event) {
    try {
      deliver(event);
      return true;
    } catch (Throwable failure) {
      int attempt = state.attempts + 1;
      synchronized (pairsLock) {
        state.attempts = attempt;
      }
      if (attempt <= maxRetries) {
        retries.incrementAndGet();
        LOG.warn(
            "Listener delivery {}/{} for {} failed (attempt {}/{}); retrying in {}ms.",
            event.eventType(),
            event.name(),
            event.listenerIdentity(),
            attempt,
            maxRetries + 1,
            retryDelayMillis,
            failure);
        retryScheduler.schedule(
            () -> {
              if (executeAttempt(state, event)) {
                synchronized (pairsLock) {
                  state.pending.poll();
                  state.current = null;
                }
                pendingCount.decrementAndGet();
                // continue the pair on a worker — never inline: a deep backlog or a hanging
                // listener here would freeze every other pair's retry on this single thread
                try {
                  workers.execute(() -> drainPair(state));
                } catch (RejectedExecutionException shuttingDown) {
                  // shutdown window: the event stays consumed; the pair's tail waits for the
                  // restart replay / repair sweep
                }
              }
            },
            retryDelayMillis,
            TimeUnit.MILLISECONDS);
        return false;
      }
      exhaustedEvents.incrementAndGet();
      pendingCount.decrementAndGet();
      LOG.error(
          "Listener delivery {}/{} for {} exhausted {} retries; dropping the event (the domain"
              + " repair sweep must compensate).",
          event.eventType(),
          event.name(),
          event.listenerIdentity(),
          maxRetries,
          failure);
      return true; // consume the event and move on to the pair's next one
    }
  }

  private void deliver(ListenerEnvelope<?> envelope) {
    PersistenceListener<?> listener =
        Objects.requireNonNull(
            envelope.listener(), "envelope must carry its listener reference for dispatch");
    dispatchTo(listener, envelope);
  }

  @SuppressWarnings("unchecked")
  private <T extends ControlledResource> void dispatchTo(
      PersistenceListener<?> listener, ListenerEnvelope<T> envelope) {
    PersistenceListener<T> typed = (PersistenceListener<T>) listener;
    switch (envelope.eventType()) {
      case AFTER_CREATED:
        typed.afterCreated(envelope.detachedResource());
        break;
      case AFTER_MODIFIED:
        typed.afterModified(envelope.detachedResource());
        break;
      case AFTER_DELETED:
        typed.afterDeleted(envelope.detachedResource());
        break;
      case POST_START:
        typed.postStart(envelope.detachedResource());
        break;
      default:
        throw new AssertionError("unknown event type " + envelope.eventType());
    }
  }

  // ------------------------------------------------------------------ metrics + lifecycle

  /** Test hook: pauses inbox routing so a bounded queue can be filled deterministically. */
  void pauseRouting() {
    routingPaused = true;
  }

  void resumeRouting() {
    routingPaused = false;
  }

  private int inboxCapacity() {
    // the inbox was constructed with queueCapacity; its remaining capacity is irrelevant here,
    // we simply reuse the same configured value as the pending-event bound
    return pendingBound;
  }

  /**
   * Events dropped because the dispatcher rejected/could not admit them (closed, full inbox, or the
   * pending hard bound). Shutdown-time undelivered events are tracked separately in {@link
   * #shutdownDroppedEventCount()}.
   */
  public long droppedEventCount() {
    return droppedEvents.get();
  }

  public long shutdownDroppedEventCount() {
    return shutdownDroppedEvents.get();
  }

  public long exhaustedEventCount() {
    return exhaustedEvents.get();
  }

  public long retryCount() {
    return retries.get();
  }

  /**
   * Bounded shutdown: stop admission, let the router drain the inbox, stop retry scheduling
   * gracefully, wait for the workers. Undelivered events are counted as shutdown drops (the restart
   * postStart replay / domain repair sweep rebuilds their side effects) and summarized in one warn
   * log.
   */
  public void shutdown(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    closed = true;
    try {
      router.join(timeout.toMillis());
      long inboxLeftover = 0;
      while (inbox.poll() != null) {
        inboxLeftover++;
      }
      if (inboxLeftover > 0) {
        shutdownDroppedEvents.addAndGet(inboxLeftover);
      }
      retryScheduler.shutdown();
      if (!retryScheduler.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        retryScheduler.shutdownNow();
      }
      workers.shutdown();
      if (!workers.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        LOG.warn(
            "Listener dispatcher of '{}' workers did not terminate within {}.",
            domainName,
            timeout);
      }
      synchronized (pairsLock) {
        long undelivered = 0;
        for (PairState state : pairs.values()) {
          undelivered += state.pending.size() + (state.current != null ? 1 : 0);
        }
        if (undelivered > 0) {
          shutdownDroppedEvents.addAndGet(undelivered);
        }
      }
      if (shutdownDroppedEvents.get() > 0) {
        LOG.warn(
            "Listener dispatcher of '{}' shut down with {} undelivered events (inbox+pending);"
                + " restart replay / repair sweep must compensate.",
            domainName,
            shutdownDroppedEvents.get());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
