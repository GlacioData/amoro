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

package org.apache.amoro.persistence.blob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * L6: the single mutation lane of one persistence domain (framework spec §5.1). Every durable
 * mutation, outcome-unknown point read and startup full-scan of a domain executes as a lane task
 * here, so the domain's read-apply-write sequences are strictly serialized and FIFO in submission
 * order. The mailbox is bounded (default capacity 1024, same as the reference actor); when it is
 * full the writer fails fast — the framework never acknowledges a success and then patches the
 * database in the background.
 *
 * <p>A task's future completes only after the task body returned on the lane; a failing task
 * completes exceptionally and never kills the lane. {@link #drain(Duration)} stops accepting
 * submissions and waits bounded for the backlog (a timeout only gives up waiting — the daemon lane
 * keeps draining so late tasks still complete; restart replay covers the rest).
 */
public final class BlobStoreActor {

  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreActor.class);

  /** Same capacity as the reference actor's mailbox (framework spec §7 queue-capacity). */
  public static final int DEFAULT_MAILBOX_CAPACITY = 1024;

  /** Mirrors the reference actor's short bounded backpressure before failing the writer. */
  private static final long OFFER_TIMEOUT_MILLIS = 10L;

  private final BlockingQueue<LaneMessage<?>> mailbox;
  private final Thread laneThread;

  private volatile boolean closed;
  private volatile boolean drained;

  private BlobStoreActor(BlockingQueue<LaneMessage<?>> mailbox, Thread laneThread) {
    this.mailbox = mailbox;
    this.laneThread = laneThread;
  }

  public static BlobStoreActor start(String domainName) {
    return start(domainName, DEFAULT_MAILBOX_CAPACITY);
  }

  public static BlobStoreActor start(String domainName, int mailboxCapacity) {
    if (domainName == null || domainName.trim().isEmpty()) {
      throw new IllegalArgumentException("domainName must not be blank");
    }
    if (mailboxCapacity <= 0) {
      throw new IllegalArgumentException("mailboxCapacity must be > 0, got " + mailboxCapacity);
    }
    BlockingQueue<LaneMessage<?>> mailbox = new ArrayBlockingQueue<LaneMessage<?>>(mailboxCapacity);
    Thread thread = new Thread(new LaneLoop(mailbox), domainName.trim() + "-mutation-lane");
    thread.setDaemon(true);
    BlobStoreActor actor = new BlobStoreActor(mailbox, thread);
    thread.start();
    return actor;
  }

  /**
   * Enqueues a deferred lane task. The task must read the latest committed state from inside the
   * lane — callers never pass a precomputed candidate.
   *
   * @throws IllegalArgumentException when description or task is null
   * @throws RejectedExecutionException when the lane is closed, the submitting thread is
   *     interrupted, or the mailbox stays full for the short backpressure window
   */
  public <T> CompletableFuture<T> submit(String description, Callable<T> laneTask) {
    if (description == null || laneTask == null) {
      throw new IllegalArgumentException("description and laneTask must not be null");
    }
    if (closed) {
      throw new RejectedExecutionException(
          "mutation lane '" + laneThread.getName() + "' no longer accepts tasks");
    }
    LaneMessage<T> message = new LaneMessage<T>(description, laneTask);
    try {
      if (!mailbox.offer(message, OFFER_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        throw new RejectedExecutionException(
            "mailbox of lane '"
                + laneThread.getName()
                + "' is full ("
                + mailbox.size()
                + " pending tasks); writer fails fast");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RejectedExecutionException("interrupted while offering to the mailbox", e);
    }
    if (closed) {
      // raced a concurrent drain: the message may sit behind the drain marker and never run,
      // which would strand the future. Remove it when it is still queued (identity-based) and
      // fail it; if the lane already took it, the message precedes the marker and completes
      // normally on the lane.
      if (mailbox.remove(message)) {
        message.future.completeExceptionally(
            new RejectedExecutionException(
                "mutation lane '" + laneThread.getName() + "' closed while submitting"));
      }
    }
    return message.future;
  }

  /**
   * Stops accepting tasks and waits at most {@code timeout} for the queued backlog to finish.
   *
   * @return true when every enqueued task (including the backlog at drain time) has completed;
   *     false when the wait gave up — the lane keeps draining in the background either way
   */
  public boolean drain(Duration timeout) {
    if (timeout == null) {
      throw new IllegalArgumentException("timeout must not be null");
    }
    if (closed) {
      return drained;
    }
    closed = true;
    // a fresh marker per drain call: a shared static marker would carry an already-completed
    // future across actor instances and make every later drain return instantly
    LaneMessage<Void> marker = LaneMessage.marker();
    marker.future.whenComplete((ignored, throwable) -> drained = true);
    long startNanos = System.nanoTime();
    try {
      // bounded enqueue: a full mailbox with a stuck lane must not block the shutdown path
      // beyond the budget (the marker queues behind the whole backlog meanwhile)
      if (!mailbox.offer(marker, timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        LOG.warn(
            "Drain of lane '{}' could not enqueue its marker within {}; the lane keeps draining.",
            laneThread.getName(),
            timeout);
        return false;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn(
          "Drain of lane '{}' was interrupted before enqueuing its marker; the lane keeps"
              + " draining.",
          laneThread.getName());
      return false;
    }
    long remainingMillis =
        timeout.toMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    try {
      marker.future.get(Math.max(0L, remainingMillis), TimeUnit.MILLISECONDS);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      // timeout or otherwise: the daemon lane keeps draining; report the give-up
      LOG.warn(
          "Drain of lane '{}' did not finish within {}; the lane keeps draining.",
          laneThread.getName(),
          timeout);
      return false;
    }
  }

  public boolean isClosed() {
    return closed;
  }

  /**
   * True when called from the lane thread itself. Persistence services use this to fail fast on
   * reentrant calls from inside a lane task (an update function calling back into the service would
   * deadlock on the single-threaded lane).
   */
  public boolean isLaneThread() {
    return Thread.currentThread() == laneThread;
  }

  // ------------------------------------------------------------------ internals

  private static final class LaneMessage<T> {

    private final String description;
    private final Callable<T> task; // null on the drain marker
    private final CompletableFuture<T> future = new CompletableFuture<T>();

    private LaneMessage(String description, Callable<T> task) {
      this.description = description;
      this.task = task;
    }

    private static LaneMessage<Void> marker() {
      return new LaneMessage<Void>("drain-marker", null);
    }

    private boolean isMarker() {
      return task == null;
    }
  }

  private static final class LaneLoop implements Runnable {
    private final BlockingQueue<LaneMessage<?>> mailbox;

    private LaneLoop(BlockingQueue<LaneMessage<?>> mailbox) {
      this.mailbox = mailbox;
    }

    @Override
    public void run() {
      while (true) {
        LaneMessage<?> message;
        try {
          message = mailbox.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return; // nothing in this class interrupts the lane; treat any interrupt as a stop
        }
        if (message.isMarker()) {
          message.future.complete(null);
          return;
        }
        runTask(message);
      }
    }

    private <T> void runTask(LaneMessage<T> message) {
      try {
        T result = message.task.call();
        message.future.complete(result);
      } catch (Throwable throwable) {
        LOG.warn("Lane task '{}' failed.", message.description, throwable);
        message.future.completeExceptionally(throwable);
      }
    }
  }
}
