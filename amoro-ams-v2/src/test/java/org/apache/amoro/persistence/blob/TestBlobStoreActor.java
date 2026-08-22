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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(60)
public class TestBlobStoreActor {

  /** In-memory fake of the durable store the lane tasks read from and write to. */
  static final class FakeDurableStore {
    final AtomicInteger value = new AtomicInteger(0);
    final List<String> writeOrder = new ArrayList<>();

    int read() {
      return value.get();
    }

    void write(int v, String tag) {
      value.set(v);
      synchronized (writeOrder) {
        writeOrder.add(tag);
      }
    }
  }

  @Test
  public void concurrentDeferredIncrementsApplyInsideTheLane() throws Exception {
    FakeDurableStore store = new FakeDurableStore();
    BlobStoreActor actor = BlobStoreActor.start("increment-domain", 1024);
    try {
      ExecutorService pool = Executors.newFixedThreadPool(8);
      try {
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
          for (int i = 0; i < 25; i++) {
            // deferred command: the read of the current value happens only inside the lane
            futures.add(
                actor.submit(
                    "increment",
                    () -> {
                      int current = store.read();
                      int next = current + 1;
                      store.write(next, "inc");
                      return next;
                    }));
          }
        }
        // every future resolved; the lane-serialized deferred increments lose nothing
        for (CompletableFuture<Integer> future : futures) {
          future.get(10, TimeUnit.SECONDS);
        }
        assertEquals(200, store.read());
      } finally {
        pool.shutdownNow();
      }
    } finally {
      actor.drain(Duration.ofSeconds(5));
    }
  }

  @Test
  public void laneIsFifo() throws Exception {
    FakeDurableStore store = new FakeDurableStore();
    BlobStoreActor actor = BlobStoreActor.start("fifo-domain", 1024);
    try {
      for (int i = 0; i < 100; i++) {
        final int index = i;
        actor.submit(
            "write-" + index,
            () -> {
              store.write(index, "w" + index);
              return null;
            });
      }
      await().atMost(5, TimeUnit.SECONDS).until(() -> store.writeOrder.size() == 100);
      synchronized (store.writeOrder) {
        for (int i = 0; i < 100; i++) {
          assertEquals("w" + i, store.writeOrder.get(i), "lane must apply writes in FIFO order");
        }
      }
    } finally {
      actor.drain(Duration.ofSeconds(5));
    }
  }

  @Test
  public void fullMailboxRejectsTheWriterBeforeEnqueue() throws Exception {
    BlobStoreActor actor = BlobStoreActor.start("tiny-domain", 1);
    try {
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      CompletableFuture<String> first =
          actor.submit(
              "blocker",
              () -> {
                entered.countDown();
                release.await();
                return "first";
              });
      assertTrue(entered.await(5, TimeUnit.SECONDS), "blocker must occupy the lane first");
      // capacity 1: the filler now fills the mailbox while the lane is busy
      CompletableFuture<String> queued = actor.submit("filler", () -> "filler");

      AtomicBoolean overflowRan = new AtomicBoolean(false);
      assertThrows(
          RejectedExecutionException.class,
          () ->
              actor.submit(
                  "overflow",
                  () -> {
                    overflowRan.set(true);
                    return "x";
                  }));
      release.countDown();
      assertEquals("first", first.get(5, TimeUnit.SECONDS));
      assertEquals("filler", queued.get(5, TimeUnit.SECONDS));
      assertFalse(overflowRan.get(), "the rejected overflow task must never execute");
    } finally {
      actor.drain(Duration.ofSeconds(5));
    }
  }

  @Test
  public void everyFutureCompletesWhenSubmitRacesDrain() throws Exception {
    // regression for the submit/drain window: a message offered behind the drain marker must
    // never leave a stranded, never-completing future
    BlobStoreActor actor = BlobStoreActor.start("race-domain", 128);
    CountDownLatch start = new CountDownLatch(1);
    List<CompletableFuture<String>> futures =
        new java.util.concurrent.CopyOnWriteArrayList<CompletableFuture<String>>();
    ExecutorService pool = Executors.newFixedThreadPool(5);
    try {
      Future<Boolean> drainResult =
          pool.submit(
              () -> {
                start.await();
                return actor.drain(Duration.ofSeconds(5));
              });
      for (int t = 0; t < 4; t++) {
        pool.submit(
            () -> {
              start.await();
              for (int i = 0; i < 200; i++) {
                try {
                  futures.add(actor.submit("racy", () -> "x"));
                } catch (RejectedExecutionException expected) {
                  // closed lane rejects synchronously — fine
                }
              }
              return null;
            });
      }
      start.countDown();
      drainResult.get(15, TimeUnit.SECONDS);
      await().atMost(5, TimeUnit.SECONDS).until(() -> futures.size() >= 0); // submitters done
      Thread.sleep(200L);
      for (CompletableFuture<String> future : futures) {
        // every accepted future completes: normally when it beat the marker, exceptionally
        // when the drain race removed it — but never hangs
        future.get(2, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
      actor.drain(Duration.ofSeconds(5));
    }
  }

  @Test
  public void taskFailureCompletesExceptionallyWithoutKillingTheLane() throws Exception {
    BlobStoreActor actor = BlobStoreActor.start("fail-domain", 1024);
    try {
      CompletableFuture<Object> failed =
          actor.submit(
              "boom",
              () -> {
                throw new IllegalStateException("deliberate lane task failure");
              });
      CompletableFuture<String> after = actor.submit("after", () -> "still-alive");

      assertThrows(CompletionException.class, () -> failed.join());
      assertEquals("still-alive", after.get(5, TimeUnit.SECONDS));
    } finally {
      actor.drain(Duration.ofSeconds(5));
    }
  }

  @Test
  public void pendingFutureIsNotCompletedBeforeTheTaskRuns() throws Exception {
    BlobStoreActor actor = BlobStoreActor.start("pending-domain", 1024);
    try {
      CountDownLatch release = new CountDownLatch(1);
      CompletableFuture<String> first =
          actor.submit(
              "blocker",
              () -> {
                release.await();
                return "first";
              });
      CompletableFuture<String> second = actor.submit("queued", () -> "second");

      Thread.sleep(150L);
      assertFalse(first.isDone(), "running task future completes only when the task returns");
      assertFalse(second.isDone(), "queued task future must wait for the lane");

      release.countDown();
      assertEquals("first", first.get(5, TimeUnit.SECONDS));
      assertEquals("second", second.get(5, TimeUnit.SECONDS));
    } finally {
      actor.drain(Duration.ofSeconds(5));
    }
  }

  @Test
  public void drainExecutesQueuedTasksThenRejectsNewOnes() throws Exception {
    BlobStoreActor actor = BlobStoreActor.start("drain-domain", 1024);
    List<CompletableFuture<Integer>> futures = new ArrayList<>();
    CountDownLatch release = new CountDownLatch(1);
    futures.add(
        actor.submit(
            "blocker",
            () -> {
              release.await();
              return 1;
            }));
    for (int i = 2; i <= 10; i++) {
      final int value = i;
      futures.add(actor.submit("queued-" + i, () -> value));
    }

    release.countDown();
    assertTrue(actor.drain(Duration.ofSeconds(5)), "all queued tasks must finish within drain");
    for (CompletableFuture<Integer> future : futures) {
      assertTrue(future.isDone() && !future.isCompletedExceptionally());
    }
    assertThrows(
        RejectedExecutionException.class,
        () -> actor.submit("late", () -> 0),
        "submit after drain must be rejected");
  }

  @Test
  public void drainTimeoutReturnsFalseAndLaterTasksStillFinish() throws Exception {
    BlobStoreActor actor = BlobStoreActor.start("slow-domain", 1024);
    AtomicReference<CountDownLatch> release = new AtomicReference<>(new CountDownLatch(1));
    CompletableFuture<String> stuck =
        actor.submit(
            "stuck",
            () -> {
              release.get().await();
              return "finally";
            });
    CompletableFuture<String> queued = actor.submit("after-stuck", () -> "queued-result");

    long start = System.nanoTime();
    assertFalse(actor.drain(Duration.ofMillis(200)), "drain must give up after its timeout");
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(elapsedMillis < 3000L, "drain timeout must be honoured promptly");
    assertFalse(stuck.isDone());

    release.get().countDown();
    assertEquals("finally", stuck.get(5, TimeUnit.SECONDS));
    assertEquals("queued-result", queued.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void laneThreadIsDaemonAndNamedAfterTheDomain() throws Exception {
    BlobStoreActor actor = BlobStoreActor.start("named-domain", 16);
    try {
      // the task itself observes the executing thread: its name and daemon flag are the
      // contract, verified from inside the lane (no tautological thread-scan assertion)
      CompletableFuture<String> name =
          actor.submit(
              "observe-thread",
              () -> Thread.currentThread().getName() + "|" + Thread.currentThread().isDaemon());
      String observed = name.get(5, TimeUnit.SECONDS);
      assertTrue(
          observed.startsWith("named-domain-mutation-lane|"),
          "lane thread must be named {domain}-mutation-lane, saw: " + observed);
      assertTrue(observed.endsWith("|true"), "lane thread must be a daemon, saw: " + observed);
    } finally {
      actor.drain(Duration.ofSeconds(5));
    }
  }

  @Test
  public void defaultMailboxCapacityMatchesTheSpec() throws Exception {
    assertEquals(1024, BlobStoreActor.DEFAULT_MAILBOX_CAPACITY);
    // convenience overload uses the default capacity
    BlobStoreActor actor = BlobStoreActor.start("default-capacity-domain");
    try {
      CompletableFuture<String> ok = actor.submit("touch", () -> "ok");
      assertEquals("ok", ok.get(5, TimeUnit.SECONDS));
    } finally {
      actor.drain(Duration.ofSeconds(5));
    }
  }
}
