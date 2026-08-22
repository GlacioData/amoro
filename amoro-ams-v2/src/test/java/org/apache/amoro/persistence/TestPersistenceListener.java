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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.serde.ResourceSerde;
import org.apache.amoro.serde.SerdeRegistry;
import org.apache.amoro.serde.VersionAwareJacksonSerde;
import org.apache.amoro.serde.VersionedResourceConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Timeout(60)
public class TestPersistenceListener {

  /** Reuses the T5 fake resource shape. */
  public static final class Res implements ControlledResource {
    public final String apiVersion;
    public final String name;
    public final long resourceVersion;

    public Res() {
      this("v1", "x", 0L);
    }

    public Res(String name, long version) {
      this("v1", name, version);
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String collection() {
      return "res";
    }

    @Override
    public long resourceVersion() {
      return resourceVersion;
    }

    @Override
    public ControlledResource withResourceVersion(long v) {
      return new Res(apiVersion, name, v);
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public Res(
        @com.fasterxml.jackson.annotation.JsonProperty("apiVersion") String apiVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("name") String name,
        @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion") long resourceVersion) {
      this.apiVersion = apiVersion;
      this.name = name;
      this.resourceVersion = resourceVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("apiVersion")
    public String getApiVersion() {
      return apiVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("name")
    public String getName() {
      return name;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion")
    public long getResourceVersion() {
      return resourceVersion;
    }
  }

  /** Recording listener with optional failure injection and scheduler callback. */
  static class RecordingListener implements PersistenceListener<Res> {
    final String id;
    final List<String> events = new CopyOnWriteArrayList<String>();
    final AtomicInteger failuresLeft = new AtomicInteger(0);
    volatile Runnable extraAction = () -> {};

    RecordingListener(String id) {
      this.id = id;
    }

    @Override
    public void afterCreated(Res resource) {
      record("created:" + resource.name() + ":" + resource.resourceVersion());
    }

    @Override
    public void afterModified(Res resource) {
      record("modified:" + resource.name() + ":" + resource.resourceVersion());
    }

    @Override
    public void afterDeleted(Res resource) {
      record("deleted:" + resource.name() + ":" + resource.resourceVersion());
    }

    @Override
    public void postStart(Res resource) {
      record("postStart:" + resource.name() + ":" + resource.resourceVersion());
      extraAction.run();
    }

    private void record(String event) {
      if (failuresLeft.getAndDecrement() > 0) {
        throw new IllegalStateException("listener failure injection for " + id);
      }
      events.add(id + ">" + event);
    }
  }

  static final class NoopHook implements DurableDeletionHook<Res> {
    @Override
    public void afterDurableDelete(Res deletedResource) {}
  }

  private FakeBlobStoreForListener blob;
  private ListenerDispatcher<Res> dispatcher;
  private ResourceSerde<Res> serde;
  private InMemoryPersistence<Res> persistence;

  @BeforeEach
  public void setUp() {
    blob = new FakeBlobStoreForListener();
    serde =
        new VersionAwareJacksonSerde<Res>(
            Res.class,
            new SerdeRegistry("v1", new ArrayList<VersionedResourceConverter>()),
            SerdeFormat.JSON,
            65536);
    dispatcher = ListenerDispatcher.start("res", 4, 1024, 3, 5L);
    persistence =
        new InMemoryPersistence<Res>(
            new PersistenceDomain("res", "amoro_resource", SerdeFormat.JSON),
            "res",
            serde,
            blob,
            1024,
            dispatcher,
            Collections.singletonList((DurableStateProjection<Res>) change -> () -> {}),
            new NoopHook());
  }

  @AfterEach
  public void tearDown() {
    persistence.shutdown(Duration.ofSeconds(5));
    dispatcher.shutdown(Duration.ofSeconds(5));
  }

  private Res created(String name) {
    return persistence.create(new Res(name, 0L)).toCompletableFuture().join();
  }

  // ------------------------------------------------------------------ tests

  @Test
  public void callbacksHappenAfterDurableSuccessInEventOrder() {
    RecordingListener listener = new RecordingListener("a");
    persistence.addListener(listener);

    created("r1");
    persistence.modify("r1", r -> r).toCompletableFuture().join();
    persistence.delete("r1").toCompletableFuture().join();

    await().atMost(5, TimeUnit.SECONDS).until(() -> listener.events.size() >= 3);
    assertEquals("a>created:r1:1", listener.events.get(0));
    assertEquals("a>modified:r1:2", listener.events.get(1));
    assertEquals("a>deleted:r1:2", listener.events.get(2));
  }

  @Test
  public void failingListenerDoesNotBlockOthersOrTheStage() {
    RecordingListener flaky = new RecordingListener("flaky");
    flaky.failuresLeft.set(100); // every attempt fails
    RecordingListener healthy = new RecordingListener("healthy");
    persistence.addListener(flaky);
    persistence.addListener(healthy);

    created("r1"); // stage succeeds even though a listener keeps failing
    created("r2");

    await().atMost(5, TimeUnit.SECONDS).until(() -> healthy.events.size() >= 2);
    assertEquals(0, flaky.events.size());
    assertTrue(dispatcher.exhaustedEventCount() >= 1, "exhausted retries are counted");
  }

  @Test
  public void retryRecoversATransientListenerFailure() {
    RecordingListener flaky = new RecordingListener("flaky");
    flaky.failuresLeft.set(1); // first attempt fails, retry succeeds
    persistence.addListener(flaky);

    created("r1");

    await().atMost(5, TimeUnit.SECONDS).until(() -> flaky.events.size() >= 1);
    assertEquals("flaky>created:r1:1", flaky.events.get(0));
    assertEquals(1, dispatcher.retryCount());
  }

  @Test
  public void fullDispatcherDropsEventsButMutationsStillSucceed() throws Exception {
    // capacity 1 with a blocked first event forces later handoffs to be dropped
    ListenerDispatcher<Res> tiny = ListenerDispatcher.start("tiny", 1, 1, 3, 5L);
    try {
      RecordingListener blocked = new RecordingListener("blocked");
      blocked.extraAction =
          () -> {
            try {
              Thread.sleep(300L);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          };
      InMemoryPersistence<Res> tinyPersistence =
          new InMemoryPersistence<Res>(
              new PersistenceDomain("tiny", "amoro_resource", SerdeFormat.JSON),
              "res",
              serde,
              new FakeBlobStoreForListener(),
              1024,
              tiny,
              Collections.singletonList((DurableStateProjection<Res>) change -> () -> {}),
              new NoopHook());
      tinyPersistence.addListener(blocked);

      tiny.pauseRouting();
      tinyPersistence.create(new Res("a", 0L)).toCompletableFuture().join(); // occupies inbox
      tinyPersistence.create(new Res("b", 0L)).toCompletableFuture().join(); // dropped
      tinyPersistence.create(new Res("c", 0L)).toCompletableFuture().join(); // dropped

      assertTrue(tiny.droppedEventCount() >= 1, "drops are counted for the alert path");
      tiny.resumeRouting();
      await().atMost(5, TimeUnit.SECONDS).until(() -> blocked.events.size() >= 1);
    } finally {
      tiny.shutdown(Duration.ofSeconds(5));
    }
  }

  @Test
  public void samePairKeepsOrderWhileOtherPairsProceed() {
    RecordingListener slowFirst = new RecordingListener("ordered");
    // the created event fails once; its retries must not let modified/deleted overtake
    slowFirst.failuresLeft.set(1);
    persistence.addListener(slowFirst);
    RecordingListener other = new RecordingListener("other-key");
    persistence.addListener(other);

    created("ordered-key");
    created("other-key"); // different pair: must not wait behind ordered-key's retry
    persistence.modify("ordered-key", r -> r).toCompletableFuture().join();
    persistence.delete("ordered-key").toCompletableFuture().join();

    await().atMost(5, TimeUnit.SECONDS).until(() -> other.events.size() >= 1);
    await().atMost(5, TimeUnit.SECONDS).until(() -> slowFirst.events.size() >= 3);
    // cross-pair order is not promised; the ordered-key PAIR must apply in handoff order
    List<String> orderedKeyEvents = new ArrayList<>();
    for (String event : slowFirst.events) {
      if (event.contains(":ordered-key")) {
        orderedKeyEvents.add(event);
      }
    }
    assertEquals(3, orderedKeyEvents.size(), "all ordered-key events deliver: " + slowFirst.events);
    assertTrue(orderedKeyEvents.get(0).endsWith("created:ordered-key:1"));
    assertTrue(orderedKeyEvents.get(1).endsWith("modified:ordered-key:2"));
    assertTrue(orderedKeyEvents.get(2).endsWith("deleted:ordered-key:2"));
    // the OTHER pair (same or different listener, other-key resource) did not wait behind the
    // ordered-key retry: its event already delivered
    assertTrue(
        other.events.stream().anyMatch(e -> e.endsWith("created:other-key:1")),
        "other pair proceeds while ordered-key retries: " + other.events);
  }

  @Test
  public void offlineReplaySchedulesEveryResourceAndSingleFlightMerges() throws Exception {
    DefaultScheduler scheduler = DefaultScheduler.create(2, 1000L);
    try {
      AtomicInteger scheduleCalls = new AtomicInteger();
      RecordingListener listener =
          new RecordingListener("replay") {
            @Override
            public void postStart(Res resource) {
              super.postStart(resource);
              scheduleCalls.incrementAndGet();
              scheduler.schedule(
                  new org.apache.amoro.control.Controller() {
                    @Override
                    public org.apache.amoro.control.ControllerKey key() {
                      return org.apache.amoro.control.ControllerKey.of("res", resource.name());
                    }

                    @Override
                    public void invoke() {
                      throw org.apache.amoro.control.TerminalState.INSTANCE;
                    }
                  });
              // duplicate schedule of the same key must merge, not duplicate
              scheduler.schedule(
                  new org.apache.amoro.control.Controller() {
                    @Override
                    public org.apache.amoro.control.ControllerKey key() {
                      return org.apache.amoro.control.ControllerKey.of("res", resource.name());
                    }

                    @Override
                    public void invoke() {
                      throw org.apache.amoro.control.TerminalState.INSTANCE;
                    }
                  });
            }
          };
      persistence.addListener(listener);
      created("p1");
      created("p2");
      created("p3");

      // simulate restart: fresh persistence over the same store replays everything
      FakeBlobStoreForListener reloadedStore = new FakeBlobStoreForListener();
      reloadedStore.rows.putAll(blob.rows);
      InMemoryPersistence<Res> restarted =
          new InMemoryPersistence<Res>(
              new PersistenceDomain("res", "amoro_resource", SerdeFormat.JSON),
              "res",
              serde,
              reloadedStore,
              1024,
              dispatcher,
              Collections.singletonList((DurableStateProjection<Res>) change -> () -> {}),
              new NoopHook());
      restarted.addListener(listener);
      restarted.postStart();

      await().atMost(5, TimeUnit.SECONDS).until(() -> scheduleCalls.get() >= 3);
      assertEquals(3, scheduler.registrySize(), "duplicate schedules merge via single-flight");
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () ->
                  listener.events.stream().filter(e -> e.startsWith("replay>postStart")).count()
                      >= 3);
    } finally {
      scheduler.shutdown(Duration.ofSeconds(5));
    }
  }

  @Test
  public void exactAttemptBudgetIsFirstCallPlusMaxRetries() {
    RecordingListener flaky = new RecordingListener("budget");
    flaky.failuresLeft.set(10); // every attempt fails: far beyond the retry budget
    persistence.addListener(flaky);

    created("r1");

    await().atMost(5, TimeUnit.SECONDS).until(() -> dispatcher.exhaustedEventCount() >= 1);
    // maxRetries=3 (setUp default): attempts = first + 3 retries = 4; the 4th failure exhausts
    assertEquals(3, dispatcher.retryCount(), "exactly maxRetries retries are spent");
    assertEquals(0, flaky.events.size());
  }

  @Test
  public void selectorWithForeignCollectionMatchesNothing() throws Exception {
    created("r1");
    List<Res> wrongCollection =
        persistence
            .select(Selector.of("other-collection", r -> true))
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertTrue(wrongCollection.isEmpty(), "foreign collection must not match");
    List<Res> rightCollection =
        persistence
            .select(Selector.of("res", r -> true))
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals(1, rightCollection.size());
  }

  /** Minimal fake store reusing the T5 shape (direct copy for test isolation). */
  static final class FakeBlobStoreForListener
      implements org.apache.amoro.persistence.blob.BlobStore {
    final java.util.concurrent.ConcurrentHashMap<String, byte[]> rows =
        new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void insert(String collection, String name, byte[] value) {
      rows.put(name, value);
    }

    @Override
    public boolean update(String collection, String name, byte[] value) {
      return rows.replace(name, value) != null;
    }

    @Override
    public boolean delete(String collection, String name) {
      return rows.remove(name) != null;
    }

    @Override
    public java.util.Optional<byte[]> find(String collection, String name) {
      return java.util.Optional.ofNullable(rows.get(name));
    }

    @Override
    public void forEach(String collection, java.util.function.BiConsumer<String, byte[]> action) {
      for (java.util.Map.Entry<String, byte[]> e : rows.entrySet()) {
        action.accept(e.getKey(), e.getValue());
      }
    }
  }
}
