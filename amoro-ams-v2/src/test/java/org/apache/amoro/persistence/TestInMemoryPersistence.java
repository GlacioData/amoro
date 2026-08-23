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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.persistence.blob.BlobStore;
import org.apache.amoro.persistence.exception.PersistenceException;
import org.apache.amoro.persistence.exception.PersistenceOutcomeUnknownException;
import org.apache.amoro.persistence.exception.PostCommitCleanupException;
import org.apache.amoro.persistence.exception.PreconditionFailedException;
import org.apache.amoro.persistence.exception.ResourceAlreadyExists;
import org.apache.amoro.persistence.exception.ResourceDoesNotExist;
import org.apache.amoro.serde.ResourceSerde;
import org.apache.amoro.serde.SerdeRegistry;
import org.apache.amoro.serde.VersionAwareJacksonSerde;
import org.apache.amoro.serde.VersionedResourceConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Timeout(90)
public class TestInMemoryPersistence {

  // ------------------------------------------------------------------ fakes

  /** Mutable-by-test fake resource: mutation lets tests prove detached-copy isolation. */
  public static final class FakeResource implements ControlledResource {
    private final String apiVersion;
    private final String name;
    private final String collection;
    private final long resourceVersion;
    private final String payload;
    private final int counter;

    public FakeResource() {
      this("v1", "unnamed", "fake", 0L, "", 0);
    }

    public FakeResource(String name, String payload, int counter) {
      this("v1", name, "fake", 0L, payload, counter);
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String collection() {
      return collection;
    }

    @Override
    public long resourceVersion() {
      return resourceVersion;
    }

    @Override
    public ControlledResource withResourceVersion(long newResourceVersion) {
      return new FakeResource(apiVersion, name, collection, newResourceVersion, payload, counter);
    }

    public String payload() {
      return payload;
    }

    public int counter() {
      return counter;
    }

    public FakeResource withPayloadAndCounter(String newPayload, int newCounter) {
      return new FakeResource(
          apiVersion, name, collection, resourceVersion, newPayload, newCounter);
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public FakeResource(
        @com.fasterxml.jackson.annotation.JsonProperty("apiVersion") String apiVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("name") String name,
        @com.fasterxml.jackson.annotation.JsonProperty("collection") String collection,
        @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion") long resourceVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("payload") String payload,
        @com.fasterxml.jackson.annotation.JsonProperty("counter") int counter) {
      this.apiVersion = apiVersion;
      this.name = name;
      this.collection = collection;
      this.resourceVersion = resourceVersion;
      this.payload = payload;
      this.counter = counter;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("apiVersion")
    public String getApiVersion() {
      return apiVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("name")
    public String getName() {
      return name;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("collection")
    public String getCollection() {
      return collection;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion")
    public long getResourceVersion() {
      return resourceVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("payload")
    public String getPayload() {
      return payload;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("counter")
    public int getCounter() {
      return counter;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FakeResource that = (FakeResource) o;
      return resourceVersion == that.resourceVersion
          && java.util.Objects.equals(apiVersion, that.apiVersion)
          && java.util.Objects.equals(name, that.name)
          && java.util.Objects.equals(collection, that.collection)
          && java.util.Objects.equals(payload, that.payload)
          && counter == that.counter;
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(
          apiVersion, name, collection, resourceVersion, payload, counter);
    }
  }

  /**
   * Scriptable in-memory BlobStore: counts operations and simulates connection failures. Scripts
   * are one-shot; {@code afterWrite} fires after the row mutation is visible (the classic
   * committed-then-connection-died window), {@code beforeWrite} fires before it.
   */
  static final class FakeBlobStore implements BlobStore {
    final java.util.concurrent.ConcurrentHashMap<String, byte[]> rows =
        new java.util.concurrent.ConcurrentHashMap<>();
    final AtomicInteger insertCalls = new AtomicInteger();
    final AtomicInteger updateCalls = new AtomicInteger();
    final AtomicInteger deleteCalls = new AtomicInteger();
    final AtomicInteger findCalls = new AtomicInteger();
    final AtomicInteger forEachCalls = new AtomicInteger();
    volatile FailureScript script;

    abstract static class FailureScript {
      void beforeWrite() {}

      void afterWrite() {}

      void onFind(String name) {}
    }

    /** The row is committed, then the connection dies — the unknown-outcome trigger. */
    static FailureScript throwAfterCommit() {
      return new FailureScript() {
        @Override
        void afterWrite() {
          throw new RuntimeException("simulated connection failure after commit");
        }
      };
    }

    /** The write fails before anything is committed — resolvable as previous state. */
    static final FailureScript THROW_BEFORE_COMMIT =
        new FailureScript() {
          @Override
          void beforeWrite() {
            throw new RuntimeException("simulated failure before commit");
          }
        };

    /** Commit-then-throw, and the resolution point read fails too — outcome unresolvable. */
    static final FailureScript COMMIT_THEN_BOTH_READS_FAIL =
        new FailureScript() {
          @Override
          void afterWrite() {
            throw new RuntimeException("simulated connection failure after commit");
          }

          @Override
          void onFind(String name) {
            throw new RuntimeException("resolution point read also failed");
          }
        };

    /** Commit-then-throw, and the point read sees an unrelated third value. */
    final FailureScript commitThenFindThirdValue =
        new FailureScript() {
          @Override
          void afterWrite() {
            throw new RuntimeException("simulated connection failure after commit");
          }

          @Override
          void onFind(String name) {
            // overwrite with a valid document that matches neither the candidate nor the
            // previous state, so the outcome is genuinely undecidable yet repairable
            rows.put(
                name,
                ("{\"apiVersion\":\"v1\",\"name\":\""
                        + name
                        + "\",\"collection\":\"fake\","
                        + "\"resourceVersion\":1,\"payload\":\"third-value\",\"counter\":0}")
                    .getBytes());
          }
        };

    private FailureScript script() {
      return script; // sticky: stays until the test replaces or clears it
    }

    @Override
    public void insert(String collection, String name, byte[] value) {
      insertCalls.incrementAndGet();
      FailureScript script = script();
      if (script != null) {
        script.beforeWrite();
      }
      if (rows.putIfAbsent(name, value) != null) {
        throw new ResourceAlreadyExists("fake", name);
      }
      if (script != null) {
        script.afterWrite();
      }
    }

    @Override
    public boolean update(String collection, String name, byte[] value) {
      updateCalls.incrementAndGet();
      FailureScript script = script();
      if (script != null) {
        script.beforeWrite();
      }
      boolean updated = rows.replace(name, value) != null;
      if (script != null) {
        script.afterWrite();
      }
      return updated;
    }

    @Override
    public boolean delete(String collection, String name) {
      deleteCalls.incrementAndGet();
      FailureScript script = script();
      if (script != null) {
        script.beforeWrite();
      }
      boolean deleted = rows.remove(name) != null;
      if (script != null) {
        script.afterWrite();
      }
      return deleted;
    }

    @Override
    public Optional<byte[]> find(String collection, String name) {
      findCalls.incrementAndGet();
      FailureScript script = script();
      if (script != null) {
        script.onFind(name);
      }
      return Optional.ofNullable(rows.get(name));
    }

    @Override
    public void forEach(String collection, BiConsumer<String, byte[]> action) {
      forEachCalls.incrementAndGet();
      for (java.util.Map.Entry<String, byte[]> entry : rows.entrySet()) {
        action.accept(entry.getKey(), entry.getValue());
      }
    }
  }

  /** Recording listener sink; ACCEPTED unless configured to drop. */
  static final class RecordingSink implements ListenerEventSink<FakeResource> {
    final List<ListenerEnvelope<FakeResource>> envelopes =
        java.util.Collections.synchronizedList(new ArrayList<ListenerEnvelope<FakeResource>>());
    volatile boolean dropEverything = false;

    @Override
    public HandoffResult handoff(ListenerEnvelope<FakeResource> event) {
      if (dropEverything) {
        return HandoffResult.DROPPED;
      }
      envelopes.add(event);
      return HandoffResult.ACCEPTED;
    }
  }

  static final class RecordingProjection implements DurableStateProjection<FakeResource> {
    final List<PersistenceChange<FakeResource>> prepared =
        java.util.Collections.synchronizedList(new ArrayList<PersistenceChange<FakeResource>>());
    final AtomicInteger commits = new AtomicInteger();
    volatile boolean failPrepare = false;

    @Override
    public PreparedProjectionUpdate prepare(PersistenceChange<FakeResource> change) {
      if (failPrepare) {
        throw new IllegalStateException("projection prepare failure (deliberate)");
      }
      prepared.add(change);
      return commits::incrementAndGet;
    }
  }

  static final class RecordingHook implements DurableDeletionHook<FakeResource> {
    final AtomicInteger calls = new AtomicInteger();
    volatile Consumer<FakeResource> behavior = r -> {};

    @Override
    public void afterDurableDelete(FakeResource deletedResource) {
      calls.incrementAndGet();
      behavior.accept(deletedResource);
    }
  }

  // ------------------------------------------------------------------ harness

  FakeBlobStore blob;
  RecordingSink sink;
  RecordingProjection projection;
  RecordingHook hook;
  ResourceSerde<FakeResource> serde;
  InMemoryPersistence<FakeResource> persistence;
  ExecutorService pool;

  @BeforeEach
  public void setUp() {
    blob = new FakeBlobStore();
    sink = new RecordingSink();
    projection = new RecordingProjection();
    hook = new RecordingHook();
    serde =
        new VersionAwareJacksonSerde<FakeResource>(
            FakeResource.class,
            new SerdeRegistry("v1", new ArrayList<VersionedResourceConverter>()),
            SerdeFormat.JSON,
            65536);
    persistence = newPersistence(1024);
    pool = Executors.newFixedThreadPool(8);
  }

  private InMemoryPersistence<FakeResource> newPersistence(int mailboxCapacity) {
    return new InMemoryPersistence<FakeResource>(
        new PersistenceDomain("test", "amoro_resource", SerdeFormat.JSON),
        "fake",
        serde,
        blob,
        mailboxCapacity,
        sink,
        java.util.Collections.singletonList(projection),
        hook);
  }

  private static FakeResource join(java.util.concurrent.CompletionStage<FakeResource> stage) {
    return stage.toCompletableFuture().join();
  }

  private static Throwable causeOf(java.util.concurrent.CompletionStage<?> stage) {
    try {
      stage.toCompletableFuture().join();
      throw new AssertionError("expected exceptional completion");
    } catch (CompletionException e) {
      return e.getCause();
    }
  }

  // ------------------------------------------------------------------ 1..4 reads & basics

  @Test
  public void readsNeverTouchTheBlobStore() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 1)));
    int finds = blob.findCalls.get();
    int forEach = blob.forEachCalls.get();

    join(persistence.get("r1"));
    persistence.select(Selector.of("fake", r -> true)).toCompletableFuture().join();

    assertEquals(finds, blob.findCalls.get(), "get must not point-read the store");
    assertEquals(forEach, blob.forEachCalls.get(), "select must not scan the store");
    assertEquals(1, blob.insertCalls.get());
  }

  @Test
  public void createResultIsDetachedFromTheArgument() throws Exception {
    FakeResource argument = new FakeResource("r-detach", "at-enqueue", 5);
    FakeResource created = join(persistence.create(argument));

    assertEquals(1, created.resourceVersion());
    assertNotSame(argument, created, "stage result must be a detached copy");
    assertNotSame(
        argument,
        join(persistence.get("r-detach")),
        "cache must hold a detached snapshot, never the caller's instance");
    assertNotSame(created, join(persistence.get("r-detach")));
  }

  @Test
  public void createRejectsNonZeroVersion() {
    FakeResource bad = (FakeResource) new FakeResource("bad", "p", 1).withResourceVersion(3);
    assertThrows(IllegalArgumentException.class, () -> persistence.create(bad));
  }

  @Test
  public void returnedValuesAndEnvelopesAreDetachedCopies() throws Exception {
    persistence.addListener(new NoOpListener());
    FakeResource created = join(persistence.create(new FakeResource("r1", "p", 7)));
    FakeResource fromGet = join(persistence.get("r1"));

    assertNotSame(created, fromGet);
    assertEquals(created, fromGet);

    // mutating returned values cannot corrupt the cache because the fake is immutable; the
    // structural guarantee is the serde round-trip — proven by version/payload equality and
    // non-identity above
    FakeResource modified = join(persistence.modify("r1", r -> r.withPayloadAndCounter("p2", 8)));
    assertEquals(2, modified.resourceVersion());
    assertEquals("p2", modified.payload());

    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> sink.envelopes.size() >= 2); // created + modified
    for (ListenerEnvelope<FakeResource> envelope : sink.envelopes) {
      assertNotSame(envelope.detachedResource(), join(persistence.get("r1")));
    }
  }

  // ------------------------------------------------------------------ 5..9 writes

  @Test
  public void concurrentSameNameCreateProducesExactlyOneWinner() throws Exception {
    List<CompletionStage<FakeResource>> stages = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      stages.add(persistence.create(new FakeResource("dup", "p" + i, i)));
    }
    int successes = 0;
    for (CompletionStage<FakeResource> stage : stages) {
      try {
        join(stage);
        successes++;
      } catch (CompletionException e) {
        assertTrue(
            e.getCause() instanceof ResourceAlreadyExists,
            "loser must fail with ResourceAlreadyExists, got " + e.getCause());
      }
    }
    assertEquals(1, successes);
    assertEquals(1, blob.insertCalls.get());
  }

  @Test
  public void updateFnChangingIdentityIsRejectedWithoutSideEffects() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 1)));
    CompletionStage<FakeResource> renamed =
        persistence.modify(
            "r1", r -> (FakeResource) new FakeResource("other", "p", 1).withResourceVersion(1));
    assertTrue(
        causeOf(renamed) instanceof IllegalArgumentException
            || causeOf(renamed) instanceof PersistenceException);

    FakeResource after = join(persistence.get("r1"));
    assertEquals(1, after.resourceVersion());
    assertEquals("p", after.payload());
  }

  @Test
  public void versionIncrementsByExactlyOnePerModify() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0)));
    for (int v = 2; v <= 6; v++) {
      FakeResource modified = join(persistence.modify("r1", r -> r));
      assertEquals(v, modified.resourceVersion());
    }
    assertEquals(6, join(persistence.get("r1")).resourceVersion());
  }

  @Test
  public void casMismatchFailsWithNoRetryAndNoSideEffects() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0))); // version 1
    // a definitely-stale CAS loses; the correct one wins
    CompletionStage<FakeResource> stale =
        persistence.modify("r1", 99L, r -> r.withPayloadAndCounter("stale", 0));
    join(persistence.modify("r1", 1L, r -> r.withPayloadAndCounter("fresh", 0))); // version 2

    assertTrue(causeOf(stale) instanceof PreconditionFailedException);
    assertEquals("fresh", join(persistence.get("r1")).payload());
    assertEquals(2, join(persistence.get("r1")).resourceVersion());
    assertEquals(
        1,
        blob.updateCalls.get(),
        "only the winning CAS write may hit the store; the stale one never writes");
  }

  @Test
  public void concurrentUnconditionalModifiesEachApplyInLane() throws Exception {
    join(persistence.create(new FakeResource("counter", "p", 0)));
    List<CompletionStage<FakeResource>> stages = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      stages.add(
          persistence.modify(
              "counter", r -> r.withPayloadAndCounter(r.payload(), r.counter() + 1)));
    }
    int maxSeen = 0;
    for (CompletionStage<FakeResource> stage : stages) {
      maxSeen = Math.max(maxSeen, join(stage).counter());
    }
    assertEquals(
        50,
        join(persistence.get("counter")).counter(),
        "each deferred increment must apply to the lane's latest value");
    assertEquals(50, maxSeen);
    assertTrue(blob.updateCalls.get() >= 50);
  }

  // ------------------------------------------------------------------ 10..13 failure isolation

  @Test
  public void updateFnThrowingLeavesCanonicalUnchanged() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 3)));
    CompletionStage<FakeResource> failed =
        persistence.modify(
            "r1",
            r -> {
              throw new IllegalStateException("update function failed");
            });
    assertTrue(causeOf(failed) instanceof IllegalStateException);
    FakeResource after = join(persistence.get("r1"));
    assertEquals(1, after.resourceVersion());
    assertEquals("p", after.payload());

    // the lane survives: the next mutation processes
    FakeResource ok = join(persistence.modify("r1", r -> r.withPayloadAndCounter("ok", 3)));
    assertEquals(2, ok.resourceVersion());
  }

  @Test
  public void reentrantCallsFromUpdateFnFailFast() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0)));
    CompletionStage<FakeResource> reentrant =
        persistence.modify(
            "r1",
            r -> {
              persistence.get("r1"); // must fail fast instead of deadlocking the lane
              return r;
            });
    assertTrue(
        causeOf(reentrant) instanceof IllegalStateException
            || causeOf(reentrant) instanceof CompletionException,
        "reentrancy must fail fast, got " + causeOf(reentrant));
    // lane healthy afterwards
    join(persistence.modify("r1", r -> r));
  }

  @Test
  public void projectionPrepareFailureAbortsBeforeTheDatabase() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0)));
    projection.failPrepare = true;
    int updatesBefore = blob.updateCalls.get();

    CompletionStage<FakeResource> failed =
        persistence.modify("r1", r -> r.withPayloadAndCounter("never", 0));
    Throwable projectionCause = causeOf(failed);
    assertTrue(
        projectionCause instanceof IllegalStateException,
        "prepare failure surfaces as-is, got " + projectionCause);
    assertEquals(updatesBefore, blob.updateCalls.get(), "no DB write after prepare failure");
    assertEquals(1, join(persistence.get("r1")).resourceVersion());
    assertEquals("p", join(persistence.get("r1")).payload());
    assertEquals(0, projection.commits.get() - 1, "no projection commit after prepare failure");
  }

  @Test
  public void databaseFailureLeavesMemoryVersionAndListenersUnchanged() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0)));
    blob.script = FakeBlobStore.THROW_BEFORE_COMMIT;
    int envelopesBefore = sink.envelopes.size();

    CompletionStage<FakeResource> failed =
        persistence.modify("r1", r -> r.withPayloadAndCounter("never", 0));
    Throwable writeCause = causeOf(failed);
    assertTrue(
        writeCause instanceof PersistenceException,
        "refuted write surfaces as a persistence failure, got " + writeCause);
    blob.script = null;
    assertEquals(1, join(persistence.get("r1")).resourceVersion());
    assertEquals("p", join(persistence.get("r1")).payload());
    assertEquals(envelopesBefore, sink.envelopes.size(), "no listener event on failure");
    assertEquals(
        1,
        projection.commits.get(),
        "the prepared projection of the failed write must be discarded");
  }

  // ------------------------------------------------------------------ 14..17 unknown outcomes

  @Test
  public void unknownInsertConfirmedByPointReadPublishes() throws Exception {
    blob.script = FakeBlobStore.throwAfterCommit();
    FakeResource created = join(persistence.create(new FakeResource("r1", "p", 0)));

    assertEquals(1, created.resourceVersion());
    assertEquals(1, join(persistence.get("r1")).resourceVersion());
    assertTrue(blob.findCalls.get() >= 1, "resolution must point-read on a fresh connection");
  }

  @Test
  public void unknownInsertRefutedByPointReadFailsWithoutPublishing() {
    // the write failed before committing; the fresh point read sees the previous state (absent)
    blob.script = FakeBlobStore.THROW_BEFORE_COMMIT;
    CompletionStage<FakeResource> failed = persistence.create(new FakeResource("r1", "p", 0));
    Throwable cause = causeOf(failed);
    assertTrue(cause instanceof PersistenceException, "got " + cause);
    assertFalse(cause instanceof PersistenceOutcomeUnknownException, "must not fence");
    assertNullSilently(persistence.get("r1"));
  }

  @Test
  public void unresolvableOutcomeFencesTheKeyUntilRepair() throws Exception {
    persistence.addListener(new NoOpListener());
    blob.script = FakeBlobStore.COMMIT_THEN_BOTH_READS_FAIL;
    CompletionStage<FakeResource> unknown = persistence.create(new FakeResource("r1", "p", 0));
    assertTrue(causeOf(unknown) instanceof PersistenceOutcomeUnknownException);

    // fenced: later writes fail fast
    CompletionStage<FakeResource> blocked = persistence.create(new FakeResource("r1", "p", 0));
    assertTrue(causeOf(blocked) instanceof PersistenceOutcomeUnknownException);

    // repair reloads the durable row (the create actually committed) and clears the fence
    blob.script = null; // the connection recovered
    persistence.repair("r1");
    assertEquals(
        1,
        join(persistence.get("r1")).resourceVersion(),
        "repair must recover the committed write into the cache");
    assertEquals(1, projection.commits.get(), "repair must publish the recovered create");
    assertEquals(
        PersistenceChange.Type.CREATE,
        projection.prepared.get(projection.prepared.size() - 1).type());
    assertEquals(1, sink.envelopes.size(), "repair must emit a compensating create event");
    assertEquals(ListenerEnvelope.EventType.AFTER_CREATED, sink.envelopes.get(0).eventType());
  }

  @Test
  public void unknownDeleteConfirmedAbsentIsSuccessNotFailure() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0)));
    blob.script = FakeBlobStore.throwAfterCommit(); // delete committed, then connection died
    FakeResource deleted = join(persistence.delete("r1"));

    assertEquals(1, deleted.resourceVersion());
    assertNullSilently(persistence.get("r1"));
  }

  // ------------------------------------------------------------------ 18 missing resources

  @Test
  public void missingResourceOperationsFailWithDoesNotExist() {
    assertTrue(causeOf(persistence.get("ghost")) instanceof ResourceDoesNotExist);
    assertTrue(causeOf(persistence.delete("ghost")) instanceof ResourceDoesNotExist);
    assertTrue(causeOf(persistence.modify("ghost", r -> r)) instanceof ResourceDoesNotExist);
  }

  // ------------------------------------------------------------------ deletion hook

  @Test
  public void hookRunsInLaneBeforeNextSameNameMutationAndStageCompletion() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0)));
    CountDownLatch hookEntered = new CountDownLatch(1);
    CountDownLatch releaseHook = new CountDownLatch(1);
    hook.behavior =
        r -> {
          hookEntered.countDown();
          try {
            releaseHook.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };

    CompletionStage<FakeResource> deleting = persistence.delete("r1");
    assertTrue(hookEntered.await(5, TimeUnit.SECONDS), "hook must run inside the delete");
    // the delete stage cannot complete while the hook blocks
    Thread.sleep(150L);
    assertFalse(
        deleting.toCompletableFuture().isDone(),
        "delete stage must not finish before the hook returns");

    // a same-name create queues behind the blocked delete/hook pair
    CompletionStage<FakeResource> recreating = persistence.create(new FakeResource("r1", "p2", 0));
    Thread.sleep(150L);
    assertFalse(
        recreating.toCompletableFuture().isDone(),
        "same-name create must wait for the hook to finish");

    releaseHook.countDown();
    assertEquals(1, join(deleting).resourceVersion());
    assertEquals(1, join(recreating).resourceVersion());
    assertEquals("p2", join(persistence.get("r1")).payload());
  }

  @Test
  public void hookFailureFencesTheNameUntilRepairCleanup() throws Exception {
    persistence.addListener(new NoOpListener());
    join(persistence.create(new FakeResource("r1", "p", 0)));
    int commitsBeforeDelete = projection.commits.get();
    int eventsBeforeDelete = sink.envelopes.size();
    hook.behavior =
        r -> {
          throw new IllegalStateException("hook cleanup failed");
        };

    CompletionStage<FakeResource> failed = persistence.delete("r1");
    assertTrue(causeOf(failed) instanceof PostCommitCleanupException);
    assertEquals(
        "p",
        join(persistence.get("r1")).payload(),
        "the stale canonical/projection remains reserved until cleanup succeeds");
    assertEquals(commitsBeforeDelete, projection.commits.get());
    assertEquals(eventsBeforeDelete, sink.envelopes.size());

    // fenced: same-name create rejected
    CompletionStage<FakeResource> blocked = persistence.create(new FakeResource("r1", "p", 0));
    assertTrue(causeOf(blocked) instanceof PostCommitCleanupException);

    // repair retries the hook with the staged snapshot, then unfences
    AtomicBoolean repairedHookSawResource = new AtomicBoolean(false);
    hook.behavior = r -> repairedHookSawResource.set(r != null && "r1".equals(r.name()));
    persistence.repair("r1");

    assertNullSilently(persistence.get("r1"));
    assertEquals(commitsBeforeDelete + 1, projection.commits.get());
    assertEquals(PersistenceChange.Type.DELETE, projection.prepared.get(1).type());
    assertEquals(eventsBeforeDelete + 1, sink.envelopes.size());
    assertEquals(ListenerEnvelope.EventType.AFTER_DELETED, sink.envelopes.get(1).eventType());

    FakeResource recreated = join(persistence.create(new FakeResource("r1", "p2", 0)));
    assertEquals(1, recreated.resourceVersion());
    assertTrue(repairedHookSawResource.get(), "repair must retry the hook with the snapshot");
  }

  // ------------------------------------------------------------------ postStart replay

  @Test
  public void postStartRebuildsFromTheStoreAndReplaysToListeners() throws Exception {
    persistence.addListener(new NoOpListener());
    join(persistence.create(new FakeResource("r1", "p1", 0)));
    join(persistence.create(new FakeResource("r2", "p2", 0)));

    RecordingSink freshSink = new RecordingSink();
    InMemoryPersistence<FakeResource> restarted =
        new InMemoryPersistence<FakeResource>(
            new PersistenceDomain("test", "amoro_resource", SerdeFormat.JSON),
            "fake",
            serde,
            blob,
            1024,
            freshSink,
            java.util.Collections.singletonList(new RecordingProjection()),
            r -> {});
    restarted.addListener(new NoOpListener());
    restarted.postStart();

    List<FakeResource> reloaded =
        restarted
            .select(Selector.of("fake", r -> true))
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals(2, reloaded.size());
    await().atMost(5, TimeUnit.SECONDS).until(() -> freshSink.envelopes.size() >= 2);
    for (ListenerEnvelope<FakeResource> envelope : freshSink.envelopes) {
      assertEquals(ListenerEnvelope.EventType.POST_START, envelope.eventType());
    }
  }

  @Test
  public void unknownModifyConfirmedByPointReadPublishes() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0)));
    blob.script = FakeBlobStore.throwAfterCommit();
    FakeResource modified = join(persistence.modify("r1", r -> r.withPayloadAndCounter("p2", 0)));

    assertEquals(2, modified.resourceVersion());
    assertEquals(2, join(persistence.get("r1")).resourceVersion());
    assertEquals("p2", join(persistence.get("r1")).payload());
  }

  @Test
  public void unknownOutcomeWithThirdValueFencesUntilRepair() throws Exception {
    persistence.addListener(new NoOpListener());
    join(persistence.create(new FakeResource("r1", "p", 0)));
    int commitsBeforeUnknown = projection.commits.get();
    int eventsBeforeUnknown = sink.envelopes.size();
    // the row exists but holds bytes matching neither the candidate nor the previous state
    blob.script = blob.commitThenFindThirdValue;
    CompletionStage<FakeResource> unknown =
        persistence.modify("r1", r -> r.withPayloadAndCounter("p2", 0));
    assertTrue(causeOf(unknown) instanceof PersistenceOutcomeUnknownException);

    blob.script = null;
    persistence.repair("r1"); // reload adopts whatever the durable row actually says
    FakeResource repaired = join(persistence.get("r1"));
    assertEquals(
        "third-value",
        repaired.payload(),
        "repair adopts the durable row verbatim after unresolvable outcome");
    assertEquals(
        commitsBeforeUnknown + 1,
        projection.commits.get(),
        "repair must publish the recovered modify");
    PersistenceChange<FakeResource> repairChange =
        projection.prepared.get(projection.prepared.size() - 1);
    assertEquals(PersistenceChange.Type.MODIFY, repairChange.type());
    assertEquals("p", repairChange.previous().payload());
    assertEquals("third-value", repairChange.current().payload());
    assertEquals(eventsBeforeUnknown + 1, sink.envelopes.size());
    assertEquals(
        ListenerEnvelope.EventType.AFTER_MODIFIED,
        sink.envelopes.get(sink.envelopes.size() - 1).eventType());
  }

  @Test
  public void unknownDeleteRepairPublishesProjectionHookAndEvent() throws Exception {
    persistence.addListener(new NoOpListener());
    join(persistence.create(new FakeResource("r1", "p", 0)));
    int commitsBeforeUnknown = projection.commits.get();
    int eventsBeforeUnknown = sink.envelopes.size();
    int hooksBeforeUnknown = hook.calls.get();

    blob.script = FakeBlobStore.COMMIT_THEN_BOTH_READS_FAIL;
    CompletionStage<FakeResource> unknown = persistence.delete("r1");
    assertTrue(causeOf(unknown) instanceof PersistenceOutcomeUnknownException);

    blob.script = null;
    persistence.repair("r1");

    assertNullSilently(persistence.get("r1"));
    assertEquals(commitsBeforeUnknown + 1, projection.commits.get());
    PersistenceChange<FakeResource> repairChange =
        projection.prepared.get(projection.prepared.size() - 1);
    assertEquals(PersistenceChange.Type.DELETE, repairChange.type());
    assertEquals("r1", repairChange.previous().name());
    assertEquals(hooksBeforeUnknown + 1, hook.calls.get());
    assertEquals(eventsBeforeUnknown + 1, sink.envelopes.size());
    assertEquals(
        ListenerEnvelope.EventType.AFTER_DELETED,
        sink.envelopes.get(sink.envelopes.size() - 1).eventType());
  }

  @Test
  public void repairProjectionPrepareFailureKeepsFenceAndOldCanonical() throws Exception {
    persistence.addListener(new NoOpListener());
    join(persistence.create(new FakeResource("r1", "p", 0)));
    int commitsBeforeUnknown = projection.commits.get();
    int eventsBeforeUnknown = sink.envelopes.size();

    blob.script = blob.commitThenFindThirdValue;
    CompletionStage<FakeResource> unknown =
        persistence.modify("r1", r -> r.withPayloadAndCounter("p2", 0));
    assertTrue(causeOf(unknown) instanceof PersistenceOutcomeUnknownException);

    blob.script = null;
    projection.failPrepare = true;
    assertThrows(CompletionException.class, () -> persistence.repair("r1"));
    assertTrue(persistence.fencedNames().contains("r1"));
    assertEquals("p", join(persistence.get("r1")).payload());
    assertEquals(commitsBeforeUnknown, projection.commits.get());
    assertEquals(eventsBeforeUnknown, sink.envelopes.size());

    projection.failPrepare = false;
    persistence.repair("r1");
    assertEquals("third-value", join(persistence.get("r1")).payload());
    assertFalse(persistence.fencedNames().contains("r1"));
  }

  @Test
  public void unknownDeleteHookFailureKeepsOldProjectionUntilRepairRetry() throws Exception {
    persistence.addListener(new NoOpListener());
    join(persistence.create(new FakeResource("r1", "p", 0)));
    int commitsBeforeUnknown = projection.commits.get();
    int eventsBeforeUnknown = sink.envelopes.size();

    blob.script = FakeBlobStore.COMMIT_THEN_BOTH_READS_FAIL;
    CompletionStage<FakeResource> unknown = persistence.delete("r1");
    assertTrue(causeOf(unknown) instanceof PersistenceOutcomeUnknownException);

    blob.script = null;
    hook.behavior = r -> { throw new IllegalStateException("repair hook failed"); };
    assertThrows(CompletionException.class, () -> persistence.repair("r1"));
    assertTrue(persistence.fencedNames().contains("r1"));
    assertEquals("p", join(persistence.get("r1")).payload());
    assertEquals(commitsBeforeUnknown, projection.commits.get());
    assertEquals(eventsBeforeUnknown, sink.envelopes.size());

    hook.behavior = r -> {};
    persistence.repair("r1");
    assertNullSilently(persistence.get("r1"));
    assertEquals(commitsBeforeUnknown + 1, projection.commits.get());
    assertEquals(eventsBeforeUnknown + 1, sink.envelopes.size());
    assertFalse(persistence.fencedNames().contains("r1"));
  }

  @Test
  public void unknownDeleteRefutedKeepsResourceAndMemory() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0)));
    blob.script = FakeBlobStore.THROW_BEFORE_COMMIT; // row still present -> previous state
    CompletionStage<FakeResource> failed = persistence.delete("r1");

    Throwable cause = causeOf(failed);
    assertTrue(cause instanceof PersistenceException, "got " + cause);
    assertFalse(cause instanceof PersistenceOutcomeUnknownException);
    blob.script = null;
    assertEquals(
        1,
        join(persistence.get("r1")).resourceVersion(),
        "refuted delete leaves the resource durable and cached");
  }

  @Test
  public void deleteCasConflictFailsWithoutSideEffects() throws Exception {
    join(persistence.create(new FakeResource("r1", "p", 0))); // version 1
    int deletesBefore = blob.deleteCalls.get();
    int envelopesBefore = sink.envelopes.size();

    CompletionStage<FakeResource> stale = persistence.delete("r1", 99L);
    assertTrue(causeOf(stale) instanceof PreconditionFailedException);
    assertEquals(deletesBefore, blob.deleteCalls.get(), "stale delete never reaches the store");
    assertEquals(1, join(persistence.get("r1")).resourceVersion());
    assertEquals(envelopesBefore, sink.envelopes.size());

    // the correct version still deletes cleanly afterwards
    blob.script = null;
    join(persistence.delete("r1", 1L));
    assertNullSilently(persistence.get("r1"));
  }

  // ------------------------------------------------------------------ helpers

  static final class NoOpListener implements PersistenceListener<FakeResource> {
    @Override
    public void afterCreated(FakeResource resource) {}

    @Override
    public void afterModified(FakeResource resource) {}

    @Override
    public void afterDeleted(FakeResource resource) {}

    @Override
    public void postStart(FakeResource existingResource) {}
  }

  private void assertNullSilently(CompletionStage<FakeResource> stage) {
    Throwable cause = causeOf(stage);
    assertTrue(
        cause instanceof ResourceDoesNotExist, "expected ResourceDoesNotExist, got " + cause);
  }
}
