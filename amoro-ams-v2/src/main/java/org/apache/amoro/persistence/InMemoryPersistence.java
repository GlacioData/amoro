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

import org.apache.amoro.persistence.blob.BlobStore;
import org.apache.amoro.persistence.blob.BlobStoreActor;
import org.apache.amoro.persistence.exception.PersistenceException;
import org.apache.amoro.persistence.exception.PersistenceOutcomeUnknownException;
import org.apache.amoro.persistence.exception.PostCommitCleanupException;
import org.apache.amoro.persistence.exception.PreconditionFailedException;
import org.apache.amoro.persistence.exception.ResourceAlreadyExists;
import org.apache.amoro.persistence.exception.ResourceDoesNotExist;
import org.apache.amoro.serde.DeserializedResource;
import org.apache.amoro.serde.ResourceSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

/**
 * L5: the rebuildable in-memory read projection of one domain plus the durable-first write path
 * (framework spec §5.1). Callers' threads only build a logical {@link MutationCommand}; the
 * domain's single {@link BlobStoreActor} lane executes read-latest → detached copy → precondition
 * check → apply updateFn → assign version → serialize → projection prepare → DB write → (durable
 * success) publish canonical cache → commit projections → deletion hook → listener handoff →
 * complete the stage.
 *
 * <p>The database is the source of truth; this cache is rebuildable via {@link #postStart()}. Every
 * value crossing a boundary (create argument at enqueue, updateFn input/output, stage results,
 * get/select returns, listener envelopes) is a serde round-trip detached copy, so no caller can
 * alias the canonical snapshot. Reads never touch the store.
 */
public final class InMemoryPersistence<R extends ControlledResource>
    implements PersistenceService<R> {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryPersistence.class);

  private final PersistenceDomain domain;
  private final String resourceCollection;
  private final ResourceSerde<R> serde;
  private final BlobStore blobStore;
  private final BlobStoreActor lane;
  private final ListenerEventSink<R> eventSink;
  private final List<DurableStateProjection<R>> projections;
  private final DurableDeletionHook<R> deletionHook;

  /**
   * Canonical committed snapshots. Writers are serialized by the lane; readers get serde round-trip
   * detached copies, so entries are effectively immutable once published.
   */
  private final ConcurrentHashMap<String, R> canonical = new ConcurrentHashMap<>();

  private final CopyOnWriteArrayList<PersistenceListener<R>> listeners =
      new CopyOnWriteArrayList<>();

  /** Names fenced after an unresolved commit outcome or a failed deletion hook. */
  private final ConcurrentHashMap<String, Fence> fenced = new ConcurrentHashMap<>();

  /** Staged snapshot of a delete whose hook failed, retained for the repair retry. */
  private final ConcurrentHashMap<String, R> pendingHookCleanup = new ConcurrentHashMap<>();

  private enum Fence {
    OUTCOME_UNKNOWN,
    CLEANUP_PENDING
  }

  public InMemoryPersistence(
      PersistenceDomain domain,
      String resourceCollection,
      ResourceSerde<R> serde,
      BlobStore blobStore,
      int mailboxCapacity,
      ListenerEventSink<R> eventSink,
      List<DurableStateProjection<R>> projections,
      DurableDeletionHook<R> deletionHook) {
    this.domain = Objects.requireNonNull(domain, "domain");
    this.resourceCollection = Objects.requireNonNull(resourceCollection, "resourceCollection");
    this.serde = Objects.requireNonNull(serde, "serde");
    this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
    this.eventSink = Objects.requireNonNull(eventSink, "eventSink");
    this.projections = List.copyOf(Objects.requireNonNull(projections, "projections"));
    this.deletionHook = Objects.requireNonNull(deletionHook, "deletionHook");
    this.lane = BlobStoreActor.start(domain.domainName(), mailboxCapacity);
  }

  // ------------------------------------------------------------------ public API

  @Override
  public CompletionStage<R> create(R resource) {
    Objects.requireNonNull(resource, "resource");
    if (resource.resourceVersion() != 0L) {
      throw new IllegalArgumentException(
          "create argument must carry resourceVersion 0, got " + resource.resourceVersion());
    }
    // detach at enqueue time: the caller keeps its instance and can never change the candidate
    MutationCommand<R> command = MutationCommand.create(serde.detachedCopy(resource));
    return submit("create " + resource.name(), () -> executeCreate(command));
  }

  @Override
  public CompletionStage<R> modify(String id, Function<R, R> updateFn) {
    return modify(id, null, updateFn);
  }

  @Override
  public CompletionStage<R> modify(
      String id, long expectedResourceVersion, Function<R, R> updateFn) {
    return modify(id, Long.valueOf(expectedResourceVersion), updateFn);
  }

  private CompletionStage<R> modify(
      String id, Long expectedResourceVersion, Function<R, R> updateFn) {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(updateFn, "updateFn");
    MutationCommand<R> command =
        expectedResourceVersion == null
            ? MutationCommand.modify(id, updateFn)
            : MutationCommand.modify(id, expectedResourceVersion, updateFn);
    return submit("modify " + id, () -> executeModify(command));
  }

  @Override
  public CompletionStage<R> get(String id) {
    Objects.requireNonNull(id, "id");
    rejectLaneThread("get");
    R current = canonical.get(id);
    if (current == null) {
      return CompletableFuture.failedFuture(new ResourceDoesNotExist(domain.domainName(), id));
    }
    return CompletableFuture.completedFuture(serde.detachedCopy(current));
  }

  @Override
  public CompletionStage<R> delete(String id) {
    return delete(id, null);
  }

  @Override
  public CompletionStage<R> delete(String id, long expectedResourceVersion) {
    return delete(id, Long.valueOf(expectedResourceVersion));
  }

  private CompletionStage<R> delete(String id, Long expectedResourceVersion) {
    Objects.requireNonNull(id, "id");
    MutationCommand<R> command =
        expectedResourceVersion == null
            ? MutationCommand.delete(id)
            : MutationCommand.delete(id, expectedResourceVersion);
    return submit("delete " + id, () -> executeDelete(command));
  }

  @Override
  public CompletionStage<List<R>> select(Selector<R> selector) {
    Objects.requireNonNull(selector, "selector");
    rejectLaneThread("select");
    List<R> matches = new ArrayList<R>();
    for (R candidate : canonical.values()) {
      if (selector.collection() == null || selector.collection().equals(candidate.collection())) {
        if (selector.test(serde.detachedCopy(candidate))) {
          matches.add(serde.detachedCopy(candidate));
        }
      }
    }
    return CompletableFuture.completedFuture(matches);
  }

  @Override
  public void addListener(PersistenceListener<R> listener) {
    Objects.requireNonNull(listener, "listener");
    listeners.add(listener);
  }

  @Override
  public void postStart() {
    submit("postStart load", this::loadFromStore).join();
  }

  /**
   * Repair of one fenced name (framework spec §5.1): reload the durable row on the lane and
   * reconcile the cache; for a cleanup-pending fence the staged deletion hook is retried with the
   * staged snapshot. The fence clears only when the repair succeeded. A successful repair of a
   * CLEANUP_PENDING fence does NOT re-emit the AFTER_DELETED event that the failed delete never
   * handed off — listener-side compensation is the domain repair sweep's job (framework spec §6).
   */
  public void repair(String name) {
    Objects.requireNonNull(name, "name");
    submit(
            "repair " + name,
            () -> {
              repairInLane(name);
              return null;
            })
        .join();
  }

  /** Bounded shutdown of the domain's mutation lane. */
  public boolean shutdown(Duration timeout) {
    return lane.drain(timeout);
  }

  /** Read-only view of the currently fenced names (for health metrics and alerts, §5.2.4). */
  public java.util.Set<String> fencedNames() {
    return java.util.Collections.unmodifiableSet(fenced.keySet());
  }

  // ------------------------------------------------------------------ lane execution

  private R executeCreate(MutationCommand<R> command) {
    R detached = command.createResource();
    String name = detached.name();
    checkNotFenced(name);
    if (canonical.containsKey(name)) {
      throw new ResourceAlreadyExists(domain.domainName(), name);
    }
    if (!resourceCollection.equals(detached.collection())) {
      throw new IllegalArgumentException(
          "resource collection '"
              + detached.collection()
              + "' does not match the domain's '"
              + resourceCollection
              + "'");
    }
    // re-detach after withResourceVersion: the resource's own factory code is untrusted and
    // could hand back an aliased instance; the published canonical must be framework-isolated
    R candidate = castResource(serde.detachedCopy(castResource(detached.withResourceVersion(1L))));
    byte[] candidateBytes = serde.serialize(candidate);
    PreparedProjectionUpdate[] prepared =
        prepareProjections(PersistenceChange.created(serde.detachedCopy(candidate)));

    if (durableWrite(command.type(), name, candidateBytes, null) == Outcome.REFUTED) {
      throw new PersistenceException(
          "create of " + name + " did not commit (resolved as previous state by point read)");
    }
    publish(name, candidate, prepared);
    handoff(ListenerEnvelope.EventType.AFTER_CREATED, candidate);
    return serde.detachedCopy(candidate);
  }

  private R executeModify(MutationCommand<R> command) {
    String id = command.name();
    checkNotFenced(id);
    R current = canonical.get(id);
    if (current == null) {
      throw new ResourceDoesNotExist(domain.domainName(), id);
    }
    if (command.expectedResourceVersion() != null
        && current.resourceVersion() != command.expectedResourceVersion().longValue()) {
      throw new PreconditionFailedException(
          domain.domainName(), id, command.expectedResourceVersion(), current.resourceVersion());
    }

    R input = serde.detachedCopy(current);
    R next = command.updateFn().apply(input);
    if (next == null) {
      throw new IllegalArgumentException(
          "update function must return a new resource instance, got null for " + id);
    }
    if (!next.name().equals(current.name()) || !next.collection().equals(current.collection())) {
      throw new IllegalArgumentException(
          "update function may not change name/collection: "
              + current.name()
              + " -> "
              + next.name());
    }
    R candidate =
        castResource(
            serde.detachedCopy(
                castResource(next.withResourceVersion(current.resourceVersion() + 1L))));
    byte[] candidateBytes = serde.serialize(candidate);
    PreparedProjectionUpdate[] prepared =
        prepareProjections(PersistenceChange.modified(serde.detachedCopy(current), candidate));

    if (durableWrite(command.type(), id, candidateBytes, current) == Outcome.REFUTED) {
      throw new PersistenceException(
          "modify of " + id + " did not commit (resolved as previous state by point read)");
    }
    publish(id, candidate, prepared);
    handoff(ListenerEnvelope.EventType.AFTER_MODIFIED, candidate);
    return serde.detachedCopy(candidate);
  }

  private R executeDelete(MutationCommand<R> command) {
    String id = command.name();
    checkNotFenced(id);
    R current = canonical.get(id);
    if (current == null) {
      throw new ResourceDoesNotExist(domain.domainName(), id);
    }
    if (command.expectedResourceVersion() != null
        && current.resourceVersion() != command.expectedResourceVersion().longValue()) {
      throw new PreconditionFailedException(
          domain.domainName(), id, command.expectedResourceVersion(), current.resourceVersion());
    }

    R detachedCurrent = serde.detachedCopy(current);
    PreparedProjectionUpdate[] prepared =
        prepareProjections(PersistenceChange.deleted(detachedCurrent));

    // for DELETE the candidate state is the row's absence (candidateBytes = null)
    if (durableWrite(command.type(), id, null, current) == Outcome.REFUTED) {
      throw new PersistenceException(
          "delete of " + id + " did not commit (resolved as previous state by point read)");
    }

    // durable delete confirmed: publish the removal, then run the in-lane hook before the
    // stage completes and before any same-name mutation dequeues
    canonical.remove(id);
    commitProjections(prepared);
    try {
      deletionHook.afterDurableDelete(detachedCurrent);
    } catch (Throwable hookFailure) {
      pendingHookCleanup.put(id, detachedCurrent);
      fenced.put(id, Fence.CLEANUP_PENDING);
      throw new PostCommitCleanupException(domain.domainName(), id, hookFailure);
    }
    handoff(ListenerEnvelope.EventType.AFTER_DELETED, detachedCurrent);
    return serde.detachedCopy(detachedCurrent);
  }

  private Void loadFromStore() {
    List<R> lazyUpgrades = new ArrayList<R>();
    blobStore.forEach(
        resourceCollection,
        (name, bytes) -> {
          DeserializedResource<R> deserialized = serde.deserialize(bytes);
          canonical.put(name, deserialized.resource());
          if (deserialized.modifiedDuringDeserialization()) {
            lazyUpgrades.add(deserialized.resource());
          }
        });
    // lazy serde upgrades write the latest version back to the store (no downtime migration)
    for (R upgraded : lazyUpgrades) {
      blobStore.update(resourceCollection, upgraded.name(), serde.serialize(upgraded));
    }
    // rebuild the domain projections from the durable state (framework spec §6 startup chain)
    for (R resource : canonical.values()) {
      PreparedProjectionUpdate[] prepared =
          prepareProjections(PersistenceChange.created(serde.detachedCopy(resource)));
      commitProjections(prepared);
    }
    for (R resource : canonical.values()) {
      handoff(ListenerEnvelope.EventType.POST_START, resource);
    }
    return null;
  }

  private void repairInLane(String name) {
    Fence fence = fenced.get(name);
    if (fence == Fence.CLEANUP_PENDING) {
      R staged = pendingHookCleanup.get(name);
      if (staged != null) {
        deletionHook.afterDurableDelete(staged); // throws -> stays fenced
        pendingHookCleanup.remove(name);
      }
      fenced.remove(name);
      return;
    }
    // OUTCOME_UNKNOWN: reload the durable row and reconcile the cache with the store
    Optional<byte[]> row = blobStore.find(resourceCollection, name);
    if (row.isPresent()) {
      canonical.put(name, serde.deserialize(row.get()).resource());
    } else {
      canonical.remove(name);
    }
    fenced.remove(name);
  }

  // ------------------------------------------------------------------ durable write + resolution

  private enum Outcome {
    CONFIRMED,
    REFUTED
  }

  /**
   * Performs the store write; an indeterminate store exception is resolved with a fresh point read
   * on the same lane: candidate state → treat as committed; previous state → definitively not
   * committed; anything else (including an unreadable store) → fence the name and fail.
   *
   * @param candidateBytes null for DELETE (its candidate state is the row's absence)
   * @param previous the current committed resource; serialized lazily only on the failure path
   *     (null for CREATE, whose previous state is the row's absence)
   */
  private Outcome durableWrite(
      MutationCommand.Type type, String name, byte[] candidateBytes, R previous) {
    RuntimeException connectionFailure;
    try {
      switch (type) {
        case CREATE:
          blobStore.insert(resourceCollection, name, candidateBytes);
          return Outcome.CONFIRMED;
        case MODIFY:
          if (!blobStore.update(resourceCollection, name, candidateBytes)) {
            throw new ResourceDoesNotExist(domain.domainName(), name);
          }
          return Outcome.CONFIRMED;
        case DELETE:
          if (!blobStore.delete(resourceCollection, name)) {
            throw new ResourceDoesNotExist(domain.domainName(), name);
          }
          return Outcome.CONFIRMED;
        default:
          throw new AssertionError("unknown mutation type " + type);
      }
    } catch (ResourceAlreadyExists definitive) {
      throw definitive;
    } catch (ResourceDoesNotExist definitive) {
      throw definitive;
    } catch (RuntimeException failure) {
      connectionFailure = failure;
    }
    // previousBytes is serialized only now: the failure path is rare and the canonical cache is
    // unchanged at this point, so this is exactly the pre-write committed state
    byte[] previousBytes = previous == null ? null : serde.serialize(previous);
    return resolveUnknownOutcome(name, candidateBytes, previousBytes, connectionFailure);
  }

  private Outcome resolveUnknownOutcome(
      String name, byte[] candidateBytes, byte[] previousBytes, RuntimeException cause) {
    Optional<byte[]> pointRead;
    try {
      pointRead = blobStore.find(resourceCollection, name);
    } catch (RuntimeException readFailure) {
      fenced.put(name, Fence.OUTCOME_UNKNOWN);
      throw new PersistenceOutcomeUnknownException(domain.domainName(), name, cause);
    }
    byte[] present = pointRead.orElse(null);
    if (Arrays.equals(present, candidateBytes)) {
      return Outcome.CONFIRMED; // candidate state confirmed by the durable row
    }
    if (Arrays.equals(present, previousBytes)) {
      return Outcome.REFUTED; // previous state: the write never committed
    }
    fenced.put(name, Fence.OUTCOME_UNKNOWN);
    throw new PersistenceOutcomeUnknownException(domain.domainName(), name, cause);
  }

  // ------------------------------------------------------------------ publish + events

  private void publish(String name, R candidate, PreparedProjectionUpdate[] prepared) {
    canonical.put(name, candidate);
    commitProjections(prepared);
  }

  private PreparedProjectionUpdate[] prepareProjections(PersistenceChange<R> change) {
    PreparedProjectionUpdate[] prepared = new PreparedProjectionUpdate[projections.size()];
    for (int i = 0; i < projections.size(); i++) {
      prepared[i] = projections.get(i).prepare(change); // throws -> abort before the DB write
    }
    return prepared;
  }

  private void commitProjections(PreparedProjectionUpdate[] prepared) {
    for (PreparedProjectionUpdate update : prepared) {
      try {
        update.commit(); // contract: non-throwing, O(1) or key-count bounded
      } catch (RuntimeException contractViolation) {
        LOG.error(
            "Projection commit threw against its contract for domain {}; ignoring.",
            domain.domainName(),
            contractViolation);
      }
    }
  }

  private void handoff(ListenerEnvelope.EventType type, R resource) {
    for (PersistenceListener<R> listener : listeners) {
      ListenerEnvelope<R> envelope =
          new ListenerEnvelope<R>(
              listenerIdentity(listener),
              domain.domainName(),
              resource.name(),
              resource.resourceVersion(),
              type,
              serde.detachedCopy(resource));
      HandoffResult result;
      try {
        result = eventSink.handoff(envelope);
      } catch (RuntimeException sinkFailure) {
        // a misbehaving sink must never fail an already-durable mutation (framework spec §6)
        LOG.warn(
            "Listener sink threw while handing off {} for {}/{}; treating as dropped.",
            type,
            domain.domainName(),
            resource.name(),
            sinkFailure);
        result = HandoffResult.DROPPED;
      }
      if (result == HandoffResult.DROPPED) {
        LOG.warn(
            "Listener event {} for {}/{} was dropped by the sink; the domain repair sweep"
                + " must compensate.",
            type,
            domain.domainName(),
            resource.name());
      }
    }
  }

  private void checkNotFenced(String name) {
    Fence fence = fenced.get(name);
    if (fence == Fence.OUTCOME_UNKNOWN) {
      throw new PersistenceOutcomeUnknownException(domain.domainName(), name);
    }
    if (fence == Fence.CLEANUP_PENDING) {
      throw new PostCommitCleanupException(domain.domainName(), name);
    }
  }

  private void rejectLaneThread(String operation) {
    if (lane.isLaneThread()) {
      // fail fast instead of deadlocking the single-threaded lane on a reentrant call
      throw new IllegalStateException(
          "reentrant " + operation + " from inside the mutation lane is prohibited");
    }
  }

  private <T> CompletableFuture<T> submit(String description, Callable<T> task) {
    rejectLaneThread(description);
    try {
      return lane.submit(description, task);
    } catch (RejectedExecutionException e) {
      CompletableFuture<T> failed = new CompletableFuture<T>();
      failed.completeExceptionally(e);
      return failed;
    }
  }

  private static String listenerIdentity(PersistenceListener<?> listener) {
    return listener.getClass().getName()
        + "@"
        + Integer.toHexString(System.identityHashCode(listener));
  }

  @SuppressWarnings("unchecked")
  private static <R extends ControlledResource> R castResource(ControlledResource resource) {
    return (R) resource;
  }
}
