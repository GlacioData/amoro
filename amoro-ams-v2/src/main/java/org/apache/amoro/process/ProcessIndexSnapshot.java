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

package org.apache.amoro.process;

import org.apache.amoro.process.index.PersistentMap;
import org.apache.amoro.process.index.PersistentRankTree;
import org.apache.amoro.resources.ProcessResource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One immutable, atomically published Process read model. Every map and ordered view is a
 * structurally shared AVL snapshot, so a resource mutation touches a fixed number of {@code O(log
 * n)} paths while point reads, rank pages and repair cursors remain mutually consistent.
 */
public final class ProcessIndexSnapshot {

  private final PersistentMap<String, ProcessResource> resourcesByName;
  private final PersistentMap<String, String> activeByTableAction;
  private final PersistentMap<String, String> idempotencyByKey;
  private final PersistentRankTree<ActiveEntry> activeOrder;
  private final PersistentRankTree<ExpiryEntry> expiryOrder;
  private final PersistentMap<ReadViewKey, PersistentRankTree<ReadEntry>> readViews;

  private ProcessIndexSnapshot(
      PersistentMap<String, ProcessResource> resourcesByName,
      PersistentMap<String, String> activeByTableAction,
      PersistentMap<String, String> idempotencyByKey,
      PersistentRankTree<ActiveEntry> activeOrder,
      PersistentRankTree<ExpiryEntry> expiryOrder,
      PersistentMap<ReadViewKey, PersistentRankTree<ReadEntry>> readViews) {
    this.resourcesByName = resourcesByName;
    this.activeByTableAction = activeByTableAction;
    this.idempotencyByKey = idempotencyByKey;
    this.activeOrder = activeOrder;
    this.expiryOrder = expiryOrder;
    this.readViews = readViews;
  }

  public static ProcessIndexSnapshot empty() {
    return new ProcessIndexSnapshot(
        PersistentMap.empty(),
        PersistentMap.empty(),
        PersistentMap.empty(),
        PersistentRankTree.empty(),
        PersistentRankTree.empty(),
        PersistentMap.empty());
  }

  /** Compatibility/testing view. Production paths use point/rank APIs and never materialize it. */
  public Map<String, ProcessResource> resourcesByName() {
    Map<String, ProcessResource> materialized = new LinkedHashMap<>();
    for (Map.Entry<String, ProcessResource> entry : resourcesByName) {
      materialized.put(entry.getKey(), entry.getValue());
    }
    return Collections.unmodifiableMap(materialized);
  }

  public int resourceCount() {
    return resourcesByName.size();
  }

  public Optional<ProcessResource> find(String name) {
    return Optional.ofNullable(resourcesByName.get(name));
  }

  public Optional<String> activeProcessOf(String tableId, String action) {
    return Optional.ofNullable(activeByTableAction.get(tableActionKey(tableId, action)));
  }

  public Optional<String> idempotentHolderOf(String tableId, String action, String keyHash) {
    return Optional.ofNullable(
        idempotencyByKey.get(tableActionKey(tableId, action) + "|" + keyHash));
  }

  public List<ActiveEntry> activeAfter(ActiveEntry cursorExclusive, int limit) {
    return activeOrder.entriesAfter(cursorExclusive, limit);
  }

  public int activeCount() {
    return activeOrder.size();
  }

  public List<ExpiryEntry> expiryAfter(ExpiryEntry cursorExclusive, int limit) {
    return expiryOrder.entriesAfter(cursorExclusive, limit);
  }

  /** Compatibility/testing view, materialized only when explicitly requested. */
  public List<String> expiryOrder() {
    List<String> entries = new ArrayList<>(expiryOrder.size());
    for (ExpiryEntry entry : expiryOrder) {
      entries.add(entry.finishedAt + "|" + entry.name);
    }
    return Collections.unmodifiableList(entries);
  }

  public int listTotal(String tableId, String action, String phase) {
    PersistentRankTree<ReadEntry> view =
        readViews.get(new ReadViewKey(tableId, normalize(action), normalize(phase)));
    return view == null ? 0 : view.size();
  }

  public List<ProcessResource> list(
      String tableId, String action, String phase, long offset, int limit) {
    if (offset < 0 || limit < 0) {
      throw new IllegalArgumentException("offset and limit must be non-negative");
    }
    if (offset > Integer.MAX_VALUE || limit == 0) {
      return Collections.emptyList();
    }
    PersistentRankTree<ReadEntry> view =
        readViews.get(new ReadViewKey(tableId, normalize(action), normalize(phase)));
    if (view == null || offset >= view.size()) {
      return Collections.emptyList();
    }
    int boundedCapacity = Math.min(limit, view.size() - (int) offset);
    List<ProcessResource> page = new ArrayList<>(boundedCapacity);
    for (ReadEntry entry : view.rankSlice((int) offset, limit)) {
      ProcessResource resource = resourcesByName.get(entry.name);
      if (resource == null) {
        throw new IllegalStateException("read view references missing Process " + entry.name);
      }
      page.add(resource);
    }
    return Collections.unmodifiableList(page);
  }

  ProcessIndexSnapshot apply(ProcessResource previous, ProcessResource current) {
    PersistentMap<String, ProcessResource> resources = resourcesByName;
    PersistentMap<String, String> active = activeByTableAction;
    PersistentMap<String, String> idempotent = idempotencyByKey;
    PersistentRankTree<ActiveEntry> activeTree = activeOrder;
    PersistentRankTree<ExpiryEntry> expiryTree = expiryOrder;
    PersistentMap<ReadViewKey, PersistentRankTree<ReadEntry>> views = readViews;
    boolean sameIdentity =
        previous != null && current != null && previous.name().equals(current.name());
    boolean sameAdmission =
        sameIdentity
            && tableActionKey(previous).equals(tableActionKey(current))
            && previous
                .spec()
                .request()
                .idempotencyKeyHash()
                .equals(current.spec().request().idempotencyKeyHash());
    boolean sameActiveEntry =
        sameAdmission
            && !ProcessFinality.isFinal(previous)
            && !ProcessFinality.isFinal(current)
            && ActiveEntry.of(previous).compareTo(ActiveEntry.of(current)) == 0;
    boolean sameReadMembership =
        sameIdentity
            && previous.spec().table().tableId().equals(current.spec().table().tableId())
            && previous.spec().action().equals(current.spec().action())
            && previous.status().phase().equals(current.status().phase())
            && previous.spec().createdAt().equals(current.spec().createdAt());

    if (previous != null) {
      String tableAction = tableActionKey(previous);
      if (!sameActiveEntry && previous.name().equals(active.get(tableAction))) {
        active = active.remove(tableAction);
      }
      if (!sameAdmission) {
        idempotent =
            idempotent.remove(tableAction + "|" + previous.spec().request().idempotencyKeyHash());
      }
      if (!ProcessFinality.isFinal(previous) && !sameActiveEntry) {
        activeTree = activeTree.remove(ActiveEntry.of(previous));
      }
      if (eligibleForExpiry(previous)) {
        expiryTree = expiryTree.remove(ExpiryEntry.of(previous));
      }
      if (!sameReadMembership) {
        views = removeReadViews(views, previous);
      }
      resources = resources.remove(previous.name());
    }

    if (current != null) {
      String tableAction = tableActionKey(current);
      String idempotencyScope = tableAction + "|" + current.spec().request().idempotencyKeyHash();
      if (!sameAdmission) {
        String idempotencyIncumbent = idempotent.get(idempotencyScope);
        if (idempotencyIncumbent != null && !idempotencyIncumbent.equals(current.name())) {
          throw new ProcessIndexConflictException(
              "IDEMPOTENCY_KEY", idempotencyScope, idempotencyIncumbent, current.name());
        }
        idempotent = idempotent.put(idempotencyScope, current.name());
      }
      if (!ProcessFinality.isFinal(current)) {
        if (!sameActiveEntry) {
          String activeIncumbent = active.get(tableAction);
          if (activeIncumbent != null && !activeIncumbent.equals(current.name())) {
            throw new ProcessIndexConflictException(
                "ACTIVE_PROCESS", tableAction, activeIncumbent, current.name());
          }
          active = active.put(tableAction, current.name());
          activeTree = activeTree.add(ActiveEntry.of(current));
        }
      }
      if (eligibleForExpiry(current)) {
        expiryTree = expiryTree.add(ExpiryEntry.of(current));
      }
      resources = resources.put(current.name(), current);
      if (!sameReadMembership) {
        views = addReadViews(views, current);
      }
    }
    return new ProcessIndexSnapshot(resources, active, idempotent, activeTree, expiryTree, views);
  }

  private static PersistentMap<ReadViewKey, PersistentRankTree<ReadEntry>> addReadViews(
      PersistentMap<ReadViewKey, PersistentRankTree<ReadEntry>> views, ProcessResource resource) {
    ReadEntry entry = ReadEntry.of(resource);
    for (ReadViewKey key : ReadViewKey.keysOf(resource)) {
      PersistentRankTree<ReadEntry> tree = views.get(key);
      views =
          views.put(key, (tree == null ? PersistentRankTree.<ReadEntry>empty() : tree).add(entry));
    }
    return views;
  }

  private static PersistentMap<ReadViewKey, PersistentRankTree<ReadEntry>> removeReadViews(
      PersistentMap<ReadViewKey, PersistentRankTree<ReadEntry>> views, ProcessResource resource) {
    ReadEntry entry = ReadEntry.of(resource);
    for (ReadViewKey key : ReadViewKey.keysOf(resource)) {
      PersistentRankTree<ReadEntry> tree = views.get(key);
      if (tree == null) {
        continue;
      }
      PersistentRankTree<ReadEntry> next = tree.remove(entry);
      views = next.isEmpty() ? views.remove(key) : views.put(key, next);
    }
    return views;
  }

  private static boolean eligibleForExpiry(ProcessResource resource) {
    return ProcessFinality.isFinal(resource) && resource.status().finishedAt() != null;
  }

  private static String tableActionKey(ProcessResource resource) {
    return tableActionKey(resource.spec().table().tableId(), resource.spec().action());
  }

  private static String tableActionKey(String tableId, String action) {
    return tableId + "|" + action;
  }

  private static String normalize(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  public static final class ActiveEntry implements Comparable<ActiveEntry> {
    private final String createdAt;
    private final String name;

    public ActiveEntry(String createdAt, String name) {
      this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
      this.name = Objects.requireNonNull(name, "name");
    }

    static ActiveEntry of(ProcessResource resource) {
      return new ActiveEntry(resource.spec().createdAt(), resource.name());
    }

    public String createdAt() {
      return createdAt;
    }

    public String name() {
      return name;
    }

    @Override
    public int compareTo(ActiveEntry other) {
      int byTime = Instant.parse(createdAt).compareTo(Instant.parse(other.createdAt));
      return byTime != 0 ? byTime : name.compareTo(other.name);
    }
  }

  public static final class ExpiryEntry implements Comparable<ExpiryEntry> {
    private final String finishedAt;
    private final String name;
    private final long resourceVersion;

    public ExpiryEntry(String finishedAt, String name, long resourceVersion) {
      this.finishedAt = Objects.requireNonNull(finishedAt, "finishedAt");
      this.name = Objects.requireNonNull(name, "name");
      this.resourceVersion = resourceVersion;
    }

    static ExpiryEntry of(ProcessResource resource) {
      return new ExpiryEntry(
          resource.status().finishedAt(), resource.name(), resource.resourceVersion());
    }

    public String finishedAt() {
      return finishedAt;
    }

    public String name() {
      return name;
    }

    public long resourceVersion() {
      return resourceVersion;
    }

    @Override
    public int compareTo(ExpiryEntry other) {
      int byTime = Instant.parse(finishedAt).compareTo(Instant.parse(other.finishedAt));
      return byTime != 0 ? byTime : name.compareTo(other.name);
    }
  }

  private static final class ReadEntry implements Comparable<ReadEntry> {
    private final String createdAt;
    private final String name;

    private ReadEntry(String createdAt, String name) {
      this.createdAt = createdAt;
      this.name = name;
    }

    private static ReadEntry of(ProcessResource resource) {
      return new ReadEntry(resource.spec().createdAt(), resource.name());
    }

    @Override
    public int compareTo(ReadEntry other) {
      int newestFirst = Instant.parse(other.createdAt).compareTo(Instant.parse(createdAt));
      return newestFirst != 0 ? newestFirst : other.name.compareTo(name);
    }
  }

  private static final class ReadViewKey implements Comparable<ReadViewKey> {
    private final String tableId;
    private final String action;
    private final String phase;

    private ReadViewKey(String tableId, String action, String phase) {
      this.tableId = tableId;
      this.action = action;
      this.phase = phase;
    }

    private static List<ReadViewKey> keysOf(ProcessResource resource) {
      String tableId = resource.spec().table().tableId();
      String action = resource.spec().action();
      String phase = resource.status().phase();
      return List.of(
          new ReadViewKey(tableId, null, null),
          new ReadViewKey(tableId, action, null),
          new ReadViewKey(tableId, null, phase),
          new ReadViewKey(tableId, action, phase));
    }

    @Override
    public int compareTo(ReadViewKey other) {
      int table = tableId.compareTo(other.tableId);
      if (table != 0) {
        return table;
      }
      int byAction = nullable(action).compareTo(nullable(other.action));
      return byAction != 0 ? byAction : nullable(phase).compareTo(nullable(other.phase));
    }

    private static String nullable(String value) {
      return value == null ? "" : value;
    }
  }
}
