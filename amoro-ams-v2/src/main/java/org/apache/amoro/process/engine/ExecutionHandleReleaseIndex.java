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

package org.apache.amoro.process.engine;

import org.apache.amoro.persistence.DurableStateProjection;
import org.apache.amoro.persistence.PersistenceChange;
import org.apache.amoro.persistence.PreparedProjectionUpdate;
import org.apache.amoro.resources.ProcessResource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Reconstructable cleanup index for execution handles whose terminal Process result is durable.
 *
 * <p>The projection only publishes after the Process DB write commits. Pending entries are ordered
 * by their retry deadline without scanning Process resources. Successful releases are remembered
 * for the current lifecycle so subsequent Process writes do not re-add them; restart intentionally
 * rebuilds all retained handles and relies on the engine's idempotent {@code release} contract.
 */
public final class ExecutionHandleReleaseIndex implements DurableStateProjection<ProcessResource> {

  private static final int STRIPES = 64;
  private static final long[] RETRY_MILLIS = {
    3_000L, 3_000L, 5_000L, 8_000L, 13_000L, 21_000L, 34_000L, 55_000L
  };

  private final ConcurrentHashMap<HandleKey, ReleaseEntry> byHandle = new ConcurrentHashMap<>();
  private final ConcurrentSkipListMap<DueKey, HandleKey> dueOrder = new ConcurrentSkipListMap<>();
  private final ConcurrentHashMap<String, Set<HandleKey>> byProcess = new ConcurrentHashMap<>();
  private final Set<HandleKey> releasedThisLifecycle = ConcurrentHashMap.newKeySet();
  private final Object[] locks = new Object[STRIPES];

  public ExecutionHandleReleaseIndex() {
    for (int index = 0; index < locks.length; index++) {
      locks[index] = new Object();
    }
  }

  @Override
  public PreparedProjectionUpdate prepare(PersistenceChange<ProcessResource> change) {
    Set<HandleKey> previous = handlesOf(change.previous());
    Set<HandleKey> current = handlesOf(change.current());
    Set<HandleKey> removed = new HashSet<>(previous);
    removed.removeAll(current);
    List<ReleaseSeed> added = seedsOf(change.current());
    added.removeIf(seed -> previous.contains(seed.key));
    return () -> {
      for (HandleKey key : removed) {
        removeResourceReference(key);
      }
      for (ReleaseSeed seed : added) {
        add(seed);
      }
    };
  }

  /**
   * Claims up to {@code limit} due entries. Claimed entries leave the due order until completion.
   */
  public List<ReleaseEntry> claimDue(Instant now, int limit) {
    Objects.requireNonNull(now, "now");
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be > 0");
    }
    List<ReleaseEntry> claimed = new ArrayList<>();
    int visited = 0;
    while (visited < limit) {
      java.util.Map.Entry<DueKey, HandleKey> first = dueOrder.firstEntry();
      if (first == null || first.getKey().nextReleaseAt.compareTo(now.toString()) > 0) {
        break;
      }
      visited++;
      HandleKey key = first.getValue();
      synchronized (lock(key)) {
        ReleaseEntry entry = byHandle.get(key);
        if (entry == null || entry.inFlight || !first.getKey().equals(entry.dueKey())) {
          dueOrder.remove(first.getKey(), key);
          continue;
        }
        if (!dueOrder.remove(first.getKey(), key)) {
          continue;
        }
        ReleaseEntry inFlight = entry.withInFlight(true);
        byHandle.put(key, inFlight);
        claimed.add(inFlight);
      }
    }
    return Collections.unmodifiableList(claimed);
  }

  public void releaseSucceeded(ReleaseEntry claimed) {
    HandleKey key = claimed.key;
    synchronized (lock(key)) {
      ReleaseEntry current = byHandle.get(key);
      if (current == null || !current.inFlight) {
        return;
      }
      byHandle.remove(key, current);
      removeProcessKey(current.processName, key);
      releasedThisLifecycle.add(key);
    }
  }

  public void releaseFailed(ReleaseEntry claimed, Instant now) {
    HandleKey key = claimed.key;
    synchronized (lock(key)) {
      ReleaseEntry current = byHandle.get(key);
      if (current == null || !current.inFlight) {
        return;
      }
      int nextAttempt = Math.min(current.retryAttempt + 1, RETRY_MILLIS.length - 1);
      long delay = RETRY_MILLIS[Math.min(current.retryAttempt, RETRY_MILLIS.length - 1)];
      ReleaseEntry retry =
          new ReleaseEntry(
              key,
              current.processName,
              current.finishedAt,
              now.plusMillis(delay).toString(),
              nextAttempt,
              false);
      byHandle.put(key, retry);
      dueOrder.put(retry.dueKey(), key);
    }
  }

  public boolean hasPendingForProcess(String processName) {
    Set<HandleKey> keys = byProcess.get(processName);
    return keys != null && !keys.isEmpty();
  }

  public int pendingCount() {
    return byHandle.size();
  }

  private void add(ReleaseSeed seed) {
    synchronized (lock(seed.key)) {
      if (releasedThisLifecycle.contains(seed.key) || byHandle.containsKey(seed.key)) {
        return;
      }
      ReleaseEntry entry =
          new ReleaseEntry(seed.key, seed.processName, seed.finishedAt, seed.finishedAt, 0, false);
      byHandle.put(seed.key, entry);
      byProcess
          .computeIfAbsent(seed.processName, ignored -> ConcurrentHashMap.newKeySet())
          .add(seed.key);
      dueOrder.put(entry.dueKey(), seed.key);
    }
  }

  private void removeResourceReference(HandleKey key) {
    synchronized (lock(key)) {
      ReleaseEntry entry = byHandle.remove(key);
      if (entry != null) {
        dueOrder.remove(entry.dueKey(), key);
        removeProcessKey(entry.processName, key);
      }
      releasedThisLifecycle.remove(key);
    }
  }

  private void removeProcessKey(String processName, HandleKey key) {
    Set<HandleKey> keys = byProcess.get(processName);
    if (keys == null) {
      return;
    }
    keys.remove(key);
    if (keys.isEmpty()) {
      byProcess.remove(processName, keys);
    }
  }

  private Object lock(HandleKey key) {
    return locks[(key.hashCode() & Integer.MAX_VALUE) % locks.length];
  }

  private static Set<HandleKey> handlesOf(ProcessResource resource) {
    Set<HandleKey> handles = new HashSet<>();
    for (ReleaseSeed seed : seedsOf(resource)) {
      handles.add(seed.key);
    }
    return handles;
  }

  private static List<ReleaseSeed> seedsOf(ProcessResource resource) {
    if (resource == null) {
      return new ArrayList<>();
    }
    List<ReleaseSeed> seeds = new ArrayList<>();
    ProcessResource.ProcessAttempt current = resource.status().attempt();
    if (current != null && current.externalId() != null && current.finishedAt() != null) {
      HandleKey key = new HandleKey(resource.spec().executionEngine(), current.externalId());
      seeds.add(new ReleaseSeed(key, resource.name(), current.finishedAt()));
    }
    for (ProcessResource.AttemptSummary attempt : resource.status().attemptHistory()) {
      if (attempt.externalId() != null && attempt.finishedAt() != null) {
        HandleKey key = new HandleKey(resource.spec().executionEngine(), attempt.externalId());
        seeds.add(new ReleaseSeed(key, resource.name(), attempt.finishedAt()));
      }
    }
    return seeds;
  }

  public static final class HandleKey {
    private final String executionEngine;
    private final String externalId;

    public HandleKey(String executionEngine, String externalId) {
      this.executionEngine = Objects.requireNonNull(executionEngine, "executionEngine");
      this.externalId = Objects.requireNonNull(externalId, "externalId");
    }

    public String executionEngine() {
      return executionEngine;
    }

    public String externalId() {
      return externalId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof HandleKey)) {
        return false;
      }
      HandleKey that = (HandleKey) other;
      return executionEngine.equals(that.executionEngine) && externalId.equals(that.externalId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(executionEngine, externalId);
    }
  }

  public static final class ReleaseEntry {
    private final HandleKey key;
    private final String processName;
    private final String finishedAt;
    private final String nextReleaseAt;
    private final int retryAttempt;
    private final boolean inFlight;

    private ReleaseEntry(
        HandleKey key,
        String processName,
        String finishedAt,
        String nextReleaseAt,
        int retryAttempt,
        boolean inFlight) {
      this.key = key;
      this.processName = processName;
      this.finishedAt = finishedAt;
      this.nextReleaseAt = nextReleaseAt;
      this.retryAttempt = retryAttempt;
      this.inFlight = inFlight;
    }

    public HandleKey key() {
      return key;
    }

    public String processName() {
      return processName;
    }

    public String nextReleaseAt() {
      return nextReleaseAt;
    }

    public int retryAttempt() {
      return retryAttempt;
    }

    private DueKey dueKey() {
      return new DueKey(nextReleaseAt, key.executionEngine, key.externalId);
    }

    private ReleaseEntry withInFlight(boolean value) {
      return new ReleaseEntry(key, processName, finishedAt, nextReleaseAt, retryAttempt, value);
    }
  }

  private static final class ReleaseSeed {
    private final HandleKey key;
    private final String processName;
    private final String finishedAt;

    private ReleaseSeed(HandleKey key, String processName, String finishedAt) {
      this.key = key;
      this.processName = processName;
      this.finishedAt = finishedAt;
    }
  }

  private static final class DueKey implements Comparable<DueKey> {
    private final String nextReleaseAt;
    private final String executionEngine;
    private final String externalId;

    private DueKey(String nextReleaseAt, String executionEngine, String externalId) {
      this.nextReleaseAt = nextReleaseAt;
      this.executionEngine = executionEngine;
      this.externalId = externalId;
    }

    @Override
    public int compareTo(DueKey other) {
      int byTime = nextReleaseAt.compareTo(other.nextReleaseAt);
      if (byTime != 0) {
        return byTime;
      }
      int byEngine = executionEngine.compareTo(other.executionEngine);
      return byEngine != 0 ? byEngine : externalId.compareTo(other.externalId);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof DueKey && compareTo((DueKey) other) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(nextReleaseAt, executionEngine, externalId);
    }
  }
}
