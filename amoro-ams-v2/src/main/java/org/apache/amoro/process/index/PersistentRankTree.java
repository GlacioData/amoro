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

package org.apache.amoro.process.index;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable ordered set with rank lookup, backed by subtree-sized structurally shared AVL nodes.
 */
public final class PersistentRankTree<E> implements Iterable<E> {

  private static final Object PRESENT = new Object();

  private final PersistentMap<E, Object> entries;

  private PersistentRankTree(PersistentMap<E, Object> entries) {
    this.entries = entries;
  }

  /** Returns an empty tree ordered by the entries' natural order. */
  public static <E extends Comparable<? super E>> PersistentRankTree<E> empty() {
    return empty(Comparator.naturalOrder());
  }

  /** Returns an empty tree ordered by {@code comparator}. */
  public static <E> PersistentRankTree<E> empty(Comparator<? super E> comparator) {
    return new PersistentRankTree<>(PersistentMap.empty(comparator));
  }

  public int size() {
    return entries.size();
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public boolean contains(E entry) {
    return entries.containsKey(Objects.requireNonNull(entry, "entry"));
  }

  public PersistentRankTree<E> add(E entry) {
    return new PersistentRankTree<>(entries.put(Objects.requireNonNull(entry, "entry"), PRESENT));
  }

  public PersistentRankTree<E> remove(E entry) {
    return new PersistentRankTree<>(entries.remove(Objects.requireNonNull(entry, "entry")));
  }

  /** Returns entries in the half-open rank range {@code [offset, offset + limit)}. */
  public List<E> rankSlice(int offset, int limit) {
    return rankSliceWithStats(offset, limit).entries();
  }

  /**
   * Returns up to {@code limit} entries strictly after {@code cursorExclusive}. A null cursor
   * starts at the first entry, and a removed cursor remains a valid ordering boundary.
   */
  public List<E> entriesAfter(E cursorExclusive, int limit) {
    return values(entries.entriesAfterWithStats(cursorExclusive, limit).entries());
  }

  @Override
  public Iterator<E> iterator() {
    Iterator<Map.Entry<E, Object>> iterator = entries.iterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public E next() {
        return iterator.next().getKey();
      }
    };
  }

  QueryResult<E> rankSliceWithStats(int offset, int limit) {
    PersistentMap.QueryResult<E, Object> result = entries.entriesByRankWithStats(offset, limit);
    return new QueryResult<>(values(result.entries()), result.visitedNodes());
  }

  int height() {
    return entries.height();
  }

  long lastMutationAllocatedNodes() {
    return entries.lastMutationAllocatedNodes();
  }

  long lastMutationVisitedNodes() {
    return entries.lastMutationVisitedNodes();
  }

  private static <E> List<E> values(List<Map.Entry<E, Object>> entries) {
    return entries.stream().map(Map.Entry::getKey).toList();
  }

  static final class QueryResult<E> {
    private final List<E> entries;
    private final long visitedNodes;

    private QueryResult(List<E> entries, long visitedNodes) {
      this.entries = entries;
      this.visitedNodes = visitedNodes;
    }

    List<E> entries() {
      return entries;
    }

    long visitedNodes() {
      return visitedNodes;
    }
  }
}
