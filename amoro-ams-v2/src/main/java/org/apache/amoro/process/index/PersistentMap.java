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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * An immutable ordered map implemented as a structurally shared AVL tree.
 *
 * <p>Every mutation creates only nodes on the search path and nodes required by AVL rotations;
 * untouched subtrees are shared by the old and new snapshots. Keys and values must be non-null. The
 * comparator is part of a snapshot's identity and is reused by all derived snapshots.
 */
public final class PersistentMap<K, V> implements Iterable<Map.Entry<K, V>> {

  private final Comparator<? super K> comparator;
  private final Node<K, V> root;
  private final long lastMutationAllocatedNodes;
  private final long lastMutationVisitedNodes;

  private PersistentMap(
      Comparator<? super K> comparator,
      Node<K, V> root,
      long lastMutationAllocatedNodes,
      long lastMutationVisitedNodes) {
    this.comparator = comparator;
    this.root = root;
    this.lastMutationAllocatedNodes = lastMutationAllocatedNodes;
    this.lastMutationVisitedNodes = lastMutationVisitedNodes;
  }

  /** Returns an empty map ordered by the keys' natural order. */
  public static <K extends Comparable<? super K>, V> PersistentMap<K, V> empty() {
    return empty(Comparator.naturalOrder());
  }

  /** Returns an empty map ordered by {@code comparator}. */
  public static <K, V> PersistentMap<K, V> empty(Comparator<? super K> comparator) {
    return new PersistentMap<>(Objects.requireNonNull(comparator, "comparator"), null, 0, 0);
  }

  public int size() {
    return size(root);
  }

  public boolean isEmpty() {
    return root == null;
  }

  public V get(K key) {
    Objects.requireNonNull(key, "key");
    Node<K, V> current = root;
    while (current != null) {
      int comparison = comparator.compare(key, current.key);
      if (comparison == 0) {
        return current.value;
      }
      current = comparison < 0 ? current.left : current.right;
    }
    return null;
  }

  public boolean containsKey(K key) {
    return get(key) != null;
  }

  /** Returns a new snapshot containing {@code key}; this snapshot is never modified. */
  public PersistentMap<K, V> put(K key, V value) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(value, "value");
    MutationCounter counter = new MutationCounter();
    Node<K, V> updated = put(root, key, value, counter);
    return new PersistentMap<>(comparator, updated, counter.allocatedNodes, counter.visitedNodes);
  }

  /** Returns a new snapshot without {@code key}; removing an absent key allocates no tree node. */
  public PersistentMap<K, V> remove(K key) {
    Objects.requireNonNull(key, "key");
    MutationCounter counter = new MutationCounter();
    Node<K, V> updated = remove(root, key, counter);
    return new PersistentMap<>(comparator, updated, counter.allocatedNodes, counter.visitedNodes);
  }

  /**
   * Returns up to {@code limit} entries strictly after {@code cursorExclusive} in comparator order.
   * A null cursor starts at the first entry. The cursor does not need to exist in this snapshot.
   */
  public List<Map.Entry<K, V>> entriesAfter(K cursorExclusive, int limit) {
    return entriesAfterWithStats(cursorExclusive, limit).entries();
  }

  @Override
  public Iterator<Map.Entry<K, V>> iterator() {
    return new TreeIterator<>(root);
  }

  QueryResult<K, V> entriesAfterWithStats(K cursorExclusive, int limit) {
    requireNonNegative(limit, "limit");
    if (limit == 0 || root == null) {
      return new QueryResult<>(List.of(), 0);
    }
    QueryCounter counter = new QueryCounter();
    int offset = cursorExclusive == null ? 0 : rankStrictlyAfter(cursorExclusive, counter);
    return entriesByRankWithStats(offset, limit, counter);
  }

  QueryResult<K, V> entriesByRankWithStats(int offset, int limit) {
    requireNonNegative(offset, "offset");
    requireNonNegative(limit, "limit");
    return entriesByRankWithStats(offset, limit, new QueryCounter());
  }

  int height() {
    return height(root);
  }

  long lastMutationAllocatedNodes() {
    return lastMutationAllocatedNodes;
  }

  long lastMutationVisitedNodes() {
    return lastMutationVisitedNodes;
  }

  private QueryResult<K, V> entriesByRankWithStats(int offset, int limit, QueryCounter counter) {
    if (limit == 0 || offset >= size()) {
      return new QueryResult<>(List.of(), counter.visitedNodes);
    }
    long requestedEnd = (long) offset + limit;
    int endExclusive = (int) Math.min(size(), requestedEnd);
    List<Map.Entry<K, V>> entries = new ArrayList<>(endExclusive - offset);
    collectRankRange(root, offset, endExclusive, entries, counter);
    return new QueryResult<>(List.copyOf(entries), counter.visitedNodes);
  }

  private Node<K, V> put(Node<K, V> node, K key, V value, MutationCounter counter) {
    if (node == null) {
      return node(key, value, null, null, counter);
    }
    counter.visitedNodes++;
    int comparison = comparator.compare(key, node.key);
    if (comparison == 0) {
      if (Objects.equals(value, node.value)) {
        return node;
      }
      return node(key, value, node.left, node.right, counter);
    }
    if (comparison < 0) {
      Node<K, V> left = put(node.left, key, value, counter);
      return balance(node(node.key, node.value, left, node.right, counter), counter);
    }
    Node<K, V> right = put(node.right, key, value, counter);
    return balance(node(node.key, node.value, node.left, right, counter), counter);
  }

  private Node<K, V> remove(Node<K, V> node, K key, MutationCounter counter) {
    if (node == null) {
      return null;
    }
    counter.visitedNodes++;
    int comparison = comparator.compare(key, node.key);
    if (comparison < 0) {
      Node<K, V> left = remove(node.left, key, counter);
      if (left == node.left) {
        return node;
      }
      return balance(node(node.key, node.value, left, node.right, counter), counter);
    }
    if (comparison > 0) {
      Node<K, V> right = remove(node.right, key, counter);
      if (right == node.right) {
        return node;
      }
      return balance(node(node.key, node.value, node.left, right, counter), counter);
    }
    if (node.left == null) {
      return node.right;
    }
    if (node.right == null) {
      return node.left;
    }
    ExtractedMinimum<K, V> extracted = extractMinimum(node.right, counter);
    return balance(
        node(extracted.key, extracted.value, node.left, extracted.remainder, counter), counter);
  }

  private ExtractedMinimum<K, V> extractMinimum(Node<K, V> node, MutationCounter counter) {
    counter.visitedNodes++;
    if (node.left == null) {
      return new ExtractedMinimum<>(node.key, node.value, node.right);
    }
    ExtractedMinimum<K, V> extracted = extractMinimum(node.left, counter);
    Node<K, V> remainder =
        balance(node(node.key, node.value, extracted.remainder, node.right, counter), counter);
    return new ExtractedMinimum<>(extracted.key, extracted.value, remainder);
  }

  private Node<K, V> balance(Node<K, V> node, MutationCounter counter) {
    int balance = height(node.left) - height(node.right);
    if (balance > 1) {
      if (height(node.left.left) < height(node.left.right)) {
        Node<K, V> left = rotateLeft(node.left, counter);
        node = node(node.key, node.value, left, node.right, counter);
      }
      return rotateRight(node, counter);
    }
    if (balance < -1) {
      if (height(node.right.right) < height(node.right.left)) {
        Node<K, V> right = rotateRight(node.right, counter);
        node = node(node.key, node.value, node.left, right, counter);
      }
      return rotateLeft(node, counter);
    }
    return node;
  }

  private Node<K, V> rotateLeft(Node<K, V> node, MutationCounter counter) {
    Node<K, V> pivot = node.right;
    Node<K, V> left = node(node.key, node.value, node.left, pivot.left, counter);
    return node(pivot.key, pivot.value, left, pivot.right, counter);
  }

  private Node<K, V> rotateRight(Node<K, V> node, MutationCounter counter) {
    Node<K, V> pivot = node.left;
    Node<K, V> right = node(node.key, node.value, pivot.right, node.right, counter);
    return node(pivot.key, pivot.value, pivot.left, right, counter);
  }

  private Node<K, V> node(
      K key, V value, Node<K, V> left, Node<K, V> right, MutationCounter counter) {
    counter.allocatedNodes++;
    return new Node<>(key, value, left, right);
  }

  private int rankStrictlyAfter(K cursorExclusive, QueryCounter counter) {
    Objects.requireNonNull(cursorExclusive, "cursorExclusive");
    int rank = 0;
    Node<K, V> current = root;
    while (current != null) {
      counter.visitedNodes++;
      int comparison = comparator.compare(cursorExclusive, current.key);
      if (comparison < 0) {
        current = current.left;
      } else {
        rank += size(current.left) + 1;
        current = current.right;
      }
    }
    return rank;
  }

  private static <K, V> void collectRankRange(
      Node<K, V> node,
      int fromInclusive,
      int toExclusive,
      List<Map.Entry<K, V>> entries,
      QueryCounter counter) {
    if (node == null || fromInclusive >= toExclusive) {
      return;
    }
    counter.visitedNodes++;
    int leftSize = size(node.left);
    if (fromInclusive < leftSize) {
      collectRankRange(node.left, fromInclusive, Math.min(toExclusive, leftSize), entries, counter);
    }
    if (fromInclusive <= leftSize && leftSize < toExclusive) {
      entries.add(Map.entry(node.key, node.value));
    }
    if (toExclusive > leftSize + 1) {
      collectRankRange(
          node.right,
          Math.max(0, fromInclusive - leftSize - 1),
          toExclusive - leftSize - 1,
          entries,
          counter);
    }
  }

  private static void requireNonNegative(int value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " must be non-negative");
    }
  }

  private static int height(Node<?, ?> node) {
    return node == null ? 0 : node.height;
  }

  private static int size(Node<?, ?> node) {
    return node == null ? 0 : node.size;
  }

  static final class QueryResult<K, V> {
    private final List<Map.Entry<K, V>> entries;
    private final long visitedNodes;

    private QueryResult(List<Map.Entry<K, V>> entries, long visitedNodes) {
      this.entries = entries;
      this.visitedNodes = visitedNodes;
    }

    List<Map.Entry<K, V>> entries() {
      return entries;
    }

    long visitedNodes() {
      return visitedNodes;
    }
  }

  private static final class Node<K, V> {
    private final K key;
    private final V value;
    private final Node<K, V> left;
    private final Node<K, V> right;
    private final int height;
    private final int size;

    private Node(K key, V value, Node<K, V> left, Node<K, V> right) {
      this.key = key;
      this.value = value;
      this.left = left;
      this.right = right;
      this.height = 1 + Math.max(height(left), height(right));
      this.size = 1 + size(left) + size(right);
    }
  }

  private static final class ExtractedMinimum<K, V> {
    private final K key;
    private final V value;
    private final Node<K, V> remainder;

    private ExtractedMinimum(K key, V value, Node<K, V> remainder) {
      this.key = key;
      this.value = value;
      this.remainder = remainder;
    }
  }

  private static final class MutationCounter {
    private long allocatedNodes;
    private long visitedNodes;
  }

  private static final class QueryCounter {
    private long visitedNodes;
  }

  private static final class TreeIterator<K, V> implements Iterator<Map.Entry<K, V>> {
    private final Deque<Node<K, V>> stack = new ArrayDeque<>();

    private TreeIterator(Node<K, V> root) {
      pushLeft(root);
    }

    @Override
    public boolean hasNext() {
      return !stack.isEmpty();
    }

    @Override
    public Map.Entry<K, V> next() {
      if (stack.isEmpty()) {
        throw new NoSuchElementException();
      }
      Node<K, V> next = stack.pop();
      pushLeft(next.right);
      return Map.entry(next.key, next.value);
    }

    private void pushLeft(Node<K, V> node) {
      Node<K, V> current = node;
      while (current != null) {
        stack.push(current);
        current = current.left;
      }
    }
  }
}
