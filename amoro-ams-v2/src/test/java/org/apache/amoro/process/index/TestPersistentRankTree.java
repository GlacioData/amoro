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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/** Contract and complexity tests for the immutable subtree-sized rank tree. */
public class TestPersistentRankTree {

  @Test
  public void rankSlicesAndOlderSnapshotsRemainStable() {
    PersistentRankTree<Integer> empty = PersistentRankTree.empty();
    PersistentRankTree<Integer> first = empty.add(4).add(1).add(3).add(2).add(5);
    PersistentRankTree<Integer> removed = first.remove(3);

    assertTrue(empty.isEmpty());
    assertEquals(List.of(1, 2, 3, 4, 5), first.rankSlice(0, 10));
    assertEquals(List.of(2, 3, 4), first.rankSlice(1, 3));
    assertEquals(List.of(4, 5), first.rankSlice(3, 20));
    assertEquals(List.of(), first.rankSlice(first.size(), 1));
    assertEquals(List.of(1, 2, 4, 5), removed.rankSlice(0, 10));
    assertEquals(List.of(1, 2, 3, 4, 5), first.rankSlice(0, 10));
    assertThrows(IllegalArgumentException.class, () -> first.rankSlice(-1, 1));
    assertThrows(IllegalArgumentException.class, () -> first.rankSlice(0, -1));
  }

  @Test
  public void entriesAfterUsesComparatorBoundaryEvenWhenCursorWasRemoved() {
    PersistentRankTree<Integer> tree =
        PersistentRankTree.<Integer>empty(Comparator.reverseOrder())
            .add(1)
            .add(5)
            .add(2)
            .add(4)
            .add(3);

    assertEquals(List.of(5, 4), tree.entriesAfter(null, 2));
    assertEquals(List.of(2, 1), tree.entriesAfter(3, 10));
    assertEquals(List.of(2, 1), tree.remove(3).entriesAfter(3, 10));
    assertEquals(List.of(), tree.entriesAfter(0, 5));
  }

  @Test
  public void randomizedMutationsAndDeepRankPageMatchTreeSet() {
    int count = 100_000;
    List<Integer> input = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      input.add(i);
    }
    Collections.shuffle(input, new Random(18_618L));

    PersistentRankTree<Integer> tree = PersistentRankTree.empty();
    TreeSet<Integer> reference = new TreeSet<>();
    long maxAllocated = 0;
    for (int value : input) {
      tree = tree.add(value);
      reference.add(value);
      maxAllocated = Math.max(maxAllocated, tree.lastMutationAllocatedNodes());
    }
    Collections.shuffle(input, new Random(45_109L));
    for (int i = 0; i < count / 3; i++) {
      tree = tree.remove(input.get(i));
      reference.remove(input.get(i));
      maxAllocated = Math.max(maxAllocated, tree.lastMutationAllocatedNodes());
    }

    assertEquals(new ArrayList<>(reference), tree.rankSlice(0, tree.size()));
    int offset = tree.size() - 50;
    PersistentRankTree.QueryResult<Integer> page = tree.rankSliceWithStats(offset, 25);
    assertEquals(new ArrayList<>(reference).subList(offset, offset + 25), page.entries());

    int logarithmicHeightBound = 2 * ceilLog2(count + 1);
    assertTrue(tree.height() <= logarithmicHeightBound, "height=" + tree.height());
    assertTrue(maxAllocated <= 4L * logarithmicHeightBound + 8, "allocated=" + maxAllocated);
    assertTrue(
        page.visitedNodes() <= logarithmicHeightBound + page.entries().size() + 2,
        "rank slice visited=" + page.visitedNodes());
    assertFalse(tree.contains(input.get(0)));
  }

  private static int ceilLog2(int value) {
    return 32 - Integer.numberOfLeadingZeros(value - 1);
  }
}
