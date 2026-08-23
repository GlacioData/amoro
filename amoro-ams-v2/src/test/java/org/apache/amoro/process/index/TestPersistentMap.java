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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/** Contract and complexity tests for the structurally shared persistent map. */
public class TestPersistentMap {

  @Test
  public void mutationsNeverChangeOlderSnapshots() {
    PersistentMap<Integer, String> empty = PersistentMap.empty();
    PersistentMap<Integer, String> one = empty.put(1, "one");
    PersistentMap<Integer, String> two = one.put(2, "two");
    PersistentMap<Integer, String> replaced = two.put(1, "ONE");
    PersistentMap<Integer, String> removed = replaced.remove(2);

    assertTrue(empty.isEmpty());
    assertEquals("one", one.get(1));
    assertNull(one.get(2));
    assertEquals("one", two.get(1));
    assertEquals("two", two.get(2));
    assertEquals("ONE", replaced.get(1));
    assertEquals("two", replaced.get(2));
    assertEquals("ONE", removed.get(1));
    assertNull(removed.get(2));
    assertEquals(2, two.size());
    assertEquals(1, removed.size());
  }

  @Test
  public void supportsComparatorOrderAndStableExclusiveCursor() {
    PersistentMap<Integer, String> map =
        PersistentMap.<Integer, String>empty(Comparator.reverseOrder())
            .put(1, "one")
            .put(3, "three")
            .put(2, "two")
            .put(5, "five")
            .put(4, "four");

    assertEquals(List.of(5, 4, 3), keys(map.entriesAfter(null, 3)));
    assertEquals(List.of(2, 1), keys(map.entriesAfter(3, 10)));

    PersistentMap<Integer, String> withoutCursor = map.remove(3);
    assertEquals(List.of(2, 1), keys(withoutCursor.entriesAfter(3, 10)));
    assertEquals(List.of(), keys(withoutCursor.entriesAfter(0, 10)));
    assertEquals(List.of(), withoutCursor.entriesAfter(null, 0));
  }

  @Test
  public void oneHundredThousandRandomKeysMatchReferenceWithinLogarithmicBounds() {
    int count = 100_000;
    List<Integer> keys = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      keys.add(i);
    }
    Collections.shuffle(keys, new Random(8_241_337L));

    PersistentMap<Integer, Integer> map = PersistentMap.empty();
    TreeMap<Integer, Integer> reference = new TreeMap<>();
    long maxAllocated = 0;
    long maxVisited = 0;
    for (int key : keys) {
      map = map.put(key, -key);
      reference.put(key, -key);
      maxAllocated = Math.max(maxAllocated, map.lastMutationAllocatedNodes());
      maxVisited = Math.max(maxVisited, map.lastMutationVisitedNodes());
    }

    Collections.shuffle(keys, new Random(91_337L));
    for (int i = 0; i < count / 2; i++) {
      int key = keys.get(i);
      map = map.remove(key);
      reference.remove(key);
      maxAllocated = Math.max(maxAllocated, map.lastMutationAllocatedNodes());
      maxVisited = Math.max(maxVisited, map.lastMutationVisitedNodes());
    }

    assertEquals(reference.size(), map.size());
    Iterator<Map.Entry<Integer, Integer>> actual = map.iterator();
    for (Map.Entry<Integer, Integer> expected : reference.entrySet()) {
      assertTrue(actual.hasNext());
      assertEquals(expected, actual.next());
    }
    assertFalse(actual.hasNext());

    int logarithmicHeightBound = 2 * ceilLog2(count + 1);
    assertTrue(map.height() <= logarithmicHeightBound, "height=" + map.height());
    assertTrue(maxVisited <= logarithmicHeightBound + 2, "visited=" + maxVisited);
    assertTrue(maxAllocated <= 4L * logarithmicHeightBound + 8, "allocated=" + maxAllocated);
  }

  private static List<Integer> keys(List<Map.Entry<Integer, String>> entries) {
    return entries.stream().map(Map.Entry::getKey).toList();
  }

  private static int ceilLog2(int value) {
    return 32 - Integer.numberOfLeadingZeros(value - 1);
  }
}
