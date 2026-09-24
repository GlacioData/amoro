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

package org.apache.amoro.formats.paimon.optimizing.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.stats.SimpleStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

class TestPaimonFixedBucketFileWindow {
  @Test
  void classifiesEachEntryOnceInsteadOfEveryHeapComparison() {
    java.util.concurrent.atomic.AtomicInteger calls =
        new java.util.concurrent.atomic.AtomicInteger();
    PaimonFixedBucketFileWindow window =
        new PaimonFixedBucketFileWindow(
            unit(),
            2,
            3,
            true,
            entry -> {
              calls.incrementAndGet();
              return true;
            });
    for (int i = 19; i >= 0; i--) {
      window.add(file("file" + i, i * 10, i * 10 + 9));
    }
    window.finish();
    assertEquals(20, calls.get());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void advancingPreservesTrailingRunAndTerminates(boolean tail) {
    List<ManifestEntry> entries =
        new ArrayList<>(
            Arrays.asList(
                file("small0", 0, 9),
                file("healthy0", 10, 19),
                file("small1", 20, 29),
                file("small2", 30, 39),
                file("small3", 40, 49)));
    if (!tail) {
      entries.add(file("healthy1", 30, 39));
      entries.set(3, file("small2", 40, 49));
      entries.set(4, file("small3", 60, 69));
      entries.add(file("healthy2", 50, 59));
    }
    PaimonFixedBucketFileWindow window =
        new PaimonFixedBucketFileWindow(
            unit(), 2, 3, true, entry -> entry.fileName().startsWith("small"));
    Collections.shuffle(entries, new Random(42));
    entries.forEach(window::add);
    window.finish();
    entries.forEach(window::verifyScopeEntry);
    window.finishScopeProof();
    assertTrue(window.canAdvance());
    PaimonFixedBucketFileWindow next = window.advance();
    entries.forEach(next::add);
    next.finish();
    entries.forEach(next::verifyScopeEntry);
    next.finishScopeProof();
    assertTrue(next.safe());
    assertFalse(next.canAdvance());
    if (tail) {
      assertEquals(Arrays.asList("small1", "small2", "small3"), names(next));
      assertTrue(names(next).stream().noneMatch(next::hasBarrierBefore));
    } else {
      assertTrue(next.hasBarrierBefore("small3"));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 42})
  void boundedProblemSuffixRequiresCompleteOrderIndependentScopeProof(int seed) {
    List<ManifestEntry> entries =
        new ArrayList<>(
            Arrays.asList(
                file("healthy0.orc", 0, 9), file("healthy1.orc", 10, 19),
                file("healthy2.orc", 20, 29), file("small0.orc", 30, 39),
                file("small1.orc", 40, 49), file("small2.orc", 50, 59)));
    Collections.shuffle(entries, new Random(seed));
    PaimonFixedBucketFileWindow window =
        new PaimonFixedBucketFileWindow(
            unit(), 2, 3, true, entry -> entry.fileName().startsWith("small"));
    entries.forEach(window::add);
    window.finish();
    assertEquals(Arrays.asList("small0.orc", "small1.orc", "small2.orc"), names(window));
    assertFalse(window.safe());
    assertTrue(window.needsScopeProof());
    // Paimon ac223f47, BaseAppendFileStoreWrite.java:187-222 concatenates exact ordered inputs.
    // A suffix is safe only after proving that every omitted active file is outside its range.
    Collections.shuffle(entries, new Random(seed + 17));
    entries.forEach(window::verifyScopeEntry);
    window.finishScopeProof();
    assertTrue(window.safe());
    assertFalse(window.provenPrefix());
    assertFalse(window.needsScopeProof());
    assertEquals(6, window.totalFileCount());
    assertEquals(3, window.omittedFileCount());
    assertTrue(names(window).stream().noneMatch(window::hasBarrierBefore));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "valid",
        "leftOverlap",
        "middleOverlap",
        "rightOverlap",
        "incomplete",
        "duplicate"
      })
  void provesBothBoundariesAndKeepsAnOmittedMiddleFileAsABarrier(String scenario) {
    List<ManifestEntry> entries =
        new ArrayList<>(
            Arrays.asList(
                file("left.orc", 0, "leftOverlap".equals(scenario) ? 10 : 9),
                file("small0.orc", 10, 19),
                file("middle.orc", "middleOverlap".equals(scenario) ? 19 : 20, 29),
                file("small1.orc", 30, 39),
                file("right.orc", "rightOverlap".equals(scenario) ? 39 : 40, 49)));
    PaimonFixedBucketFileWindow window =
        new PaimonFixedBucketFileWindow(
            unit(), 2, 2, true, entry -> entry.fileName().startsWith("small"));
    entries.forEach(window::add);
    window.finish();
    assertTrue(window.needsScopeProof());
    if ("incomplete".equals(scenario)) {
      entries.remove(4);
    } else if ("duplicate".equals(scenario)) {
      entries.set(4, entries.get(1));
    }
    Collections.reverse(entries);
    entries.forEach(window::verifyScopeEntry);
    window.finishScopeProof();
    assertEquals("valid".equals(scenario), window.safe());
    if (window.safe()) {
      assertTrue(window.hasBarrierBefore("small1.orc"));
      assertFalse(window.hasBarrierBefore("small0.orc"));
    }
  }

  @Test
  void unitKeyCopiesItsPartitionAndKeepsBucketsDistinct() {
    BinaryRow original = BinaryRow.singleColumn(7);
    PaimonAppendUnitKey unit = new PaimonAppendUnitKey(original, 1);
    Set<PaimonAppendUnitKey> keys = new HashSet<>(Collections.singletonList(unit));
    original.setInt(0, 8);
    BinaryRow returned = unit.partition();
    returned.setInt(0, 9);

    // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
    // paimon-core/src/main/java/org/apache/paimon/manifest/FileEntry.java:49-57.
    // A planning unit owns both partition and bucket; mutable BinaryRow aliases must not move it.
    assertEquals(7, unit.partition().getInt(0));
    assertEquals(1, unit.bucket());
    PaimonAppendUnitKey equal = new PaimonAppendUnitKey(BinaryRow.singleColumn(7), 1);
    assertEquals(unit, equal);
    assertEquals(unit.hashCode(), equal.hashCode());
    assertTrue(keys.contains(equal));
    assertNotEquals(unit, new PaimonAppendUnitKey(BinaryRow.singleColumn(7), 0));
    assertNotEquals(unit, new PaimonAppendUnitKey(BinaryRow.singleColumn(8), 1));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 42})
  void retainsTheSameProvenPrefixWhenManifestOrderChanges(int seed) {
    List<ManifestEntry> entries =
        new ArrayList<>(
            Arrays.asList(
                file("a.orc", 0, 9),
                file("b.orc", 10, 19),
                file("c.orc", 20, 29),
                file("d.orc", 30, 39),
                file("e.orc", 40, 49)));
    Collections.shuffle(entries, new Random(seed));
    PaimonFixedBucketFileWindow window = window(3, true);
    entries.forEach(window::add);
    window.finish();

    // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
    // append/BucketedAppendCompactManager.java:84-85 orders by sequence, while
    // operation/BaseAppendFileStoreWrite.java:187-215 rewrites in supplied order.
    // The bounded Amoro prefix must therefore be independent of manifest traversal order.
    assertEquals(Arrays.asList("a.orc", "b.orc", "c.orc"), names(window));
    assertEquals(unit(), window.unit());
    assertEquals(2, window.totalBuckets());
    assertEquals(5, window.totalFileCount());
    assertEquals(2, window.omittedFileCount());
    assertFalse(window.complete());
    assertTrue(window.provenPrefix());
    assertTrue(window.safe());
    assertReasonPresent(window);
    for (ManifestEntry retained : window.files()) {
      assertEquals(unit().partition(), retained.partition());
      assertEquals(unit().bucket(), retained.bucket());
      assertEquals(2, retained.totalBuckets());
    }
  }

  @ParameterizedTest
  @CsvSource({"9,true", "10,false", "11,false"})
  void requiresStrictSeparationFromTheFirstDiscardedSequence(long retainedMax, boolean safe) {
    ManifestEntry first = file("first.orc", 0, retainedMax);
    ManifestEntry next = file("next.orc", 10, 19);
    for (boolean evict : Arrays.asList(false, true)) {
      PaimonFixedBucketFileWindow window = window(1, true);
      window.add(evict ? next : first);
      window.add(evict ? first : next);
      window.add(file("last.orc", 30, 39));
      window.finish();

      // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:203-206 initializes the
      // rewritten sequence from the first input. Equality at the omitted boundary is overlap,
      // whether that omitted file was discarded immediately or evicted from the retained heap.
      assertEquals(Collections.singletonList("first.orc"), names(window));
      assertEquals(3, window.totalFileCount());
      assertEquals(2, window.omittedFileCount());
      assertFalse(window.complete());
      assertEquals(safe, window.provenPrefix());
      assertEquals(safe, window.safe());
      assertReasonPresent(window);
    }
  }

  @Test
  void keepsTheEarliestOmittedBoundaryAcrossBothEvictionAndDiscard() {
    PaimonFixedBucketFileWindow window = window(1, true);
    window.add(file("evicted.orc", 10, 19));
    window.add(file("retained.orc", 0, 10));
    window.add(file("later-discard.orc", 100, 109));
    window.finish();

    // Paimon ac223f47, append/BucketedAppendCompactManager.java:84-85 relies on sequence order.
    // A later discarded file cannot overwrite the earlier boundary and hide an overlap.
    assertEquals(Collections.singletonList("retained.orc"), names(window));
    assertFalse(window.safe());
    assertFalse(window.provenPrefix());
    assertEquals(2, window.omittedFileCount());
    assertReasonPresent(window);
  }

  @Test
  void completeWindowPublishesImmutableSortedFactsAndFreezesFurtherAdds() {
    PaimonFixedBucketFileWindow window = window(4, true);
    assertThrows(IllegalStateException.class, window::files);
    window.add(file("later.orc", 20, 29));
    window.add(file("earlier.orc", 0, 9));
    window.finish();

    // Paimon ac223f47, manifest/FileEntry.java:49-57 supplies immutable planning identity facts.
    // Freezing an Amoro window prevents later traversal from changing an already-approved scope.
    assertEquals(Arrays.asList("earlier.orc", "later.orc"), names(window));
    assertEquals(2, window.totalFileCount());
    assertEquals(0, window.omittedFileCount());
    assertTrue(window.complete());
    assertTrue(window.safe());
    assertThrows(UnsupportedOperationException.class, () -> window.files().clear());
    assertThrows(IllegalStateException.class, () -> window.add(file("too-late.orc", 30, 39)));
    assertEquals(Arrays.asList("earlier.orc", "later.orc"), names(window));
    assertEquals(2, window.totalFileCount());
    assertEquals(unit(), window.unit());
    assertEquals(2, window.totalBuckets());
  }

  @ParameterizedTest
  @ValueSource(strings = {"overlap", "negativeMin", "negativeMax", "reversed"})
  void rejectsInvalidOrderedSequenceFactsEvenWhenAllFilesWereRetained(String invalid) {
    ManifestEntry second;
    switch (invalid) {
      case "overlap":
        second = file("second.orc", 9, 19);
        break;
      case "negativeMin":
        second = file("second.orc", -1, 4);
        break;
      case "negativeMax":
        second = file("second.orc", 0, -1);
        break;
      default:
        second = file("second.orc", 20, 19);
    }
    PaimonFixedBucketFileWindow window = window(4, true);
    window.add(file("first.orc", 0, 9));
    window.add(second);
    window.finish();

    // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187-215 does not repair or sort
    // a supplied range. Having complete metadata does not make overlapping ranges safe.
    assertEquals(2, window.totalFileCount());
    assertEquals(0, window.omittedFileCount());
    assertTrue(window.complete());
    assertFalse(window.safe());
    assertFalse(window.provenPrefix());
    assertReasonPresent(window);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rejectsDuplicateFileOwnershipInEitherOrderingMode(boolean ordered) {
    PaimonFixedBucketFileWindow window = window(4, ordered);
    ManifestEntry duplicate = file("same.orc", 0, 9);
    window.add(duplicate);
    window.add(duplicate);
    window.finish();

    // Paimon ac223f47, manifest/FileEntry.java:76-95 defines one file identity per unit.
    // Unordered sequence overlap is legal, but it never licenses rewriting one file twice.
    assertFalse(window.safe());
    assertReasonPresent(window);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"partition", "bucket", "totalBuckets", "unknownBucket", "unknownTotalBuckets"})
  void rejectsManifestEntriesOutsideTheDeclaredBucketLayout(String invalid) {
    BinaryRow partition = unit().partition();
    int bucket = unit().bucket();
    int totalBuckets = 2;
    if ("partition".equals(invalid)) {
      partition = BinaryRow.singleColumn(8);
    } else if ("bucket".equals(invalid)) {
      bucket = 0;
    } else if ("totalBuckets".equals(invalid)) {
      totalBuckets = 3;
    } else if ("unknownBucket".equals(invalid)) {
      bucket = -1;
    } else {
      totalBuckets = -1;
    }
    PaimonFixedBucketFileWindow window = window(4, true);
    window.add(file("first.orc", 0, 9));
    window.add(
        ManifestEntry.create(
            FileKind.ADD, partition, bucket, totalBuckets, metadata("invalid.orc", 10, 19, 128)));
    window.finish();

    // Paimon ac223f47, manifest/FileEntry.java:49-57 carries the physical bucket layout.
    // A bounded planning window must not merge files from another bucket or unknown layout.
    assertFalse(window.safe());
    assertFalse(window.provenPrefix());
    assertReasonPresent(window);
    assertEquals(window.totalFileCount() == window.files().size(), window.complete());
    assertEquals(window.totalFileCount() - window.files().size(), window.omittedFileCount());
    assertEquals(unit(), window.unit());
    assertEquals(2, window.totalBuckets());
  }

  @Test
  void unorderedFilesMayOverlapAndUseStableMaximumSequenceAndNameTieBreaks() {
    PaimonFixedBucketFileWindow window = window(4, false);
    window.add(file("z.orc", 0, 9));
    window.add(file("b.orc", 0, 4));
    window.add(file("a.orc", 0, 9));
    window.finish();

    // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
    // operation/BucketedAppendFileStoreWrite.java:75-85 ignores old sequences for unordered
    // write-only files. Legitimate overlaps retain unique file ownership and stable ordering.
    assertTrue(window.safe());
    assertTrue(window.complete());
    assertEquals(3, window.totalFileCount());
    assertEquals(0, window.omittedFileCount());
    assertEquals(Arrays.asList("b.orc", "a.orc", "z.orc"), names(window));
  }

  @Test
  void fileCapacityDoesNotIntroduceAHardDataByteLimit() {
    PaimonFixedBucketFileWindow window = window(1, true);
    window.add(
        ManifestEntry.create(
            FileKind.ADD,
            unit().partition(),
            unit().bucket(),
            2,
            metadata("large.orc", 0, 9, 512L * 1024 * 1024)));
    window.finish();

    // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187 accepts exact supplied files.
    // This Amoro limit bounds retained metadata count, preserving the agreed best-effort bytes.
    assertTrue(window.safe());
    assertTrue(window.complete());
    assertEquals(1, window.files().size());
    assertEquals(512L * 1024 * 1024, window.files().get(0).file().fileSize());
  }

  private static PaimonAppendUnitKey unit() {
    return new PaimonAppendUnitKey(BinaryRow.singleColumn(7), 1);
  }

  private static PaimonFixedBucketFileWindow window(int capacity, boolean ordered) {
    return new PaimonFixedBucketFileWindow(unit(), 2, capacity, ordered);
  }

  private static ManifestEntry file(String name, long minimum, long maximum) {
    return ManifestEntry.create(
        FileKind.ADD,
        unit().partition(),
        unit().bucket(),
        2,
        metadata(name, minimum, maximum, 128));
  }

  private static DataFileMeta metadata(String name, long minimum, long maximum, long bytes) {
    // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/io/
    // DataFileMeta.java:96-132. Use the actual append metadata representation for malformed ranges.
    return DataFileMeta.forAppend(
        name,
        bytes,
        Math.max(1L, maximum - minimum + 1),
        SimpleStats.EMPTY_STATS,
        minimum,
        maximum,
        0,
        Collections.emptyList(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static List<String> names(PaimonFixedBucketFileWindow window) {
    return window.files().stream().map(ManifestEntry::fileName).collect(Collectors.toList());
  }

  private static void assertReasonPresent(PaimonFixedBucketFileWindow window) {
    assertNotNull(window.reason());
    assertFalse(window.reason().isEmpty());
  }
}
