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

package org.apache.amoro.formats.paimon.optimizing.primary;

import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyOptimizingEvaluator.assertNoNativeGraph;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyOptimizingEvaluator.entry;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyOptimizingEvaluator.file;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyOptimizingEvaluator.fsCatalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyOptimizingEvaluator.healthFixture;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyOptimizingEvaluator.l0Runs;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyOptimizingEvaluator.strategyOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyOptimizingEvaluator.HealthFixture;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class TestPaimonAnalysisWorkLifetime {
  @Test
  void releasesCompletedBucketsInOrder(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = fsCatalog(warehouse)) {
      List<ManifestEntry> entries = new ArrayList<>();
      for (int bucket = 2; bucket >= 0; bucket--) {
        entries.addAll(l0Runs(bucket, 3));
      }
      HealthFixture fixture =
          healthFixture(
              catalog, "lifetime", strategyOptions(), entries, false, BucketMode.HASH_FIXED);
      Probe probe = new Probe();
      try (PaimonPrimaryKeySnapshotWork work =
          PaimonPrimaryKeySnapshotWork.analyze(fixture.table, fixture.context, true, probe)) {
        assertTrue(work.validForPlanning());
        assertEquals(9, work.pendingInput().getDataFileCount());
        assertEquals(Arrays.asList(0, 1, 2), probe.released);
        assertNoNativeGraph(work, Collections.newSetFromMap(new IdentityHashMap<>()));
      }
      assertEquals(3, probe.lists.size());
      probe.assertReleased();
    }
  }

  @Test
  void laterLevelsFailureWinsOverEarlierDvFailure(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = fsCatalog(warehouse)) {
      List<ManifestEntry> entries = new ArrayList<>(l0Runs(0, 3));
      entries.add(entry(BinaryRow.EMPTY_ROW, 1, file("overlap-a", 100, 10, 1, 1, 1)));
      entries.add(entry(BinaryRow.EMPTY_ROW, 1, file("overlap-b", 100, 10, 1, 1, 2)));
      HealthFixture fixture = dvFixture(catalog, "mixed_failure", entries);
      KeyValueFileStore store = spy((KeyValueFileStore) fixture.table.store());
      IndexFileHandler handler = mock(IndexFileHandler.class);
      doReturn(handler).when(store).newIndexFileHandler();
      doReturn(store).when(fixture.table).store();
      when(handler.scan(any(), eq("DELETION_VECTORS"), anySet()))
          .thenThrow(new IllegalStateException("DV unavailable"));
      Probe probe = new Probe();
      try (PaimonPrimaryKeySnapshotWork work =
          PaimonPrimaryKeySnapshotWork.analyze(fixture.table, fixture.context, true, probe)) {
        assertFalse(work.validForPlanning());
        assertEquals(
            Collections.singletonList("SNAPSHOT_SCAN_FAILED"),
            work.healthDetails().getReasonCodes());
        assertNoNativeGraph(work, Collections.newSetFromMap(new IdentityHashMap<>()));
      }
      verify(store, never()).newIndexFileHandler();
      probe.assertReleased();
    }
  }

  @Test
  void laterDvFailureReleasesRemainingBucketsAndPreservesStructuralFallback(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = fsCatalog(warehouse)) {
      List<ManifestEntry> entries = new ArrayList<>(l0Runs(0, 3));
      entries.addAll(l0Runs(1, 3));
      HealthFixture fixture = dvFixture(catalog, "later_dv", entries);
      KeyValueFileStore store = spy((KeyValueFileStore) fixture.table.store());
      IndexFileHandler handler = mock(IndexFileHandler.class);
      doReturn(handler).when(store).newIndexFileHandler();
      doReturn(store).when(fixture.table).store();
      Probe probe = new Probe();
      IndexFileMeta invalid = mock(IndexFileMeta.class);
      when(invalid.dvRanges())
          .thenAnswer(
              ignored -> {
                assertEquals(Collections.singletonList(0), probe.released);
                assertTrue(probe.lists.get(0).isEmpty());
                assertFalse(probe.lists.get(1).isEmpty());
                return null;
              });
      when(handler.scan(any(), eq("DELETION_VECTORS"), anySet()))
          .thenReturn(
              Collections.singletonMap(
                  Pair.of(BinaryRow.EMPTY_ROW, 1), Collections.singletonList(invalid)));
      try (PaimonPrimaryKeySnapshotWork work =
          PaimonPrimaryKeySnapshotWork.analyze(fixture.table, fixture.context, true, probe)) {
        assertTrue(work.validForPlanning());
        assertEquals(
            Collections.singletonList("DELETE_METADATA_READ_FAILED"),
            work.healthDetails().getReasonCodes());
        assertEquals(6, work.pendingInput().getDataFileCount());
        assertEquals(600, work.pendingInput().getDataFileSize());
        assertEquals("3", work.healthDetails().getMetrics().get("maxSortedRunCount"));
        assertEquals(Arrays.asList(0, 1), probe.released);
        assertNoNativeGraph(work, Collections.newSetFromMap(new IdentityHashMap<>()));
      }
      probe.assertReleased();
    }
  }

  @Test
  void aggregateOverflowReleasesAllValidatedBuckets(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = fsCatalog(warehouse)) {
      List<ManifestEntry> entries =
          Arrays.asList(
              entry(BinaryRow.EMPTY_ROW, 0, file("large-a", 1, Long.MAX_VALUE, 0, 1, 1)),
              entry(BinaryRow.EMPTY_ROW, 1, file("large-b", 1, 1, 0, 2, 2)));
      HealthFixture fixture =
          healthFixture(
              catalog, "overflow", strategyOptions(), entries, false, BucketMode.HASH_FIXED);
      Probe probe = new Probe();
      try (PaimonPrimaryKeySnapshotWork work =
          PaimonPrimaryKeySnapshotWork.analyze(fixture.table, fixture.context, true, probe)) {
        assertFalse(work.validForPlanning());
        assertEquals(
            Collections.singletonList("SNAPSHOT_SCAN_FAILED"),
            work.healthDetails().getReasonCodes());
      }
      assertEquals(2, probe.lists.size());
      probe.assertReleased();
    }
  }

  private static HealthFixture dvFixture(Catalog catalog, String name, List<ManifestEntry> entries)
      throws Exception {
    Map<String, String> options = strategyOptions();
    options.put("deletion-vectors.enabled", "true");
    return healthFixture(catalog, name, options, entries, false, BucketMode.HASH_FIXED);
  }

  private static final class Probe implements PaimonPrimaryKeySnapshotWork.BucketObserver {
    private final Map<Integer, List<DataFileMeta>> lists = new LinkedHashMap<>();
    private final List<Integer> released = new ArrayList<>();

    @Override
    public void validated(int bucket, List<DataFileMeta> files, Levels levels) {
      // All Levels validation precedes any per-bucket DV processing/release.
      assertTrue(released.isEmpty());
      assertFalse(files.isEmpty());
      assertTrue(levels.numberOfSortedRuns() > 0);
      lists.put(bucket, files);
    }

    @Override
    public void released(int bucket) {
      if (lists.containsKey(bucket)) {
        assertTrue(lists.get(bucket).isEmpty());
      }
      for (int previous : released) {
        if (lists.containsKey(previous)) {
          assertTrue(lists.get(previous).isEmpty());
        }
      }
      released.add(bucket);
    }

    private void assertReleased() {
      lists.values().forEach(files -> assertTrue(files.isEmpty()));
    }
  }
}
