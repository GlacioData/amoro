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

package org.apache.amoro.formats.paimon.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tests pin down the cursor + offset pagination contract of {@link SnapShotsScanUtils}.
 *
 * <p>Historically the UI sends BOTH {@code lastSnapshotId} (cursor) AND {@code (page-1)*pageSize}
 * (offset) when navigating to page 2+. Because the scan origin is already anchored at {@code
 * lastSnapshotId} in cursor mode, applying the offset a second time skipped a whole page and
 * surfaced as empty/incomplete pages. The fix ignores {@code offset} whenever a cursor is present;
 * these tests lock that behaviour down and also guard the no-cursor / empty-table boundaries.
 */
class TestSnapShotsScanUtils {

  /**
   * Builds a fake {@link SnapshotManager} covering ids in {@code [earliest, latest]} inclusive,
   * matching Paimon 1.4.2 {@link SnapshotManager#tryGetSnapshot(long)} and closed-interval {@link
   * SnapshotManager#snapshotsWithinRange(Optional, Optional)} semantics.
   */
  private static SnapshotManager mockSnapshotManager(long earliest, long latest) {
    SnapshotManager manager = mock(SnapshotManager.class);

    if (latest < earliest) {
      when(manager.earliestSnapshot()).thenReturn(null);
      when(manager.earliestSnapshotId()).thenReturn(null);
      when(manager.latestSnapshotId()).thenReturn(null);
      return manager;
    }

    Snapshot earliestSnap = mock(Snapshot.class);
    when(earliestSnap.id()).thenReturn(earliest);
    when(manager.earliestSnapshot()).thenReturn(earliestSnap);
    when(manager.earliestSnapshotId()).thenReturn(earliest);
    when(manager.latestSnapshotId()).thenReturn(latest);

    try {
      for (long id = earliest; id <= latest; id++) {
        Snapshot s = mock(Snapshot.class);
        when(s.id()).thenReturn(id);
        when(manager.tryGetSnapshot(id)).thenReturn(s);
      }
    } catch (java.io.FileNotFoundException impossible) {
      throw new AssertionError(impossible);
    }

    when(manager.snapshotsWithinRange(any(), any()))
        .thenAnswer(
            (InvocationOnMock invocation) -> {
              Optional<Long> upper = invocation.getArgument(0);
              Optional<Long> lower = invocation.getArgument(1);
              long max = Math.min(latest, upper.orElse(latest));
              long min = Math.max(earliest, lower.orElse(earliest));
              List<Snapshot> out = new ArrayList<>();
              for (long id = min; id <= max; id++) {
                Snapshot s = mock(Snapshot.class);
                when(s.id()).thenReturn(id);
                out.add(s);
              }
              return out.iterator();
            });

    return manager;
  }

  private static List<Long> idsOf(Pair<List<Snapshot>, Integer> result) {
    return result.getLeft().stream().map(Snapshot::id).collect(Collectors.toList());
  }

  @Test
  void cursorModeIgnoresOffset() {
    // 20 snapshots (ids 1..20), simulating the UI jumping to page 2 with both
    // lastSnapshotId (cursor = 10) and offset ((page-1)*pageSize = 5) set. The
    // fix zeros out the offset when a cursor is present: the scan must start
    // accumulating at ids immediately below the cursor, NOT five further down.
    SnapshotManager manager = mockSnapshotManager(1L, 20L);

    List<Long> ids = idsOf(SnapShotsScanUtils.getSnapshotsWithPagination(manager, null, 5, 5, 10L));

    assertFalse(ids.isEmpty(), "cursor mode must not return empty when data exists");
    assertTrue(
        ids.get(0) < 10L, "first returned id must live below the cursor, not at or above it");
    // Double-skip bug would have dropped ids 9,8,7,6,5 and started at 4. The
    // fix guarantees the page starts at the id immediately below the cursor.
    assertEquals(
        9L,
        ids.get(0),
        "cursor mode must start immediately below the cursor; seeing id<9 means offset was re-applied");
    assertTrue(
        ids.contains(9L), "id 9 must be present — it was incorrectly skipped before the fix");
    assertTrue(
        ids.contains(5L), "id 5 must be present — it was incorrectly skipped before the fix");
  }

  @Test
  void offsetModeStillSkips() {
    // No cursor: the original offset semantics must remain — the five newest
    // snapshots (16..20) must be skipped before accumulation begins, so they
    // must not appear in the result. The scan may continue further than limit
    // because scanSnapshots gathers whole ranges, but the skip is what matters.
    SnapshotManager manager = mockSnapshotManager(1L, 20L);

    List<Long> ids =
        idsOf(SnapShotsScanUtils.getSnapshotsWithPagination(manager, null, 5, 5, null));

    assertFalse(ids.isEmpty(), "offset-based first page must not return empty when data exists");
    assertFalse(ids.contains(20L), "offset=5 must have skipped id 20");
    assertFalse(ids.contains(19L), "offset=5 must have skipped id 19");
    assertFalse(ids.contains(18L), "offset=5 must have skipped id 18");
    assertFalse(ids.contains(17L), "offset=5 must have skipped id 17");
    assertFalse(ids.contains(16L), "offset=5 must have skipped id 16");
    // No offset would have produced ids starting near 19; with offset=5 the
    // first surviving id must be <= 15 (i.e. the skip took effect).
    assertTrue(ids.get(0) <= 15L, "offset=5 must begin accumulation at id 15 or below");
  }

  @Test
  void cursorFirstPageEquivalent() {
    // Offset already zero; the fix must preserve the no-offset cursor path.
    // First id below cursor=10 is 9 (upper-exclusive mock semantic).
    SnapshotManager manager = mockSnapshotManager(1L, 20L);

    List<Long> ids = idsOf(SnapShotsScanUtils.getSnapshotsWithPagination(manager, null, 5, 0, 10L));

    assertFalse(ids.isEmpty(), "cursor mode with offset=0 must not return empty");
    assertTrue(ids.get(0) < 10L, "cursor mode with offset=0 must still land below the cursor");
    assertEquals(9L, ids.get(0), "first id below cursor=10 is 9 (upper-exclusive)");
  }

  @Test
  void emptySnapshotManagerBoundary() {
    // latestSnapshotId() null → scanWithPagination returns an empty Pair; no NPE.
    SnapshotManager manager = mockSnapshotManager(1L, 0L);

    Pair<List<Snapshot>, Integer> result =
        assertDoesNotThrow(
            () -> SnapShotsScanUtils.getSnapshotsWithPagination(manager, null, 5, 5, 10L));

    assertTrue(result.getLeft().isEmpty(), "empty snapshot manager must yield empty list");
    assertEquals(0, result.getRight(), "empty snapshot manager must yield zero count");
  }

  @Test
  void realEmptyTableWithNullEarliestSnapshotDoesNotThrowNpeAndClosedRangeSkipsExpired()
      throws Exception {
    // Real Paimon SnapshotManager returns null for BOTH earliestSnapshot() and earliestSnapshotId()
    // on a 0-snapshot table.
    SnapshotManager emptyReal = mock(SnapshotManager.class);
    when(emptyReal.earliestSnapshot()).thenReturn(null);
    when(emptyReal.earliestSnapshotId()).thenReturn(null);
    when(emptyReal.latestSnapshotId()).thenReturn(null);

    Pair<List<Snapshot>, Integer> emptyRes =
        SnapShotsScanUtils.getSnapshotsWithPagination(emptyReal, null, 5, 0, null);
    assertTrue(emptyRes.getLeft().isEmpty());
    assertEquals(0, emptyRes.getRight());

    // Closed-interval Paimon SnapshotManager [1..6] where snapshot-4 expired concurrently
    // (tryGetSnapshot(4) throws FileNotFoundException, snapshot(4) throws RuntimeException).
    SnapshotManager closedManager = mock(SnapshotManager.class);
    when(closedManager.earliestSnapshotId()).thenReturn(1L);
    when(closedManager.latestSnapshotId()).thenReturn(6L);
    for (long id = 1L; id <= 6L; id++) {
      if (id == 4L) {
        when(closedManager.tryGetSnapshot(4L))
            .thenThrow(new java.io.FileNotFoundException("snapshot-4 expired"));
        when(closedManager.snapshot(4L))
            .thenThrow(new RuntimeException(new java.io.FileNotFoundException("snapshot-4")));
      } else {
        Snapshot s = mock(Snapshot.class);
        when(s.id()).thenReturn(id);
        when(s.commitKind()).thenReturn(Snapshot.CommitKind.COMPACT);
        when(closedManager.tryGetSnapshot(id)).thenReturn(s);
        when(closedManager.snapshot(id)).thenReturn(s);
      }
    }

    // Page 1 (limit=2): [6, 5]; Page 2 with cursor=5L must NOT duplicate 5L and must skip expired
    // 4L cleanly to return [3, 2]!
    List<Long> page1 =
        idsOf(SnapShotsScanUtils.getSnapshotsWithPagination(closedManager, null, 2, 0, null));
    assertEquals(java.util.Arrays.asList(6L, 5L), page1);
    List<Long> page2 =
        idsOf(SnapShotsScanUtils.getSnapshotsWithPagination(closedManager, null, 2, 2, 5L));
    assertEquals(java.util.Arrays.asList(3L, 2L), page2);

    Pair<List<Long>, Integer> mappedPage2 =
        SnapShotsScanUtils.getMappedSnapshotsWithPagination(
            closedManager, s -> Optional.of(s.id()), 2, 2, 5L);
    assertEquals(java.util.Arrays.asList(3L, 2L), mappedPage2.getLeft());
  }
}
