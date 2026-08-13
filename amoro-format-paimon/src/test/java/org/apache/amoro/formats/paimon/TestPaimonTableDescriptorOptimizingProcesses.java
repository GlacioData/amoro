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

package org.apache.amoro.formats.paimon;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Pins down the progressive snapshot pagination contract of {@link
 * PaimonTableDescriptor#getOptimizingProcessesInfo}.
 *
 * <p>Before JSTP-2356 the method did the inverse: it paginated COMPACT snapshots first (via {@code
 * SnapShotsScanUtils.getSnapshotsWithPagination}) and then applied the {@code type}/{@code status}
 * predicates in memory, so any filter that was sparser than the page size produced pages where
 * "current page is empty but later pages have matches" — the pager couldn't tell the difference
 * from "no more matches at all". The total shown to the UI was worse still: a heuristic {@code
 * (latestSnapshotId - earliestSnapshotId) / 2L * 0.6} that ignored the filter entirely and NPE'd on
 * a freshly-created empty table because {@code latestSnapshotId()} returns {@code null} there.
 *
 * <p>The current implementation keeps the endpoint stateless and scans progressively through
 * Paimon's {@code snapshotsWithinRange} API until it fills the filtered page or reaches the
 * earliest snapshot. Totals are exact when history is exhausted and otherwise include one next-page
 * sentinel. These tests lock:
 *
 * <ul>
 *   <li>NPE short-circuit on an empty table (no iteration, no scans).
 *   <li>Optimizer queries use progressive {@code snapshotsWithinRange}, not full {@code
 *       snapshots()}.
 *   <li>{@code lastSnapshot} acts as a stateless cursor for the next older page.
 *   <li>Status filters that cannot match landed compact snapshots return empty immediately.
 *   <li>Non-COMPACT snapshots are excluded by {@code commitKind}.
 * </ul>
 */
class TestPaimonTableDescriptorOptimizingProcesses {

  private static final TableIdentifier TID = TableIdentifier.of("cat", "db", "t");

  @Test
  void tableOptimizingTypesExposeMajor() {
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Map<String, String> types = descriptor.getTableOptimizingTypes(mock(AmoroTable.class));

    assertEquals("FULL", types.get("FULL"));
    assertEquals("MAJOR", types.get("MAJOR"));
    assertEquals("MINOR", types.get("MINOR"));
  }

  /**
   * Assembles an {@link AmoroTable} whose internal object graph reaches the {@link SnapshotManager}
   * that the tests care about. The chain exercised by {@code getOptimizingProcessesInfo} is {@code
   * amoroTable.originalTable() -> FileStoreTable.store() -> FileStore.snapshotManager()}, plus
   * {@code manifestListFactory()} / {@code manifestFileFactory()} for per-snapshot info building.
   * All other table state is harmless defaults.
   */
  private static AmoroTable<?> wireAmoroTable(
      SnapshotManager snapshotManager, ManifestList manifestList, ManifestFile manifestFile) {
    FileStore<?> store = mock(FileStore.class);
    when(store.snapshotManager()).thenReturn(snapshotManager);

    // ManifestFile / ManifestList are pulled via a factory(). Both factories follow the same
    // shape: factory.create() -> instance.
    org.apache.paimon.manifest.ManifestList.Factory listFactory =
        mock(org.apache.paimon.manifest.ManifestList.Factory.class);
    when(listFactory.create()).thenReturn(manifestList);
    when(store.manifestListFactory()).thenReturn(listFactory);

    org.apache.paimon.manifest.ManifestFile.Factory fileFactory =
        mock(org.apache.paimon.manifest.ManifestFile.Factory.class);
    when(fileFactory.create()).thenReturn(manifestFile);
    when(store.manifestFileFactory()).thenReturn(fileFactory);

    FileStoreTable table = mock(FileStoreTable.class);
    when(table.store()).thenAnswer(inv -> store);
    // Non-primary-key table keeps optimizingType at MINOR unless hasMaxLevels flips it; that
    // behaviour is not relevant for the emptyTable / nonCompactSnapshotsExcluded tests but
    // the call must not NPE on primaryKeys().
    when(table.primaryKeys()).thenReturn(Collections.emptyList());
    // Fix numLevels explicitly so the test is independent of the default evolution in Paimon.
    // maxLevel = numLevels - 1, so num-levels=5 → maxLevel=4 (matches armSnapshotWithLevel's 4).
    HashMap<String, String> options = new HashMap<>();
    options.put("num-levels", "5");
    when(table.options()).thenReturn(options);

    AmoroTable<?> amoroTable = mock(AmoroTable.class);
    when(((AmoroTable) amoroTable).originalTable()).thenReturn(table);
    when(amoroTable.id()).thenReturn(TID);
    return amoroTable;
  }

  /**
   * Same as {@link #wireAmoroTable} but flags the table as primary-key so {@code hasMaxLevels}
   * determines FULL vs MINOR. Needed for the type-filter test, which relies on {@code
   * optimizingType} discriminating within a single call.
   */
  private static AmoroTable<?> wirePrimaryKeyAmoroTable(
      SnapshotManager snapshotManager, ManifestList manifestList, ManifestFile manifestFile) {
    AmoroTable<?> amoroTable = wireAmoroTable(snapshotManager, manifestList, manifestFile);
    FileStoreTable table = (FileStoreTable) amoroTable.originalTable();
    when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
    return amoroTable;
  }

  private static Snapshot mockSnapshot(long id, Snapshot.CommitKind kind) {
    Snapshot s = mock(Snapshot.class);
    when(s.id()).thenReturn(id);
    when(s.commitKind()).thenReturn(kind);
    when(s.timeMillis()).thenReturn(id * 1000L);
    return s;
  }

  /**
   * Arms the manifest stack for a snapshot so that {@code toOptimizingProcessInfo} walks exactly
   * one manifest entry at the given level. level == maxLevel (4) on a primary-key table produces
   * {@code optimizingType=FULL}; level &lt; maxLevel produces {@code MINOR}.
   */
  private static void armSnapshotWithLevel(
      ManifestList manifestList,
      ManifestFile manifestFile,
      Snapshot snapshot,
      int level,
      String manifestName) {
    ManifestFileMeta meta = mock(ManifestFileMeta.class);
    when(meta.fileName()).thenReturn(manifestName);
    when(manifestList.readDeltaManifests(snapshot)).thenReturn(Collections.singletonList(meta));

    DataFileMeta file = mock(DataFileMeta.class);
    when(file.level()).thenReturn(level);
    when(file.rowCount()).thenReturn(1L);
    when(file.deleteRowCount()).thenReturn(Optional.of(0L));
    when(file.fileSize()).thenReturn(10L);
    // creationTime() is consulted via getFileCreationTimeMillis; the helper asks for
    // file().creationTime().getMillisecond(). Timestamp is a final class and Mockito 4.11
    // cannot mock it without inline-mocks, so use a real zero-valued Timestamp instead.
    when(file.creationTime()).thenReturn(org.apache.paimon.data.Timestamp.fromEpochMillis(0L));

    ManifestEntry entry = mock(ManifestEntry.class);
    when(entry.file()).thenReturn(file);
    when(entry.bucket()).thenReturn(0);
    when(entry.kind()).thenReturn(FileKind.ADD);
    when(manifestFile.read(manifestName)).thenReturn(Collections.singletonList(entry));
  }

  /** Arms an empty manifest for a snapshot — keeps {@code hasMaxLevels=false}, yielding MINOR. */
  private static void armSnapshotEmpty(ManifestList manifestList, Snapshot snapshot) {
    when(manifestList.readDeltaManifests(snapshot)).thenReturn(Collections.emptyList());
  }

  @Test
  void emptyTableReturnsZeroWithoutNpe() throws Exception {
    // latestSnapshotId() == null is the empty-table signal. The old heuristic total expression
    // unboxed the null before any early return could fire. The fix must short-circuit before
    // touching snapshots() / manifest readers.
    SnapshotManager manager = mock(SnapshotManager.class);
    when(manager.latestSnapshotId()).thenReturn(null);
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);

    AmoroTable<?> amoroTable = wireAmoroTable(manager, manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> result =
        assertDoesNotThrow(
            () -> descriptor.getOptimizingProcessesInfo(amoroTable, null, null, 10, 0, null));

    assertTrue(result.getLeft().isEmpty(), "empty table must yield empty page");
    assertEquals(0, result.getRight(), "empty table must report zero total");
    verify(manager, never()).snapshots();
    verify(manager, never()).earliestSnapshotId();
  }

  @Test
  void optimizerPaginationUsesProgressiveSnapshotRange() throws Exception {
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);

    List<Snapshot> firstWindow = new ArrayList<>();
    for (long id = 16; id <= 20; id++) {
      Snapshot snapshot =
          mockSnapshot(id, id == 20 ? Snapshot.CommitKind.COMPACT : Snapshot.CommitKind.APPEND);
      firstWindow.add(snapshot);
      if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
        armSnapshotEmpty(manifestList, snapshot);
      }
    }

    SnapshotManager manager = mock(SnapshotManager.class);
    when(manager.latestSnapshotId()).thenReturn(20L);
    when(manager.earliestSnapshotId()).thenReturn(1L);
    when(manager.snapshotsWithinRange(any(), any())).thenReturn(firstWindow.iterator());

    AmoroTable<?> amoroTable = wireAmoroTable(manager, manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> result =
        descriptor.getOptimizingProcessesInfo(amoroTable, null, null, 5, 0, null);

    assertEquals(1, result.getLeft().size(), "the exhausted history has one compact snapshot");
    assertEquals(1, result.getRight(), "the exhausted history reports the exact matching total");
    assertEquals("20", result.getLeft().get(0).getProcessId());
    verify(manager, never()).snapshots();
  }

  @Test
  void optimizerPaginationUsesLastSnapshotAsCursor() throws Exception {
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);

    List<Snapshot> cursorWindow = new ArrayList<>();
    for (long id = 11; id <= 15; id++) {
      Snapshot snapshot =
          mockSnapshot(id, id == 15 ? Snapshot.CommitKind.COMPACT : Snapshot.CommitKind.APPEND);
      cursorWindow.add(snapshot);
      if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
        armSnapshotEmpty(manifestList, snapshot);
      }
    }

    SnapshotManager manager = mock(SnapshotManager.class);
    when(manager.latestSnapshotId()).thenReturn(20L);
    when(manager.earliestSnapshotId()).thenReturn(1L);
    when(manager.snapshotsWithinRange(any(), any())).thenReturn(cursorWindow.iterator());

    AmoroTable<?> amoroTable = wireAmoroTable(manager, manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> result =
        descriptor.getOptimizingProcessesInfo(amoroTable, null, null, 5, 5, "16");

    assertEquals(Collections.singletonList("15"), processIds(result.getLeft()));
    assertEquals(
        6, result.getRight(), "the exhausted cursor page reports offset plus returned rows");
    verify(manager, never()).snapshots();
  }

  private static List<String> processIds(List<OptimizingProcessInfo> processes) {
    return processes.stream().map(OptimizingProcessInfo::getProcessId).collect(Collectors.toList());
  }

  private static SnapshotManager mockRangeSnapshotManager(List<Snapshot> snapshots) {
    SnapshotManager manager = mock(SnapshotManager.class);
    long earliest =
        snapshots.stream().mapToLong(Snapshot::id).min().orElseThrow(AssertionError::new);
    long latest = snapshots.stream().mapToLong(Snapshot::id).max().orElseThrow(AssertionError::new);
    when(manager.earliestSnapshotId()).thenReturn(earliest);
    when(manager.latestSnapshotId()).thenReturn(latest);
    when(manager.snapshotsWithinRange(any(), any()))
        .thenAnswer(
            (InvocationOnMock invocation) -> {
              Optional<Long> optionalMax = invocation.getArgument(0);
              Optional<Long> optionalMin = invocation.getArgument(1);
              long max = optionalMax.orElse(latest);
              long min = optionalMin.orElse(earliest);
              return snapshots.stream()
                  .filter(snapshot -> snapshot.id() >= min && snapshot.id() <= max)
                  .sorted((left, right) -> Long.compare(left.id(), right.id()))
                  .collect(Collectors.toList())
                  .iterator();
            });
    return manager;
  }

  @Test
  void sparseCompactHistoryRemainsReachable() throws Exception {
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);
    List<Snapshot> snapshots = new ArrayList<>();
    Snapshot compact = mockSnapshot(1L, Snapshot.CommitKind.COMPACT);
    snapshots.add(compact);
    armSnapshotEmpty(manifestList, compact);
    for (long id = 2L; id <= 30L; id++) {
      snapshots.add(mockSnapshot(id, Snapshot.CommitKind.APPEND));
    }

    SnapshotManager manager = mockRangeSnapshotManager(snapshots);
    AmoroTable<?> amoroTable = wireAmoroTable(manager, manifestList, manifestFile);

    Pair<List<OptimizingProcessInfo>, Integer> result =
        new PaimonTableDescriptor().getOptimizingProcessesInfo(amoroTable, null, null, 5, 0, null);

    assertEquals(
        Collections.singletonList("1"),
        processIds(result.getLeft()),
        "a compact snapshot older than the first raw-id window must remain reachable");
    assertEquals(1, result.getRight(), "the exhausted history has one matching process");
  }

  @Test
  void compactWithoutAddedFilesUsesCommitTimeAsStartTime() throws Exception {
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);
    Snapshot compact = mockSnapshot(7L, Snapshot.CommitKind.COMPACT);
    armSnapshotEmpty(manifestList, compact);
    SnapshotManager manager = mockRangeSnapshotManager(Collections.singletonList(compact));
    AmoroTable<?> amoroTable = wireAmoroTable(manager, manifestList, manifestFile);

    OptimizingProcessInfo process =
        new PaimonTableDescriptor()
            .getOptimizingProcessesInfo(amoroTable, null, null, 5, 0, null)
            .getLeft()
            .get(0);

    assertEquals(compact.timeMillis(), process.getStartTime());
    assertEquals(0L, process.getDuration());
  }

  @Test
  void cursorPagesRemainContiguousWithoutDuplicates() throws Exception {
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);
    List<Snapshot> snapshots = new ArrayList<>();
    for (long id = 1L; id <= 8L; id++) {
      Snapshot compact = mockSnapshot(id, Snapshot.CommitKind.COMPACT);
      snapshots.add(compact);
      armSnapshotEmpty(manifestList, compact);
    }

    AmoroTable<?> amoroTable =
        wireAmoroTable(mockRangeSnapshotManager(snapshots), manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> first =
        descriptor.getOptimizingProcessesInfo(amoroTable, null, null, 3, 0, null);
    Pair<List<OptimizingProcessInfo>, Integer> second =
        descriptor.getOptimizingProcessesInfo(amoroTable, null, null, 3, 3, "6");
    Pair<List<OptimizingProcessInfo>, Integer> third =
        descriptor.getOptimizingProcessesInfo(amoroTable, null, null, 3, 6, "3");

    assertEquals(Arrays.asList("8", "7", "6"), processIds(first.getLeft()));
    assertEquals(Arrays.asList("5", "4", "3"), processIds(second.getLeft()));
    assertEquals(Arrays.asList("2", "1"), processIds(third.getLeft()));
    assertEquals(4, first.getRight(), "first page exposes one next-page sentinel");
    assertEquals(7, second.getRight(), "middle page exposes one next-page sentinel");
    assertEquals(8, third.getRight(), "last page reports the exact exhausted total");
  }

  @Test
  void sparseTypeFilterIsAppliedBeforePagination() throws Exception {
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);
    List<Snapshot> snapshots = new ArrayList<>();
    for (long id = 1L; id <= 10L; id++) {
      Snapshot compact = mockSnapshot(id, Snapshot.CommitKind.COMPACT);
      snapshots.add(compact);
      int level = id == 1L || id == 5L || id == 10L ? 4 : 1;
      armSnapshotWithLevel(manifestList, manifestFile, compact, level, "m" + id);
    }

    AmoroTable<?> amoroTable =
        wirePrimaryKeyAmoroTable(mockRangeSnapshotManager(snapshots), manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> first =
        descriptor.getOptimizingProcessesInfo(amoroTable, "FULL", null, 2, 0, null);
    Pair<List<OptimizingProcessInfo>, Integer> second =
        descriptor.getOptimizingProcessesInfo(amoroTable, "FULL", null, 2, 2, "5");

    assertEquals(Arrays.asList("10", "5"), processIds(first.getLeft()));
    assertEquals(Collections.singletonList("1"), processIds(second.getLeft()));
    assertEquals(3, first.getRight(), "sparse FULL results fill the page before pagination");
    assertEquals(3, second.getRight(), "the final filtered page reports the exact total");
  }

  @Test
  void filterAppliedBeforePagination_typeFull() throws Exception {
    // All five snapshots are FULL and the supplied history is exhausted, so total is exact.
    List<Snapshot> all = new ArrayList<>();
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);
    for (long id = 16; id <= 20; id++) {
      Snapshot s = mockSnapshot(id, Snapshot.CommitKind.COMPACT);
      all.add(s);
      armSnapshotWithLevel(manifestList, manifestFile, s, 4, "m" + id);
    }

    SnapshotManager manager = mock(SnapshotManager.class);
    when(manager.latestSnapshotId()).thenReturn(20L);
    when(manager.earliestSnapshotId()).thenReturn(1L);
    when(manager.snapshotsWithinRange(any(), any())).thenReturn(all.iterator());

    AmoroTable<?> amoroTable = wirePrimaryKeyAmoroTable(manager, manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> result =
        descriptor.getOptimizingProcessesInfo(amoroTable, "FULL", null, 5, 0, null);

    assertEquals(5, result.getLeft().size(), "page size must be 5 (limit)");
    assertEquals(5, result.getRight(), "exhausted filtered history reports the exact total");
    result.getLeft().forEach(p -> assertEquals("FULL", p.getOptimizingType()));
    verify(manager, never()).snapshots();
  }

  @Test
  void filterAppliedBeforePagination_statusMatches() throws Exception {
    // All landed COMPACT snapshots are translated as SUCCESS. SUCCESS scans progressively;
    // non-SUCCESS can return empty immediately without touching snapshot history.
    List<Snapshot> all = new ArrayList<>();
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);
    for (long id = 16; id <= 20; id++) {
      Snapshot s = mockSnapshot(id, Snapshot.CommitKind.COMPACT);
      all.add(s);
      armSnapshotEmpty(manifestList, s);
    }

    SnapshotManager manager = mock(SnapshotManager.class);
    when(manager.latestSnapshotId()).thenReturn(20L);
    when(manager.earliestSnapshotId()).thenReturn(1L);
    when(manager.snapshotsWithinRange(any(), any())).thenReturn(all.iterator());

    AmoroTable<?> amoroTable = wireAmoroTable(manager, manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> matched =
        descriptor.getOptimizingProcessesInfo(amoroTable, null, ProcessStatus.SUCCESS, 5, 0, null);
    assertEquals(5, matched.getLeft().size(), "limit=5 over 20 matches must yield page of 5");
    assertEquals(5, matched.getRight(), "exhausted SUCCESS history reports the exact total");

    Pair<List<OptimizingProcessInfo>, Integer> miss =
        descriptor.getOptimizingProcessesInfo(amoroTable, null, ProcessStatus.RUNNING, 5, 0, null);
    assertTrue(miss.getLeft().isEmpty(), "no snapshot has status=RUNNING, page must be empty");
    assertEquals(0, miss.getRight(), "non-SUCCESS filters have no landed Paimon compact rows");
  }

  @Test
  void pageBeyondFilteredRangeIsEmpty() throws Exception {
    // 10 COMPACT FULL snapshots, ask for offset=10 (exactly past the end). The page is empty
    // but the total is 10 — not zero and not a heuristic. The old code's heuristic total was
    // detached from the real filtered count so the UI's pager could render a page number beyond
    // the true end.
    List<Snapshot> all = new ArrayList<>();
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);
    for (long id = 1; id <= 10; id++) {
      Snapshot s = mockSnapshot(id, Snapshot.CommitKind.COMPACT);
      all.add(s);
      armSnapshotWithLevel(manifestList, manifestFile, s, 4, "m" + id);
    }

    SnapshotManager manager = mockRangeSnapshotManager(all);

    AmoroTable<?> amoroTable = wirePrimaryKeyAmoroTable(manager, manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> result =
        descriptor.getOptimizingProcessesInfo(amoroTable, "FULL", null, 10, 10, null);

    assertTrue(result.getLeft().isEmpty(), "offset past end must yield empty page");
    assertEquals(10, result.getRight(), "the exhausted history reports the exact total");
    verify(manager, never()).snapshots();
  }

  @Test
  void nonCompactSnapshotsExcluded() throws Exception {
    // Mix 5 COMPACT + 5 APPEND snapshots. With no type/status filter the result must contain
    // only the COMPACT ones — non-COMPACT is filtered by commitKind at the snapshot scan step,
    // not by the type/status predicates that run later. Total == 5 proves the commitKind gate
    // drops APPEND before they reach the materialisation/filter stages.
    List<Snapshot> all = new ArrayList<>();
    ManifestList manifestList = mock(ManifestList.class);
    ManifestFile manifestFile = mock(ManifestFile.class);
    for (long id = 1; id <= 5; id++) {
      Snapshot s = mockSnapshot(id, Snapshot.CommitKind.COMPACT);
      all.add(s);
      armSnapshotEmpty(manifestList, s);
    }
    for (long id = 6; id <= 10; id++) {
      Snapshot s = mockSnapshot(id, Snapshot.CommitKind.APPEND);
      all.add(s);
      // APPEND snapshots must not be passed to manifest readers — if they were, the unstubbed
      // readDeltaManifests would return null and NPE the loop. That's a useful co-assertion.
    }

    SnapshotManager manager = mock(SnapshotManager.class);
    when(manager.latestSnapshotId()).thenReturn(10L);
    when(manager.earliestSnapshotId()).thenReturn(1L);
    when(manager.snapshotsWithinRange(any(), any())).thenReturn(all.iterator());

    AmoroTable<?> amoroTable = wireAmoroTable(manager, manifestList, manifestFile);
    PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

    Pair<List<OptimizingProcessInfo>, Integer> result =
        descriptor.getOptimizingProcessesInfo(amoroTable, null, null, 100, 0, null);

    assertEquals(5, result.getLeft().size(), "only the 5 COMPACT snapshots must surface");
    assertEquals(5, result.getRight(), "the scan reaches earliest, so no sentinel is needed");
    verify(manager, never()).snapshots();
  }
}
