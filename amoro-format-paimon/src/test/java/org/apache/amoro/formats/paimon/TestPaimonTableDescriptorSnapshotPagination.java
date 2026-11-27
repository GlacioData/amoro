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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class TestPaimonTableDescriptorSnapshotPagination {
  private final FileIO io = mock(FileIO.class);
  private final SnapshotManager manager = mock(SnapshotManager.class);
  private final FileStoreTable table = mock(FileStoreTable.class);
  private final AmoroTable<?> amoro = mock(AmoroTable.class);
  private final PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

  @BeforeEach
  void setup() throws Exception {
    doReturn(table).when(amoro).originalTable();
    when(table.snapshotManager()).thenReturn(manager);
    when(manager.copyWithBranch("main")).thenReturn(manager);
    when(manager.fileIO()).thenReturn(io);
    when(manager.snapshotDirectory()).thenReturn(new Path("/table/snapshot"));
    when(manager.snapshotPath(anyLong()))
        .thenAnswer(i -> new Path("/table/snapshot/snapshot-" + i.getArgument(0)));
    when(io.exists(any())).thenReturn(true);
    hints(1L, 50L);
    when(manager.tryGetSnapshot(anyLong()))
        .thenAnswer(
            i -> {
              long id = i.getArgument(0);
              Snapshot snapshot = mock(Snapshot.class);
              when(snapshot.id()).thenReturn(id);
              when(snapshot.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
              return snapshot;
            });
  }

  private void hints(Long min, Long max) throws Exception {
    when(io.readOverwrittenFileUtf8(new Path("/table/snapshot/EARLIEST")))
        .thenReturn(Optional.ofNullable(min).map(Object::toString));
    when(io.readOverwrittenFileUtf8(new Path("/table/snapshot/LATEST")))
        .thenReturn(Optional.ofNullable(max).map(Object::toString));
  }

  private Pair<List<AmoroSnapshotsOfTable>, Long> page(OperationType operation, int offset) {
    return descriptor.getSnapshots(amoro, null, operation, 25, offset, null);
  }

  @Test
  void fixedCandidateRangeHasNoManifestOrHistoryScan() throws Exception {
    Pair<List<AmoroSnapshotsOfTable>, Long> first = page(OperationType.ALL, 0);
    assertEquals(50L, first.getRight());
    assertEquals(25, first.getLeft().size());
    assertEquals("50", first.getLeft().get(0).getSnapshotId());
    assertEquals("26", first.getLeft().get(24).getSnapshotId());
    assertEquals(0L, first.getLeft().get(0).getFileCount());
    assertNull(first.getLeft().get(0).getFileSize());
    assertEquals("25", page(OperationType.ALL, 25).getLeft().get(0).getSnapshotId());
    verify(manager, times(50)).tryGetSnapshot(anyLong());
    verify(manager, never()).snapshotCount();
    verify(manager, never()).snapshots();
    verify(manager, never()).latestSnapshotId();
    verify(manager, never()).earliestSnapshotId();
    verify(table, never()).store();
    verify(io, never()).listStatus(any());
  }

  @Test
  void expandedMetadataUsesOnlySnapshotJsonAndPreservesChartKeys() throws Exception {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(50L);
    when(snapshot.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
    when(snapshot.schemaId()).thenReturn(7L);
    when(snapshot.watermark()).thenReturn(1789603200000L);
    when(snapshot.changelogRecordCount()).thenReturn(12L);
    when(snapshot.baseManifestListSize()).thenReturn(2048L);
    when(snapshot.deltaManifestListSize()).thenReturn(4096L);
    when(snapshot.changelogManifestListSize()).thenReturn(1024L);
    when(manager.tryGetSnapshot(50L)).thenReturn(snapshot);

    AmoroSnapshotsOfTable row = page(OperationType.ALL, 0).getLeft().get(0);
    Map<String, String> summary = row.getSummary();
    assertEquals("7", summary.get("schemaId"));
    assertEquals("1789603200000", summary.get("watermark"));
    assertEquals("12", summary.get("changelogRecordCount"));
    assertEquals("2048", summary.get("baseManifestListSize"));
    assertEquals("4096", summary.get("deltaManifestListSize"));
    assertEquals("1024", summary.get("changelogManifestListSize"));
    assertEquals("12", row.getRecordsSummaryForChart().get("changelog-records"));
    assertFalse(summary.containsKey("changelog-records"));
    verify(snapshot, never()).baseManifestList();
    verify(snapshot).deltaManifestList();
    verify(snapshot, never()).changelogManifestList();
    verify(table, never()).store();
    verify(manager, times(25)).tryGetSnapshot(anyLong());
  }

  @Test
  void missingMetadataIsUnknownButZeroIsPresent() {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
    when(snapshot.watermark()).thenReturn(null);
    when(snapshot.changelogRecordCount()).thenReturn(null);
    when(snapshot.baseManifestListSize()).thenReturn(null);
    when(snapshot.deltaManifestListSize()).thenReturn(null);
    when(snapshot.changelogManifestListSize()).thenReturn(null);
    Map<String, String> missing = PaimonSnapshotMetadata.summary(snapshot).getSummary();
    assertEquals("0", missing.get("schemaId"));
    for (String key :
        new String[] {
          "watermark", "changelogRecordCount", "baseManifestListSize", "deltaManifestListSize"
        }) {
      assertEquals("—", missing.get(key));
    }
    assertFalse(missing.containsKey("changelogManifestListSize"));

    when(snapshot.watermark()).thenReturn(0L);
    when(snapshot.changelogRecordCount()).thenReturn(0L);
    when(snapshot.baseManifestListSize()).thenReturn(0L);
    when(snapshot.deltaManifestListSize()).thenReturn(0L);
    when(snapshot.changelogManifestListSize()).thenReturn(0L);
    Map<String, String> present = PaimonSnapshotMetadata.summary(snapshot).getSummary();
    for (String key :
        new String[] {
          "watermark",
          "changelogRecordCount",
          "baseManifestListSize",
          "deltaManifestListSize",
          "changelogManifestListSize"
        }) {
      assertEquals("0", present.get(key));
    }
  }

  @Test
  void filterAndExpiredHoleNeverRefill() throws Exception {
    when(manager.tryGetSnapshot(49)).thenThrow(new FileNotFoundException());
    Pair<List<AmoroSnapshotsOfTable>, Long> page = page(OperationType.OPTIMIZING, 0);
    assertEquals(50L, page.getRight());
    assertTrue(page.getLeft().isEmpty());
    verify(manager, times(25)).tryGetSnapshot(anyLong());
    verify(manager, never()).tryGetSnapshot(25);
  }

  @Test
  void emptyRequiresAbsentDirectory() throws Exception {
    hints(null, null);
    when(io.exists(new Path("/table/snapshot"))).thenReturn(false);
    assertEquals(0L, page(OperationType.ALL, 0).getRight());
    when(io.exists(new Path("/table/snapshot"))).thenReturn(true);
    assertEquals(
        SnapshotReadException.Reason.UNAVAILABLE,
        assertThrows(SnapshotReadException.class, () -> page(OperationType.ALL, 0)).getReason());
  }

  @Test
  void missingEarliestOnlyUsesSnapshotOneAndNeverLists() throws Exception {
    hints(null, 50L);
    when(io.exists(new Path("/table/snapshot/EARLIEST"))).thenReturn(false);
    assertEquals(50L, page(OperationType.ALL, 0).getRight());
    when(io.exists(new Path("/table/snapshot/snapshot-1"))).thenReturn(false);
    assertThrows(SnapshotReadException.class, () -> page(OperationType.ALL, 0));
    verify(io, never()).listStatus(any());
  }

  @Test
  void invalidOrStaleHintsFailClosed() throws Exception {
    for (Long[] pair : new Long[][] {{0L, 50L}, {51L, 50L}, {1L, null}, {null, null}}) {
      hints(pair[0], pair[1]);
      assertThrows(SnapshotReadException.class, () -> page(OperationType.ALL, 0));
    }
    hints(1L, 50L);
    when(io.exists(new Path("/table/snapshot/snapshot-50"))).thenReturn(false);
    assertThrows(SnapshotReadException.class, () -> page(OperationType.ALL, 0));
    verify(manager, never()).tryGetSnapshot(anyLong());
  }

  @Test
  void longTotalAndOutOfRangePages() throws Exception {
    hints(1L, Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, page(OperationType.ALL, 0).getRight());
    hints(1L, 10L);
    assertTrue(page(OperationType.ALL, 25).getLeft().isEmpty());
    assertThrows(
        SnapshotReadException.class,
        () -> descriptor.getSnapshots(amoro, "main", OperationType.ALL, 26, 0, null));
  }

  @Test
  void tagUsesOnlyItsPinnedSnapshotAndRequiresMatchingDetailId() throws Exception {
    BranchManager branches = mock(BranchManager.class);
    TagManager tags = mock(TagManager.class);
    when(table.branchManager()).thenReturn(branches);
    when(table.tagManager()).thenReturn(tags);
    Snapshot pinned = manager.tryGetSnapshot(7);
    Tag tag = mock(Tag.class);
    when(tag.trimToSnapshot()).thenReturn(pinned);
    when(tags.get("retained")).thenReturn(Optional.of(tag));
    assertEquals(
        "7",
        descriptor
            .getSnapshots(amoro, "retained", OperationType.ALL, 25, 0, null)
            .getLeft()
            .get(0)
            .getSnapshotId());
    assertTrue(
        descriptor
            .getSnapshots(amoro, "retained", OperationType.ALL, 25, 25, null)
            .getLeft()
            .isEmpty());
    assertEquals(
        pinned,
        PaimonSnapshotMetadata.resolve(table, "retained", 7, new PaimonSnapshotReadDeadline()));
    assertEquals(
        SnapshotReadException.Reason.INVALID_REQUEST,
        assertThrows(
                SnapshotReadException.class,
                () ->
                    PaimonSnapshotMetadata.resolve(
                        table, "retained", 8, new PaimonSnapshotReadDeadline()))
            .getReason());
    when(tags.get("retained")).thenReturn(Optional.empty());
    assertEquals(
        SnapshotReadException.Reason.NOT_FOUND,
        assertThrows(
                SnapshotReadException.class,
                () ->
                    PaimonSnapshotMetadata.resolve(
                        table, "retained", 7, new PaimonSnapshotReadDeadline()))
            .getReason());
    verify(table, never()).store();
    verify(io, never()).listStatus(any());
  }

  @Test
  void namedBranchUsesItsOwnManagerAndInvalidRefsAreRejected() throws Exception {
    BranchManager branches = mock(BranchManager.class);
    when(table.branchManager()).thenReturn(branches);
    when(branches.branchExists("branch-a")).thenReturn(true);
    SnapshotManager branch = mock(SnapshotManager.class);
    when(manager.copyWithBranch("branch-a")).thenReturn(branch);
    Snapshot snapshot = mock(Snapshot.class);
    when(branch.tryGetSnapshot(3)).thenReturn(snapshot);
    assertEquals(
        snapshot,
        PaimonSnapshotMetadata.resolve(table, "branch-a", 3, new PaimonSnapshotReadDeadline()));
    verify(manager, never()).tryGetSnapshot(anyLong());
    for (String invalid :
        new String[] {"", " ", ".", "..", "../main", "branch/main", "branch\\main"}) {
      assertThrows(SnapshotReadException.class, () -> PaimonSnapshotMetadata.ref(invalid));
    }
  }

  @Test
  void unreadableEarliestHintCannotBeTreatedAsMissing() throws Exception {
    hints(null, 50L);
    when(io.exists(new Path("/table/snapshot/EARLIEST"))).thenReturn(true);
    assertEquals(
        SnapshotReadException.Reason.UNAVAILABLE,
        assertThrows(SnapshotReadException.class, () -> page(OperationType.ALL, 0)).getReason());
    verify(manager, never()).tryGetSnapshot(anyLong());
  }

  @Test
  void nonMainBranchWithoutHintFilesResolvesSnapshotsViaBranchManager() throws Exception {
    BranchManager branches = mock(BranchManager.class);
    when(table.branchManager()).thenReturn(branches);
    when(branches.branchExists("branch-dev")).thenReturn(true);

    SnapshotManager branchMgr = mock(SnapshotManager.class);
    when(manager.copyWithBranch("branch-dev")).thenReturn(branchMgr);
    when(branchMgr.branch()).thenReturn("branch-dev");
    when(branchMgr.fileIO()).thenReturn(io);
    Path branchSnapshotDir = new Path("/table/branch/branch-branch-dev/snapshot");
    when(branchMgr.snapshotDirectory()).thenReturn(branchSnapshotDir);
    when(branchMgr.snapshotPath(7L)).thenReturn(new Path(branchSnapshotDir, "snapshot-7"));
    // Newly branched from a tag at snapshot-7: EARLIEST and LATEST hint files do NOT exist.
    when(io.readOverwrittenFileUtf8(new Path(branchSnapshotDir, "EARLIEST")))
        .thenReturn(Optional.empty());
    when(io.readOverwrittenFileUtf8(new Path(branchSnapshotDir, "LATEST")))
        .thenReturn(Optional.empty());
    when(io.exists(new Path(branchSnapshotDir, "EARLIEST"))).thenReturn(false);
    when(io.exists(new Path(branchSnapshotDir, "LATEST"))).thenReturn(false);
    when(io.exists(branchSnapshotDir)).thenReturn(true);
    when(io.exists(new Path(branchSnapshotDir, "snapshot-7"))).thenReturn(true);
    when(branchMgr.earliestSnapshotId()).thenReturn(7L);
    when(branchMgr.latestSnapshotId()).thenReturn(7L);
    Snapshot snap7 = mock(Snapshot.class);
    when(snap7.id()).thenReturn(7L);
    when(snap7.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
    when(branchMgr.tryGetSnapshot(7L)).thenReturn(snap7);

    Pair<List<AmoroSnapshotsOfTable>, Long> branchPage =
        descriptor.getSnapshots(amoro, "branch-dev", OperationType.ALL, 25, 0, null);
    assertEquals(1L, branchPage.getRight());
    assertEquals(1, branchPage.getLeft().size());
    assertEquals("7", branchPage.getLeft().get(0).getSnapshotId());

    // After a new commit (snapshot-8) on the branch, LATEST hint exists (8) while EARLIEST hint is
    // still absent because no snapshot expiration has run on the branch yet.
    when(branchMgr.snapshotPath(8L)).thenReturn(new Path(branchSnapshotDir, "snapshot-8"));
    when(io.exists(new Path(branchSnapshotDir, "snapshot-8"))).thenReturn(true);
    when(io.readOverwrittenFileUtf8(new Path(branchSnapshotDir, "LATEST")))
        .thenReturn(Optional.of("8"));
    Snapshot snap8 = mock(Snapshot.class);
    when(snap8.id()).thenReturn(8L);
    when(snap8.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
    when(branchMgr.tryGetSnapshot(8L)).thenReturn(snap8);

    Pair<List<AmoroSnapshotsOfTable>, Long> afterCommitPage =
        descriptor.getSnapshots(amoro, "branch-dev", OperationType.ALL, 25, 0, null);
    assertEquals(2L, afterCommitPage.getRight());
    assertEquals(2, afterCommitPage.getLeft().size());
    assertEquals("8", afterCommitPage.getLeft().get(0).getSnapshotId());
    assertEquals("7", afterCommitPage.getLeft().get(1).getSnapshotId());
  }
}
