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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class TestPaimonSnapshotStatistics {
  @TempDir Path warehouse;
  private FileStoreTable table;
  private FileStore<?> store;
  private ManifestList list;
  private FileIO io;
  private Snapshot snapshot;
  private final AtomicReference<SeekableInputStream> input = new AtomicReference<>();
  private Runnable afterOpen = () -> {};

  @BeforeEach
  void setup() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    try (Catalog catalog = PaimonCatalogFactory.paimonCatalog(options, new Configuration())) {
      catalog.createDatabase("db", true);
      Identifier id = Identifier.create("db", "tbl");
      catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
      table = spy((FileStoreTable) catalog.getTable(id));
      store = spy(table.store());
    }
    doReturn(store).when(table).store();
    list = spy(store.manifestListFactory().create());
    FileIO original = list.fileIO();
    io = spy(original);
    doAnswer(
            invocation -> {
              SeekableInputStream stream = spy(original.newInputStream(invocation.getArgument(0)));
              input.set(stream);
              afterOpen.run();
              return stream;
            })
        .when(io)
        .newInputStream(any());
    doReturn(io).when(list).fileIO();
    ManifestList.Factory factory = mock(ManifestList.Factory.class);
    when(factory.create()).thenReturn(list);
    doReturn(factory).when(store).manifestListFactory();
    snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(1L);
    when(snapshot.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
    clearInvocations(table, store);
  }

  private ManifestFileMeta meta(long added, long deleted, long bytes) {
    // Deliberately no corresponding manifest file: listing must never open its entries.
    return new ManifestFileMeta(
        "absent-manifest",
        bytes,
        added,
        deleted,
        SimpleStats.EMPTY_STATS,
        0,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private void write(ManifestFileMeta... metas) {
    org.apache.paimon.utils.Pair<String, Long> written = list.write(Arrays.asList(metas));
    when(snapshot.deltaManifestList()).thenReturn(written.getLeft());
    when(snapshot.deltaManifestListSize()).thenReturn(written.getRight());
  }

  private AmoroSnapshotsOfTable read() {
    return PaimonSnapshotMetadata.summary(table, snapshot, new PaimonSnapshotReadDeadline());
  }

  @Test
  void aggregatesRawCountsWithoutOpeningEntryFiles() throws Exception {
    write(meta(10, 30, 100), meta(3, 1, 200));
    when(snapshot.totalRecordCount()).thenReturn(99L);
    AmoroSnapshotsOfTable row = read();
    assertEquals(-18L, row.getFileCount());
    assertEquals(99L, row.getRecords());
    assertNull(row.getFileSize());
    assertEquals("13", row.getSummary().get("addedFiles"));
    assertEquals("31", row.getSummary().get("deletedFiles"));
    assertEquals("44", row.getSummary().get("deltaEntries"));
    assertEquals("2", row.getSummary().get("deltaManifestCount"));
    assertEquals("300", row.getSummary().get("deltaManifestBytes"));
    assertEquals(3, row.getFilesSummaryForChart().size());
    assertEquals("13", row.getFilesSummaryForChart().get("addedFiles"));
    assertEquals("31", row.getFilesSummaryForChart().get("deletedFiles"));
    assertEquals("-18", row.getFilesSummaryForChart().get("netFileChange"));
    verify(store, never()).manifestFileFactory();
    verify(list, never()).readDeltaManifests(any());
    verify(snapshot, never()).baseManifestList();
    verify(snapshot, never()).changelogManifestList();
    verify(io, times(1)).newInputStream(any());
    verify(input.get()).close();
  }

  @Test
  void countsBeyondIntegerRangeRemainExact() {
    write(meta(3000000000L, 1, 1));
    AmoroSnapshotsOfTable row = read();
    assertEquals(2999999999L, row.getFileCount());
    assertEquals("3000000001", row.getSummary().get("deltaEntries"));
  }

  @Test
  void absentAndEmptyListsHaveZeroStatistics() {
    assertEquals(0L, read().getFileCount());
    verify(table, never()).store();
    write();
    AmoroSnapshotsOfTable row = read();
    assertEquals(0L, row.getFileCount());
    assertEquals("0", row.getSummary().get("deltaEntries"));
    assertEquals("0", row.getSummary().get("deltaManifestCount"));
    assertEquals("0", row.getSummary().get("deltaManifestBytes"));
    assertEquals("0", row.getFilesSummaryForChart().get("netFileChange"));
    assertTrue(PaimonSnapshotMetadata.summary(snapshot).getFilesSummaryForChart().isEmpty());
  }

  @Test
  void statisticsScanTheWholeListNotOnlyTheFirst25MetadataRows() {
    ManifestFileMeta[] metas = new ManifestFileMeta[70];
    Arrays.fill(metas, meta(2, 1, 10));
    write(metas);
    AmoroSnapshotsOfTable row = read();
    assertEquals(70L, row.getFileCount());
    assertEquals("210", row.getSummary().get("deltaEntries"));
    assertEquals("70", row.getSummary().get("deltaManifestCount"));
    assertEquals("700", row.getSummary().get("deltaManifestBytes"));
  }

  @Test
  void missingListFailsInsteadOfReportingZero() {
    when(snapshot.deltaManifestList()).thenReturn("missing-list");
    when(snapshot.deltaManifestListSize()).thenReturn(null);
    assertEquals(
        SnapshotReadException.Reason.READ_FAILED,
        assertThrows(SnapshotReadException.class, this::read).getReason());
  }

  @Test
  void unknownListSizeUsesTheSameStatistics() {
    write(meta(5, 0, 7));
    when(snapshot.deltaManifestListSize()).thenReturn(null);
    assertEquals(5L, read().getFileCount());
  }

  @Test
  void invalidAndOverflowingCountsFailInsteadOfPublishingPartialTotals() throws Exception {
    for (ManifestFileMeta[] metas :
        new ManifestFileMeta[][] {
          {meta(-1, 0, 1)},
          {meta(0, -1, 1)},
          {meta(0, 0, -1)},
          {meta(Long.MAX_VALUE, 1, 1)},
          {meta(Long.MAX_VALUE, 0, 1), meta(1, 0, 1)},
          {meta(0, 0, Long.MAX_VALUE), meta(0, 0, 1)}
        }) {
      write(metas);
      assertEquals(
          SnapshotReadException.Reason.READ_FAILED,
          assertThrows(SnapshotReadException.class, this::read).getReason());
      verify(input.get()).close();
    }
  }

  @Test
  void timeoutAfterOpeningStillClosesInput() throws Exception {
    write(meta(1, 0, 10));
    AtomicLong clock = new AtomicLong();
    PaimonSnapshotReadDeadline deadline = new PaimonSnapshotReadDeadline(clock::get, 1);
    afterOpen = () -> clock.set(2);
    assertEquals(
        SnapshotReadException.Reason.TIMEOUT,
        assertThrows(
                SnapshotReadException.class,
                () -> PaimonSnapshotMetadata.summary(table, snapshot, deadline))
            .getReason());
    verify(input.get()).close();
  }

  @Test
  void pageOnlyOpensListsForItsMatchingCandidateSnapshots() throws Exception {
    write(meta(1, 0, 10));
    SnapshotManager manager = mock(SnapshotManager.class);
    FileIO hints = mock(FileIO.class);
    org.apache.paimon.fs.Path dir = new org.apache.paimon.fs.Path("/snapshots");
    doReturn(manager).when(table).snapshotManager();
    when(manager.copyWithBranch("main")).thenReturn(manager);
    when(manager.fileIO()).thenReturn(hints);
    when(manager.snapshotDirectory()).thenReturn(dir);
    when(hints.exists(any())).thenReturn(true);
    when(hints.readOverwrittenFileUtf8(new org.apache.paimon.fs.Path(dir, "EARLIEST")))
        .thenReturn(Optional.of("1"));
    when(hints.readOverwrittenFileUtf8(new org.apache.paimon.fs.Path(dir, "LATEST")))
        .thenReturn(Optional.of("50"));
    for (long id = 26; id <= 50; id++) {
      when(manager.tryGetSnapshot(id)).thenReturn(snapshot);
    }
    assertEquals(
        25,
        PaimonSnapshotMetadata.page(
                table, "main", OperationType.ALL, 25, 0, new PaimonSnapshotReadDeadline())
            .getLeft()
            .size());
    verify(io, times(25)).newInputStream(any());
    verify(manager, never()).tryGetSnapshot(25);
    assertEquals(
        Collections.emptyList(),
        PaimonSnapshotMetadata.page(
                table, "main", OperationType.OPTIMIZING, 25, 0, new PaimonSnapshotReadDeadline())
            .getLeft());
    verify(io, times(25)).newInputStream(any());
    verify(store, never()).manifestFileFactory();
  }
}
