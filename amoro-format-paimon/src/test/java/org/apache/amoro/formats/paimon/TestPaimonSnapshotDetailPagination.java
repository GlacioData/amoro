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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.SnapshotDetailPage;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Real Paimon Avro files, plus row-reader probes for consumption and close boundaries. */
class TestPaimonSnapshotDetailPagination {
  @TempDir Path warehouse;
  private FileStore<?> store;
  private ManifestList list;
  private ManifestFile files;
  private Snapshot snapshot;
  private int opened;
  private int closed;
  private int listRows;
  private int fileRows;
  private int converted;
  private int fileHasNext;
  private Runnable afterOpen = () -> {};
  private Runnable afterRow = () -> {};
  private Runnable afterDto = () -> {};
  private Runnable afterClose = () -> {};
  private boolean failDto;
  private boolean failClose;

  @BeforeEach
  void setup() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    try (Catalog catalog = PaimonCatalogFactory.paimonCatalog(options, new Configuration())) {
      catalog.createDatabase("db", true);
      Identifier id = Identifier.create("db", "tbl");
      catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
      store = spy(((FileStoreTable) catalog.getTable(id)).store());
    }
    list = spy(store.manifestListFactory().create());
    files = spy(store.manifestFileFactory().create());
    ManifestList.Factory listFactory = mock(ManifestList.Factory.class);
    ManifestFile.Factory fileFactory = mock(ManifestFile.Factory.class);
    when(listFactory.create()).thenReturn(list);
    when(fileFactory.create()).thenReturn(files);
    doReturn(listFactory).when(store).manifestListFactory();
    doReturn(fileFactory).when(store).manifestFileFactory();
    snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(1L);
  }

  private PaimonSnapshotRows track(PaimonSnapshotRows reader, boolean meta) {
    opened++;
    afterOpen.run();
    return new PaimonSnapshotRows(reader.deadline) {
      @Override
      long skip(long count) {
        return reader.skip(count);
      }

      @Override
      public boolean hasNext() {
        if (!meta) {
          fileHasNext++;
        }
        return reader.hasNext();
      }

      @Override
      public InternalRow next() {
        if (meta) {
          listRows++;
        } else {
          fileRows++;
        }
        InternalRow row = reader.next();
        afterRow.run();
        return row;
      }

      @Override
      public void close() throws Exception {
        try {
          reader.close();
        } finally {
          closed++;
          afterClose.run();
          if (failClose) {
            throw new java.io.IOException("fixture close failure");
          }
        }
      }
    };
  }

  private ManifestFileMeta manifest(int count) {
    List<ManifestEntry> entries = new ArrayList<>();
    DataFileMeta data = dataFile("same-file");
    for (int i = 0; i < count; i++) {
      entries.add(
          ManifestEntry.create(
              i % 2 == 0 ? FileKind.ADD : FileKind.DELETE, BinaryRow.EMPTY_ROW, 0, 1, data));
    }
    return files.write(entries).get(0);
  }

  private DataFileMeta dataFile(String name) {
    return DataFileMeta.forAppend(
        name,
        128,
        1,
        SimpleStats.EMPTY_STATS,
        0,
        0,
        0,
        Collections.emptyList(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private void manifests(List<ManifestFileMeta> metas) {
    org.apache.paimon.utils.Pair<String, Long> location = list.write(metas);
    when(snapshot.deltaManifestList()).thenReturn(location.getLeft());
    when(snapshot.deltaManifestListSize()).thenReturn(location.getRight());
  }

  private SnapshotDetailPage read(String token) {
    return read(token, new PaimonSnapshotReadDeadline());
  }

  private SnapshotDetailPage read(String token, PaimonSnapshotReadDeadline deadline) {
    PaimonSnapshotDetailReader reader = spy(new PaimonSnapshotDetailReader(deadline));
    try {
      doAnswer(i -> track((PaimonSnapshotRows) i.callRealMethod(), i.getArgument(0) == list))
          .when(reader)
          .openRows(any(), any(), any(), any(), anyBoolean());
    } catch (java.io.IOException e) {
      throw new AssertionError(e);
    }
    return reader.read(
        store,
        snapshot,
        "catalog.db.tbl",
        "main",
        25,
        token,
        entry -> {
          converted++;
          afterDto.run();
          if (failDto) {
            throw new IllegalStateException("fixture DTO failure");
          }
          return new PartitionFileBaseInfo(
              null,
              "BASE_FILE",
              0L,
              "bucket-0",
              entry.file().fileName(),
              entry.file().fileSize(),
              entry.kind().name());
        });
  }

  @Test
  void largeManifestStopsAtPageThenResumesWithoutConvertingPrefix() {
    manifests(Collections.singletonList(manifest(10000)));
    SnapshotDetailPage first = read(null);
    assertEquals(25, first.getList().size());
    assertEquals(25, converted);
    assertEquals(25, fileRows);
    assertEquals(25, fileHasNext, "no lookahead after the 25th returned entry");
    assertEquals(1, listRows);
    assertEquals(opened, closed);
    String firstFile = first.getList().get(0).getFile();
    SnapshotDetailPage second = read(first.getNextCursor());
    assertEquals(25, second.getList().size());
    assertEquals(50, converted);
    assertEquals(50, fileRows); // Prefix encoding is skipped without constructing InternalRows.
    assertEquals(firstFile, second.getList().get(0).getFile());
    assertEquals("DELETE", second.getList().get(0).getOperation());
    assertEquals("ADD", second.getList().get(1).getOperation());
    assertEquals(opened, closed);
    assertEquals("ADD", first.getList().get(0).getOperation());
    assertEquals("DELETE", first.getList().get(1).getOperation());
  }

  @Test
  void crossesManifestsAndMayNeedExtraTerminalPage() {
    manifests(Arrays.asList(manifest(10), manifest(40)));
    SnapshotDetailPage first = read(null);
    SnapshotDetailPage second = read(first.getNextCursor());
    assertEquals(25, first.getList().size());
    assertEquals(25, second.getList().size());
    assertNotNull(second.getNextCursor());
    SnapshotDetailPage terminal = read(second.getNextCursor());
    assertTrue(terminal.getList().isEmpty());
    assertNull(terminal.getNextCursor());
    assertEquals(50, converted);
    assertEquals(opened, closed);
  }

  @Test
  void emptyManifestBudgetDoesNotOpenTwentySixthMetaOrAnyEmptyFile() {
    ManifestFileMeta empty =
        new ManifestFileMeta(
            "not-opened", 0, 0, 0, SimpleStats.EMPTY_STATS, 0, null, null, null, null, null, null);
    List<ManifestFileMeta> metas = new ArrayList<>(Collections.nCopies(30, empty));
    metas.add(manifest(1));
    manifests(metas);
    SnapshotDetailPage first = read(null);
    assertTrue(first.getList().isEmpty());
    assertNotNull(first.getNextCursor());
    assertEquals(25, listRows);
    assertEquals(1, opened);
    SnapshotDetailPage second = read(first.getNextCursor());
    assertEquals(1, second.getList().size());
    assertEquals(1, converted);
    assertEquals(31, listRows, "ManifestList prefix must not construct repeated metadata rows");
    assertNull(second.getNextCursor());
    assertEquals(opened, closed);
  }

  @Test
  void uniqueEntriesAcrossManyManifestsResumeAtExactLogicalPosition() {
    List<ManifestFileMeta> metas = new ArrayList<>();
    for (int manifest = 0; manifest < 31; manifest++) {
      List<ManifestEntry> entries = new ArrayList<>();
      for (int row = 0; row < 3; row++) {
        int id = manifest * 3 + row;
        entries.add(
            ManifestEntry.create(
                id % 2 == 0 ? FileKind.ADD : FileKind.DELETE,
                BinaryRow.EMPTY_ROW,
                0,
                1,
                dataFile("file-" + id)));
      }
      metas.addAll(files.write(entries));
    }
    manifests(metas);
    int expected = 0;
    String cursor = null;
    do {
      SnapshotDetailPage page = read(cursor);
      assertTrue(page.getList().size() <= 25);
      for (PartitionFileBaseInfo row : page.getList()) {
        assertEquals("file-" + expected, row.getFile());
        assertEquals(expected % 2 == 0 ? "ADD" : "DELETE", row.getOperation());
        expected++;
      }
      cursor = page.getNextCursor();
    } while (cursor != null);
    assertEquals(93, expected);
    assertEquals(93, fileRows);
    assertEquals(opened, closed);
  }

  @Test
  void invalidCursorAndCrossContextRejected() {
    manifests(Collections.singletonList(manifest(50)));
    String token = read(null).getNextCursor();
    assertThrows(SnapshotReadException.class, () -> read("not base64"));
    assertThrows(
        SnapshotReadException.class, () -> read(String.join("", Collections.nCopies(4097, "a"))));
    when(snapshot.id()).thenReturn(2L);
    assertEquals(
        SnapshotReadException.Reason.INVALID_REQUEST,
        assertThrows(SnapshotReadException.class, () -> read(token)).getReason());
  }

  @Test
  void cursorBeyondEofAndEntryCountRejected() {
    manifests(Collections.singletonList(manifest(50)));
    byte[] bytes = Base64.getUrlDecoder().decode(read(null).getNextCursor());
    ByteBuffer.wrap(bytes).putLong(33, 9);
    assertThrows(
        SnapshotReadException.class, () -> read(Base64.getUrlEncoder().encodeToString(bytes)));
    ByteBuffer.wrap(bytes).putLong(33, 0).putLong(41, 51);
    assertThrows(
        SnapshotReadException.class, () -> read(Base64.getUrlEncoder().encodeToString(bytes)));
    assertEquals(opened, closed);
  }

  @Test
  void corruptCountClosesBothReadersWithoutPartialResponse() {
    ManifestFileMeta actual = manifest(1);
    manifests(
        Collections.singletonList(
            new ManifestFileMeta(
                actual.fileName(),
                actual.fileSize(),
                10,
                0,
                actual.partitionStats(),
                actual.schemaId(),
                null,
                null,
                null,
                null,
                null,
                null)));
    assertEquals(
        SnapshotReadException.Reason.READ_FAILED,
        assertThrows(SnapshotReadException.class, () -> read(null)).getReason());
    assertEquals(opened, closed);
  }

  @Test
  void deadlineAndInterruptionReleaseReaders() {
    manifests(Collections.singletonList(manifest(100)));
    AtomicLong ticks = new AtomicLong();
    PaimonSnapshotReadDeadline deadline =
        new PaimonSnapshotReadDeadline(ticks::incrementAndGet, 40);
    assertEquals(
        SnapshotReadException.Reason.TIMEOUT,
        assertThrows(SnapshotReadException.class, () -> read(null, deadline)).getReason());
    assertEquals(opened, closed);
    Thread.currentThread().interrupt();
    try {
      assertEquals(
          SnapshotReadException.Reason.UNAVAILABLE,
          assertThrows(SnapshotReadException.class, () -> read(null)).getReason());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void closeFailuresPreservePrimaryFailureAndCloseBothReaders() {
    manifests(Collections.singletonList(manifest(50)));
    failDto = true;
    failClose = true;
    SnapshotReadException failure = assertThrows(SnapshotReadException.class, () -> read(null));
    assertEquals("fixture DTO failure", failure.getCause().getMessage());
    assertEquals(2, failure.getSuppressed().length);
    assertEquals(opened, closed);
  }

  @Test
  void timeoutAfterOpenClosesTheOpenedReader() {
    manifests(Collections.singletonList(manifest(50)));
    AtomicLong clock = new AtomicLong();
    afterOpen = () -> clock.set(100);
    assertEquals(
        SnapshotReadException.Reason.TIMEOUT,
        assertThrows(
                SnapshotReadException.class,
                () -> read(null, new PaimonSnapshotReadDeadline(clock::get, 100)))
            .getReason());
    assertEquals(1, opened);
    assertEquals(opened, closed);
  }

  @Test
  void timeoutAfterDtoAndAfterCloseNeverReturnsPartialPage() {
    manifests(Collections.singletonList(manifest(50)));
    AtomicLong clock = new AtomicLong();
    afterDto = () -> clock.set(100);
    assertThrows(
        SnapshotReadException.class,
        () -> read(null, new PaimonSnapshotReadDeadline(clock::get, 100)));
    assertEquals(opened, closed);
    clock.set(0);
    afterDto = () -> {};
    afterClose = () -> clock.set(100);
    assertThrows(
        SnapshotReadException.class,
        () -> read(null, new PaimonSnapshotReadDeadline(clock::get, 100)));
    assertEquals(opened, closed);
  }

  @Test
  void deadlineCoversEntryPrefixAndManifestPrefixWithoutReset() {
    manifests(Collections.singletonList(manifest(100)));
    String token = read(null).getNextCursor();
    AtomicLong clock = new AtomicLong();
    int before = converted;
    assertThrows(
        SnapshotReadException.class,
        () -> read(token, new PaimonSnapshotReadDeadline(clock::incrementAndGet, 20)));
    assertEquals(before, converted);
    assertEquals(opened, closed);

    afterRow = () -> {};
    ManifestFileMeta empty =
        new ManifestFileMeta(
            "unused", 0, 0, 0, SimpleStats.EMPTY_STATS, 0, null, null, null, null, null, null);
    manifests(Collections.nCopies(50, empty));
    String metaToken = read(null).getNextCursor();
    clock.set(0);
    assertThrows(
        SnapshotReadException.class,
        () -> read(metaToken, new PaimonSnapshotReadDeadline(clock::incrementAndGet, 20)));
    assertEquals(before, converted);
    assertEquals(opened, closed);
  }
}
