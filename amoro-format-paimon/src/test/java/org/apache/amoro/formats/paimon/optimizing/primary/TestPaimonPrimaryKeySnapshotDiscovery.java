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

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.append;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.commit;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

class TestPaimonPrimaryKeySnapshotDiscovery {
  @ParameterizedTest
  @CsvSource({"0,2", "2,4", "31,33"})
  void shortGapDoesNotUseNativeFallback(int gap, int expectedProbes) throws Exception {
    Fixture fixture = new Fixture(10, 10 + gap, null);
    assertEquals(10 + gap, fixture.restore().snapshotId());
    assertEquals(expectedProbes, fixture.probes.get());
    verify(fixture.manager, never()).snapshotIdStream();
  }

  @Test
  void currentHintKeepsFastPath() throws Exception {
    Fixture fixture = new Fixture(10, 1010, "1010");
    assertEquals(1010, fixture.restore().snapshotId());
    assertEquals(3, fixture.probes.get());
    verify(fixture.manager, never()).snapshotIdStream();
  }

  @ParameterizedTest
  @ValueSource(ints = {32, 1000})
  void longGapUsesOneNativeLookupAndStopsProbing(int gap) throws Exception {
    Fixture fixture = new Fixture(10, 10 + gap, null);
    assertEquals(10 + gap, fixture.restore().snapshotId());
    assertEquals(33, fixture.probes.get());
    verify(fixture.manager).snapshotIdStream();
    verify(fixture.manager).snapshot(10L + gap);
  }

  @Test
  void staleHintAlsoHasBoundedProbes() throws Exception {
    Fixture fixture = new Fixture(10, 1010, "100");
    assertEquals(1010, fixture.restore().snapshotId());
    assertEquals(
        34, fixture.probes.get(), "Hint validation, 32 successors and selected snapshot check");
    verify(fixture.manager).snapshotIdStream();
  }

  @Test
  void repeatedTaskDiscoveryDoesNotScaleWithSnapshotGap() throws Exception {
    Fixture fixture = new Fixture(10, 1010, null);
    for (int i = 0; i < 16; i++) {
      assertEquals(1010, fixture.restore().snapshotId());
    }
    assertEquals(528, fixture.probes.get());
    verify(fixture.manager, times(16)).snapshotIdStream();
    System.out.println(
        "SNAPSHOT_DISCOVERY tasks=16 gap=1000 directExists="
            + fixture.probes.get()
            + " nativeLookups=16");
  }

  @Test
  void fallbackFailureDoesNotRestoreAnOlderSnapshot() throws Exception {
    Fixture fixture = new Fixture(10, 1010, null);
    IllegalStateException failure = new IllegalStateException("listing unavailable");
    when(fixture.manager.snapshotIdStream()).thenThrow(failure);
    assertSame(failure, assertThrows(IllegalStateException.class, fixture::restore));
    verify(fixture.manager, never()).snapshot(anyLong());
    assertEquals(32, fixture.probes.get());
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, 10, 41})
  void missingOrRegressedFallbackCannotUsePartialProgress(long result) throws Exception {
    Fixture fixture = new Fixture(10, 1010, null);
    when(fixture.manager.snapshotIdStream())
        .thenAnswer(ignored -> result < 0 ? Stream.empty() : Stream.of(result));
    assertThrows(IllegalStateException.class, fixture::restore);
    verify(fixture.manager, never()).snapshot(anyLong());
  }

  @Test
  void maximumSnapshotIdDoesNotOverflow() throws Exception {
    Fixture fixture = new Fixture(Long.MAX_VALUE, Long.MAX_VALUE, null);
    assertEquals(Long.MAX_VALUE, fixture.restore().snapshotId());
    assertEquals(1, fixture.probes.get());
    verify(fixture.manager, never()).snapshotIdStream();
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "-1"})
  void realNativeFallbackExecutesAndCommitsLatestRows(String bucket, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(false);
      opts.put("bucket", bucket);
      opts.put("deletion-vectors.enabled", "true");
      FileStoreTable raw = table(catalog, "bounded_restore", opts);
      append(raw, GenericRow.of(1, 10, 10, 1L));
      SnapshotManager manager = spy(raw.snapshotManager());
      FileStoreTable observed = spy(raw);
      doReturn(manager).when(observed).snapshotManager();
      PaimonPrimaryKeyCompactionInput planned = input(observed, OptimizingType.MINOR, 501L);
      for (int i = 2; i <= 41; i++) {
        append(raw, GenericRow.of(1, i, i, (long) i));
      }
      HintFileUtils.deleteLatestHint(manager.fileIO(), manager.snapshotDirectory());
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(planned).execute();
      verify(manager).snapshotIdStream();
      commit(raw, planned, output);
      assertEquals(Collections.singletonList("1|41|41|41"), rows(raw));
    }
  }

  @Test
  void expiredPlannedSnapshotListsOnceEvenWhenItsSuccessorIsAbsent() throws Exception {
    Fixture fixture = new Fixture(10, 1010, "10");
    doReturn(false).when(fixture.manager).snapshotExists(10L);
    doReturn(false).when(fixture.manager).snapshotExists(11L);
    assertEquals(1010, fixture.restore().snapshotId());
    verify(fixture.manager).snapshotIdStream();
    verify(fixture.manager, never()).snapshot(10L);
  }

  @Test
  void selectedMaximumExpiringDoesNotFallBackOrListAgain() throws Exception {
    Fixture fixture = new Fixture(10, 1010, null);
    when(fixture.manager.snapshotIdStream()).thenReturn(Stream.of(1009L, 1010L));
    doReturn(false).when(fixture.manager).snapshotExists(1010L);
    assertThrows(IllegalStateException.class, fixture::restore);
    verify(fixture.manager).snapshotIdStream();
    verify(fixture.manager, never()).snapshot(anyLong());
  }

  @Test
  void enumerationIoFailurePreservesCause() throws Exception {
    Fixture fixture = new Fixture(10, 1010, null);
    IOException failure = new IOException("permission denied");
    when(fixture.manager.snapshotIdStream()).thenThrow(failure);
    assertSame(failure, assertThrows(UncheckedIOException.class, fixture::restore).getCause());
    verify(fixture.manager).snapshotIdStream();
  }

  @Test
  void existsAndSnapshotReadFailuresDoNotTriggerRetryListing() throws Exception {
    Fixture fixture = new Fixture(10, 12, null);
    IllegalStateException failure = new IllegalStateException("storage timeout");
    when(fixture.manager.snapshotExists(11L)).thenThrow(failure);
    assertSame(failure, assertThrows(IllegalStateException.class, fixture::restore));
    verify(fixture.manager, never()).snapshotIdStream();
    Fixture listed = new Fixture(10, 1010, null);
    when(listed.manager.snapshot(1010L)).thenThrow(failure);
    assertSame(failure, assertThrows(IllegalStateException.class, listed::restore));
    verify(listed.manager).snapshotIdStream();
  }

  private static Snapshot snapshot(long id) {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(id);
    return snapshot;
  }

  private static class Fixture {
    final FileStoreTable table = mock(FileStoreTable.class);
    final SnapshotManager manager = mock(SnapshotManager.class);
    final AtomicInteger probes = new AtomicInteger();
    final long planned;

    @SuppressWarnings("unchecked")
    Fixture(long planned, long latest, String hint) throws Exception {
      this.planned = planned;
      FileStore store = mock(FileStore.class);
      FileStoreScan scan = mock(FileStoreScan.class);
      FileIO io = mock(FileIO.class);
      when(table.options()).thenReturn(Collections.emptyMap());
      when(table.store()).thenReturn(store);
      when(table.snapshotManager()).thenReturn(manager);
      when(store.newScan()).thenReturn(scan);
      when(manager.fileIO()).thenReturn(io);
      when(manager.snapshotDirectory())
          .thenReturn(new org.apache.paimon.fs.Path("file:/tmp/discovery-test"));
      when(io.readOverwrittenFileUtf8(any())).thenReturn(Optional.ofNullable(hint));
      when(manager.snapshotExists(anyLong()))
          .thenAnswer(
              call -> {
                probes.incrementAndGet();
                long id = call.getArgument(0);
                return id > 0 && id <= latest;
              });
      doReturn(snapshot(latest)).when(manager).snapshot(latest);
      when(manager.snapshotIdStream()).thenAnswer(ignored -> Stream.of(latest));
    }

    PaimonPrimaryKeySnapshotWriteRestore restore() {
      return new PaimonPrimaryKeySnapshotWriteRestore(table, planned, Collections.emptyList());
    }
  }
}
