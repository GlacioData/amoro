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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class TestPaimonPrimaryKeyPreparedRestore {
  @Test
  void secondUnitFailurePreventsAllWriters(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable old = table(catalog, "schema", options(true));
      append(old, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyCompactionInput planned = input(old, OptimizingType.MINOR, 99L);
      FileStoreTable tracked = spy(old);
      FileStore<?> store = spy(old.store());
      FileStoreScan scan = spy(old.store().newScan());
      doReturn(store).when(tracked).store();
      doReturn(scan).when(store).newScan();
      java.util.concurrent.atomic.AtomicInteger reads =
          new java.util.concurrent.atomic.AtomicInteger();
      doAnswer(
              invocation -> {
                if (reads.incrementAndGet() == 1) {
                  return invocation.callRealMethod();
                }
                return new java.util.Iterator<org.apache.paimon.manifest.ManifestEntry>() {
                  @Override
                  public boolean hasNext() {
                    throw new IllegalStateException("second unit iterator failed");
                  }

                  @Override
                  public org.apache.paimon.manifest.ManifestEntry next() {
                    throw new java.util.NoSuchElementException();
                  }
                };
              })
          .when(scan)
          .readFileIterator();
      PaimonBucketCompactionUnit second =
          new PaimonBucketCompactionUnit(
              SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW), 1, 1, 1, 1, 1);
      PaimonPrimaryKeyCompactionInput multi =
          new PaimonPrimaryKeyCompactionInput(
              wrap(tracked),
              Arrays.asList(planned.getUnits().get(0), second),
              planned.getOptimizingType(),
              false,
              planned.getTargetSnapshotId(),
              planned.getCommitUser(),
              planned.getCommitIdentifier());
      assertThrows(
          IllegalStateException.class,
          () -> new PaimonPrimaryKeyCompactionExecutor(multi).execute());
      verify(scan, times(2)).readFileIterator();
      verify(tracked, never()).copy(anyMap());
      verify(tracked, never()).newBatchWriteBuilder();
      assertEquals(planned.getTargetSnapshotId(), old.latestSnapshot().get().id());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void indexReadFailureAfterFilesPreventsWriter(boolean dynamic, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(true);
      opts.put("bucket", dynamic ? "-1" : "1");
      opts.put("deletion-vectors.enabled", String.valueOf(!dynamic));
      FileStoreTable table = table(catalog, "index_failure", opts);
      append(table, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyCompactionInput planned = input(table, OptimizingType.MINOR, 99L);
      FileStoreTable tracked = spy(table);
      FileStore<?> store = spy(table.store());
      FileStoreScan scan = spy(table.store().newScan());
      IndexFileHandler indexes = spy(store.newIndexFileHandler());
      doReturn(store).when(tracked).store();
      doReturn(scan).when(store).newScan();
      doReturn(indexes).when(store).newIndexFileHandler();
      org.apache.paimon.Snapshot snapshot = table.latestSnapshot().get();
      IllegalStateException failure = new IllegalStateException("index scan failed");
      if (dynamic) {
        doThrow(failure).when(indexes).scanHashIndex(snapshot, BinaryRow.EMPTY_ROW, 0);
      } else {
        doThrow(failure)
            .when(indexes)
            .scan(
                snapshot,
                org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX,
                BinaryRow.EMPTY_ROW,
                0);
      }
      PaimonPrimaryKeyCompactionInput input =
          new PaimonPrimaryKeyCompactionInput(
              wrap(tracked),
              planned.getUnits(),
              planned.getOptimizingType(),
              false,
              planned.getTargetSnapshotId(),
              planned.getCommitUser(),
              planned.getCommitIdentifier());
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () -> new PaimonPrimaryKeyCompactionExecutor(input).execute());
      assertEquals(failure, error.getCause());
      assertEquals(
          "Failed to prepare Paimon compaction inputs: table="
              + input.getTable().id()
              + ", plannedSnapshot="
              + planned.getTargetSnapshotId(),
          error.getMessage());
      verify(scan).readFileIterator();
      verify(tracked, never()).copy(anyMap());
      verify(tracked, never()).newBatchWriteBuilder();
      assertEquals(snapshot.id(), table.latestSnapshot().get().id());
    }
  }

  @ParameterizedTest
  @CsvSource({"1,false", "1,true", "-1,false", "-1,true"})
  void frozenRestoreRejectsUnexpectedKeysAndFlagsAndCopiesCollections(
      String bucket, boolean dv, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(true);
      opts.put("bucket", bucket);
      opts.put("deletion-vectors.enabled", String.valueOf(dv));
      FileStoreTable table = table(catalog, "schema", opts);
      append(table, GenericRow.of(1, 10, 20, 1L));
      PaimonPrimaryKeyCompactionInput in = input(table, OptimizingType.MINOR, 99L);
      PaimonPrimaryKeySnapshotWriteRestore restore =
          new PaimonPrimaryKeySnapshotWriteRestore(table, in.getTargetSnapshotId(), in.getUnits());
      boolean dynamic = bucket.equals("-1");
      RestoreFiles files = restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, dynamic, dv);
      assertEquals(1, files.dataFiles().size());
      files.dataFiles().clear();
      assertEquals(1, restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, dynamic, dv).dataFiles().size());
      if (dv) {
        files.deleteVectorsIndex().clear();
        assertEquals(
            Collections.emptyList(),
            restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, dynamic, true).deleteVectorsIndex());
      }
      assertThrows(
          IllegalStateException.class,
          () -> restore.restoreFiles(BinaryRow.EMPTY_ROW, 1, dynamic, dv));
      assertThrows(
          IllegalStateException.class,
          () -> restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, !dynamic, dv));
      assertThrows(
          IllegalStateException.class,
          () -> restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, dynamic, !dv));
      assertThrows(
          IllegalStateException.class,
          () ->
              new PaimonPrimaryKeySnapshotWriteRestore(
                  table,
                  in.getTargetSnapshotId(),
                  Arrays.asList(in.getUnits().get(0), in.getUnits().get(0))));
      long selectedSnapshot = restore.snapshotId();
      append(table, GenericRow.of(2, 11, 21, 2L));
      assertEquals(
          selectedSnapshot,
          restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, dynamic, dv).snapshot().id());
      assertEquals(1, restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, dynamic, dv).dataFiles().size());
    }
  }
}
