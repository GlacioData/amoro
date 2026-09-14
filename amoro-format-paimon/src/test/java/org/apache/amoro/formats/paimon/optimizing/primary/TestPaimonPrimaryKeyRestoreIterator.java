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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** 使用真实 manifest 对照原生 plan 与生产恢复路径，包含全部文件字段及索引。 */
public class TestPaimonPrimaryKeyRestoreIterator {
  @ParameterizedTest
  @CsvSource({
    "1,false,false,false", "1,false,false,true", "1,false,true,false", "1,false,true,true",
    "1,true,false,false", "1,true,false,true", "1,true,true,false", "1,true,true,true",
    "-1,false,false,false", "-1,false,false,true", "-1,false,true,false", "-1,false,true,true",
    "-1,true,false,false", "-1,true,false,true", "-1,true,true,false", "-1,true,true,true"
  })
  void preparedFilesMatchNativePlan(
      String bucket, boolean dv, boolean drop, boolean partitioned, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      catalog.createDatabase("db", true);
      Schema.Builder schema =
          Schema.newBuilder()
              .column("p", DataTypes.INT())
              .column("id", DataTypes.INT())
              .column("value", DataTypes.STRING());
      if (partitioned) {
        schema.primaryKey("p", "id").partitionKeys("p");
      } else {
        schema.primaryKey("id");
      }
      Map<String, String> opts = options(false);
      opts.put("bucket", bucket);
      opts.put("deletion-vectors.enabled", String.valueOf(dv));
      opts.put(
          org.apache.paimon.CoreOptions.MANIFEST_DELETE_FILE_DROP_STATS.key(),
          String.valueOf(drop));
      opts.forEach(schema::option);
      Identifier id = Identifier.create("db", "restore");
      catalog.createTable(id, schema.build(), false);
      FileStoreTable table = (FileStoreTable) catalog.getTable(id);
      append(table, row(1), row(2));
      append(table, row(3));
      List<ManifestEntry> initial = table.store().newScan().plan().files();
      BinaryRow partition = initial.get(0).partition().copy();
      // 真实 compact 生成 manifest DELETE，使比较不局限于纯 ADD 快照。
      org.apache.paimon.table.sink.BatchWriteBuilder builder =
          table.copy(Collections.singletonMap("write-only", "false")).newBatchWriteBuilder();
      try (org.apache.paimon.disk.IOManager io =
              org.apache.paimon.disk.IOManager.create(warehouse.toString());
          org.apache.paimon.table.sink.BatchTableWrite writer = builder.newWrite();
          org.apache.paimon.table.sink.BatchTableCommit commit = builder.newCommit()) {
        ((org.apache.paimon.table.sink.InnerTableWrite) writer).withIOManager(io);
        writer.compact(partition, 0, true);
        commit.commit(writer.prepareCommit());
      }
      append(
          table,
          GenericRow.ofKind(
              RowKind.DELETE, 7, 2, org.apache.paimon.data.BinaryString.fromString("value-2")));
      Snapshot snapshot = table.latestSnapshot().get();
      FileStoreScan nativeScan =
          table.store().newScan().withSnapshot(snapshot).withPartitionBucket(partition, 0);
      if (drop) {
        nativeScan.dropStats();
      }
      List<DataFileMeta> expected = new ArrayList<>();
      Integer totalBuckets = WriteRestore.extractDataFiles(nativeScan.plan().files(), expected);
      assertFalse(expected.isEmpty());
      PaimonPrimaryKeySnapshotWriteRestore restore =
          new PaimonPrimaryKeySnapshotWriteRestore(
              table, snapshot.id(), Arrays.asList(unit(partition, 0), unit(partition, 999)));
      RestoreFiles result = restore.restoreFiles(partition, 0, bucket.equals("-1"), dv);
      assertEquals(sorted(expected), sorted(result.dataFiles()));
      assertEquals(totalBuckets, result.totalBuckets());
      assertEquals(snapshot.id(), result.snapshot().id());
      if (bucket.equals("-1")) {
        assertEquals(
            table.store().newIndexFileHandler().scanHashIndex(snapshot, partition, 0).orElse(null),
            result.dynamicBucketIndex());
      }
      if (dv) {
        assertEquals(
            table
                .store()
                .newIndexFileHandler()
                .scan(snapshot, DELETION_VECTORS_INDEX, partition, 0),
            result.deleteVectorsIndex());
      }
      RestoreFiles empty = restore.restoreFiles(partition, 999, bucket.equals("-1"), dv);
      assertNull(empty.totalBuckets());
      assertEquals(Collections.emptyList(), empty.dataFiles());
      result.dataFiles().clear();
      append(table, row(4));
      RestoreFiles repeated = restore.restoreFiles(partition, 0, bucket.equals("-1"), dv);
      assertEquals(sorted(expected), sorted(repeated.dataFiles()));
      assertEquals(snapshot.id(), repeated.snapshot().id());
      assertThrows(
          IllegalStateException.class,
          () -> restore.restoreFiles(partition, 998, bucket.equals("-1"), dv));
    }
  }

  @Test
  void mismatchedBucketCountsFailLikeNative() {
    ManifestEntry first = mock(ManifestEntry.class);
    ManifestEntry second = mock(ManifestEntry.class);
    when(first.totalBuckets()).thenReturn(4);
    when(second.totalBuckets()).thenReturn(8);
    List<ManifestEntry> entries = Arrays.asList(first, second);
    RuntimeException expected =
        assertThrows(
            RuntimeException.class,
            () -> WriteRestore.extractDataFiles(entries, new ArrayList<>()));
    RuntimeException actual =
        assertThrows(
            RuntimeException.class,
            () ->
                PaimonPrimaryKeySnapshotWriteRestore.extractDataFiles(
                    entries.iterator(), new ArrayList<>()));
    assertEquals(expected.getMessage(), actual.getMessage());
    assertNull(
        PaimonPrimaryKeySnapshotWriteRestore.extractDataFiles(
            Collections.emptyIterator(), new ArrayList<>()));
  }

  private static GenericRow row(int id) {
    return GenericRow.of(7, id, org.apache.paimon.data.BinaryString.fromString("value-" + id));
  }

  private static PaimonBucketCompactionUnit unit(BinaryRow partition, int bucket) {
    return new PaimonBucketCompactionUnit(
        SerializationUtils.serializeBinaryRow(partition), bucket, 1, 1L, 1L, 1L);
  }

  private static List<DataFileMeta> sorted(List<DataFileMeta> files) {
    List<DataFileMeta> copy = new ArrayList<>(files);
    copy.sort(Comparator.comparing(DataFileMeta::fileName));
    return copy;
  }
}
