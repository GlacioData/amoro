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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.compact;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestPaimonPrimaryKeySchemaDeletionVectors {
  @ParameterizedTest
  @ValueSource(strings = {"1", "-1"})
  void schemaRefreshPreservesExistingDeletionVectors(String bucket, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable old = tableWithDeletionVector(catalog, bucket);
      PaimonPrimaryKeyCompactionInput planned = input(old, OptimizingType.MINOR, 3);
      assertEquals(1, cardinality(old, indexes(old)));
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Collections.singletonList(SchemaChange.addColumn("new_col", DataTypes.INT())),
          false);
      FileStoreTable current = (FileStoreTable) catalog.getTable(Identifier.create("db", "schema"));
      append(current, GenericRow.of(3, 12, 22, 3L, 12345));
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(planned).execute();
      assertTrue(output.getCompactedFileCount() > 0);
      assertTrue(output.getProducedFileCount() > 0);
      commit(current, planned, output);
      List<String> rows = TestPaimonPrimaryKeySchemaEvolution.read(current);
      Collections.sort(rows);
      assertEquals(Arrays.asList("1|11|2|null", "2|10|1|null", "3|12|3|12345"), rows);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "-1"})
  void preparedDeletionVectorsDoNotFollowLaterIndexReplacement(
      String bucket, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable table = tableWithDeletionVector(catalog, bucket);
      PaimonPrimaryKeyCompactionInput planned = input(table, OptimizingType.MINOR, 3);
      PaimonPrimaryKeySnapshotWriteRestore restore =
          new PaimonPrimaryKeySnapshotWriteRestore(
              table, planned.getTargetSnapshotId(), planned.getUnits());
      RestoreFiles prepared =
          restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, bucket.equals("-1"), true);
      List<IndexFileMeta> expected = new ArrayList<>(prepared.deleteVectorsIndex());
      assertEquals(1, cardinality(table, expected));
      prepared.deleteVectorsIndex().clear();
      assertEquals(
          expected,
          restore
              .restoreFiles(BinaryRow.EMPTY_ROW, 0, bucket.equals("-1"), true)
              .deleteVectorsIndex());
      append(table, GenericRow.of(2, 12, 22, 3L));
      compact(table, OptimizingType.MINOR, 3);
      assertNotEquals(expected, indexes(table));
      RestoreFiles frozen = restore.restoreFiles(BinaryRow.EMPTY_ROW, 0, bucket.equals("-1"), true);
      assertEquals(planned.getTargetSnapshotId(), frozen.snapshot().id());
      assertEquals(expected, frozen.deleteVectorsIndex());
      assertEquals(1, cardinality(table, frozen.deleteVectorsIndex()));
      // 这里只验证恢复快照固定；不在另一个 compactor 已提交后再提交旧结果。
    }
  }

  private static FileStoreTable tableWithDeletionVector(Catalog catalog, String bucket)
      throws Exception {
    Map<String, String> opts = options(true);
    opts.put("bucket", bucket);
    opts.put("deletion-vectors.enabled", "true");
    FileStoreTable table = table(catalog, "schema", opts);
    append(table, GenericRow.of(1, 10, 20, 1L), GenericRow.of(2, 10, 20, 1L));
    compact(table, OptimizingType.FULL, 1);
    append(table, GenericRow.of(1, 11, 21, 2L));
    compact(table, OptimizingType.MINOR, 2);
    return table;
  }

  private static List<IndexFileMeta> indexes(FileStoreTable table) {
    return table
        .store()
        .newIndexFileHandler()
        .scan(table.latestSnapshot().get(), DELETION_VECTORS_INDEX, BinaryRow.EMPTY_ROW, 0);
  }

  private static long cardinality(FileStoreTable table, List<IndexFileMeta> indexes) {
    IndexFileHandler handler = table.store().newIndexFileHandler();
    return handler.readAllDeletionVectors(BinaryRow.EMPTY_ROW, 0, indexes).values().stream()
        .mapToLong(DeletionVector::getCardinality)
        .sum();
  }
}
