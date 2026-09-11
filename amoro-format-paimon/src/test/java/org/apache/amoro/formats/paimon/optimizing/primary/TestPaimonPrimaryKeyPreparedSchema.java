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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.files;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** 真实 executor 在刷新后遇到再次 DDL，也只能合并已检查的文件。 */
public class TestPaimonPrimaryKeyPreparedSchema {
  @ParameterizedTest
  @CsvSource({
    "1,false,false",
    "1,false,true",
    "1,true,false",
    "1,true,true",
    "-1,false,false",
    "-1,false,true",
    "-1,true,false",
    "-1,true,true"
  })
  void preparedFilesKeepSchemaAndIndexesTogether(
      String bucket, boolean dv, boolean lateDdl, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(true);
      opts.put("bucket", bucket);
      opts.put("num-sorted-run.compaction-trigger", "2");
      opts.put("deletion-vectors.enabled", String.valueOf(dv));
      FileStoreTable old = table(catalog, "prepared", opts);
      Map<String, String> originalOptions = new java.util.HashMap<>(old.options());
      append(old, GenericRow.of(1, 10, 20, 1L));
      append(old, GenericRow.of(1, 11, 21, 2L));
      PaimonPrimaryKeyCompactionInput input = input(old, OptimizingType.MINOR, 91L);
      Identifier id = Identifier.create("db", "prepared");
      catalog.alterTable(
          id, Collections.singletonList(SchemaChange.addColumn("new_col", DataTypes.INT())), false);
      FileStoreTable schema1 = (FileStoreTable) catalog.getTable(id);
      append(schema1, GenericRow.of(1, 12, 22, 3L, 12345));

      FileStoreTable tracked = spy(old);
      doAnswer(
              invocation -> {
                FileStoreTable selected = old.copyWithLatestSchema();
                if (lateDdl) {
                  catalog.alterTable(
                      id,
                      Collections.singletonList(
                          SchemaChange.addColumn("later_col", DataTypes.INT())),
                      false);
                  FileStoreTable schema2 = (FileStoreTable) catalog.getTable(id);
                  append(schema2, GenericRow.of(2, 20, 30, 4L, 23456, 34567));
                }
                return spy(selected);
              })
          .when(tracked)
          .copyWithLatestSchema();
      input =
          new PaimonPrimaryKeyCompactionInput(
              wrap(tracked),
              input.getUnits(),
              input.getOptimizingType(),
              input.isFullCompaction(),
              input.getTargetSnapshotId(),
              input.getCommitUser(),
              input.getCommitIdentifier());
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(input).execute();
      assertEquals(3, output.getCompactedFileCount());
      assertEquals(1, output.getProducedFileCount());
      verify(tracked, times(1)).copyWithLatestSchema();
      assertEquals(0, old.schema().id());
      assertEquals(originalOptions, old.options());
      FileStoreTable latest = (FileStoreTable) catalog.getTable(id);
      commit(latest, input, output);
      List<String> result = rowsWithNewColumns(latest);
      List<String> expected =
          lateDdl
              ? Arrays.asList("1|12345|null", "2|23456|34567")
              : Collections.singletonList("1|12345");
      if (dv && lateDdl) {
        // 原生 DV 模式未合并的 L0 不立即可见；先证明本轮未消费的新schema文件仍在。
        assertEquals(Collections.singletonList("1|12345|null"), result);
        assertTrue(files(latest).stream().anyMatch(f -> f.schemaId() == 2 && f.level() == 0));
        // 后续任务使用 schema 2 合并该剩余输入，验证新增列值没有丢失。
        compact(latest, OptimizingType.FULL, 92L);
        result = rowsWithNewColumns(latest);
      }
      assertEquals(expected, result);
    }
  }

  private static List<String> rowsWithNewColumns(FileStoreTable table) throws Exception {
    List<String> result = new ArrayList<>();
    ReadBuilder builder = table.newReadBuilder();
    try (RecordReader<InternalRow> reader =
        builder.newRead().createReader(builder.newScan().plan())) {
      reader.forEachRemaining(
          r ->
              result.add(
                  r.getInt(0)
                      + "|"
                      + (r.isNullAt(4) ? "null" : r.getInt(4))
                      + (r.getFieldCount() == 6
                          ? "|" + (r.isNullAt(5) ? "null" : r.getInt(5))
                          : "")));
    }
    Collections.sort(result);
    return result;
  }
}
