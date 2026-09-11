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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.utils.SerializationUtil;
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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestPaimonPrimaryKeySchemaEvolution {
  @ParameterizedTest
  @CsvSource({
    "1,MINOR,false", "1,MINOR,true", "1,MAJOR,false", "1,MAJOR,true",
    "-1,MINOR,false", "-1,MINOR,true", "-1,MAJOR,false", "-1,MAJOR,true"
  })
  void serializedInputRetainsOldSchemaWhileRestoreLoadsNewFiles(
      String bucket, OptimizingType type, boolean fresh, @TempDir Path warehouse) throws Exception {
    try (Catalog c = catalog(warehouse)) {
      Map<String, String> opts = options(false);
      opts.put("bucket", bucket);
      opts.put("num-sorted-run.compaction-trigger", "2");
      FileStoreTable old = table(c, "schema", opts);
      append(old, GenericRow.of(1, 10, 20, 1L));
      append(old, GenericRow.of(1, 11, 21, 2L));
      PaimonPrimaryKeyCompactionInput planned = input(old, type, 99L);
      ByteBuffer encoded = SerializationUtil.simpleSerialize(planned);
      byte[] bytes = new byte[encoded.remaining()];
      encoded.get(bytes);
      PaimonPrimaryKeyCompactionInput in = SerializationUtil.simpleDeserialize(bytes);
      c.alterTable(
          Identifier.create("db", "schema"),
          Arrays.asList(SchemaChange.addColumn("new_col", DataTypes.INT())),
          false);
      FileStoreTable now = (FileStoreTable) c.getTable(Identifier.create("db", "schema"));
      append(now, GenericRow.of(1, 12, 22, 3L, 12345));
      if (fresh) {
        in =
            new PaimonPrimaryKeyCompactionInput(
                wrap(now),
                planned.getUnits(),
                type,
                false,
                planned.getTargetSnapshotId(),
                planned.getCommitUser(),
                planned.getCommitIdentifier());
      }
      encoded = SerializationUtil.simpleSerialize(in);
      bytes = new byte[encoded.remaining()];
      encoded.get(bytes);
      in = SerializationUtil.simpleDeserialize(bytes);
      assertEquals(fresh ? 1L : 0L, ((FileStoreTable) in.getTable().originalTable()).schema().id());
      assertEquals(planned.getTargetSnapshotId(), in.getTargetSnapshotId());
      assertEquals(Arrays.asList("1|12|3|12345"), read(now));
      PaimonPrimaryKeyCompactionOutput out = new PaimonPrimaryKeyCompactionExecutor(in).execute();
      assertEquals(3, out.getCompactedFileCount());
      assertEquals(1, out.getProducedFileCount());
      commit(now, in, out);
      // 旧任务和新 schema 对照均必须保留新增列值。
      assertEquals(
          Arrays.asList("1|12|3|12345"),
          read((FileStoreTable) c.getTable(Identifier.create("db", "schema"))));
    }
  }

  static List<String> read(FileStoreTable t) throws Exception {
    List<String> result = new ArrayList<>();
    ReadBuilder b = t.newReadBuilder();
    try (RecordReader<InternalRow> r = b.newRead().createReader(b.newScan().plan())) {
      r.forEachRemaining(
          x ->
              result.add(
                  x.getInt(0)
                      + "|"
                      + x.getInt(1)
                      + "|"
                      + x.getLong(3)
                      + "|"
                      + (x.isNullAt(4) ? "null" : x.getInt(4))));
    }
    return result;
  }
}
