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

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SerializationUtils;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** F3 的真实主键表夹具；动态 HASH 场景只验证显式 bucket 0。 */
public final class PaimonPrimaryKeyIntervalTestSupport {
  private PaimonPrimaryKeyIntervalTestSupport() {}

  public static Catalog catalog(Path warehouse) {
    Map<String, String> props = new HashMap<>();
    props.put("warehouse", warehouse.toUri().toString());
    return PaimonCatalogFactory.paimonCatalog(props, new Configuration());
  }

  public static Map<String, String> options(boolean interval) {
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "1");
    options.put("write-only", "true");
    options.put("self-optimizing.enabled", "true");
    options.put("num-sorted-run.compaction-trigger", "3");
    options.put("num-sorted-run.stop-trigger", "10");
    options.put("compaction.size-ratio", "1");
    if (interval) {
      options.put("compaction.optimization-interval", "1h");
    }
    return options;
  }

  public static FileStoreTable table(Catalog catalog, String name, Map<String, String> options)
      throws Exception {
    catalog.createDatabase("db", true);
    Schema.Builder schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("a", DataTypes.INT())
            .column("b", DataTypes.INT())
            .column("seq", DataTypes.BIGINT())
            .primaryKey("id");
    options.forEach(schema::option);
    Identifier id = Identifier.create("db", name);
    catalog.createTable(id, schema.build(), false);
    return (FileStoreTable) catalog.getTable(id);
  }

  public static void append(FileStoreTable table, GenericRow... rows) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      for (GenericRow row : rows) {
        if (table.bucketMode() == BucketMode.HASH_DYNAMIC) {
          write.write(row, 0);
        } else {
          write.write(row);
        }
      }
      commit.commit(write.prepareCommit());
    }
  }

  static List<DataFileMeta> files(FileStoreTable table) {
    return table.store().newScan().plan().files().stream()
        .map(e -> e.file())
        .collect(Collectors.toList());
  }

  static Set<String> fileNames(FileStoreTable table) {
    return files(table).stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
  }

  public static PaimonTable wrap(FileStoreTable table) {
    return new PaimonTable(TableIdentifier.of("test", "db", table.name()), table);
  }

  static PaimonPrimaryKeyCompactionInput input(
      FileStoreTable table, OptimizingType type, long identifier) {
    List<DataFileMeta> files = files(table);
    PaimonBucketCompactionUnit unit =
        new PaimonBucketCompactionUnit(
            SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW),
            0,
            files.size(),
            files.stream().mapToLong(DataFileMeta::fileSize).sum(),
            files.stream().mapToLong(DataFileMeta::rowCount).sum(),
            1L);
    return new PaimonPrimaryKeyCompactionInput(
        wrap(table),
        Collections.singletonList(unit),
        type,
        type == OptimizingType.FULL,
        table.latestSnapshot().get().id(),
        "f3-interval",
        identifier);
  }

  static void commit(
      FileStoreTable table,
      PaimonPrimaryKeyCompactionInput input,
      PaimonPrimaryKeyCompactionOutput output)
      throws Exception {
    PaimonPrimaryKeyCompactionTask task =
        new PaimonPrimaryKeyCompactionTask(1L, "bucket", input, new HashMap<>());
    ByteBuffer encoded = SerializationUtil.simpleSerialize(output);
    byte[] bytes = new byte[encoded.remaining()];
    encoded.get(bytes);
    task.setOutputBytes(bytes);
    new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(task)).commit();
  }

  public static PaimonPrimaryKeyCompactionOutput compact(
      FileStoreTable table, OptimizingType type, long identifier) throws Exception {
    PaimonPrimaryKeyCompactionInput input = input(table, type, identifier);
    PaimonPrimaryKeyCompactionOutput output =
        new PaimonPrimaryKeyCompactionExecutor(input).execute();
    commit(table, input, output);
    return output;
  }

  static List<String> rows(FileStoreTable table) throws Exception {
    ReadBuilder builder = table.newReadBuilder();
    List<String> rows = new ArrayList<>();
    try (RecordReader<InternalRow> reader =
        builder.newRead().createReader(builder.newScan().plan())) {
      reader.forEachRemaining(
          row ->
              rows.add(
                  row.getInt(0)
                      + "|"
                      + (row.isNullAt(1) ? "null" : row.getInt(1))
                      + "|"
                      + (row.isNullAt(2) ? "null" : row.getInt(2))
                      + "|"
                      + (row.isNullAt(3) ? "null" : row.getLong(3))));
    }
    Collections.sort(rows);
    return rows;
  }

  public static OptimizingConfig config() {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastInterval(0)
        .setFullTriggerInterval(-1);
  }

  public static PaimonPrimaryKeyOptimizingEvaluation evaluate(FileStoreTable table) {
    return PaimonPrimaryKeyOptimizingEvaluator.evaluate(
        table, table.name(), config(), 0, 0, null, System.currentTimeMillis());
  }

  public static Set<String> history(FileStoreTable table) throws Exception {
    GenericRow[] rows = new GenericRow[10000];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = GenericRow.of(i, i * 73, i * 97, (long) i);
    }
    append(table, rows);
    compact(table, OptimizingType.FULL, 1L);
    return fileNames(table);
  }

  public static void increments(FileStoreTable table, int count, long round) throws Exception {
    for (int i = 0; i < count; i++) {
      append(table, GenericRow.of(1, i, i, round * 10 + i));
    }
  }
}
