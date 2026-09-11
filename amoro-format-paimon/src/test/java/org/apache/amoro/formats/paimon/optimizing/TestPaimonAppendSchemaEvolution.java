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

package org.apache.amoro.formats.paimon.optimizing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonOptimizingPlanner;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.AppendCompactTaskSerializer;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TestPaimonAppendSchemaEvolution {
  @ParameterizedTest
  @CsvSource({
    "planned-before-ddl,false,false",
    "stale-table-new-plan,false,false",
    "refreshed-control,false,false",
    "planned-before-ddl,true,false",
    "stale-table-new-plan,true,false",
    "refreshed-control,true,false",
    "planned-before-ddl,true,true",
    "stale-table-new-plan,true,true",
    "refreshed-control,true,true"
  })
  void compareAppendSchemaWindows(
      String mode, boolean rowTracking, boolean dv, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      Identifier id = Identifier.create("db", "append_schema");
      catalog.createTable(
          id,
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .column("a", DataTypes.INT())
              .option("bucket", "-1")
              .option("row-tracking.enabled", String.valueOf(rowTracking))
              .option("deletion-vectors.enabled", String.valueOf(dv))
              .option("write-only", "true")
              .option("self-optimizing.enabled", "true")
              .option("target-file-size", "32 mb")
              .option("compaction.min.file-num", "2")
              .build(),
          false);
      AppendOnlyFileStoreTable old = (AppendOnlyFileStoreTable) catalog.getTable(id);
      append(old, GenericRow.of(1, 10));
      append(old, GenericRow.of(2, 20));
      List<PaimonCompactionTask> tasks = mode.equals("planned-before-ddl") ? plan(old) : null;
      catalog.alterTable(
          id, Collections.singletonList(SchemaChange.addColumn("new_col", DataTypes.INT())), false);
      AppendOnlyFileStoreTable current = (AppendOnlyFileStoreTable) catalog.getTable(id);
      append(current, GenericRow.of(3, 30, 12345));
      if (tasks == null) {
        tasks = plan(old);
      }
      assertEquals(1, tasks.size());
      PaimonCompactionInput original = tasks.get(0).getInput();
      PaimonCompactionInput input = original;
      if (mode.equals("refreshed-control")) {
        input =
            new PaimonCompactionInput(
                wrap(old.copyWithLatestSchema()),
                original.getTaskBytes(),
                original.getSerializerVersion(),
                original.getCommitUser(),
                original.getPartitionPath(),
                original.getTargetSnapshotId(),
                original.getCommitIdentifier());
      }
      input = SerializationUtil.simpleDeserialize(bytes(input));
      List<Long> schemas =
          new AppendCompactTaskSerializer()
              .deserialize(input.getSerializerVersion(), input.getTaskBytes()).compactBefore()
                  .stream()
                  .map(f -> f.schemaId())
                  .collect(Collectors.toList());
      assertEquals(Arrays.asList("1|10|null", "2|20|null", "3|30|12345"), rows(current));
      long snapshotBefore = current.latestSnapshot().get().id();
      PaimonCompactionOutput output = new PaimonCompactionExecutor(input).execute();
      assertEquals(snapshotBefore, current.latestSnapshot().get().id());
      assertEquals(mode.equals("planned-before-ddl") ? 2 : 3, output.getCompactedFileCount());
      assertEquals(1, output.getProducedFileCount());
      PaimonCompactionTask task = new PaimonCompactionTask(1L, "", input, Collections.emptyMap());
      task.setOutputBytes(bytes(output));
      new PaimonTableCommit(
              current,
              Collections.singletonList(task),
              input.getCommitUser(),
              input.getCommitIdentifier())
          .commit();
      List<String> actual = rows((AppendOnlyFileStoreTable) catalog.getTable(id));
      assertEquals(Arrays.asList("1|10|null", "2|20|null", "3|30|12345"), actual);
      System.out.println(
          "APPEND_SCHEMA mode="
              + mode
              + " tableSchema="
              + ((FileStoreTable) input.getTable().originalTable()).schema().id()
              + " inputSchemas="
              + schemas
              + " after="
              + actual);
    }
  }

  static PaimonTable wrap(FileStoreTable table) {
    return new PaimonTable(TableIdentifier.of("test", "db", "append_schema"), table);
  }

  static List<PaimonCompactionTask> plan(AppendOnlyFileStoreTable table) {
    PaimonOptimizingPlanner planner =
        new PaimonOptimizingPlanner(
            wrap(table),
            1L,
            77L,
            1.0,
            64L * 1024 * 1024,
            new OptimizingConfig()
                .setEnabled(true)
                .setMinorLeastInterval(0)
                .setFullTriggerInterval(-1)
                .setFullRewriteAllFiles(false)
                .setMaxTaskSize(64L * 1024 * 1024),
            0L,
            0L,
            0L,
            null);
    return new ArrayList<>(planner.plan().getTasks());
  }

  private static byte[] bytes(Object value) {
    ByteBuffer buffer = SerializationUtil.simpleSerialize(value);
    byte[] result = new byte[buffer.remaining()];
    buffer.get(result);
    return result;
  }

  static void append(Table table, GenericRow row) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      write.write(row);
      commit.commit(write.prepareCommit());
    }
  }

  static List<String> rows(FileStoreTable table) throws Exception {
    List<String> result = new ArrayList<>();
    ReadBuilder builder = table.newReadBuilder();
    try (RecordReader<InternalRow> reader =
        builder.newRead().createReader(builder.newScan().plan())) {
      reader.forEachRemaining(
          r ->
              result.add(
                  r.getInt(0) + "|" + r.getInt(1) + "|" + (r.isNullAt(2) ? "null" : r.getInt(2))));
    }
    Collections.sort(result);
    return result;
  }
}
