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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.AppendCompactTaskSerializer;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class TestPaimonAppendMinorTaskLegality {
  @TempDir Path warehouse;

  @ParameterizedTest
  @CsvSource({
    "-1,false,1,0", "-1,false,2,1", "-1,false,3,1",
    "-1,true,1,1", "-1,true,2,1", "-1,true,3,2",
    "1,false,1,0", "1,false,2,1", "1,false,3,1"
  })
  void minorInputsExecuteAndCommitWithoutLosingTheTail(
      int buckets, boolean deletionVectors, int fileCount, int expectedTasks) throws Exception {
    try (Catalog catalog = catalog()) {
      AppendOnlyFileStoreTable table = createTable(catalog, buckets, deletionVectors, fileCount);
      PaimonTable wrapped = new PaimonTable(TableIdentifier.of("test", "db", "legal"), table);
      OptimizingConfig config =
          new OptimizingConfig()
              .setEnabled(true)
              .setMinorLeastInterval(0)
              .setFullTriggerInterval(-1)
              .setMaxTaskSize(64L * 1024 * 1024);
      PaimonOptimizingPlanner planner =
          new PaimonOptimizingPlanner(wrapped, 1, 901, 4, 64L * 1024 * 1024, config, 0, 0, 0, null);
      OptimizingPlanResult<PaimonCompactionTask> plan = planner.plan();
      assertEquals(expectedTasks, plan.getTasks().size());
      if (!plan.getTasks().isEmpty()) {
        assertEquals(OptimizingType.MINOR, plan.getOptimizingType());
      }
      for (PaimonCompactionTask task : plan.getTasks()) {
        PaimonCompactionInput input = task.getInput();
        int inputs =
            buckets == -1
                ? new AppendCompactTaskSerializer()
                    .deserialize(input.getSerializerVersion(), input.getTaskBytes())
                    .compactBefore()
                    .size()
                : new PaimonFixedBucketTaskSerializer()
                    .deserialize(input.getSerializerVersion(), input.getTaskBytes())
                    .files()
                    .size();
        assertTrue(deletionVectors || inputs > 1);
        PaimonCompactionOutput output = new PaimonCompactionExecutor(input).execute();
        assertNotNull(output);
        ByteBuffer serialized = SerializationUtil.simpleSerialize(output);
        byte[] bytes = new byte[serialized.remaining()];
        serialized.get(bytes);
        task.setOutputBytes(bytes);
      }
      if (!plan.getTasks().isEmpty()) {
        PaimonCompactionInput input = plan.getTasks().iterator().next().getInput();
        new PaimonTableCommit(
                table, plan.getTasks(), input.getCommitUser(), input.getCommitIdentifier())
            .commit();
      }
      List<Integer> actual = new ArrayList<>();
      ReadBuilder read = table.newReadBuilder();
      try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
        reader.forEachRemaining(row -> actual.add(row.getInt(0)));
      }
      Collections.sort(actual);
      List<Integer> expected = new ArrayList<>();
      for (int id = 0; id < fileCount; id++) {
        expected.add(id);
      }
      assertEquals(expected, actual, "an unplanned singleton must stay readable after commit");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nativeSingletonContractUsesTheTableOption(boolean deletionVectors) throws Exception {
    try (Catalog catalog = catalog()) {
      // There are no deleted rows or actual DVs: the table option alone permits a singleton.
      AppendOnlyFileStoreTable table = createTable(catalog, -1, deletionVectors, 1);
      List<DataFileMeta> files = new ArrayList<>();
      table.store().newScan().readFileIterator().forEachRemaining(entry -> files.add(entry.file()));
      assertEquals(1, files.size());
      BaseAppendFileStoreWrite write = table.store().newWrite("native-contract");
      try {
        // Paimon 1.4.2 source:
        // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/append/AppendCompactTask.java#L70-L74
        AppendCompactTask task = new AppendCompactTask(BinaryRow.EMPTY_ROW, files);
        if (deletionVectors) {
          assertNotNull(task.doCompact(table, write));
        } else {
          assertThrows(IllegalArgumentException.class, () -> task.doCompact(table, write));
        }
      } finally {
        write.close();
      }
    }
  }

  private Catalog catalog() throws Exception {
    Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration());
    catalog.createDatabase("db", false);
    return catalog;
  }

  private AppendOnlyFileStoreTable createTable(
      Catalog catalog, int buckets, boolean deletionVectors, int fileCount) throws Exception {
    Schema.Builder schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .option("bucket", String.valueOf(buckets))
            .option("write-only", "true")
            .option("self-optimizing.enabled", "true")
            .option("compaction.min.file-num", "1")
            .option("target-file-size", "1 mb")
            .option("deletion-vectors.enabled", String.valueOf(deletionVectors));
    if (buckets > 0) {
      schema.option("bucket-key", "id").option("bucket-append-ordered", "true");
    }
    Identifier id = Identifier.create("db", "legal");
    catalog.createTable(id, schema.build(), false);
    AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) catalog.getTable(id);
    for (int row = 0; row < fileCount; row++) {
      BatchWriteBuilder builder = table.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(row));
        commit.commit(write.prepareCommit());
      }
    }
    return table;
  }
}
