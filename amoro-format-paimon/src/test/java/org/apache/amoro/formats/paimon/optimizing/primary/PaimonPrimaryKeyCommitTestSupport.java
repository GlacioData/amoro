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

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonPrimaryKeyOptimizingPlanner;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Real native fixtures, shared by maintenance, dynamic-index and timing tests. */
public final class PaimonPrimaryKeyCommitTestSupport implements AutoCloseable {
  static final String PARTITION = "2000-01-01";
  final Catalog catalog;
  final Identifier id;

  public PaimonPrimaryKeyCommitTestSupport(
      Path warehouse, boolean dynamic, Map<String, String> extra) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    catalog = PaimonCatalogFactory.paimonCatalog(props, new Configuration());
    catalog.createDatabase("db", true);
    id = Identifier.create("db", "commit_test");
    Map<String, String> options = new HashMap<>();
    options.put(CoreOptions.WRITE_ONLY.key(), "true");
    options.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");
    options.put(CoreOptions.BUCKET.key(), dynamic ? "-1" : "1");
    options.put("file.format", "avro");
    options.put("num-sorted-run.compaction-trigger", "2");
    options.putAll(extra);
    catalog.createTable(
        id,
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("dt", DataTypes.STRING())
            .primaryKey("id", "dt")
            .partitionKeys("dt")
            .options(options)
            .build(),
        false);
  }

  public FileStoreTable table() throws Exception {
    return (FileStoreTable) catalog.getTable(id);
  }

  public void writeGeneration(int generation) throws Exception {
    writeGeneration(generation, 4, 2);
  }

  void writeGeneration(int generation, int keys, int dynamicBuckets) throws Exception {
    FileStoreTable table = table();
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite()) {
      for (int key = 0; key < keys; key++) {
        GenericRow row =
            GenericRow.of(
                key,
                BinaryString.fromString("value-" + generation + "-" + key),
                BinaryString.fromString(PARTITION));
        if (table.bucketMode() == BucketMode.HASH_DYNAMIC) {
          // Native writers maintain real hash-index files for explicitly assigned dynamic buckets.
          // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/test/java/org/apache/paimon/index/DynamicBucketIndexMaintainerTest.java#L101-L147
          write.write(row, key % dynamicBuckets);
        } else {
          write.write(row);
        }
      }
      try (BatchTableCommit commit = builder.newCommit()) {
        commit.commit(write.prepareCommit());
      }
    }
  }

  List<PaimonPrimaryKeyCompactionTask> prepareTasks() throws Exception {
    return prepareTasks(100L, 7L);
  }

  public List<PaimonPrimaryKeyCompactionTask> prepareTasks(long tableId, long processId)
      throws Exception {
    PaimonTable table =
        new PaimonTable(TableIdentifier.of("catalog", "db", id.getObjectName()), table());
    OptimizingConfig config =
        new OptimizingConfig()
            .setEnabled(true)
            .setMinorLeastFileCount(2)
            .setMinorLeastInterval(0)
            .setFullTriggerInterval(-1)
            .setFullRewriteAllFiles(false)
            .setMaxTaskSize(64L * 1024 * 1024);
    List<PaimonPrimaryKeyCompactionTask> tasks =
        new ArrayList<>(
            new PaimonPrimaryKeyOptimizingPlanner(
                    table, tableId, processId, 4.0, 64L * 1024 * 1024, config, 0L, 0L, 0L, null)
                .plan()
                .getTasks());
    assertFalse(tasks.isEmpty());
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(task.getInput()).execute();
      assertFalse(output.getCommitMessageBytesList().isEmpty());
      ByteBuffer buffer = SerializationUtil.simpleSerialize(output);
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      task.setOutputBytes(bytes);
    }
    return tasks;
  }

  static List<CommitMessage> messages(List<PaimonPrimaryKeyCompactionTask> tasks) throws Exception {
    List<CommitMessage> messages = new ArrayList<>();
    for (PaimonPrimaryKeyCompactionTask task : tasks) {
      messages.addAll(
          CommitMessageSerializer.deserializeAll(task.getOutput().getCommitMessageBytesList()));
    }
    return messages;
  }

  static void nativeCommit(
      FileStoreTable table, List<PaimonPrimaryKeyCompactionTask> tasks, CommitMode mode)
      throws Exception {
    PaimonPrimaryKeyCompactionInput input = tasks.get(0).getInput();
    try (StreamTableCommit commit = table.newCommit(input.getCommitUser())) {
      if (mode == CommitMode.NORMAL) {
        commit.commit(input.getCommitIdentifier(), messages(tasks));
      } else {
        commit.filterAndCommit(
            Collections.singletonMap(input.getCommitIdentifier(), messages(tasks)));
      }
    }
  }

  public static List<String> rows(FileStoreTable table) throws Exception {
    ReadBuilder builder = table.newReadBuilder();
    List<String> rows = new ArrayList<>();
    try (RecordReader<InternalRow> reader =
        builder.newRead().createReader(builder.newScan().plan())) {
      reader.forEachRemaining(
          row -> rows.add(row.getInt(0) + "|" + row.getString(1) + "|" + row.getString(2)));
    }
    Collections.sort(rows);
    return rows;
  }

  @Override
  public void close() throws Exception {
    catalog.close();
  }
}
