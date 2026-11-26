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
import org.apache.amoro.formats.paimon.PaimonTableDescriptor;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizingExecutor;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DisplayName("PaimonCompactionExecutor")
public class TestPaimonCompactionExecutor {

  @Test
  @DisplayName("Factory initialize + createExecutor return a wired PaimonCompactionExecutor")
  void testFactoryCreateExecutor() {
    PaimonCompactionExecutorFactory factory = new PaimonCompactionExecutorFactory();
    factory.initialize(new HashMap<>());
    PaimonCompactionInput input = new PaimonCompactionInput();
    OptimizingExecutor<PaimonCompactionOutput> executor = factory.createExecutor(input);
    assertNotNull(executor);
    assertTrue(executor instanceof PaimonCompactionExecutor);
  }

  @Test
  @DisplayName("execute() throws IllegalStateException when table or taskBytes are missing")
  void testExecuteRejectsMissingInput() {
    PaimonCompactionInput input = new PaimonCompactionInput();
    PaimonCompactionExecutor executor = new PaimonCompactionExecutor(input);
    IllegalStateException ex = assertThrows(IllegalStateException.class, executor::execute);
    assertTrue(ex.getMessage().contains("missing required fields"));
  }

  @Test
  @DisplayName("execute() rejects missing commit identity before compact files are written")
  void testExecuteRejectsMissingCommitIdentity(@TempDir Path warehouse) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    Catalog catalog = PaimonCatalogFactory.paimonCatalog(props, new Configuration());
    Identifier id = Identifier.create("db1", "t_invalid_identity");
    createAppendTable(catalog, id, 5, new HashMap<>());
    PaimonCompactionInput valid = planTasks(catalog, id, 1L, 3L).get(0).getInput();

    PaimonCompactionInput missingUser =
        new PaimonCompactionInput(
            valid.getTable(),
            valid.getTaskBytes(),
            valid.getSerializerVersion(),
            "",
            valid.getPartitionPath(),
            valid.getTargetSnapshotId(),
            valid.getCommitIdentifier());
    IllegalStateException missingUserEx =
        assertThrows(
            IllegalStateException.class, () -> new PaimonCompactionExecutor(missingUser).execute());
    assertTrue(missingUserEx.getMessage().contains("missing commitUser"));

    PaimonCompactionInput missingIdentifier =
        new PaimonCompactionInput(
            valid.getTable(),
            valid.getTaskBytes(),
            valid.getSerializerVersion(),
            valid.getCommitUser(),
            valid.getPartitionPath(),
            valid.getTargetSnapshotId(),
            0L);
    IllegalStateException missingIdentifierEx =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonCompactionExecutor(missingIdentifier).execute());
    assertTrue(missingIdentifierEx.getMessage().contains("invalid commitIdentifier"));
  }

  @Test
  @DisplayName("PaimonCompactionInput supports option/options/getOptions via BaseOptimizingInput")
  void testCompactionInputOptions() {
    PaimonCompactionInput input = new PaimonCompactionInput();
    input.option("key1", "value1");
    Map<String, String> batch = new HashMap<>();
    batch.put("key2", "value2");
    batch.put("key3", "value3");
    input.options(batch);
    assertEquals(3, input.getOptions().size());
    assertEquals("value1", input.getOptions().get("key1"));
    assertEquals("value2", input.getOptions().get("key2"));
    assertEquals("value3", input.getOptions().get("key3"));
  }

  @Test
  @DisplayName("PaimonCompactionOutput summary exposes Paimon + dashboard keys (zeros by default)")
  void testCompactionOutputSummary() {
    PaimonCompactionOutput output = new PaimonCompactionOutput();
    assertNotNull(output.summary());
    // Outputs expose four native counters and six dashboard counters.
    assertEquals(10, output.summary().size());
    assertEquals("0", output.summary().get(PaimonCompactionOutput.COMPACTED_FILES));
    assertEquals("0", output.summary().get(PaimonCompactionOutput.PRODUCED_FILES));
    assertEquals("0", output.summary().get(PaimonCompactionOutput.DASHBOARD_INPUT_DATA_FILES));
    assertEquals("0", output.summary().get(PaimonCompactionOutput.DASHBOARD_OUTPUT_DATA_FILES));
  }

  @Test
  @DisplayName("PaimonCompactionInput round-trips via SerializationUtil when empty")
  void testInputSerialization() {
    PaimonCompactionInput input = new PaimonCompactionInput();
    input.option("key1", "value1");
    input.option("key2", "value2");
    ByteBuffer buffer = SerializationUtil.simpleSerialize(input);
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    PaimonCompactionInput deserialized = SerializationUtil.simpleDeserialize(bytes);
    assertNotNull(deserialized);
    assertEquals("value1", deserialized.getOptions().get("key1"));
    assertEquals("value2", deserialized.getOptions().get("key2"));
  }

  @Test
  @DisplayName("End-to-end: committed compaction remains visible after sparse append history")
  void testEndToEndCompaction(@TempDir Path warehouse) throws Exception {
    // 1) FileSystem Paimon catalog + tiny append-only table with 5 small commits.
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    Catalog catalog = PaimonCatalogFactory.paimonCatalog(props, new Configuration());
    Identifier id = Identifier.create("db1", "t_e2e");
    createAppendTable(catalog, id, 5, new HashMap<>());
    long filesBefore = countDataFiles((AppendOnlyFileStoreTable) catalog.getTable(id));

    // 2) Planner produces tasks.
    List<PaimonCompactionTask> tasks = planTasks(catalog, id, 1L, 1L);
    assertTrue(tasks.size() >= 1);

    long snapshotBeforeExecute =
        ((AppendOnlyFileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot().id();

    // 3) Executor runs each task and returns CommitMessage bytes without committing a snapshot.
    for (PaimonCompactionTask t : tasks) {
      PaimonCompactionOutput output = new PaimonCompactionExecutor(t.getInput()).execute();
      assertNotNull(output.getCommitMessageBytes());
      assertTrue(output.getCompactedFileCount() >= 1);
      ByteBuffer outputBytes = SerializationUtil.simpleSerialize(output);
      byte[] bytes = new byte[outputBytes.remaining()];
      outputBytes.get(bytes);
      t.setOutputBytes(bytes);
    }
    AppendOnlyFileStoreTable afterExecute = (AppendOnlyFileStoreTable) catalog.getTable(id);
    assertEquals(
        snapshotBeforeExecute,
        afterExecute.snapshotManager().latestSnapshot().id(),
        "Paimon optimizer executor must return CommitMessage bytes without committing snapshots");

    // 4) AMS-side committer is the only component that advances the Paimon snapshot.
    PaimonCompactionTask firstTask = tasks.get(0);
    new PaimonTableCommit(
            (AppendOnlyFileStoreTable) catalog.getTable(id),
            tasks,
            firstTask.getInput().getCommitUser(),
            firstTask.getInput().getCommitIdentifier())
        .commit();
    Snapshot latestSnapshot =
        ((AppendOnlyFileStoreTable) catalog.getTable(id)).snapshotManager().latestSnapshot();
    assertEquals(snapshotBeforeExecute + 1, latestSnapshot.id());
    assertEquals(Snapshot.CommitKind.COMPACT, latestSnapshot.commitKind());
    long compactSnapshotId = latestSnapshot.id();

    long filesAfter = countDataFiles((AppendOnlyFileStoreTable) catalog.getTable(id));
    assertTrue(filesAfter < filesBefore, "After compaction file count must drop");

    // Push the COMPACT snapshot outside a single 25-snapshot raw window. The dashboard query must
    // keep scanning backwards and return it instead of showing an unreachable empty first page.
    Table appendTable = catalog.getTable(id);
    for (int i = 0; i < 26; i++) {
      writeOne(appendTable, 100 + i, "history-" + i);
    }
    PaimonTable paimonTable =
        new PaimonTable(
            TableIdentifier.of("test_catalog", id.getDatabaseName(), id.getObjectName()),
            catalog.getTable(id));
    Pair<List<OptimizingProcessInfo>, Integer> history =
        new PaimonTableDescriptor()
            .getOptimizingProcessesInfo(paimonTable, null, null, 25, 0, null);

    assertEquals(1, history.getRight(), "real snapshot history contains one compaction");
    assertEquals(1, history.getLeft().size());
    assertEquals(Long.toString(compactSnapshotId), history.getLeft().get(0).getProcessId());
  }

  @Test
  @DisplayName("execute() configures write type when Paimon row tracking is enabled")
  void testExecutorUsesRowTrackingWriteType(@TempDir Path warehouse) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    Catalog catalog = PaimonCatalogFactory.paimonCatalog(props, new Configuration());
    Identifier id = Identifier.create("db1", "t_row_tracking");
    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put("row-tracking.enabled", "true");
    createAppendTable(catalog, id, 5, tableOptions);
    AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) catalog.getTable(id);
    assertTrue(table.coreOptions().rowTrackingEnabled());
    long filesBefore = countDataFiles(table);

    List<PaimonCompactionTask> tasks = planTasks(catalog, id, 1L, 2L);
    assertTrue(tasks.size() >= 1);

    for (PaimonCompactionTask task : tasks) {
      PaimonCompactionOutput output = new PaimonCompactionExecutor(task.getInput()).execute();
      assertNotNull(output.getCommitMessageBytes());
      assertTrue(output.getCommitMessageBytes().length > 0);
      ByteBuffer outputBytes = SerializationUtil.simpleSerialize(output);
      byte[] bytes = new byte[outputBytes.remaining()];
      outputBytes.get(bytes);
      task.setOutputBytes(bytes);
    }

    PaimonCompactionTask firstTask = tasks.get(0);
    new PaimonTableCommit(
            (AppendOnlyFileStoreTable) catalog.getTable(id),
            tasks,
            firstTask.getInput().getCommitUser(),
            firstTask.getInput().getCommitIdentifier())
        .commit();

    AppendOnlyFileStoreTable afterCommit = (AppendOnlyFileStoreTable) catalog.getTable(id);
    assertEquals(
        Snapshot.CommitKind.COMPACT, afterCommit.snapshotManager().latestSnapshot().commitKind());
    assertTrue(
        countDataFiles(afterCommit) < filesBefore,
        "Row-tracking compaction must commit produced files and reduce file count");
  }

  @Test
  @DisplayName("Unaware DV executor binds targetSnapshotId when concurrent DML merges DV indexes")
  void testUnawareDvExecutorBindsTargetSnapshotWhenConcurrentDmlMergesDvIndex(
      @TempDir Path warehouse) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    Catalog catalog = PaimonCatalogFactory.paimonCatalog(props, new Configuration());
    Identifier id = Identifier.create("db1", "t_unaware_dv_bound");
    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put("deletion-vectors.enabled", "true");
    tableOptions.put("target-file-size", "64 kb");
    tableOptions.put("compaction.delete-ratio-threshold", "0.1");
    createAppendTable(catalog, id, 0, tableOptions);
    Table raw = catalog.getTable(id);
    for (int f = 0; f < 4; f++) {
      BatchWriteBuilder builder = raw.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(f * 2, BinaryString.fromString("keep-" + f)));
        write.write(GenericRow.of(f * 2 + 1, BinaryString.fromString("drop-" + f)));
        commit.commit(write.prepareCommit());
      }
    }

    AppendOnlyFileStoreTable append = (AppendOnlyFileStoreTable) catalog.getTable(id);
    List<DataFileMeta> files = new ArrayList<>();
    append
        .newReadBuilder()
        .newScan()
        .plan()
        .splits()
        .forEach(s -> files.addAll(((DataSplit) s).dataFiles()));
    assertEquals(4, files.size());

    // Commit two separate DV index files so planning produces two disjoint DV groups.
    for (int g = 0; g < 2; g++) {
      AppendOnlyFileStoreTable latestTable = (AppendOnlyFileStoreTable) catalog.getTable(id);
      // Pass null snapshot so each pair is persisted into its own independent DV index file.
      org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer m =
          org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer.forUnawareAppend(
              latestTable.store().newIndexFileHandler(),
              null,
              org.apache.paimon.data.BinaryRow.EMPTY_ROW);
      for (int k = g * 2; k < g * 2 + 2; k++) {
        org.apache.paimon.deletionvectors.BitmapDeletionVector dv =
            new org.apache.paimon.deletionvectors.BitmapDeletionVector();
        dv.delete(1);
        m.notifyNewDeletionVector(files.get(k).fileName(), dv);
      }
      List<org.apache.paimon.index.IndexFileMeta> written =
          m.persist().stream()
              .filter(e -> e.kind() == org.apache.paimon.manifest.FileKind.ADD)
              .map(org.apache.paimon.manifest.IndexManifestEntry::indexFile)
              .collect(java.util.stream.Collectors.toList());
      try (BatchTableCommit commit = latestTable.newBatchWriteBuilder().newCommit()) {
        commit.commit(
            Collections.singletonList(
                new org.apache.paimon.table.sink.CommitMessageImpl(
                    org.apache.paimon.data.BinaryRow.EMPTY_ROW,
                    0,
                    -1,
                    org.apache.paimon.io.DataIncrement.emptyIncrement(),
                    new org.apache.paimon.io.CompactIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        written,
                        Collections.emptyList()))));
      }
    }

    // Plan 2 tasks (one per DV index file) by setting maxTaskSize smaller than combined 4 files.
    PaimonTable paimonTable =
        new PaimonTable(
            TableIdentifier.of("test_catalog", "db1", "t_unaware_dv_bound"), catalog.getTable(id));
    long singlePairSize = files.get(0).fileSize() + files.get(1).fileSize() + 64L;
    PaimonOptimizingPlanner planner =
        new PaimonOptimizingPlanner(
            paimonTable,
            1L,
            10L,
            4.0,
            singlePairSize,
            new OptimizingConfig()
                .setEnabled(true)
                .setMinorLeastInterval(1)
                .setFullTriggerInterval(-1)
                .setMaxTaskSize(singlePairSize),
            0L,
            0L,
            0L,
            null);
    List<PaimonCompactionTask> plannedTasks = new ArrayList<>(planner.plan().getTasks());
    assertEquals(2, plannedTasks.size());

    // Simulate concurrent DML merging all 4 files' DVs into a single new index file in S_next.
    AppendOnlyFileStoreTable beforeExec = (AppendOnlyFileStoreTable) catalog.getTable(id);
    org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer concurrentDml =
        org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer.forUnawareAppend(
            beforeExec.store().newIndexFileHandler(),
            beforeExec.snapshotManager().latestSnapshot(),
            org.apache.paimon.data.BinaryRow.EMPTY_ROW);
    for (DataFileMeta df : files) {
      org.apache.paimon.deletionvectors.BitmapDeletionVector dv =
          new org.apache.paimon.deletionvectors.BitmapDeletionVector();
      dv.delete(1);
      concurrentDml.notifyNewDeletionVector(df.fileName(), dv);
    }
    List<org.apache.paimon.index.IndexFileMeta> dmlAdd = new ArrayList<>();
    List<org.apache.paimon.index.IndexFileMeta> dmlDel = new ArrayList<>();
    for (org.apache.paimon.manifest.IndexManifestEntry e : concurrentDml.persist()) {
      if (e.kind() == org.apache.paimon.manifest.FileKind.ADD) {
        dmlAdd.add(e.indexFile());
      } else {
        dmlDel.add(e.indexFile());
      }
    }
    assertEquals(1, dmlAdd.size(), "Concurrent DML must repack all 4 DVs into 1 merged index file");
    try (BatchTableCommit commit = beforeExec.newBatchWriteBuilder().newCommit()) {
      commit.commit(
          Collections.singletonList(
              new org.apache.paimon.table.sink.CommitMessageImpl(
                  org.apache.paimon.data.BinaryRow.EMPTY_ROW,
                  0,
                  -1,
                  org.apache.paimon.io.DataIncrement.emptyIncrement(),
                  new org.apache.paimon.io.CompactIncrement(
                      Collections.emptyList(),
                      Collections.emptyList(),
                      Collections.emptyList(),
                      dmlAdd,
                      dmlDel))));
    }

    // Execute both tasks: they MUST bind to targetSnapshotId rather than reading S_next's merged
    // index!
    org.apache.paimon.table.sink.CommitMessageSerializer serializer =
        new org.apache.paimon.table.sink.CommitMessageSerializer();
    java.util.Set<String> deletedIndexNames = new java.util.HashSet<>();
    int totalDeletedIndexCount = 0;
    long totalOutputRows = 0L;
    for (PaimonCompactionTask task : plannedTasks) {
      PaimonCompactionOutput out = new PaimonCompactionExecutor(task.getInput()).execute();
      org.apache.paimon.table.sink.CommitMessageImpl msg =
          (org.apache.paimon.table.sink.CommitMessageImpl)
              serializer.deserialize(out.getCommitMessageVersion(), out.getCommitMessageBytes());
      for (org.apache.paimon.index.IndexFileMeta deletedIdx :
          msg.compactIncrement().deletedIndexFiles()) {
        totalDeletedIndexCount++;
        deletedIndexNames.add(deletedIdx.fileName());
      }
      for (DataFileMeta after : msg.compactIncrement().compactAfter()) {
        totalOutputRows += after.rowCount();
      }
      ByteBuffer buf = SerializationUtil.simpleSerialize(out);
      byte[] arr = new byte[buf.remaining()];
      buf.get(arr);
      task.setOutputBytes(arr);
    }
    assertEquals(4L, totalOutputRows, "DV-deleted rows must be physically filtered during rewrite");
    assertEquals(
        totalDeletedIndexCount,
        deletedIndexNames.size(),
        "Bound-snapshot execution must never emit duplicate DELETE for the concurrent merged index");

    // Committing stale tasks against S_next fails atomically via OptimizingCommitException (no data
    // loss).
    assertThrows(
        org.apache.amoro.exception.OptimizingCommitException.class,
        () ->
            new PaimonTableCommit(
                    (AppendOnlyFileStoreTable) catalog.getTable(id),
                    plannedTasks,
                    plannedTasks.get(0).getInput().getCommitUser(),
                    plannedTasks.get(0).getInput().getCommitIdentifier())
                .commit());
  }

  @Test
  @DisplayName("Unaware DV executor fails fast before opening writer when targetSnapshotId expired")
  void testUnawareDvExecutorFailsFastWhenTargetSnapshotExpired(@TempDir Path warehouse)
      throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    Catalog catalog = PaimonCatalogFactory.paimonCatalog(props, new Configuration());
    Identifier id = Identifier.create("db1", "t_unaware_dv_expired");
    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put("deletion-vectors.enabled", "true");
    createAppendTable(catalog, id, 3, tableOptions);
    List<PaimonCompactionTask> planned = planTasks(catalog, id, 1L, 11L);
    assertEquals(1, planned.size());

    AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) catalog.getTable(id);
    long targetSnapshotId = planned.get(0).getInput().getTargetSnapshotId();
    writeOne(table, 99, "later");
    table.snapshotManager().deleteSnapshot(targetSnapshotId);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () -> new PaimonCompactionExecutor(planned.get(0).getInput()).execute());
    assertTrue(ex.getMessage().contains("Target snapshot"));
  }

  private static Table createAppendTable(
      Catalog catalog, Identifier id, int rows, Map<String, String> tableOptions) throws Exception {
    catalog.createDatabase(id.getDatabaseName(), true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .option("bucket", "-1")
            .option("target-file-size", "1 kb")
            .option("compaction.min.file-num", "2")
            .option("write-only", "true")
            .option(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");
    tableOptions.forEach(builder::option);
    catalog.createTable(id, builder.build(), true);
    Table table = catalog.getTable(id);
    for (int i = 0; i < rows; i++) {
      writeOne(table, i, "r-" + i);
    }
    return catalog.getTable(id);
  }

  private static List<PaimonCompactionTask> planTasks(
      Catalog catalog, Identifier id, long tableId, long processId) throws Exception {
    PaimonTable paimonTable =
        new PaimonTable(
            TableIdentifier.of("test_catalog", id.getDatabaseName(), id.getObjectName()),
            catalog.getTable(id));
    PaimonOptimizingPlanner planner =
        new PaimonOptimizingPlanner(
            paimonTable,
            tableId,
            processId,
            1.0,
            64L * 1024 * 1024,
            new OptimizingConfig()
                .setEnabled(true)
                .setMinorLeastInterval(1)
                .setFullTriggerInterval(-1)
                .setFullRewriteAllFiles(false)
                .setMaxTaskSize(64L * 1024 * 1024),
            0L,
            0L,
            0L,
            null);
    OptimizingPlanResult<PaimonCompactionTask> plan = planner.plan();
    return new ArrayList<>(plan.getTasks());
  }

  private static void writeOne(Table table, int id, String name) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite()) {
      write.write(GenericRow.of(id, BinaryString.fromString(name)));
      List<CommitMessage> messages = write.prepareCommit();
      try (BatchTableCommit commit = builder.newCommit()) {
        commit.commit(messages);
      }
    }
  }

  private static long countDataFiles(AppendOnlyFileStoreTable table) throws Exception {
    return table.newReadBuilder().newScan().plan().splits().stream()
        .filter(s -> s instanceof DataSplit)
        .mapToLong(s -> ((DataSplit) s).dataFiles().size())
        .sum();
  }
}
