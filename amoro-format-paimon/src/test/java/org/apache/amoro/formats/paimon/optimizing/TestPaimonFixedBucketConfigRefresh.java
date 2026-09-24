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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalog;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonOptimizingPlanner;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class TestPaimonFixedBucketConfigRefresh {
  @ParameterizedTest
  @ValueSource(strings = {"supported", "clustering", "dv"})
  void publishedPlannerInputSurvivesDdlWhileNextRoundUsesCurrentPolicy(
      String mode, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(
            warehouse,
            true,
            true,
            "dv".equals(mode)
                ? Collections.singletonMap("deletion-vectors.modifiable", "true")
                : Collections.emptyMap())) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      List<String> rows = fixture.snapshotRows();
      PaimonCatalog catalog = new PaimonCatalog(fixture.catalog, "test");
      PaimonTable before = (PaimonTable) catalog.loadTable("db", "fixed_append");
      PaimonOptimizingPlanner oldPlanner = planner(before, 301L, null);
      OptimizingPlanResult<PaimonCompactionTask> oldPlan = oldPlanner.plan();
      assertEquals(1, oldPlan.getTasks().size());
      PaimonCompactionInput published = oldPlan.getTasks().get(0).getInput();
      byte[] persistedInput = bytes(published);
      FormatTableAnalysis oldAnalysis = oldPlanner.tableAnalysis().get();

      List<SchemaChange> changes =
          new ArrayList<>(
              Arrays.asList(
                  SchemaChange.setOption("bucket-append-ordered", "false"),
                  SchemaChange.setOption("file-index.range-bitmap.columns", "payload"),
                  SchemaChange.setOption("compaction.force-rewrite-all-files", "false")));
      if ("clustering".equals(mode)) {
        changes.add(SchemaChange.setOption("clustering.columns", "hm"));
        changes.add(SchemaChange.setOption("clustering.incremental", "true"));
      } else if ("dv".equals(mode)) {
        // Paimon ac223f47, schema/SchemaManager.java:1209-1229 requires this native opt-in;
        // the separate forbidden-change test keeps the default prohibition covered.
        changes.add(SchemaChange.setOption("deletion-vectors.enabled", "true"));
      }
      fixture.catalog.alterTable(fixture.identifier, changes, false);
      PaimonTable refreshed = (PaimonTable) catalog.loadTable("db", "fixed_append");
      AppendOnlyFileStoreTable nativeTable = (AppendOnlyFileStoreTable) refreshed.originalTable();
      assertEquals(oldPlan.getTargetSnapshotId(), nativeTable.latestSnapshot().get().id());
      assertNotEquals(
          oldAnalysis.key(),
          PaimonHealthEvaluationContext.capture(
                  nativeTable, refreshed.id().toString(), null, refreshed.properties())
              .key());

      // Paimon ac223f47, table/AbstractFileStoreTable.java:382-393 retains old options in
      // copyWithLatestSchema. A new catalog load must govern only the next planning round.
      PaimonOptimizingPlanner nextPlanner = planner(refreshed, 302L, oldAnalysis);
      OptimizingPlanResult<PaimonCompactionTask> nextPlan = nextPlanner.plan();
      if ("supported".equals(mode)) {
        assertEquals(1, nextPlan.getTasks().size());
        PaimonCompactionInput next = nextPlan.getTasks().get(0).getInput();
        PaimonFixedBucketTask nextFixed = decode(next);
        assertFalse(nextFixed.ordered());
        assertFalse(nextFixed.rewriteAll());
        assertEquals(PaimonAppendRewriteOptions.Source.PAIMON_EXPLICIT, nextFixed.rewriteSource());
        assertEquals(
            "payload",
            next.getTable().originalTable().options().get("file-index.range-bitmap.columns"));
        assertNotEquals(oldAnalysis.key(), nextPlanner.tableAnalysis().get().key());
      } else {
        assertTrue(nextPlan.getTasks().isEmpty());
        assertEquals(
            "dv".equals(mode)
                ? "FIXED_APPEND_DV_UNSUPPORTED"
                : "FIXED_APPEND_CLUSTERING_UNSUPPORTED",
            PaimonAppendCapabilities.fixedBucketRejection(nativeTable).get());
      }

      PaimonCompactionInput recovered = SerializationUtil.simpleDeserialize(persistedInput);
      PaimonFixedBucketTask bound = decode(recovered);
      assertTrue(bound.ordered());
      assertTrue(bound.rewriteAll());
      assertEquals(PaimonAppendRewriteOptions.Source.AMORO_DEFAULT, bound.rewriteSource());
      assertEquals(
          "hm",
          recovered.getTable().originalTable().options().get("file-index.range-bitmap.columns"));
      assertEquals(decode(published).rewriteFingerprint(), bound.rewriteFingerprint());
      PaimonCompactionTask task =
          new PaimonCompactionTask(
              101L, recovered.getPartitionPath(), recovered, Collections.emptyMap());
      PaimonCompactionOutput output = new PaimonCompactionExecutor(recovered).execute();
      CommitMessageImpl message =
          (CommitMessageImpl)
              new CommitMessageSerializer()
                  .deserialize(output.getCommitMessageVersion(), output.getCommitMessageBytes());
      assertFalse(message.compactIncrement().compactAfter().isEmpty());
      assertTrue(
          message.compactIncrement().compactAfter().stream()
              .allMatch(file -> file.schemaId() == bound.schemaId()));
      task.setOutputBytes(bytes(output));
      new PaimonTableCommit(
              nativeTable,
              Collections.singletonList(task),
              recovered.getCommitUser(),
              recovered.getCommitIdentifier())
          .commit();
      long committed = fixture.table.latestSnapshot().get().id();
      Set<String> live = liveFiles(fixture);
      // Paimon ac223f47, table/sink/TableCommitImpl.java:277-290 filters persisted identity.
      // Replaying the published result must not rebind to the now unsupported/current writer.
      new PaimonTableCommit(
              nativeTable,
              Collections.singletonList(task),
              recovered.getCommitUser(),
              recovered.getCommitIdentifier())
          .commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(committed, fixture.table.latestSnapshot().get().id());
      assertEquals(live, liveFiles(fixture));
      assertEquals(rows, fixture.snapshotRows());
    }
  }

  private static PaimonOptimizingPlanner planner(
      PaimonTable table, long processId, FormatTableAnalysis analysis) {
    OptimizingConfig config =
        new OptimizingConfig()
            .setEnabled(true)
            .setMinorLeastInterval(-1)
            .setFullTriggerInterval(0)
            .setFullRewriteAllFiles(true)
            .setMaxTaskSize(64L * 1024 * 1024);
    return new PaimonOptimizingPlanner(
        table, 101L, processId, 1.0D, 64L * 1024 * 1024, config, 0L, 0L, 0L, null, analysis);
  }

  private static PaimonFixedBucketTask decode(PaimonCompactionInput input) {
    return new PaimonFixedBucketTaskSerializer()
        .deserialize(input.getSerializerVersion(), input.getTaskBytes());
  }

  private static byte[] bytes(Object value) {
    ByteBuffer serialized = SerializationUtil.simpleSerialize(value);
    byte[] result = new byte[serialized.remaining()];
    serialized.get(result);
    return result;
  }

  private static Set<String> liveFiles(PaimonFixedBucketTestSupport fixture) {
    return fixture.units().values().stream()
        .flatMap(List::stream)
        .map(ManifestEntry::fileName)
        .collect(Collectors.toSet());
  }

  @ParameterizedTest
  @CsvSource({
    "bucket-append-ordered,false",
    "file-index.range-bitmap.columns,payload",
    "compaction.force-rewrite-all-files,false"
  })
  void nextRoundLoadsPersistedOptionsWithoutNewDataSnapshot(
      String key, String value, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      long snapshot = fixture.table.latestSnapshot().get().id();
      long oldSchema = fixture.table.schema().id();
      String oldValue = fixture.table.options().get(key);
      assertNotEquals(value, oldValue);
      PaimonCatalog amoroCatalog = new PaimonCatalog(fixture.catalog, "test");
      PaimonTable current = (PaimonTable) amoroCatalog.loadTable("db", "fixed_append");
      ByteBuffer serialized = SerializationUtil.simpleSerialize(current);
      byte[] plannedTable = new byte[serialized.remaining()];
      serialized.get(plannedTable);
      // Source: https://paimon.apache.org/docs/1.4/program-api/catalog-api/
      // Paimon ac223f47, paimon-core/src/main/java/org/apache/paimon/schema/
      // SchemaManager.java:252 commits persisted options without a data snapshot.
      fixture.catalog.alterTable(
          fixture.identifier, Collections.singletonList(SchemaChange.setOption(key, value)), false);
      // Paimon ac223f47d916f4c8e9c07fd1745db448e4e7d0d8:
      // paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java:382
      // copyWithLatestSchema deliberately preserves old options; reload through the catalog.
      FileStoreTable schemaOnly = fixture.table.copyWithLatestSchema();
      assertEquals(oldValue, schemaOnly.options().get(key));
      assertEquals(oldSchema + 1, schemaOnly.schema().id());
      FileStoreTable nextRound =
          (FileStoreTable) amoroCatalog.loadTable("db", "fixed_append").originalTable();
      assertEquals(value, nextRound.options().get(key));
      assertEquals(oldSchema + 1, nextRound.schema().id());
      assertEquals(snapshot, nextRound.latestSnapshot().get().id());
      assertEquals(oldValue, fixture.table.options().get(key));
      assertEquals(oldSchema, fixture.table.schema().id());
      PaimonTable restored = SerializationUtil.simpleDeserialize(plannedTable);
      assertEquals(oldValue, restored.originalTable().options().get(key));
      assertEquals(oldSchema, ((FileStoreTable) restored.originalTable()).schema().id());
    }
  }

  @ParameterizedTest
  @CsvSource({"deletion-vectors.enabled,true", "bucket,-1"})
  void nativeForbiddenChangesRemainFailures(String key, String value, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      long schema = fixture.table.schema().id();
      // Paimon same SHA, schema/SchemaManager.java:1187 checkAlterTableOption.
      // Next-round visibility does not bypass native restrictions or claim DDL atomicity.
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              fixture.catalog.alterTable(
                  fixture.identifier,
                  Collections.singletonList(SchemaChange.setOption(key, value)),
                  false));
      FileStoreTable reloaded = (FileStoreTable) fixture.catalog.getTable(fixture.identifier);
      assertEquals(schema, reloaded.schema().id());
      assertEquals(1, reloaded.latestSnapshot().get().id());
    }
  }
}
