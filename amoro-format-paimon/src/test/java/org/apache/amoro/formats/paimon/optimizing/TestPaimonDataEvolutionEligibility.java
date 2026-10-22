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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.AppendCompactTaskSerializer;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TestPaimonDataEvolutionEligibility {
  @TempDir Path warehouse;

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void excludesDataEvolutionWhileRetainingRowTrackingAndHealth(boolean evolution, boolean full)
      throws Exception {
    try (Catalog catalog = catalog()) {
      AppendOnlyFileStoreTable table = table(catalog, evolution);
      PaimonTable wrapped = wrap(table);
      OptimizingConfig config = config(full);
      OptimizationContext context = context(config);
      PendingInputResult result = wrapped.evaluatePendingInput(context, 100).get();
      assertEquals(!evolution, result.optimizingNecessary());
      assertEquals(
          !evolution,
          ((PaimonPendingInput) result.optimizingPendingInput()).isSameSnapshotPlanningPending());
      PaimonAppendSnapshotAnalysis analysis =
          (PaimonAppendSnapshotAnalysis) result.tableAnalysis().get();
      assertEquals(!evolution, analysis.plannerFactsAvailable());
      assertTrue(((PaimonPendingInput) result.pendingInput()).getHealthScore() >= 0);
      assertEquals(2, ((PaimonPendingInput) result.pendingInput()).getDataFileCount());
      PaimonOptimizingPlanner planner =
          new PaimonOptimizingPlanner(wrapped, 1, 1, 4, 64L * 1024 * 1024, config, 0, 0, 0, null);
      assertEquals(!evolution, planner.isNecessary());
      assertEquals(evolution, planner.plan().getTasks().isEmpty());
    }
  }

  @Test
  void planningFingerprintIncludesTheUnsupportedCapability() throws Exception {
    try (Catalog catalog = catalog()) {
      AppendOnlyFileStoreTable before = table(catalog, false);
      AppendOnlyFileStoreTable after = table(catalog, true);
      assertEquals(before.schema().id(), after.schema().id());
      assertEquals(before.latestSnapshot().get().id(), after.latestSnapshot().get().id());
      OptimizationContext context = context(config(false));
      assertNotEquals(
          PaimonHealthEvaluationContext.captureAt(before, "t", context, null, 1000).key(),
          PaimonHealthEvaluationContext.captureAt(after, "t", context, null, 1000).key());
    }
  }

  @Test
  void rejectsRecoveredOrdinaryTaskBeforeWritingAnyFiles() throws Exception {
    try (Catalog catalog = catalog()) {
      AppendOnlyFileStoreTable table = table(catalog, true);
      List<DataFileMeta> files = new ArrayList<>();
      table.store().newScan().readFileIterator().forEachRemaining(entry -> files.add(entry.file()));
      AppendCompactTaskSerializer serializer = new AppendCompactTaskSerializer();
      PaimonCompactionInput input =
          new PaimonCompactionInput(
              wrap(table),
              serializer.serialize(new AppendCompactTask(BinaryRow.EMPTY_ROW, files)),
              serializer.getVersion(),
              "legacy-evolution",
              "",
              table.latestSnapshot().get().id(),
              1L);
      PaimonCompactionInput restored =
          (PaimonCompactionInput)
              SerializationUtil.simpleDeserialize(SerializationUtil.simpleSerialize(input).array());
      Set<Path> before = physicalFiles();
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class, () -> new PaimonCompactionExecutor(restored).execute());
      assertTrue(failure.getMessage().contains("DATA_EVOLUTION"));
      assertEquals(before, physicalFiles());
      assertEquals(2L, table.latestSnapshot().get().id());
    }
  }

  private Set<Path> physicalFiles() throws Exception {
    try (Stream<Path> paths = Files.walk(warehouse)) {
      return paths.filter(Files::isRegularFile).collect(Collectors.toSet());
    }
  }

  @ParameterizedTest
  @EnumSource(CommitMode.class)
  void rejectsRecoveredSuccessBeforeCommitOrReplay(CommitMode mode) throws Exception {
    try (Catalog catalog = catalog()) {
      AppendOnlyFileStoreTable table = table(catalog, true);
      List<DataFileMeta> files = new ArrayList<>();
      table.store().newScan().readFileIterator().forEachRemaining(entry -> files.add(entry.file()));
      String user = "legacy-evolution";
      files.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));
      // Create a valid Paimon output to model an already successful persisted task. Amoro must
      // still refuse it: recovery of SUCCESS tasks does not call the executor again.
      CommitMessage message =
          new DataEvolutionCompactTask(BinaryRow.EMPTY_ROW, files, false).doCompact(table, user);
      CommitMessageSerializer messages = new CommitMessageSerializer();
      PaimonCompactionOutput output =
          new PaimonCompactionOutput(
              messages.serialize(message), messages.getVersion(), 2, 0, 1, 0);
      AppendCompactTaskSerializer tasks = new AppendCompactTaskSerializer();
      PaimonCompactionInput input =
          new PaimonCompactionInput(
              wrap(table),
              tasks.serialize(new AppendCompactTask(BinaryRow.EMPTY_ROW, files)),
              tasks.getVersion(),
              user,
              "",
              table.latestSnapshot().get().id(),
              1L);
      PaimonCompactionTask task = new PaimonCompactionTask(1, "", input, Collections.emptyMap());
      task.setOutputBytes(SerializationUtil.simpleSerialize(output).array());
      Set<Path> before = physicalFiles();
      OptimizingCommitException failure =
          assertThrows(
              OptimizingCommitException.class,
              () ->
                  new PaimonTableCommit(table, Collections.singletonList(task), user, 1L)
                      .commit(mode));
      assertTrue(failure.getMessage().contains("DATA_EVOLUTION"));
      assertEquals(before, physicalFiles());
      assertEquals(2L, table.latestSnapshot().get().id());
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

  private AppendOnlyFileStoreTable table(Catalog catalog, boolean evolution) throws Exception {
    Identifier id = Identifier.create("db", "evolution-" + evolution);
    catalog.createTable(
        id,
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .option("bucket", "-1")
            .option("write-only", "true")
            .option("self-optimizing.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", String.valueOf(evolution))
            .option("compaction.min.file-num", "2")
            .build(),
        false);
    AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) catalog.getTable(id);
    for (int row = 0; row < 2; row++) {
      BatchWriteBuilder builder = table.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        write.write(GenericRow.of(row));
        commit.commit(write.prepareCommit());
      }
    }
    return table;
  }

  private static PaimonTable wrap(AppendOnlyFileStoreTable table) {
    return new PaimonTable(TableIdentifier.of("test", "db", "evolution"), table);
  }

  private static OptimizingConfig config(boolean full) {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastInterval(0)
        .setFullTriggerInterval(full ? 1 : -1)
        .setMaxTaskSize(64L * 1024 * 1024);
  }

  private static OptimizationContext context(OptimizingConfig config) {
    OptimizationContext context = mock(OptimizationContext.class);
    when(context.getOptimizingConfig()).thenReturn(config);
    when(context.isIdle()).thenReturn(true);
    return context;
  }
}
