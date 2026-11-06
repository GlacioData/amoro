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

package org.apache.amoro.formats.paimon.optimizing.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionTask;
import org.apache.amoro.formats.paimon.optimizing.PaimonFixedBucketTask;
import org.apache.amoro.formats.paimon.optimizing.PaimonFixedBucketTaskSerializer;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.amoro.table.TableIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.AppendCompactTaskSerializer;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/** Candidate coverage must be checked before reusing a bounded snapshot analysis. */
class TestPaimonAppendAnalysisCoverage {
  static Stream<Arguments> coverageCases() {
    // source filtered, candidate limit, target (all / same / other), fallback count, FULL
    Object[][] cases = {
      {false, 2, "other", 1, false},
      {false, 2, "all", 0, false},
      {false, 20, "other", 0, false},
      {false, 20, "all", 0, false},
      {true, 20, "all", 1, false},
      {true, 20, "other", 1, false},
      {true, 20, "same", 1, false},
      {true, 2, "other", 1, false},
      {false, 2, "other", 1, true},
      {true, 20, "all", 1, true}
    };
    return Stream.of("unaware", "fixed-ordered", "fixed-unordered")
        .flatMap(
            mode ->
                Arrays.stream(cases).map(c -> Arguments.of(mode, c[0], c[1], c[2], c[3], c[4])));
  }

  @ParameterizedTest(name = "{0}: filtered={1}, limit={2}, target={3}, full={5}")
  @MethodSource("coverageCases")
  void suppliedAnalysisPreservesDirectScanTasks(
      String mode,
      boolean sourceFiltered,
      int limit,
      String target,
      int fallback,
      boolean full,
      @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      Identifier id = Identifier.create("db", "coverage");
      boolean fixed = !mode.equals("unaware");
      Schema.Builder schema =
          Schema.newBuilder()
              .column("dt", DataTypes.INT())
              .column("id", DataTypes.INT())
              .partitionKeys("dt")
              .option("bucket", fixed ? "1" : "-1")
              .option("bucket-append-ordered", Boolean.toString(mode.equals("fixed-ordered")))
              .option("write-only", "true")
              .option("self-optimizing.enabled", "true")
              .option("compaction.file-num-limit", Integer.toString(limit))
              .option("compaction.min.file-num", "2")
              .option("target-file-size", "64 mb");
      if (fixed) {
        schema.option("bucket-key", "id");
      }
      catalog.createTable(id, schema.build(), false);
      AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) catalog.getTable(id);
      for (int partition = 0; partition < 3; partition++) {
        for (int round = 0; round < 3; round++) {
          BatchWriteBuilder builder = table.newBatchWriteBuilder();
          try (BatchTableWrite write = builder.newWrite();
              BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.of(partition, round));
            commit.commit(write.prepareCommit());
          }
        }
      }
      PaimonTable wrapper = new PaimonTable(TableIdentifier.of("test", "db", "coverage"), table);
      PaimonHealthEvaluationContext context =
          PaimonHealthEvaluationContext.capture(
              table, wrapper.id().toString(), null, wrapper.properties());
      PaimonAppendSnapshotAnalysis analysis =
          sourceFiltered
              ? PaimonAppendFileScanner.analyze(table, context, filter(table, 0))
              : (PaimonAppendSnapshotAnalysis)
                  wrapper.evaluatePendingInput(null, 100).get().tableAnalysis().get();
      assertEquals(9, analysis.fullSnapshotFileCount());
      assertEquals(limit < (sourceFiltered ? 3 : 9), analysis.plannerFilesTruncated());
      int sourcePartition = analysis.plannerFiles().get(0).partition().getInt(0);
      int targetPartition = target.equals("same") ? sourcePartition : (sourcePartition + 1) % 3;
      Predicate targetFilter = target.equals("all") ? null : filter(table, targetPartition);
      if (!sourceFiltered && limit == 2 && !target.equals("all")) {
        assertTrue(
            analysis.plannerFiles().stream()
                .noneMatch(f -> f.partition().getInt(0) == targetPartition));
      }
      PaimonOptimizingPlanner reused = planner(wrapper, targetFilter, analysis, full);
      PaimonOptimizingPlanner direct = planner(wrapper, targetFilter, null, full);
      OptimizingPlanResult<PaimonCompactionTask> expected = direct.plan();
      OptimizingPlanResult<PaimonCompactionTask> actual = reused.plan();
      assertFalse(expected.getTasks().isEmpty(), "The target range must be actionable");
      assertEquals(
          taskFacts(expected, fixed),
          taskFacts(actual, fixed),
          "Partition, bucket, full file metadata and order must match");
      assertEquals(expected.getTargetSnapshotId(), actual.getTargetSnapshotId());
      assertEquals(fallback, reused.fallbackScanCount());
      assertEquals(1, direct.fallbackScanCount());
      PaimonAppendSnapshotAnalysis actualAnalysis =
          (PaimonAppendSnapshotAnalysis) reused.tableAnalysis().get();
      assertEquals(
          analysis.pendingInput().getHealthScore(), actualAnalysis.pendingInput().getHealthScore());
      assertEquals(healthFacts(analysis), healthFacts(actualAnalysis));
    }
  }

  private static ObjectNode healthFacts(PaimonAppendSnapshotAnalysis analysis) {
    ObjectNode details = new ObjectMapper().valueToTree(analysis.healthDetails());
    // A fallback evaluates at a new wall-clock instant; every health fact must still match.
    ((ObjectNode) details.get("structure")).remove("evaluatedAtMillis");
    return details;
  }

  private static Predicate filter(AppendOnlyFileStoreTable table, int partition) {
    PredicateBuilder builder = new PredicateBuilder(table.schema().logicalPartitionType());
    return builder.equal(builder.indexOf("dt"), partition);
  }

  private static PaimonOptimizingPlanner planner(
      PaimonTable table, Predicate filter, PaimonAppendSnapshotAnalysis analysis, boolean full) {
    OptimizingConfig config =
        new OptimizingConfig()
            .setEnabled(true)
            .setMinorLeastInterval(-1)
            .setFullTriggerInterval(full ? 1 : -1)
            .setFullRewriteAllFiles(full)
            .setMaxTaskSize(64L * 1024 * 1024);
    return new PaimonOptimizingPlanner(
        table, 1L, 1L, 8.0, 64L * 1024 * 1024, config, 0L, 0L, 0L, filter, analysis);
  }

  private static List<List<Object>> taskFacts(
      OptimizingPlanResult<PaimonCompactionTask> plan, boolean fixed) throws Exception {
    List<List<Object>> facts = new ArrayList<>();
    for (PaimonCompactionTask task : plan.getTasks()) {
      int version = task.getInput().getSerializerVersion();
      byte[] bytes = task.getInput().getTaskBytes();
      if (fixed) {
        PaimonFixedBucketTask nativeTask =
            new PaimonFixedBucketTaskSerializer().deserialize(version, bytes);
        facts.add(
            Arrays.asList(
                nativeTask.partition(),
                nativeTask.bucket(),
                nativeTask.totalBuckets(),
                nativeTask.optimizingType(),
                nativeTask.ordered(),
                nativeTask.rewriteAll(),
                nativeTask.files()));
      } else {
        AppendCompactTask nativeTask =
            new AppendCompactTaskSerializer().deserialize(version, bytes);
        facts.add(Arrays.asList(nativeTask.partition(), nativeTask.compactBefore()));
      }
    }
    return facts;
  }
}
