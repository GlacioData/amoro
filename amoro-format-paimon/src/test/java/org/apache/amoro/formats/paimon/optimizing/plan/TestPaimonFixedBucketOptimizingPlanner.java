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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendRewriteOptions;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionTask;
import org.apache.amoro.formats.paimon.optimizing.PaimonFixedBucketTask;
import org.apache.amoro.formats.paimon.optimizing.PaimonFixedBucketTaskSerializer;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** End-to-end planner contract for HASH_FIXED append tables. */
class TestPaimonFixedBucketOptimizingPlanner {
  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "true,100000",
    "true,100001",
    "false,100001",
    "true,100002",
    "false,100003",
    "true,200001"
  })
  void splitsNativeFileCountBeforeConstructingBoundedTasks(
      boolean ordered, int count, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = baseOptions(ordered);
      options.put("bucket", "1");
      options.put("target-file-size", "256 mb");
      options.put("source.split.open-file-cost", "0 b");
      options.put("compaction.file-num-limit", Integer.toString(count));
      Identifier id = Identifier.create("db", "large_file_count");
      AppendOnlyFileStoreTable table = createTable(catalog, id, options);
      List<DataFileMeta> files = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        files.add(
            DataFileMeta.forAppend(
                "data-" + i + ".parquet",
                128,
                10,
                org.apache.paimon.stats.SimpleStats.EMPTY_STATS,
                i * 10L,
                i * 10L + 9,
                table.schema().id(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null));
      }
      // Count windows are validation bounds, not ordered barriers. Exercise both a trailing
      // singleton that needs the next window and a leading singleton that needs the prior one.
      if (count == 100002 || count == 100003) {
        byte[] index = new byte[8 * 1024 * 1024];
        char[] chars = new char[index.length];
        Arrays.fill(chars, 'x');
        String extra = new String(chars);
        int large = count == 100002 ? 99999 : 100000;
        files.set(large, files.get(large).copy(index).copy(Arrays.asList(extra, extra)));
        if (count == 100002) {
          files.set(large - 1, files.get(large - 1).copy(index));
        } else {
          for (int i = large + 1; i < count; i++) {
            files.set(
                i,
                files
                    .get(i)
                    .copy(index)
                    .copy(Collections.singletonList(extra.substring(0, 4 * 1024 * 1024))));
          }
        }
      }
      // Source: Paimon ac223f47, paimon-api/CoreOptions.java:885-891 defines a default,
      // not a hard maximum. Real native manifests exercise planning; data files are synthetic.
      try (org.apache.paimon.table.sink.StreamTableCommit commit = table.newCommit("count-seed")) {
        commit.commit(
            1L,
            Collections.singletonList(
                new org.apache.paimon.table.sink.CommitMessageImpl(
                    org.apache.paimon.data.BinaryRow.singleColumn(10),
                    0,
                    1,
                    new org.apache.paimon.io.DataIncrement(
                        files, Collections.emptyList(), Collections.emptyList()),
                    org.apache.paimon.io.CompactIncrement.emptyIncrement())));
      }
      OptimizingPlanResult<PaimonCompactionTask> plan =
          planner(wrap(table, id), 8D, configFor(OptimizingType.FULL, true), null).plan();
      assertEquals(OptimizingType.FULL, plan.getOptimizingType());
      assertEquals(count == 100003 ? 3 : (count + 99999) / 100000, plan.getTasks().size());
      Set<String> published = new HashSet<>();
      long previousMaxSequence = -1;
      for (PaimonCompactionTask task : plan.getTasks()) {
        PaimonFixedBucketTask decoded = decode(task);
        assertTrue(decoded.files().size() >= 2);
        assertTrue(decoded.files().size() <= 100000);
        assertTrue(task.getInput().getTaskBytes().length <= 32 * 1024 * 1024);
        assertEquals(0, decoded.bucket());
        assertEquals(1, decoded.totalBuckets());
        assertEquals(10, decoded.partition().getInt(0));
        assertEquals(ordered, decoded.ordered());
        for (DataFileMeta file : decoded.files()) {
          assertTrue(published.add(file.fileName()));
          if (ordered) {
            assertEquals(previousMaxSequence + 1, file.minSequenceNumber());
            previousMaxSequence = file.maxSequenceNumber();
          }
        }
      }
      assertEquals(
          files.stream().map(DataFileMeta::fileName).collect(Collectors.toSet()), published);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "MINOR,true,38,1024,2",
    "MINOR,true,32,1024,2",
    "MINOR,false,32,1024,2",
    "MAJOR,true,32,1024,2",
    "MAJOR,false,32,1024,2",
    "FULL,true,32,1024,2",
    "FULL,false,32,1024,2",
    "MAJOR,true,4,188743680,0"
  })
  void publishesBoundedTasksWhenNativeManifestMetadataExceedsOneEnvelope(
      OptimizingType type,
      boolean ordered,
      int fileCount,
      long fileSize,
      int expectedTasks,
      @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      Map<String, String> options = baseOptions(ordered);
      options.put("bucket", "1");
      options.put("target-file-size", "256 mb");
      options.put("source.split.open-file-cost", "0 b");
      if (type == OptimizingType.MAJOR) {
        options.put("compaction.small-file-ratio", "0");
      }
      Identifier id = Identifier.create("db", "large_metadata");
      AppendOnlyFileStoreTable table = createTable(catalog, id, options);
      List<org.apache.paimon.io.DataFileMeta> files = new ArrayList<>();
      byte[] index = new byte[(fileCount == 4 ? 8 : 1) * 1024 * 1024];
      for (int i = 0; i < fileCount; i++) {
        files.add(
            org.apache.paimon.io.DataFileMeta.forAppend(
                "data-" + i + ".parquet",
                fileSize,
                10,
                org.apache.paimon.stats.SimpleStats.EMPTY_STATS,
                i * 10L,
                i * 10L + 9,
                table.schema().id(),
                Collections.emptyList(),
                index,
                null,
                null,
                null,
                null,
                null));
      }
      // Metadata-only planner fixture: native manifests are real, data/index content is synthetic.
      // Physical rewrite/index correctness is verified by the separate native and MySQL suites.
      try (org.apache.paimon.table.sink.StreamTableCommit commit =
          table.newCommit("metadata-seed")) {
        commit.commit(
            1L,
            Collections.singletonList(
                new org.apache.paimon.table.sink.CommitMessageImpl(
                    org.apache.paimon.data.BinaryRow.singleColumn(10),
                    0,
                    1,
                    new org.apache.paimon.io.DataIncrement(
                        files, Collections.emptyList(), Collections.emptyList()),
                    org.apache.paimon.io.CompactIncrement.emptyIncrement())));
      }
      long inputLimit = 16L * 1024 * 1024 * 1024;
      OptimizingPlanResult<PaimonCompactionTask> plan =
          new PaimonOptimizingPlanner(
                  wrap(table, id),
                  TABLE_ID,
                  PROCESS_ID,
                  8D,
                  inputLimit,
                  configFor(type, type == OptimizingType.FULL).setMaxTaskSize(inputLimit),
                  0L,
                  0L,
                  0L,
                  null,
                  null)
              .plan();
      assertEquals(expectedTasks, plan.getTasks().size());
      if (expectedTasks == 0) {
        // The original four-file MAJOR group has a reduction; neither rebalanced pair does.
        assertTrue(Math.ceil((double) fileCount * fileSize / (256 * 1024 * 1024)) < fileCount);
        assertEquals(2D, Math.ceil((double) 2 * fileSize / (256 * 1024 * 1024)));
        return;
      }
      assertEquals(type, plan.getOptimizingType());
      Set<String> published = new HashSet<>();
      for (PaimonCompactionTask task : plan.getTasks()) {
        assertTrue(task.getInput().getTaskBytes().length <= 32 * 1024 * 1024);
        assertEquals(type, decode(task).optimizingType());
        for (org.apache.paimon.io.DataFileMeta file : decode(task).files()) {
          assertTrue(published.add(file.fileName()));
        }
      }
      assertEquals(
          files.stream()
              .map(org.apache.paimon.io.DataFileMeta::fileName)
              .collect(Collectors.toSet()),
          published);
    }
  }

  private static final long TABLE_ID = 101L;
  private static final long PROCESS_ID = 202L;
  private static final long MAX_TASK_BYTES = 64L * 1024 * 1024;

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({"true,false", "false,false", "true,true"})
  void boundedCandidatesSkipHealthyPrefixAndCommitSmallSuffix(
      boolean ordered, boolean fragmented, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = baseOptions(ordered);
      options.put("bucket", "1");
      options.put("target-file-size", "64 mb");
      options.put("source.split.open-file-cost", "0 b");
      options.put("compaction.min.file-num", "3");
      options.put("compaction.file-num-limit", "3");
      Identifier identifier = Identifier.create("db", "bounded_suffix");
      AppendOnlyFileStoreTable table = createTable(catalog, identifier, options);
      for (int round = 0; round < 3; round++) {
        if (fragmented) {
          writeRound(table, 10, ids(0, 8), 8);
        }
        writeRound(table, 10, ids(0, 8), 64 * 1024);
      }
      List<org.apache.paimon.manifest.ManifestEntry> healthy = activeFiles(table);
      long largest = healthy.stream().mapToLong(e -> e.file().fileSize()).max().getAsLong();
      healthy =
          healthy.stream()
              .filter(e -> e.file().fileSize() > largest / 2)
              .collect(Collectors.toList());
      assertEquals(3, healthy.size());
      long target = healthy.stream().mapToLong(e -> e.file().fileSize()).min().getAsLong() / 2;
      for (int round = 0; round < 3; round++) {
        writeRound(table, 10, ids(0, 8), 8);
      }
      Set<String> healthyNames =
          healthy.stream().map(e -> e.fileName()).collect(Collectors.toSet());
      List<String> before = readRows(table);
      catalog.alterTable(
          identifier,
          Collections.singletonList(SchemaChange.setOption("target-file-size", target + " b")),
          false);
      table = (AppendOnlyFileStoreTable) catalog.getTable(identifier);
      long snapshot = table.latestSnapshot().get().id();

      // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/append/BucketedAppendCompactManager.java#L204-L229
      // Native selection advances past large files. A bounded Amoro window must not repeatedly
      // replace its three actual problem candidates with an unproductive healthy prefix.
      PaimonOptimizingPlanner planner =
          planner(wrap(table, identifier), 8.0D, configFor(OptimizingType.MINOR, false), null);
      OptimizingPlanResult<PaimonCompactionTask> result = planner.plan();
      assertEquals(1, result.getTasks().size());
      assertEquals(OptimizingType.MINOR, result.getOptimizingType());
      PaimonCompactionTask task = result.getTasks().get(0);
      PaimonFixedBucketTask fixed = decode(task);
      assertEquals(3, fixed.files().size());
      assertTrue(Collections.disjoint(healthyNames, fileNames(fixed)));
      assertEquals(snapshot, fixed.snapshotId());
      org.apache.amoro.formats.paimon.optimizing.PaimonCompactionOutput output =
          new org.apache.amoro.formats.paimon.optimizing.PaimonCompactionExecutor(task.getInput())
              .execute();
      java.nio.ByteBuffer serialized =
          org.apache.amoro.utils.SerializationUtil.simpleSerialize(output);
      byte[] bytes = new byte[serialized.remaining()];
      serialized.get(bytes);
      task.setOutputBytes(bytes);
      new org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit(
              table, result.getTasks(), planner.getCommitUser(), PROCESS_ID)
          .commit();
      assertEquals(before, readRows(table));
      assertEquals(fragmented ? 7 : 4, activeFiles(table).size());
      assertTrue(
          activeFiles(table).stream()
              .map(e -> e.fileName())
              .collect(Collectors.toSet())
              .containsAll(healthyNames));
    }
  }

  private static List<String> readRows(AppendOnlyFileStoreTable table) throws Exception {
    List<String> rows = new ArrayList<>();
    try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
        table.newRead().createReader(table.newScan().plan())) {
      org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
          batch;
      while ((batch = reader.readBatch()) != null) {
        org.apache.paimon.data.InternalRow row;
        while ((row = batch.next()) != null) {
          rows.add(row.getInt(0) + "|" + row.getInt(1) + "|" + row.getString(2));
        }
        batch.releaseBatch();
      }
    }
    Collections.sort(rows);
    return rows;
  }

  @ParameterizedTest(name = "ordered={0}, type={1}, rewriteAll={2}")
  @MethodSource("planningScenarios")
  void plansVersionedExactBucketPayloads(
      boolean ordered, OptimizingType expectedType, boolean rewriteAll, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = optionsFor(expectedType, ordered, rewriteAll);
      Identifier identifier = Identifier.create("db", "fixed_" + expectedType.name().toLowerCase());
      AppendOnlyFileStoreTable table = createTable(catalog, identifier, options);
      writeRounds(table, Arrays.asList(10, 20), roundsFor(expectedType), payloadSize(expectedType));
      PaimonTable wrapper = wrap(table, identifier);
      long snapshotId = table.latestSnapshot().get().id();

      OptimizingPlanResult<PaimonCompactionTask> result =
          planner(wrapper, 16.0D, configFor(expectedType, rewriteAll), null).plan();

      assertFalse(result.getTasks().isEmpty());
      assertEquals(expectedType, result.getOptimizingType());
      assertEquals(snapshotId, result.getTargetSnapshotId());
      assertFixedTasks(result, wrapper, expectedType, ordered, rewriteAll, snapshotId);
      assertEquals(
          2,
          result.getTasks().stream()
              .map(task -> decode(task).partition())
              .collect(Collectors.toSet())
              .size(),
          "both real partitions must remain independently plannable");
      assertEquals(
          new HashSet<>(Arrays.asList(0, 1)),
          result.getTasks().stream()
              .map(task -> decode(task).bucket())
              .collect(Collectors.toSet()));
    }
  }

  @Test
  void quotaOnlyLimitsPublishedCountAndRecomputesActualHighestType(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = baseOptions(true);
      options.put("target-file-size", "1 mb");
      options.put("compaction.small-file-ratio", "0.001");
      options.put("compaction.min.file-num", "2");
      Identifier identifier = Identifier.create("db", "fixed_quota");
      AppendOnlyFileStoreTable table = createTable(catalog, identifier, options);
      writeRounds(table, Collections.singletonList(10), 3, 8);
      writeRounds(table, Collections.singletonList(20), 2, 64 * 1024);
      long largestMinorFile =
          activeFiles(table).stream()
              .filter(entry -> entry.partition().getInt(0) == 10)
              .mapToLong(entry -> entry.file().fileSize())
              .max()
              .orElseThrow(() -> new AssertionError("missing MINOR fixture files"));
      long smallestMajorFile =
          activeFiles(table).stream()
              .filter(entry -> entry.partition().getInt(0) == 20)
              .mapToLong(entry -> entry.file().fileSize())
              .min()
              .orElseThrow(() -> new AssertionError("missing MAJOR fixture files"));
      assertTrue(
          largestMinorFile < smallestMajorFile,
          "fixture must derive thresholds from two distinct real file-size bands");
      long targetBytes = smallestMajorFile * 2;
      double smallRatio = ((double) largestMinorFile + smallestMajorFile) / 2.0D / targetBytes;
      catalog.alterTable(
          identifier,
          Arrays.asList(
              SchemaChange.setOption("target-file-size", targetBytes + " b"),
              SchemaChange.setOption("compaction.small-file-ratio", Double.toString(smallRatio))),
          false);
      table = (AppendOnlyFileStoreTable) catalog.getTable(identifier);
      PaimonTable wrapper = wrap(table, identifier);
      OptimizingConfig config = configFor(OptimizingType.MINOR, false);

      OptimizingPlanResult<PaimonCompactionTask> all = planner(wrapper, 16.0D, config, null).plan();
      OptimizingPlanResult<PaimonCompactionTask> limited =
          planner(wrapper, 1.0D, config, null).plan();

      Set<OptimizingType> allTypes =
          all.getTasks().stream()
              .map(TestPaimonFixedBucketOptimizingPlanner::decode)
              .map(PaimonFixedBucketTask::optimizingType)
              .collect(Collectors.toSet());
      assertEquals(
          new HashSet<>(Arrays.asList(OptimizingType.MINOR, OptimizingType.MAJOR)), allTypes);
      assertEquals(1, limited.getTasks().size());
      PaimonFixedBucketTask firstAll = decode(all.getTasks().get(0));
      PaimonFixedBucketTask released = decode(limited.getTasks().get(0));
      assertEquals(OptimizingType.MINOR, released.optimizingType());
      assertEquals(OptimizingType.MINOR, limited.getOptimizingType());
      assertEquals(firstAll.partition(), released.partition());
      assertEquals(firstAll.bucket(), released.bucket());
      assertEquals(fileNames(firstAll), fileNames(released));
      assertEquals(
          fileNames(released).size(),
          new HashSet<>(fileNames(released)).size(),
          "quota must not split, duplicate, or rewrite a task's exact inputs");
    }
  }

  @Test
  void leavesHealthyBucketOutOfPublishedPayloads(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = baseOptions(true);
      options.put("target-file-size", "64 mb");
      options.put("compaction.min.file-num", "3");
      Identifier identifier = Identifier.create("db", "fixed_healthy_bucket");
      AppendOnlyFileStoreTable table = createTable(catalog, identifier, options);
      // First round creates one file in each bucket. Additional rounds write only one discovered
      // bucket; the other bucket remains a non-actionable, single-file unit.
      writeRound(table, 10, ids(0, 32), 8);
      Map<Integer, Long> before = bucketFileCounts(table, 10);
      writeRound(table, 10, Collections.singletonList(0), 8);
      Map<Integer, Long> afterProbe = bucketFileCounts(table, 10);
      int problemBucket =
          afterProbe.entrySet().stream()
              .filter(entry -> entry.getValue() == before.getOrDefault(entry.getKey(), 0L) + 1)
              .map(Map.Entry::getKey)
              .findFirst()
              .orElseThrow(() -> new AssertionError("single-id write did not identify one bucket"));
      writeRound(table, 10, Collections.singletonList(0), 8);
      PaimonTable wrapper = wrap(table, identifier);

      OptimizingPlanResult<PaimonCompactionTask> result =
          planner(wrapper, 8.0D, configFor(OptimizingType.MINOR, false), null).plan();

      assertFalse(result.getTasks().isEmpty());
      assertEquals(
          Collections.singleton(problemBucket),
          result.getTasks().stream()
              .map(task -> decode(task).bucket())
              .collect(Collectors.toSet()));
      assertTrue(
          result.getTasks().stream().allMatch(task -> decode(task).partition().getInt(0) == 10));
    }
  }

  @Test
  void reusesSuppliedAnalysisWithoutRepeatingTheInitialSnapshotScan(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = baseOptions(false);
      options.put("target-file-size", "64 mb");
      options.put("compaction.min.file-num", "3");
      Identifier identifier = Identifier.create("db", "fixed_supplied_analysis");
      AppendOnlyFileStoreTable table = createTable(catalog, identifier, options);
      writeRounds(table, Arrays.asList(10, 20), 3, 16);
      PaimonTable wrapper = wrap(table, identifier);
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.capture(
              table, wrapper.id().toString(), null, wrapper.properties());
      PaimonAppendSnapshotAnalysis analysis = PaimonAppendFileScanner.analyze(table, health, null);
      PaimonOptimizingPlanner planner =
          planner(wrapper, 8.0D, configFor(OptimizingType.MINOR, false), analysis);

      OptimizingPlanResult<PaimonCompactionTask> result = planner.plan();

      assertFalse(result.getTasks().isEmpty());
      assertEquals(0, planner.fallbackScanCount());
      assertEquals(analysis, planner.tableAnalysis().orElse(null));
      assertEquals(analysis.key().getSnapshotId(), result.getTargetSnapshotId());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"dv", "clustering"})
  void rejectsUnsupportedNativeModesWithoutPublishingTasks(String mode, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = baseOptions(false);
      if ("dv".equals(mode)) {
        options.put("deletion-vectors.enabled", "true");
      } else {
        options.put("clustering.incremental", "true");
        options.put("clustering.columns", "id");
      }
      Identifier identifier = Identifier.create("db", "fixed_unsupported_" + mode);
      AppendOnlyFileStoreTable table = createTable(catalog, identifier, options);
      writeRounds(table, Arrays.asList(10, 20), 3, 8);

      OptimizingPlanResult<PaimonCompactionTask> result =
          planner(wrap(table, identifier), 8.0D, configFor(OptimizingType.MINOR, false), null)
              .plan();

      assertTrue(result.getTasks().isEmpty());
    }
  }

  private static Stream<Arguments> planningScenarios() {
    List<Arguments> scenarios = new ArrayList<>();
    for (boolean ordered : new boolean[] {true, false}) {
      scenarios.add(Arguments.of(ordered, OptimizingType.MINOR, false));
      scenarios.add(Arguments.of(ordered, OptimizingType.MAJOR, false));
      scenarios.add(Arguments.of(ordered, OptimizingType.FULL, false));
      scenarios.add(Arguments.of(ordered, OptimizingType.FULL, true));
    }
    return scenarios.stream();
  }

  private static Catalog catalog(Path warehouse) {
    return PaimonCatalogFactory.paimonCatalog(
        Collections.singletonMap("warehouse", warehouse.toUri().toString()), new Configuration());
  }

  private static AppendOnlyFileStoreTable createTable(
      Catalog catalog, Identifier identifier, Map<String, String> options) throws Exception {
    catalog.createDatabase(identifier.getDatabaseName(), true);
    Schema.Builder schema =
        Schema.newBuilder()
            .column("dt", DataTypes.INT())
            .column("id", DataTypes.INT())
            .column("payload", DataTypes.STRING())
            .partitionKeys("dt")
            .option("bucket", "2")
            .option("bucket-key", "id");
    options.forEach(schema::option);
    catalog.createTable(identifier, schema.build(), false);
    return (AppendOnlyFileStoreTable) catalog.getTable(identifier);
  }

  private static Map<String, String> baseOptions(boolean ordered) {
    Map<String, String> options = new HashMap<>();
    options.put("bucket-append-ordered", Boolean.toString(ordered));
    options.put(CoreOptions.WRITE_ONLY.key(), "true");
    options.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");
    options.put("source.split.open-file-cost", "4 kb");
    return options;
  }

  private static Map<String, String> optionsFor(
      OptimizingType type, boolean ordered, boolean rewriteAll) {
    Map<String, String> options = baseOptions(ordered);
    options.put("compaction.min.file-num", type == OptimizingType.MINOR ? "3" : "100");
    options.put("target-file-size", type == OptimizingType.MAJOR ? "1 mb" : "64 mb");
    options.put("compaction.small-file-ratio", type == OptimizingType.MAJOR ? "0.001" : "0.7");
    options.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, Boolean.toString(rewriteAll));
    return options;
  }

  private static OptimizingConfig configFor(OptimizingType type, boolean rewriteAll) {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastInterval(-1)
        .setFullTriggerInterval(type == OptimizingType.FULL ? 1 : -1)
        .setFullRewriteAllFiles(rewriteAll)
        .setMaxTaskSize(MAX_TASK_BYTES);
  }

  private static int roundsFor(OptimizingType type) {
    return type == OptimizingType.MINOR ? 3 : 2;
  }

  private static int payloadSize(OptimizingType type) {
    return type == OptimizingType.MAJOR ? 64 * 1024 : 16;
  }

  private static void writeRounds(
      AppendOnlyFileStoreTable table, List<Integer> partitions, int rounds, int payloadSize)
      throws Exception {
    for (int round = 0; round < rounds; round++) {
      for (int partition : partitions) {
        writeRound(table, partition, ids(0, 32), payloadSize);
      }
    }
  }

  private static void writeRound(
      AppendOnlyFileStoreTable table, int partition, List<Integer> ids, int payloadSize)
      throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      for (int id : ids) {
        write.write(
            GenericRow.of(
                partition, id, BinaryString.fromString(payload(partition, id, payloadSize))));
      }
      commit.commit(write.prepareCommit());
    }
  }

  private static String payload(int partition, int id, int size) {
    String seed = partition + "-" + id + "-";
    StringBuilder value = new StringBuilder(size);
    while (value.length() < size) {
      value.append(seed).append(Integer.toHexString(value.length() * 31 + id));
    }
    return value.substring(0, size);
  }

  private static List<Integer> ids(int start, int end) {
    List<Integer> ids = new ArrayList<>();
    for (int i = start; i < end; i++) {
      ids.add(i);
    }
    return ids;
  }

  private static Map<Integer, Long> bucketFileCounts(
      AppendOnlyFileStoreTable table, int partition) {
    return table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
        .stream()
        .filter(entry -> entry.kind() == FileKind.ADD)
        .filter(entry -> entry.partition().getInt(0) == partition)
        .collect(Collectors.groupingBy(entry -> entry.bucket(), Collectors.counting()));
  }

  private static PaimonTable wrap(AppendOnlyFileStoreTable table, Identifier identifier) {
    return new PaimonTable(
        TableIdentifier.of(
            "test_catalog", identifier.getDatabaseName(), identifier.getObjectName()),
        table);
  }

  private static PaimonOptimizingPlanner planner(
      PaimonTable table,
      double availableCore,
      OptimizingConfig config,
      PaimonAppendSnapshotAnalysis analysis) {
    return new PaimonOptimizingPlanner(
        table,
        TABLE_ID,
        PROCESS_ID,
        availableCore,
        MAX_TASK_BYTES,
        config,
        0L,
        0L,
        0L,
        null,
        analysis);
  }

  private static void assertFixedTasks(
      OptimizingPlanResult<PaimonCompactionTask> result,
      PaimonTable wrapper,
      OptimizingType type,
      boolean ordered,
      boolean rewriteAll,
      long snapshotId) {
    String commitUser = null;
    Set<String> allFiles = new HashSet<>();
    for (PaimonCompactionTask task : result.getTasks()) {
      PaimonCompactionInput input = task.getInput();
      assertEquals(PaimonFixedBucketTaskSerializer.VERSION, input.getSerializerVersion());
      assertEquals(PROCESS_ID, input.getCommitIdentifier());
      assertEquals(snapshotId, input.getTargetSnapshotId());
      if (commitUser == null) {
        commitUser = input.getCommitUser();
      }
      assertEquals(commitUser, input.getCommitUser());
      PaimonFixedBucketTask fixed = decode(task);
      assertEquals(wrapper.id().toString(), fixed.tableId());
      AppendOnlyFileStoreTable nativeTable = (AppendOnlyFileStoreTable) wrapper.originalTable();
      assertEquals(nativeTable.location().toString(), fixed.tablePath());
      assertEquals(snapshotId, fixed.snapshotId());
      assertEquals(nativeTable.schema().id(), fixed.schemaId());
      assertEquals(2, fixed.totalBuckets());
      assertEquals(type, fixed.optimizingType());
      assertEquals(ordered, fixed.ordered());
      assertEquals(rewriteAll, fixed.rewriteAll());
      fixed.validateBinding(wrapper);
      assertFalse(fixed.files().isEmpty());
      assertEquals(fixed.files().size(), new HashSet<>(fileNames(fixed)).size());
      for (String file : fileNames(fixed)) {
        assertTrue(allFiles.add(file), "one file cannot belong to two published tasks: " + file);
      }
    }
    assertNotNull(commitUser);
  }

  private static PaimonFixedBucketTask decode(PaimonCompactionTask task) {
    PaimonCompactionInput input = task.getInput();
    return new PaimonFixedBucketTaskSerializer()
        .deserialize(input.getSerializerVersion(), input.getTaskBytes());
  }

  private static List<String> fileNames(PaimonFixedBucketTask task) {
    return task.files().stream().map(DataFileMeta::fileName).collect(Collectors.toList());
  }

  private static List<org.apache.paimon.manifest.ManifestEntry> activeFiles(
      AppendOnlyFileStoreTable table) {
    return table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
        .stream()
        .filter(entry -> entry.kind() == FileKind.ADD)
        .collect(Collectors.toList());
  }
}
