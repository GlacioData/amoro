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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.config;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.wrap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonPrimaryKeyOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Behavior oracle built against the frozen production revision before evidence changes. */
class TestPaimonAnalysisBehaviorBaseline {
  @Test
  void matchingAnalysisUsesOneRealLiveFileEnumeration(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable raw = table(catalog, "baseline", options(false));
      for (int i = 0; i < 3; i++) {
        append(raw, GenericRow.of(i, i, i, (long) i));
      }
      ReadCounts reads = new ReadCounts(raw);
      FileStoreTable counted = reads.table;
      PaimonTable wrapped = wrap(counted);
      PaimonHealthEvaluationContext context =
          PaimonHealthEvaluationContext.capture(counted, wrapped.id().toString(), null);
      PaimonPrimaryKeyOptimizingEvaluation evaluation =
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
              counted, counted.name(), context, config(), 0, 0, null, System.currentTimeMillis());
      assertTrue(evaluation.necessary());
      assertEquals(1, reads.live.get());
      PaimonPrimaryKeyOptimizingPlanner planner =
          new PaimonPrimaryKeyOptimizingPlanner(
              wrapped,
              1,
              100,
              1,
              Long.MAX_VALUE,
              config(),
              0,
              0,
              0,
              null,
              null,
              evaluation.analysis().get());
      assertTrue(planner.isNecessary());
      assertEquals(evaluation.units().size(), planner.plan().getTasks().size());
      assertEquals(1, reads.live.get(), "Planner must reuse the supplied fixed-input analysis");
    }
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({
    "minor,1,3,1,100,true,MINOR",
    "major,1,4,1,3,true,MAJOR",
    "many_units,24,3,1,100,true,MINOR",
    "wide_partition,8,3,1,100,true,MINOR",
    "few_units_many_files,1,24,1,100,true,MINOR",
    "hash_dynamic,4,3,-1,100,true,MINOR",
    "healthy,1,1,1,100,false,MINOR",
    "deadline,1,3,1,100,false,MINOR",
    "full,2,1,1,100,true,FULL"
  })
  void recordsStableBehavior(
      String name,
      int partitions,
      int commits,
      int buckets,
      int stopTrigger,
      boolean necessary,
      String type,
      @TempDir Path temporary)
      throws Exception {
    Path warehouse =
        Paths.get(System.getProperty("analysis.baseline.warehouse", temporary.toString()));
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable raw = fixture(catalog, name, partitions, commits, buckets, stopTrigger);
      long now = raw.latestSnapshot().get().timeMillis() + 10_000L;
      OptimizingConfig config = config();
      if (name.equals("full")) {
        config.setFullTriggerInterval(1000);
      }
      long lastMinor = name.equals("deadline") ? now : 0;
      if (lastMinor != 0) {
        config.setMinorLeastInterval(1000);
      }
      ReadCounts reads = new ReadCounts(raw);
      PaimonTable wrapped = wrap(reads.table);
      PaimonHealthEvaluationContext context =
          PaimonHealthEvaluationContext.captureAt(
              reads.table, wrapped.id().toString(), null, null, now);
      Measurement refreshMeasurement = new Measurement();
      PaimonPrimaryKeyOptimizingEvaluation refresh =
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
              reads.table, raw.name(), context, config, lastMinor, 0, null, now);
      ObjectNode refreshCost = refreshMeasurement.finish();
      assertEquals(1, reads.live.get());
      int refreshWatermarks = reads.watermarks.get();
      assertEquals(necessary, refresh.necessary());
      assertEquals(type, refresh.optimizingType().name());
      PaimonPrimaryKeyOptimizingPlanner planner =
          new PaimonPrimaryKeyOptimizingPlanner(
              wrapped,
              1,
              100,
              1,
              Long.MAX_VALUE,
              config,
              lastMinor,
              0,
              0,
              null,
              null,
              refresh.analysis().get());
      Field clock = PaimonPrimaryKeyOptimizingPlanner.class.getDeclaredField("planTime");
      clock.setAccessible(true);
      clock.setLong(planner, now);
      Measurement plannerMeasurement = new Measurement();
      assertEquals(necessary, planner.isNecessary());
      OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> plan = planner.plan();
      ObjectNode plannerCost = plannerMeasurement.finish();
      assertEquals(
          1, reads.live.get(), "Matching supplied analysis must not enumerate live files again");
      assertEquals(refresh.units().size(), plan.getTasks().size());
      if (name.equals("many_units")) {
        assertTrue(
            plan.getTasks().size() > 20, "Diagnostic top-K cannot truncate candidate coverage");
      }
      ObjectMapper json = new ObjectMapper();
      ObjectNode record = json.createObjectNode();
      record.put("case", name);
      record.put("planTime", now);
      record.put("necessary", necessary);
      record.put("type", refresh.optimizingType().name());
      record.put("targetSnapshot", refresh.targetSnapshotId());
      record.put("nextEvaluationTime", refresh.nextEvaluationTime());
      record.set("pending", json.valueToTree(refresh.analysis().get().pendingInput()));
      ObjectNode health = json.valueToTree(refresh.analysis().get().healthDetails());
      assertTrue(health.path("structure").path("evaluatedAtMillis").asLong() > 0);
      ((ObjectNode) health.path("structure")).remove("evaluatedAtMillis");
      record.set("health", health);
      record.set("units", json.valueToTree(refresh.units()));
      List<Map<String, Object>> tasks = new ArrayList<>();
      for (PaimonPrimaryKeyCompactionTask task : plan.getTasks()) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("partition", task.getPartition());
        fields.put("properties", task.getProperties());
        fields.put("units", task.getInput().getUnits());
        fields.put("type", task.getInput().getOptimizingType());
        fields.put("full", task.getInput().isFullCompaction());
        fields.put("snapshot", task.getInput().getTargetSnapshotId());
        fields.put("commitIdentifier", task.getInput().getCommitIdentifier());
        assertEquals(planner.getCommitUser(), task.getInput().getCommitUser());
        assertTrue(task.getInput().getCommitUser() != null);
        tasks.add(fields);
      }
      record.set("tasks", json.valueToTree(tasks));
      Path output =
          Paths.get(System.getProperty("analysis.baseline.output", "target/analysis-baseline"));
      String expected = System.getProperty("analysis.baseline.expected");
      if (expected != null) {
        assertFalse(
            output
                .toAbsolutePath()
                .normalize()
                .equals(Paths.get(expected).toAbsolutePath().normalize()),
            "Comparison output must not overwrite the frozen oracle");
        // Round-trip both sides: JSON integers do not preserve Java IntNode/LongNode widths.
        assertEquals(
            json.readTree(Paths.get(expected, name + ".json").toFile()),
            json.readTree(json.writeValueAsBytes(record)),
            name);
      }
      java.nio.file.Files.createDirectories(output);
      json.writerWithDefaultPrettyPrinter()
          .writeValue(output.resolve(name + ".json").toFile(), record);
      ObjectNode measured = json.createObjectNode();
      measured.put("case", name);
      measured.set("refresh", refreshCost);
      measured.set("planner", plannerCost);
      measured.put("refreshLiveScans", 1);
      measured.put("plannerLiveScans", reads.live.get() - 1);
      measured.put("refreshWatermarkScans", refreshWatermarks);
      measured.put("plannerWatermarkScans", reads.watermarks.get() - refreshWatermarks);
      measured.put("javaVersion", System.getProperty("java.version"));
      int[] nativeObjects = new int[2];
      countNativeObjects(
          refresh.analysis().get(),
          java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()),
          nativeObjects);
      measured.put("retainedDataFileMeta", nativeObjects[0]);
      measured.put("retainedLevels", nativeObjects[1]);
      json.writerWithDefaultPrettyPrinter()
          .writeValue(output.resolve(name + ".measurement.json").toFile(), measured);
    }
  }

  @Test
  void continuousCommitsInvalidateEachCapturedAnalysis(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable raw = table(catalog, "continuous", options(false));
      for (int i = 0; i < 3; i++) {
        append(raw, GenericRow.of(i, i, i, (long) i));
      }
      ReadCounts reads = new ReadCounts(raw);
      PaimonTable wrapped = wrap(reads.table);
      for (int round = 0; round < 3; round++) {
        PaimonHealthEvaluationContext context =
            PaimonHealthEvaluationContext.capture(reads.table, wrapped.id().toString(), null);
        PaimonPrimaryKeySnapshotAnalysis analysis =
            PaimonPrimaryKeySnapshotAnalysis.analyze(reads.table, context);
        append(raw, GenericRow.of(100 + round, round, round, (long) round));
        PaimonPrimaryKeyOptimizingPlanner planner =
            new PaimonPrimaryKeyOptimizingPlanner(
                wrapped,
                1,
                100 + round,
                1,
                Long.MAX_VALUE,
                config(),
                0,
                0,
                0,
                null,
                null,
                analysis);
        assertTrue(planner.isNecessary());
        assertEquals(raw.latestSnapshot().get().id(), planner.plan().getTargetSnapshotId());
        assertEquals(
            (round + 1) * 2, reads.live.get(), "Each new commit requires one fresh fallback scan");
        planner.plan();
        assertEquals((round + 1) * 2, reads.live.get(), "Repeated plan access must not rescan");
      }
    }
  }

  @Test
  void missingAnalysisScansOnceAndDeadlineUsesStrictBoundary(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable raw = table(catalog, "deadline_boundary", options(false));
      for (int i = 0; i < 3; i++) {
        append(raw, GenericRow.of(i, i, i, (long) i));
      }
      ReadCounts reads = new ReadCounts(raw);
      PaimonTable wrapped = wrap(reads.table);
      long now = raw.latestSnapshot().get().timeMillis() + 10_000L;
      OptimizingConfig config = config().setMinorLeastInterval(1000);
      PaimonHealthEvaluationContext context =
          PaimonHealthEvaluationContext.captureAt(
              reads.table, wrapped.id().toString(), null, null, now);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(reads.table, context);
      PaimonPrimaryKeyOptimizingEvaluation before =
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
              reads.table, raw.name(), context, analysis, config, now, 0, null, now + 1000);
      assertFalse(before.necessary());
      assertEquals(now + 1001, before.nextEvaluationTime());
      assertTrue(
          PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                  reads.table, raw.name(), context, analysis, config, now, 0, null, now + 1001)
              .necessary());
      assertEquals(1, reads.live.get());
      PaimonPrimaryKeyOptimizingPlanner planner =
          new PaimonPrimaryKeyOptimizingPlanner(
              wrapped, 1, 100, 1, Long.MAX_VALUE, config(), 0, 0, 0, null, null, null);
      assertTrue(planner.isNecessary());
      planner.plan();
      planner.plan();
      assertEquals(
          2, reads.live.get(), "Missing analysis must cause exactly one fallback enumeration");
    }
  }

  private static FileStoreTable fixture(
      Catalog catalog, String name, int partitions, int commits, int buckets, int stopTrigger)
      throws Exception {
    catalog.createDatabase("db", true);
    Identifier id = Identifier.create("db", name);
    if (catalog.listTables("db").contains(name)) {
      return (FileStoreTable) catalog.getTable(id);
    }
    Schema.Builder schema =
        Schema.newBuilder()
            .column("part", DataTypes.STRING())
            .column("id", DataTypes.INT())
            .column("value", DataTypes.STRING())
            .partitionKeys("part")
            .primaryKey("part", "id");
    options(false).forEach(schema::option);
    schema
        .option("bucket", String.valueOf(buckets))
        .option("num-sorted-run.stop-trigger", String.valueOf(stopTrigger))
        .option(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "1 ms");
    catalog.createTable(id, schema.build(), false);
    FileStoreTable table = (FileStoreTable) catalog.getTable(id);
    for (int commit = 0; commit < commits; commit++) {
      GenericRow[] rows = new GenericRow[partitions];
      for (int part = 0; part < partitions; part++) {
        String prefix =
            name.equals("wide_partition")
                ? String.join("", java.util.Collections.nCopies(160, "x"))
                : "p";
        rows[part] =
            GenericRow.of(
                BinaryString.fromString(prefix + part),
                commit,
                BinaryString.fromString("value-" + commit));
      }
      append(table, rows);
    }
    return table;
  }

  private static final class ReadCounts {
    private final FileStoreTable table;
    private final AtomicInteger live = new AtomicInteger();
    private final AtomicInteger watermarks = new AtomicInteger();

    private ReadCounts(FileStoreTable raw) {
      table = spy(raw);
      doAnswer(
              invocation -> {
                SnapshotReader reader = spy(raw.newSnapshotReader());
                doAnswer(
                        read -> {
                          live.incrementAndGet();
                          return read.callRealMethod();
                        })
                    .when(reader)
                    .readFileIterator();
                doAnswer(
                        read -> {
                          watermarks.incrementAndGet();
                          return read.callRealMethod();
                        })
                    .when(reader)
                    .partitionEntries();
                return reader;
              })
          .when(table)
          .newSnapshotReader();
    }
  }

  private static void countNativeObjects(Object value, java.util.Set<Object> visited, int[] counts)
      throws Exception {
    if (value == null || !visited.add(value)) {
      return;
    }
    if (value instanceof org.apache.paimon.io.DataFileMeta) {
      counts[0]++;
      return;
    }
    if (value instanceof org.apache.paimon.mergetree.Levels) {
      counts[1]++;
      return;
    }
    if (value instanceof Iterable) {
      for (Object item : (Iterable<?>) value) {
        countNativeObjects(item, visited, counts);
      }
    } else if (!value.getClass().isEnum()
        && value.getClass().getName().startsWith("org.apache.amoro.")) {
      for (Class<?> type = value.getClass(); type != Object.class; type = type.getSuperclass()) {
        for (Field field : type.getDeclaredFields()) {
          if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
            field.setAccessible(true);
            countNativeObjects(field.get(value), visited, counts);
          }
        }
      }
    }
  }

  /** Calling-thread allocation and JVM-wide heap are observations, not retained-heap proofs. */
  private static final class Measurement {
    private final com.sun.management.ThreadMXBean threads =
        (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
    private final long thread = Thread.currentThread().getId();
    private final long allocated;
    private final long heap;
    private final long start;

    private Measurement() {
      java.lang.management.ManagementFactory.getMemoryPoolMXBeans().stream()
          .filter(pool -> pool.getType() == java.lang.management.MemoryType.HEAP)
          .forEach(java.lang.management.MemoryPoolMXBean::resetPeakUsage);
      threads.setThreadAllocatedMemoryEnabled(true);
      allocated = threads.getThreadAllocatedBytes(thread);
      heap =
          java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
      start = System.nanoTime();
    }

    private ObjectNode finish() {
      long elapsed = System.nanoTime() - start;
      long bytes = threads.getThreadAllocatedBytes(thread) - allocated;
      ObjectNode result = new ObjectMapper().createObjectNode();
      result.put("elapsedNanos", elapsed);
      result.put("callingThreadAllocatedBytes", bytes);
      result.put("jvmHeapUsedBefore", heap);
      result.put(
          "jvmHeapUsedAfter",
          java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
      ObjectNode peaks = result.putObject("heapPoolPeakUsedBytes");
      java.lang.management.ManagementFactory.getMemoryPoolMXBeans().stream()
          .filter(pool -> pool.getType() == java.lang.management.MemoryType.HEAP)
          .forEach(pool -> peaks.put(pool.getName(), pool.getPeakUsage().getUsed()));
      return result;
    }
  }
}
