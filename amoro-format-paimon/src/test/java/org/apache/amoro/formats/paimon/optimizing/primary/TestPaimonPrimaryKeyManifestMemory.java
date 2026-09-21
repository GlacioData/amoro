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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** 主键 manifest 内存修正的独立行为回归；真实数据扫描，仅水位故障使用注入。 */
class TestPaimonPrimaryKeyManifestMemory {

  @ParameterizedTest
  @ValueSource(ints = {1, -1})
  void fullLoadsWatermarksOnlyWhenDueAndKeepsCapturedSnapshot(int buckets, @TempDir Path path)
      throws Exception {
    try (Catalog catalog = catalog(path)) {
      FileStoreTable raw = table(catalog, buckets, true, 100, 200);
      append(raw, row(1));
      WatermarkReads reads = new WatermarkReads(raw);
      PaimonHealthEvaluationContext context = context(reads.table);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(reads.table, context);
      assertTrue(analysis.validForPlanning());
      assertEquals(0, reads.calls.get(), "健康分析不应预读 FULL 分区水位");
      long firstPlanTime = context.snapshotTimeMillis() + 10_000L;
      assertFalse(
          evaluate(reads.table, context, analysis, 1000, firstPlanTime, firstPlanTime).necessary());
      assertEquals(0, reads.calls.get());

      append(raw, row(2));
      assertTrue(raw.latestSnapshot().get().id() > context.snapshotId());
      PaimonPrimaryKeyOptimizingEvaluation full =
          evaluate(reads.table, context, analysis, 1000, firstPlanTime, firstPlanTime + 1000L);
      assertTrue(full.necessary());
      assertEquals(OptimizingType.FULL, full.optimizingType());
      assertEquals(context.snapshotId(), full.targetSnapshotId());
      assertEquals(context.snapshotId(), reads.snapshotId);
      assertEquals(1, full.units().get(0).getFileCount());
      assertEquals(1, reads.calls.get());
      assertTrue(
          evaluate(reads.table, context, analysis, 1000, 0, firstPlanTime + 2000L).necessary());
      assertEquals(2, reads.calls.get(), "复用事实不应长期持有 FULL 水位缓存");
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void disabledFullOrAbsentIdleTimeDoesNotReadWatermarks(boolean disabled, @TempDir Path path)
      throws Exception {
    try (Catalog catalog = catalog(path)) {
      FileStoreTable raw = table(catalog, 1, disabled, 100, 200);
      append(raw, row(1));
      WatermarkReads reads = new WatermarkReads(raw);
      PaimonHealthEvaluationContext context = context(reads.table);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(reads.table, context);
      assertFalse(
          evaluate(
                  reads.table,
                  context,
                  analysis,
                  disabled ? -1 : 1000,
                  0,
                  context.snapshotTimeMillis() + 10_000L)
              .necessary());
      assertEquals(0, reads.calls.get());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void ordinaryCompactionTakesPriorityWithoutWatermarkRead(boolean major, @TempDir Path path)
      throws Exception {
    try (Catalog catalog = catalog(path)) {
      FileStoreTable raw = table(catalog, 1, true, 2, major ? 2 : 10);
      append(raw, row(1));
      append(raw, row(2));
      append(raw, row(3));
      WatermarkReads reads = new WatermarkReads(raw);
      PaimonHealthEvaluationContext context = context(reads.table);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(reads.table, context);
      PaimonPrimaryKeyOptimizingEvaluation evaluation =
          evaluate(reads.table, context, analysis, 1000, 0, context.snapshotTimeMillis() + 10_000L);
      assertTrue(evaluation.necessary());
      assertEquals(
          major ? OptimizingType.MAJOR : OptimizingType.MINOR, evaluation.optimizingType());
      assertEquals(0, reads.calls.get());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void watermarkFailureOrMissingPartitionKeepsHealthAndCanRetry(boolean failure, @TempDir Path path)
      throws Exception {
    try (Catalog catalog = catalog(path)) {
      FileStoreTable raw = table(catalog, 1, true, 100, 200);
      append(raw, row(1));
      WatermarkReads reads = new WatermarkReads(raw);
      PaimonHealthEvaluationContext context = context(reads.table);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(reads.table, context);
      reads.fail = failure;
      reads.missing = !failure;
      long planTime = context.snapshotTimeMillis() + 10_000L;
      PaimonPrimaryKeyOptimizingEvaluation rejected =
          evaluate(reads.table, context, analysis, 1000, 0, planTime);
      assertFalse(rejected.necessary());
      assertEquals(1, reads.calls.get());
      assertEquals(analysis.healthDetails(), rejected.analysis().get().healthDetails());
      assertEquals(context.snapshotId(), rejected.targetSnapshotId());
      reads.fail = false;
      reads.missing = false;
      assertTrue(evaluate(reads.table, context, analysis, 1000, 0, planTime).necessary());
      assertEquals(2, reads.calls.get());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, -1})
  void analysisDropsOnlyValueStatisticsAndPreservesRealHealth(int buckets, @TempDir Path path)
      throws Exception {
    try (Catalog catalog = catalog(path)) {
      FileStoreTable raw = table(catalog, buckets, true, 100, 200);
      append(raw, row(1), row(2));
      append(
          raw,
          GenericRow.ofKind(
              RowKind.DELETE, BinaryString.fromString("p"), 2, BinaryString.fromString("removed")));
      PaimonHealthEvaluationContext context = context(raw);
      Map<String, DataFileMeta> originals = new HashMap<>();
      for (ManifestEntry entry :
          raw.store().newScan().withSnapshot(context.snapshot().get()).plan().files()) {
        originals.put(entry.file().fileName(), entry.file());
      }
      assertTrue(
          originals.values().stream()
              .anyMatch(file -> !file.valueStats().equals(SimpleStats.EMPTY_STATS)));
      assertTrue(
          originals.values().stream().anyMatch(file -> file.deleteRowCount().orElse(0L) > 0L));
      FileStoreTable fullStatsTable = spy(raw);
      doAnswer(
              invocation -> {
                SnapshotReader reader = spy(raw.newSnapshotReader());
                doReturn(reader).when(reader).dropStats();
                return reader;
              })
          .when(fullStatsTable)
          .newSnapshotReader();
      PaimonPrimaryKeySnapshotAnalysis expected =
          PaimonPrimaryKeySnapshotAnalysis.analyze(fullStatsTable, context);
      PaimonPrimaryKeySnapshotAnalysis actual =
          PaimonPrimaryKeySnapshotAnalysis.analyze(raw, context);
      assertTrue(expected.validForPlanning());
      assertEquals(expected.key(), actual.key());
      // Independent scans have different evaluation times but must retain identical facts.
      assertEquals(
          expected.healthDetails().withStructure(null), actual.healthDetails().withStructure(null));
      assertEquals(
          expected.healthDetails().getStructure().getBands(),
          actual.healthDetails().getStructure().getBands());
      assertEquals(
          expected.healthDetails().getStructure().getProblems(),
          actual.healthDetails().getStructure().getProblems());
      assertEquals(
          expected.healthDetails().getStructure().getTotalUnits(),
          actual.healthDetails().getStructure().getTotalUnits());
      assertEquals(
          originals.size(),
          actual.bucketFacts().stream().mapToInt(fact -> fact.files().size()).sum());
      for (PaimonPrimaryKeySnapshotAnalysis.BucketFacts fact : actual.bucketFacts()) {
        for (DataFileMeta file : fact.files()) {
          assertEquals(originals.get(file.fileName()).copyWithoutStats(), file);
          assertEquals(SimpleStats.EMPTY_STATS, file.valueStats());
          assertTrue(file.valueStatsCols().isEmpty());
        }
      }
    }
  }

  @Test
  void pinnedFullHonorsJsonRemovalAndActualManifestAvailability(@TempDir Path path)
      throws Exception {
    try (Catalog catalog = catalog(path)) {
      FileStoreTable raw = table(catalog, 1, true, 100, 200);
      append(raw, row(1));
      PaimonHealthEvaluationContext context = context(raw);
      Snapshot pinned = context.snapshot().get();
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(raw, context);
      long planTime = context.snapshotTimeMillis() + 10_000L;
      assertTrue(evaluate(raw, context, analysis, 1000, 0, planTime).necessary());
      List<ManifestFileMeta> manifests =
          raw.store().newScan().manifestsReader().read(pinned, ScanMode.ALL).filteredManifests;
      assertFalse(manifests.isEmpty());
      raw.snapshotManager().deleteSnapshot(pinned.id());
      assertFalse(raw.snapshotManager().snapshotExists(pinned.id()));
      assertTrue(
          evaluate(raw, context, analysis, 1000, 0, planTime).necessary(),
          "固定 Snapshot 对象不依赖 JSON 仍存在");

      raw.store().manifestFileFactory().create().delete(manifests.get(0).fileName());
      assertFalse(raw.store().manifestFileFactory().create().exists(manifests.get(0).fileName()));
      assertTrue(raw.store().manifestFileFactory().isCacheEnabled());
      assertTrue(
          evaluate(raw, context, analysis, 1000, 0, planTime).necessary(),
          "沿用已读热的原生 manifest 缓存，不附加存在性拒绝");
      FileStoreTable uncached =
          new PrimaryKeyFileStoreTable(
              raw.fileIO(), raw.location(), raw.schema(), CatalogEnvironment.empty());
      assertFalse(uncached.store().manifestFileFactory().isCacheEnabled());
      assertThrows(
          RuntimeException.class,
          () -> uncached.newSnapshotReader().withSnapshot(pinned).partitionEntries());
      PaimonPrimaryKeyOptimizingEvaluation rejected =
          evaluate(uncached, context, analysis, 1000, 0, planTime);
      assertFalse(rejected.necessary());
      assertEquals(context.snapshotId(), rejected.targetSnapshotId());
      assertEquals(analysis.healthDetails(), rejected.analysis().get().healthDetails());
    }
  }

  @Test
  void nativeDeleteWatermarkPreventsPrematureFull(@TempDir Path path) throws Exception {
    try (Catalog catalog = catalog(path)) {
      FileStoreTable raw = table(catalog, 1, true, 100, 200);
      append(raw, row(1));
      ManifestEntry old = raw.store().newScan().plan().files().get(0);
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (System.currentTimeMillis() <= old.file().creationTimeEpochMillis()
          && System.nanoTime() < deadline) {
        Thread.sleep(1);
      }
      assertTrue(System.currentTimeMillis() > old.file().creationTimeEpochMillis(), "等待毫秒时钟前进超时");
      append(raw, row(2));
      ManifestEntry recent =
          raw.store().newScan().plan().files().stream()
              .filter(entry -> !entry.file().fileName().equals(old.file().fileName()))
              .findFirst()
              .get();
      assertTrue(
          recent.file().creationTimeEpochMillis() > old.file().creationTimeEpochMillis(),
          "两个真实文件必须具有不同创建时间才能构成反例");
      // 原生文件级删除构造真正的 DELETE manifest；不模拟 partitionEntries 返回值。
      try (BatchTableCommit commit = raw.newBatchWriteBuilder().newCommit()) {
        commit.commit(
            Collections.singletonList(
                new CommitMessageImpl(
                    recent.partition(),
                    recent.bucket(),
                    recent.totalBuckets(),
                    DataIncrement.emptyIncrement(),
                    new CompactIncrement(
                        Collections.singletonList(recent.file()),
                        Collections.emptyList(),
                        Collections.emptyList()))));
      }
      PaimonHealthEvaluationContext context = context(raw);
      List<ManifestEntry> live =
          raw.store().newScan().withSnapshot(context.snapshot().get()).plan().files();
      assertEquals(1, live.size());
      assertEquals(old.file().fileName(), live.get(0).file().fileName());
      List<PartitionEntry> watermarks =
          raw.newSnapshotReader().withSnapshot(context.snapshot().get()).partitionEntries();
      assertEquals(1, watermarks.size());
      long watermark = watermarks.get(0).lastFileCreationTime();
      long liveMax = live.get(0).file().creationTimeEpochMillis();
      assertEquals(recent.file().creationTimeEpochMillis(), watermark);
      assertTrue(watermark - liveMax >= 1L);
      PaimonPrimaryKeySnapshotAnalysis analysis =
          PaimonPrimaryKeySnapshotAnalysis.analyze(raw, context);
      assertTrue(analysis.validForPlanning());
      assertEquals(liveMax, analysis.bucketFacts().get(0).unit().getLastFileCreationTime());
      assertFalse(
          evaluate(raw, context, analysis, 1, 0, watermark).necessary(),
          "live max 已满足 1 ms 空闲，但原生 DELETE 水位尚未满足，不能选 FULL");
      assertEquals(
          OptimizingType.FULL,
          evaluate(raw, context, analysis, 1, 0, watermark + 1L).optimizingType());
      assertTrue(evaluate(raw, context, analysis, 1, 0, watermark + 1L).necessary());
    }
  }

  private static PaimonHealthEvaluationContext context(FileStoreTable table) {
    return PaimonHealthEvaluationContext.capture(table, "memory-test", null);
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      FileStoreTable table,
      PaimonHealthEvaluationContext context,
      PaimonPrimaryKeySnapshotAnalysis analysis,
      int fullInterval,
      long lastFull,
      long planTime) {
    OptimizingConfig config =
        new OptimizingConfig()
            .setEnabled(true)
            .setMinorLeastFileCount(1)
            .setMinorLeastInterval(0)
            .setFullTriggerInterval(fullInterval)
            .setMaxTaskSize(Long.MAX_VALUE);
    return PaimonPrimaryKeyOptimizingEvaluator.evaluate(
        table, "memory-test", context, analysis, config, 0L, lastFull, null, planTime);
  }

  private static GenericRow row(int key) {
    return GenericRow.of(
        BinaryString.fromString("p"), key, BinaryString.fromString("value-" + key));
  }

  private static FileStoreTable table(
      Catalog catalog, int buckets, boolean idle, int trigger, int stopTrigger) throws Exception {
    catalog.createDatabase("db", true);
    Schema.Builder schema =
        Schema.newBuilder()
            .column("part", DataTypes.STRING())
            .column("id", DataTypes.INT())
            .column("value", DataTypes.STRING())
            .partitionKeys("part")
            .primaryKey("part", "id")
            .option("bucket", String.valueOf(buckets))
            .option("write-only", "true")
            .option("self-optimizing.enabled", "true")
            .option("num-sorted-run.compaction-trigger", String.valueOf(trigger))
            .option("num-sorted-run.stop-trigger", String.valueOf(stopTrigger));
    if (idle) {
      schema.option(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "1 ms");
    }
    Identifier id = Identifier.create("db", "memory");
    catalog.createTable(id, schema.build(), false);
    return (FileStoreTable) catalog.getTable(id);
  }

  private static final class WatermarkReads {
    private final FileStoreTable table;
    private final AtomicInteger calls = new AtomicInteger();
    private long snapshotId = -1L;
    private boolean fail;
    private boolean missing;

    private WatermarkReads(FileStoreTable raw) {
      table = spy(raw);
      doAnswer(
              invocation -> {
                SnapshotReader reader = spy(raw.newSnapshotReader());
                doAnswer(
                        pin -> {
                          snapshotId = ((Snapshot) pin.getArgument(0)).id();
                          return pin.callRealMethod();
                        })
                    .when(reader)
                    .withSnapshot(any(Snapshot.class));
                doAnswer(
                        read -> {
                          calls.incrementAndGet();
                          if (fail) {
                            throw new IllegalStateException("Injected partition watermark failure");
                          }
                          return missing ? Collections.emptyList() : read.callRealMethod();
                        })
                    .when(reader)
                    .partitionEntries();
                return reader;
              })
          .when(table)
          .newSnapshotReader();
    }
  }
}
