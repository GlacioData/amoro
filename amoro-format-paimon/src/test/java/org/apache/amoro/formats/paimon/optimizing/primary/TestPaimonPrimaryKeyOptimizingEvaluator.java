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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.EarlyFullCompaction;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.OffPeakHours;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@DisplayName("Paimon primary-key optimizing evaluator")
class TestPaimonPrimaryKeyOptimizingEvaluator {

  @Test
  @DisplayName("fixed Snapshot object drives exact DV health and HASH decision")
  void fixedSnapshotObjectDrivesExactDvHealthAndHashDecision(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = strategyOptions();
    options.put("deletion-vectors.enabled", "true");
    List<ManifestEntry> entries = twoRuns(100L, 100L);
    HealthFixture fixture =
        healthFixture(catalog, "t_health_exact_dv", options, entries, false, BucketMode.HASH_FIXED);

    KeyValueFileStore store = spy((KeyValueFileStore) fixture.table.store());
    IndexFileHandler handler = mock(IndexFileHandler.class);
    doReturn(handler).when(store).newIndexFileHandler();
    doReturn(store).when(fixture.table).store();
    DeletionVectorsIndexFile dvIndexFile = mock(DeletionVectorsIndexFile.class);
    when(handler.dvIndex(any(BinaryRow.class), eq(0))).thenReturn(dvIndexFile);
    when(handler.scan(eq(fixture.snapshot), eq("DELETION_VECTORS"), anySet()))
        .thenReturn(Collections.<Pair<BinaryRow, Integer>, List<IndexFileMeta>>emptyMap());
    BitmapDeletionVector deletionVector = new BitmapDeletionVector();
    for (int position = 0; position < 1; position++) {
      deletionVector.delete(position);
    }
    Map<String, DeletionVector> deletionVectors = new HashMap<>();
    deletionVectors.put(entries.get(0).file().fileName(), deletionVector);
    when(handler.readAllDeletionVectors(any(BinaryRow.class), eq(0), anyList()))
        .thenReturn(deletionVectors);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluateWithHealth(fixture, defaultConfig(), 0L, 0L, now());

    assertTrue(evaluation.necessary());
    assertTrue(evaluation.analysis().isPresent());
    PaimonPrimaryKeySnapshotAnalysis analysis = evaluation.analysis().get();
    assertEquals(
        1L,
        analysis.pendingInput().getDeletionVectorRecordCount(),
        analysis.healthDetails().getReasonCodes().toString());
    assertTrue(analysis.pendingInput().getHealthScore() >= 0);
    assertEquals(
        java.util.Arrays.asList(
            "SORTED_RUN", "MATERIALIZED_DELETE", "FILE_SIZE_AUXILIARY", "SNAPSHOT_ACTIVITY"),
        componentCodes(analysis));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("baselineSnapshotId"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("timeThresholdMillis"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("newSnapshotCount"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("snapshotPressure"));
    assertEquals("0", analysis.healthDetails().getMetrics().get("tombstoneRecordCount"));
    assertEquals(
        Integer.toString(fixture.context.compactionTrigger()),
        analysis.healthDetails().getMetrics().get("compactionTrigger"));
    assertEquals(
        Integer.toString(fixture.context.stopTrigger()),
        analysis.healthDetails().getMetrics().get("stopTrigger"));
    assertEquals(
        Integer.toString(fixture.context.numLevels()),
        analysis.healthDetails().getMetrics().get("numLevels"));
    verify(fixture.reader).withSnapshot(fixture.snapshot);
    verify(fixture.reader).readFileIterator();
    verify(handler).scan(eq(fixture.snapshot), eq("DELETION_VECTORS"), anySet());
  }

  @Test
  @DisplayName("invalid scoring config renders effective primary-key thresholds as N/A")
  void invalidScoringConfigRendersEffectiveThresholdsAsUnavailable(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    HealthFixture fixture =
        healthFixture(
            catalog,
            "t_health_invalid_config_metrics",
            strategyOptions(),
            twoRuns(100L, 100L),
            false,
            BucketMode.HASH_FIXED);
    doThrow(new IllegalArgumentException("invalid options")).when(fixture.table).coreOptions();
    PaimonHealthEvaluationContext invalidContext =
        PaimonHealthEvaluationContext.capture(fixture.table, fixture.id.toString(), null);

    PaimonPrimaryKeySnapshotAnalysis analysis =
        PaimonPrimaryKeySnapshotAnalysis.analyze(fixture.table, invalidContext);

    assertEquals("N/A", analysis.healthDetails().getMetrics().get("targetFileSize"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("compactionFileSize"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("compactionTrigger"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("stopTrigger"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("numLevels"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("tombstoneRecordCount"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("deletionVectorRecordCount"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("newSnapshotCount"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("snapshotTimeDistanceMillis"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("snapshotPressure"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("timePressure"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("activityPressure"));
  }

  @Test
  @DisplayName("missing tombstone metadata invalidates health but keeps HASH decision")
  void missingTombstoneInvalidatesHealthButKeepsHashDecision(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    List<ManifestEntry> entries =
        java.util.Arrays.asList(
            entry(BinaryRow.EMPTY_ROW, 0, fileWithDeleteCount("l0", 100L, 10L, 0, 1, null)),
            entry(BinaryRow.EMPTY_ROW, 0, fileWithDeleteCount("l2", 100L, 10L, 2, 2, 0L)));
    HealthFixture fixture =
        healthFixture(
            catalog,
            "t_health_missing_delete",
            strategyOptions(),
            entries,
            false,
            BucketMode.HASH_FIXED);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluateWithHealth(fixture, defaultConfig(), 0L, 0L, now());

    assertTrue(evaluation.necessary());
    assertEquals(1, evaluation.units().size());
    PaimonPrimaryKeySnapshotAnalysis analysis = evaluation.analysis().get();
    assertEquals(-1, analysis.pendingInput().getHealthScore());
    assertEquals(2, analysis.pendingInput().getDataFileCount());
    assertEquals(200L, analysis.pendingInput().getDataFileSize());
    assertEquals(2, analysis.pendingInput().getMaxSortedRunCount());
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("tombstoneRecordCount"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("deletionVectorRecordCount"));
    assertTrue(
        analysis
            .healthDetails()
            .getReasonCodes()
            .contains(PaimonPrimaryKeyHealthEvaluator.DELETE_METADATA_INCOMPLETE));
  }

  @Test
  @DisplayName("DV read failure invalidates health but keeps complete HASH Levels facts")
  void dvReadFailureInvalidatesHealthButKeepsHashDecision(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = strategyOptions();
    options.put("deletion-vectors.enabled", "true");
    HealthFixture fixture =
        healthFixture(
            catalog,
            "t_health_dv_failure",
            options,
            twoRuns(100L, 100L),
            false,
            BucketMode.HASH_FIXED);
    KeyValueFileStore store = spy((KeyValueFileStore) fixture.table.store());
    IndexFileHandler handler = mock(IndexFileHandler.class);
    doReturn(handler).when(store).newIndexFileHandler();
    doReturn(store).when(fixture.table).store();
    when(handler.scan(eq(fixture.snapshot), eq("DELETION_VECTORS"), anySet()))
        .thenThrow(new IllegalStateException("DV index unavailable"));

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluateWithHealth(fixture, defaultConfig(), 0L, 0L, now());

    assertTrue(evaluation.necessary());
    assertEquals(2L, evaluation.units().get(0).getSortedRunCount());
    PaimonPrimaryKeySnapshotAnalysis analysis = evaluation.analysis().get();
    assertEquals(-1, analysis.pendingInput().getHealthScore());
    assertEquals(2, analysis.pendingInput().getDataFileCount());
    assertEquals(200L, analysis.pendingInput().getDataFileSize());
    assertEquals("2", analysis.healthDetails().getMetrics().get("maxSortedRunCount"));
    assertTrue(
        analysis
            .healthDetails()
            .getReasonCodes()
            .contains(PaimonPrimaryKeyHealthEvaluator.SNAPSHOT_SCAN_FAILED));
  }

  @Test
  @DisplayName("KEY_DYNAMIC evaluates health without planner units")
  void keyDynamicEvaluatesHealthWithoutPlannerUnits(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    HealthFixture fixture =
        healthFixture(
            catalog,
            "t_health_key_dynamic",
            strategyOptions(),
            twoRuns(100L, 100L),
            false,
            BucketMode.KEY_DYNAMIC);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluateWithHealth(fixture, defaultConfig(), 0L, 0L, now());

    assertFalse(evaluation.necessary());
    assertTrue(evaluation.units().isEmpty());
    assertTrue(evaluation.analysis().isPresent());
    assertTrue(evaluation.analysis().get().pendingInput().getHealthScore() >= 0);
    assertEquals(
        "N/A", evaluation.analysis().get().healthDetails().getMetrics().get("snapshotPressure"));
    assertEquals(
        "N/A", evaluation.analysis().get().healthDetails().getMetrics().get("timePressure"));
    assertEquals(
        "0.0", evaluation.analysis().get().healthDetails().getMetrics().get("activityPressure"));
    assertTrue(
        evaluation
            .analysis()
            .get()
            .healthDetails()
            .getReasonCodes()
            .contains(PaimonPrimaryKeyHealthEvaluator.KEY_DYNAMIC_OPTIMIZING_UNSUPPORTED));
  }

  @Test
  @DisplayName("reused scanner partition rows are copied before grouping")
  void reusedScannerPartitionRowsAreCopiedBeforeGrouping(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    HealthFixture fixture =
        healthFixture(
            catalog,
            "t_health_reused_partition",
            strategyOptions(),
            Collections.emptyList(),
            false,
            BucketMode.HASH_FIXED);
    BinaryRow reusedPartition = BinaryRow.singleColumn(0);
    ManifestEntry first = entry(reusedPartition, 0, file("first", 100L, 10L, 0, 1, 1L));
    ManifestEntry second = entry(reusedPartition, 0, file("second", 100L, 10L, 0, 2, 2L));
    Iterator<ManifestEntry> iterator =
        new Iterator<ManifestEntry>() {
          private int index;

          @Override
          public boolean hasNext() {
            return index < 2;
          }

          @Override
          public ManifestEntry next() {
            reusedPartition.setInt(0, ++index);
            return index == 1 ? first : second;
          }
        };
    when(fixture.reader.readFileIterator()).thenReturn(iterator);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluateWithHealth(fixture, defaultConfig(), 0L, 0L, now());

    assertTrue(evaluation.analysis().isPresent());
    assertEquals(2, evaluation.analysis().get().activePartitionCount());
    assertEquals(2, evaluation.analysis().get().pendingInput().getEffectiveUnitCount());
  }

  @Test
  @DisplayName("bucket facts use stable partition and bucket order instead of manifest order")
  void bucketFactsHaveStableOrder(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    List<ManifestEntry> reverseBucketOrder = new ArrayList<>();
    reverseBucketOrder.addAll(l0Runs(1, 2));
    reverseBucketOrder.addAll(l0Runs(0, 2));
    HealthFixture fixture =
        healthFixture(
            catalog,
            "t_health_stable_bucket_order",
            primaryKeyOptions(),
            reverseBucketOrder,
            false,
            BucketMode.HASH_FIXED);

    PaimonPrimaryKeySnapshotAnalysis analysis =
        PaimonPrimaryKeySnapshotAnalysis.analyze(fixture.table, fixture.context);
    List<Integer> orderedBuckets = new ArrayList<>();
    analysis.bucketFacts().forEach(facts -> orderedBuckets.add(facts.bucket()));

    assertEquals(java.util.Arrays.asList(0, 1), orderedBuckets);
  }

  @Test
  @DisplayName("health facts are identical for different manifest file orders")
  void healthFactsDoNotDependOnManifestFileOrder(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    List<ManifestEntry> forward =
        java.util.Arrays.asList(
            entry(BinaryRow.EMPTY_ROW, 0, file("later", 101L, 11L, 0, 2, 2L)),
            entry(BinaryRow.EMPTY_ROW, 0, file("earlier", 99L, 9L, 0, 1, 1L)),
            entry(BinaryRow.EMPTY_ROW, 0, file("level-one", 200L, 20L, 1, 3, 3L)));
    List<ManifestEntry> reverse = new ArrayList<>(forward);
    Collections.reverse(reverse);
    HealthFixture forwardFixture =
        healthFixture(
            catalog,
            "t_health_forward_file_order",
            primaryKeyOptions(),
            forward,
            false,
            BucketMode.HASH_FIXED);
    HealthFixture reverseFixture =
        healthFixture(
            catalog,
            "t_health_reverse_file_order",
            primaryKeyOptions(),
            reverse,
            false,
            BucketMode.HASH_FIXED);

    PaimonPrimaryKeySnapshotAnalysis forwardAnalysis =
        PaimonPrimaryKeySnapshotAnalysis.analyze(forwardFixture.table, forwardFixture.context);
    PaimonPrimaryKeySnapshotAnalysis reverseAnalysis =
        PaimonPrimaryKeySnapshotAnalysis.analyze(reverseFixture.table, reverseFixture.context);

    assertEquals(
        forwardAnalysis.pendingInput().getHealthScore(),
        reverseAnalysis.pendingInput().getHealthScore());
    assertEquals(
        forwardAnalysis.healthDetails().getMetrics(), reverseAnalysis.healthDetails().getMetrics());
    assertEquals(
        fileNames(forwardAnalysis.bucketFacts().get(0)),
        fileNames(reverseAnalysis.bucketFacts().get(0)));
  }

  @Test
  @DisplayName("partition watermarks are immutable facts captured during analysis")
  void partitionWatermarksAreCapturedBeforeAnalysisReturns(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    BinaryRow partition = BinaryRow.singleColumn(BinaryString.fromString("p"));
    HealthFixture fixture =
        healthFixture(
            catalog,
            "t_health_watermark",
            options,
            Collections.singletonList(
                entry(partition, 0, file("data", 100L, 10L, 0, 1, 1L, 1_000L))),
            true,
            BucketMode.HASH_FIXED);
    when(fixture.reader.partitionEntries())
        .thenReturn(
            Collections.singletonList(new PartitionEntry(partition, 1L, 100L, 10L, 1_000L, 1)));

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluateWithHealth(
            fixture,
            defaultConfig().setMinorLeastInterval(Integer.MAX_VALUE).setFullTriggerInterval(1),
            10_000L,
            0L,
            10_000L);

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    PaimonPrimaryKeySnapshotAnalysis analysis = evaluation.analysis().get();
    when(fixture.reader.partitionEntries())
        .thenThrow(new IllegalStateException("reader must not be retained"));
    assertEquals(
        1_000L,
        analysis.partitionWatermark(SerializationUtils.serializeBinaryRow(partition)).longValue());
    verify(fixture.reader).partitionEntries();
  }

  @Test
  @DisplayName("enabled table without snapshot is not necessary")
  void noSnapshotIsNotNecessary(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    Identifier id = createPrimaryKeyTable(catalog, "t_no_snapshot", options);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig(), 0, 0, System.currentTimeMillis());

    assertFalse(evaluation.necessary());
    assertEquals(-1L, evaluation.targetSnapshotId());
  }

  @Test
  @DisplayName("deprecated primary-key enabled option no longer gates evaluation")
  void deprecatedPrimaryKeyEnabledOptionNoLongerGatesEvaluation(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_disabled", Collections.emptyMap());
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertFalse(evaluation.necessary());
    assertEquals(-1L, evaluation.targetSnapshotId());
    verify(table).latestSnapshot();
    verify(table, never()).newSnapshotReader();
  }

  @Test
  @DisplayName("fixed snapshot files determine physical files and sorted runs")
  void fixedSnapshotFilesDeterminePhysicalFilesAndSortedRuns(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "2");
    options.put("num-sorted-run.stop-trigger", "3");
    Identifier id = createPrimaryKeyTable(catalog, "t_fixed_snapshot", options);

    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(101L);
    Snapshot newerSnapshot = mock(Snapshot.class);
    when(newerSnapshot.id()).thenReturn(102L);
    doReturn(Optional.of(snapshot), Optional.of(newerSnapshot)).when(table).latestSnapshot();

    BinaryRow partition = BinaryRow.EMPTY_ROW;
    List<ManifestEntry> entries =
        java.util.Arrays.asList(
            entry(partition, 0, file("l0-1", 10L, 2L, 0, 1, 1L, 1_700_000_000_001L)),
            entry(partition, 0, file("l0-2", 20L, 3L, 0, 2, 2L, 1_700_000_000_002L)),
            entry(partition, 0, file("l2-1", 30L, 4L, 2, 3, 3L, 1_700_000_000_003L)),
            entry(partition, 0, file("l2-2", 40L, 5L, 2, 4, 4L, 1_700_000_000_004L)));
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenReturn(reader);
    when(reader.readFileIterator()).thenReturn(entries.iterator());
    SnapshotReader newerReader = mock(SnapshotReader.class);
    when(newerReader.withSnapshot(newerSnapshot)).thenReturn(newerReader);
    when(newerReader.readFileIterator())
        .thenReturn(
            Collections.singletonList(
                    entry(BinaryRow.EMPTY_ROW, 99, file("new-snapshot-only", 999L, 9L, 0, 9, 9L)))
                .iterator());
    doReturn(reader, newerReader).when(table).newSnapshotReader();

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.MINOR, evaluation.optimizingType());
    assertFalse(evaluation.fullCompaction());
    assertEquals(101L, evaluation.targetSnapshotId());
    assertEquals(1, evaluation.units().size());
    PaimonBucketCompactionUnit unit = evaluation.units().get(0);
    assertEquals(4L, unit.getFileCount());
    assertEquals(3L, unit.getSortedRunCount());
    assertEquals(100L, unit.getFileSizeInBytes());
    assertEquals(14L, unit.getRecordCount());
    assertEquals(entries.get(3).file().creationTimeEpochMillis(), unit.getLastFileCreationTime());
    assertEquals(0, unit.getBucket());
    verify(table).latestSnapshot();
    verify(table).newSnapshotReader();
    verify(reader).withSnapshot(snapshot);
    verify(reader).readFileIterator();
    verify(reader, never()).bucketEntries();
    verify(reader, never()).partitionEntries();
    verifyNoInteractions(newerReader);
  }

  @Test
  @DisplayName("fixed snapshot failure does not fall back to latest")
  void fixedSnapshotFailureDoesNotFallBackToLatest(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_snapshot_failure", primaryKeyOptions());
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(202L);
    doReturn(Optional.of(snapshot)).when(table).latestSnapshot();
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenThrow(new IllegalStateException("expired snapshot"));
    doReturn(reader).when(table).newSnapshotReader();

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertFalse(evaluation.necessary());
    assertEquals(202L, evaluation.targetSnapshotId());
    verify(reader).withSnapshot(snapshot);
    verify(reader, never()).readFileIterator();
  }

  @Test
  @DisplayName("partial fixed snapshot scan is discarded after iterator failure")
  void partialFixedSnapshotScanIsDiscardedAfterIteratorFailure(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id = createPrimaryKeyTable(catalog, "t_partial_snapshot_failure", strategyOptions());
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(203L);
    Snapshot newerSnapshot = mock(Snapshot.class);
    when(newerSnapshot.id()).thenReturn(204L);
    doReturn(Optional.of(snapshot), Optional.of(newerSnapshot)).when(table).latestSnapshot();

    List<ManifestEntry> partialCandidate = twoRuns(100L, 100L);
    java.util.Iterator<ManifestEntry> failingIterator =
        new java.util.Iterator<ManifestEntry>() {
          private int index;

          @Override
          public boolean hasNext() {
            if (index < partialCandidate.size()) {
              return true;
            }
            throw new IllegalStateException("manifest scan interrupted");
          }

          @Override
          public ManifestEntry next() {
            return partialCandidate.get(index++);
          }
        };
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenReturn(reader);
    when(reader.readFileIterator()).thenReturn(failingIterator);
    SnapshotReader newerReader = mock(SnapshotReader.class);
    when(newerReader.withSnapshot(newerSnapshot)).thenReturn(newerReader);
    when(newerReader.readFileIterator())
        .thenReturn(
            Collections.singletonList(
                    entry(BinaryRow.EMPTY_ROW, 99, file("new-snapshot-only", 999L, 9L, 0, 9, 9L)))
                .iterator());
    doReturn(reader, newerReader).when(table).newSnapshotReader();

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertFalse(evaluation.necessary());
    assertEquals(203L, evaluation.targetSnapshotId());
    verify(table).latestSnapshot();
    verify(table).newSnapshotReader();
    verify(reader).withSnapshot(snapshot);
    verify(reader).readFileIterator();
    verifyNoInteractions(newerReader);
  }

  @Test
  @DisplayName("latest snapshot failure is fail closed")
  void latestSnapshotFailureIsFailClosed(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id =
        createPrimaryKeyTable(catalog, "t_latest_snapshot_failure", primaryKeyOptions());
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    doThrow(new IllegalStateException("metadata unavailable")).when(table).latestSnapshot();

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertFalse(evaluation.necessary());
    assertEquals(-1L, evaluation.targetSnapshotId());
    verify(table, never()).newSnapshotReader();
  }

  @Test
  @DisplayName("partition normal candidate does not read FULL idle watermark")
  void partitionNormalCandidateDoesNotReadFullIdleWatermark(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Identifier id =
        createPartitionedPrimaryKeyTable(catalog, "t_partition_normal", strategyOptions());
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(251L);
    doReturn(Optional.of(snapshot)).when(table).latestSnapshot();

    BinaryRow partition = BinaryRow.singleColumn(BinaryString.fromString("p"));
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenReturn(reader);
    when(reader.readFileIterator())
        .thenReturn(
            java.util.Arrays.asList(
                    entry(partition, 0, file("l0", 100L, 1L, 0, 1, 1L)),
                    entry(partition, 0, file("l2", 100L, 1L, 2, 2, 2L)))
                .iterator());
    when(reader.partitionEntries()).thenThrow(new IllegalStateException("FULL-only metadata"));
    doReturn(reader).when(table).newSnapshotReader();

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.MINOR, evaluation.optimizingType());
    assertFalse(evaluation.fullCompaction());
    verify(reader, never()).partitionEntries();
  }

  @Test
  @DisplayName("partition FULL idle matches equal partition content across BinaryRow instances")
  void partitionFullIdleMatchesEqualPartitionContent(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "1s");
    Identifier id = createPartitionedPrimaryKeyTable(catalog, "t_partition_idle", options);
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(252L);
    doReturn(Optional.of(snapshot)).when(table).latestSnapshot();

    long planTime = 10_000L;
    BinaryRow livePartition = BinaryRow.singleColumn(BinaryString.fromString("p"));
    BinaryRow watermarkPartition = BinaryRow.singleColumn(BinaryString.fromString("p"));
    assertNotSame(livePartition, watermarkPartition);
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenReturn(reader);
    when(reader.readFileIterator())
        .thenReturn(
            Collections.singletonList(
                    entry(livePartition, 0, file("old-live", 10L, 1L, 0, 1, 1L, 1_000L)))
                .iterator());
    when(reader.partitionEntries())
        .thenReturn(
            Collections.singletonList(
                new PartitionEntry(watermarkPartition, 1L, 10L, 1L, 1_000L, 1)));
    doReturn(reader).when(table).newSnapshotReader();

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table,
            id.getObjectName(),
            defaultConfig().setFullTriggerInterval(1),
            0L,
            0L,
            null,
            planTime);

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    assertTrue(evaluation.fullCompaction());
    assertEquals(252L, evaluation.targetSnapshotId());
    verify(table).newSnapshotReader();
    verify(reader).partitionEntries();
  }

  @Test
  @DisplayName("recent DELETE watermark still delays partition FULL")
  void recentDeleteWatermarkDelaysPartitionFull(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "1s");
    BinaryRow livePartition = BinaryRow.singleColumn(BinaryString.fromString("p"));
    BinaryRow watermarkPartition = BinaryRow.singleColumn(BinaryString.fromString("p"));
    assertNotSame(livePartition, watermarkPartition);
    SnapshotFixture fixture =
        fixedSnapshotFixture(
            catalog,
            "t_partition_recent_delete",
            options,
            Collections.singletonList(
                entry(livePartition, 0, file("old-live", 10L, 1L, 0, 1, 1L, 1_000L))),
            true);
    when(fixture.reader.partitionEntries())
        .thenReturn(
            Collections.singletonList(
                new PartitionEntry(watermarkPartition, 1L, 10L, 1L, 9_500L, 1)));

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            fixture.table,
            fixture.id.getObjectName(),
            defaultConfig().setFullTriggerInterval(1),
            0L,
            0L,
            null,
            10_000L);

    assertFalse(evaluation.necessary());
    verify(fixture.reader).partitionEntries();
  }

  @Test
  @DisplayName("partition FULL selects every bucket only from idle partitions")
  void partitionFullSelectsEveryBucketOnlyFromIdlePartitions(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "1s");
    BinaryRow oldLivePartition = BinaryRow.singleColumn(BinaryString.fromString("old"));
    BinaryRow recentLivePartition = BinaryRow.singleColumn(BinaryString.fromString("recent"));
    BinaryRow oldWatermarkPartition = BinaryRow.singleColumn(BinaryString.fromString("old"));
    BinaryRow recentWatermarkPartition = BinaryRow.singleColumn(BinaryString.fromString("recent"));
    assertNotSame(oldLivePartition, oldWatermarkPartition);
    assertNotSame(recentLivePartition, recentWatermarkPartition);
    List<ManifestEntry> entries =
        java.util.Arrays.asList(
            entry(oldLivePartition, 0, file("old-0", 10L, 1L, 0, 1, 1L, 1_000L)),
            entry(recentLivePartition, 0, file("recent-0", 10L, 1L, 0, 2, 2L, 1_000L)),
            entry(oldLivePartition, 1, file("old-1", 10L, 1L, 0, 3, 3L, 1_000L)),
            entry(recentLivePartition, 1, file("recent-1", 10L, 1L, 0, 4, 4L, 1_000L)));
    SnapshotFixture fixture =
        fixedSnapshotFixture(catalog, "t_partition_mixed_idle", options, entries, true);
    when(fixture.reader.partitionEntries())
        .thenReturn(
            java.util.Arrays.asList(
                new PartitionEntry(oldWatermarkPartition, 2L, 20L, 2L, 1_000L, 2),
                new PartitionEntry(recentWatermarkPartition, 2L, 20L, 2L, 9_500L, 2)));

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            fixture.table,
            fixture.id.getObjectName(),
            defaultConfig().setFullTriggerInterval(1),
            0L,
            0L,
            null,
            10_000L);

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    assertEquals(java.util.Arrays.asList(0, 1), buckets(evaluation.units()));
    byte[] oldPartitionBytes = SerializationUtils.serializeBinaryRow(oldLivePartition);
    for (PaimonBucketCompactionUnit unit : evaluation.units()) {
      assertArrayEquals(oldPartitionBytes, unit.getPartitionBytes());
    }
  }

  @Test
  @DisplayName("unpartitioned FULL selects only idle buckets")
  void unpartitionedFullSelectsOnlyIdleBuckets(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "1s");
    List<ManifestEntry> entries =
        java.util.Arrays.asList(
            entry(BinaryRow.EMPTY_ROW, 0, file("old-0", 10L, 1L, 0, 1, 1L, 1_000L)),
            entry(BinaryRow.EMPTY_ROW, 1, file("recent-1", 10L, 1L, 0, 2, 2L, 9_500L)));
    assertEquals(1_000L, entries.get(0).file().creationTimeEpochMillis());
    assertEquals(9_500L, entries.get(1).file().creationTimeEpochMillis());
    SnapshotFixture fixture =
        fixedSnapshotFixture(catalog, "t_unpartitioned_mixed_idle", options, entries, false);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            fixture.table,
            fixture.id.getObjectName(),
            defaultConfig().setFullTriggerInterval(1),
            0L,
            0L,
            null,
            10_000L);

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    assertEquals(Collections.singletonList(0), buckets(evaluation.units()));
    verify(fixture.reader, never()).partitionEntries();
  }

  @Test
  @DisplayName("partition FULL guards do not read idle watermarks")
  void partitionFullGuardsDoNotReadIdleWatermarks(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    BinaryRow partition = BinaryRow.singleColumn(BinaryString.fromString("p"));
    List<ManifestEntry> entries =
        Collections.singletonList(entry(partition, 0, file("single", 10L, 1L, 0, 1, 1L, 1_000L)));

    Map<String, String> intervalOptions = primaryKeyOptions();
    intervalOptions.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "1s");
    SnapshotFixture intervalNotDue =
        fixedSnapshotFixture(
            catalog, "t_partition_full_interval_guard", intervalOptions, entries, true);
    when(intervalNotDue.reader.partitionEntries())
        .thenThrow(new IllegalStateException("interval guard must run first"));

    PaimonPrimaryKeyOptimizingEvaluation intervalEvaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            intervalNotDue.table,
            intervalNotDue.id.getObjectName(),
            defaultConfig().setFullTriggerInterval(1_000),
            0L,
            9_500L,
            null,
            10_000L);

    assertFalse(intervalEvaluation.necessary());
    verify(intervalNotDue.reader, never()).partitionEntries();

    SnapshotFixture idleOptionMissing =
        fixedSnapshotFixture(
            catalog, "t_partition_full_idle_guard", primaryKeyOptions(), entries, true);
    when(idleOptionMissing.reader.partitionEntries())
        .thenThrow(new IllegalStateException("idle option guard must run first"));

    PaimonPrimaryKeyOptimizingEvaluation idleEvaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            idleOptionMissing.table,
            idleOptionMissing.id.getObjectName(),
            defaultConfig().setFullTriggerInterval(1),
            0L,
            0L,
            null,
            10_000L);

    assertFalse(idleEvaluation.necessary());
    verify(idleOptionMissing.reader, never()).partitionEntries();
  }

  @Test
  @DisplayName("GENTLE lookup can become normal candidate on the second official pick")
  void gentleLookupCanBecomeCandidateOnSecondOfficialPick(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("deletion-vectors.enabled", "true");
    options.put("lookup-compact", "gentle");
    options.put("lookup-compact.max-interval", "2");
    options.put("num-sorted-run.compaction-trigger", "2");
    Identifier id = createPrimaryKeyTable(catalog, "t_gentle_second_pick", options);
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(303L);
    doReturn(Optional.of(snapshot)).when(table).latestSnapshot();

    List<ManifestEntry> entries =
        java.util.Arrays.asList(
            entry(BinaryRow.EMPTY_ROW, 0, file("l0", 1L, 1L, 0, 1, 1L)),
            entry(BinaryRow.EMPTY_ROW, 0, file("l2", 1000L, 1L, 2, 2, 2L)));
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenReturn(reader);
    when(reader.readFileIterator()).thenReturn(entries.iterator());
    doReturn(reader).when(table).newSnapshotReader();

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.MINOR, evaluation.optimizingType());
    assertFalse(evaluation.fullCompaction());
  }

  @Test
  @DisplayName("official strategy presence matrix matches Paimon 1.4.2")
  void officialStrategyPresenceMatrixMatchesPaimon(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    List<ManifestEntry> universalAbsent = twoRuns(100L, 1_000L);
    List<ManifestEntry> sizeAmplification = twoRuns(3_000L, 1_000L);
    List<ManifestEntry> sizeRatio = twoRuns(100L, 100L);

    assertStrategyPresence(
        catalog, "t_universal_absent", strategyOptions(), universalAbsent, false);
    assertStrategyPresence(
        catalog, "t_size_amplification", strategyOptions(), sizeAmplification, true);
    assertStrategyPresence(catalog, "t_size_ratio", strategyOptions(), sizeRatio, true);

    Map<String, String> earlyInterval = strategyOptions();
    earlyInterval.put("compaction.optimization-interval", "1h");
    assertStrategyPresence(catalog, "t_early_interval", earlyInterval, universalAbsent, true);

    Map<String, String> earlyTotal = strategyOptions();
    earlyTotal.put("compaction.total-size-threshold", "2kb");
    assertStrategyPresence(catalog, "t_early_total", earlyTotal, universalAbsent, true);

    Map<String, String> earlyIncremental = strategyOptions();
    earlyIncremental.put("compaction.incremental-size-threshold", "1b");
    assertStrategyPresence(catalog, "t_early_incremental", earlyIncremental, universalAbsent, true);

    Map<String, String> radical = strategyOptions();
    radical.put("deletion-vectors.enabled", "true");
    radical.put("lookup-compact", "radical");
    assertStrategyPresence(catalog, "t_radical", radical, universalAbsent, true);

    Map<String, String> forceLevelZero = strategyOptions();
    forceLevelZero.put("compaction.force-up-level-0", "true");
    assertStrategyPresence(catalog, "t_force_level_zero", forceLevelZero, universalAbsent, true);

    int currentHour = LocalDateTime.now().getHour();
    Map<String, String> offPeak = strategyOptions();
    offPeak.put("compaction.offpeak.start.hour", String.valueOf((currentHour + 23) % 24));
    offPeak.put("compaction.offpeak.end.hour", String.valueOf((currentHour + 2) % 24));
    offPeak.put("compaction.offpeak-ratio", "100");
    assertStrategyPresence(catalog, "t_off_peak", offPeak, twoRuns(100L, 150L), true);
  }

  @Test
  @DisplayName("CoreOptions clamps stop trigger to compaction trigger")
  void coreOptionsClampStopTriggerBeforeMajorDecision(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "3");
    options.put("num-sorted-run.stop-trigger", "2");

    SnapshotFixture equalToEffectiveStop =
        fixedSnapshotFixture(catalog, "t_stop_clamp_equal", options, l0Runs(0, 3), false);
    PaimonPrimaryKeyOptimizingEvaluation minor =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            equalToEffectiveStop.table,
            equalToEffectiveStop.id.getObjectName(),
            defaultConfig(),
            0L,
            0L,
            null,
            now());

    assertEquals(
        3, CoreOptions.fromMap(equalToEffectiveStop.table.options()).numSortedRunStopTrigger());
    assertTrue(minor.necessary());
    assertEquals(OptimizingType.MINOR, minor.optimizingType());

    SnapshotFixture aboveEffectiveStop =
        fixedSnapshotFixture(catalog, "t_stop_clamp_above", options, l0Runs(0, 4), false);
    PaimonPrimaryKeyOptimizingEvaluation major =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            aboveEffectiveStop.table,
            aboveEffectiveStop.id.getObjectName(),
            defaultConfig(),
            0L,
            0L,
            null,
            now());

    assertTrue(major.necessary());
    assertEquals(OptimizingType.MAJOR, major.optimizingType());
    assertFalse(major.fullCompaction());
  }

  @Test
  @DisplayName("invalid MAJOR ratio fails closed during evaluation")
  void invalidMajorRatioFailsClosedDuringEvaluation(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "1.001");
    Identifier id = createPrimaryKeyTable(catalog, "t_invalid_ratio", options);
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertFalse(evaluation.necessary());
    assertEquals(-1L, evaluation.targetSnapshotId());
    verify(table, never()).latestSnapshot();
    verify(table, never()).newSnapshotReader();
  }

  @Test
  @DisplayName("Amoro file count does not replace Paimon official strategy")
  void amoroFileCountDoesNotReplacePaimonOfficialStrategy(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    Identifier id = createPrimaryKeyTable(catalog, "t_minor_amoro_trigger", options);
    writeCommits(catalog.getTable(id), 2);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig().setMinorLeastFileCount(2), 0, 0, now());

    assertFalse(evaluation.necessary());
  }

  @Test
  @DisplayName("explicit Paimon native trigger suppresses lower Amoro trigger")
  void explicitPaimonNativeTriggerSuppressesLowerAmoroTrigger(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "3");
    Identifier id = createPrimaryKeyTable(catalog, "t_minor_native_trigger", options);
    writeCommits(catalog.getTable(id), 2);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig().setMinorLeastFileCount(2), 0, 0, now());

    assertFalse(evaluation.necessary());
  }

  @Test
  @DisplayName("major candidates have higher priority than minor candidates")
  void majorHasHigherPriorityThanMinor(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "2");
    options.put("num-sorted-run.stop-trigger", "2");
    Identifier id = createPrimaryKeyTable(catalog, "t_major", options);
    writeCommits(catalog.getTable(id), 3);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig(), 0, 0, now());

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.MAJOR, evaluation.optimizingType());
    assertFalse(evaluation.fullCompaction());
  }

  @Test
  @DisplayName("minor interval falls through to eligible FULL planning")
  void minorIntervalFallsThroughToEligibleFull(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("write-only", "true");
    options.put("num-sorted-run.compaction-trigger", "2");
    options.put("num-sorted-run.stop-trigger", "5");
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    Identifier id = createPrimaryKeyTable(catalog, "t_minor_falls_through", options);
    writeCommits(catalog.getTable(id), 3);
    long planTime = now();
    OptimizingConfig config =
        defaultConfig().setMinorLeastInterval(Integer.MAX_VALUE).setFullTriggerInterval(1);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, config, planTime, 0L, planTime);

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    assertTrue(evaluation.fullCompaction());
  }

  @Test
  @DisplayName("MAJOR cap uses all active buckets and deterministic priority")
  void majorCapUsesAllActiveBucketsAndDeterministicPriority() {
    PaimonBucketCompactionUnit byRuns = unit(new byte[] {9}, 9, 1, 5, 1);
    PaimonBucketCompactionUnit byFiles = unit(new byte[] {8}, 8, 6, 4, 1);
    PaimonBucketCompactionUnit bySize = unit(new byte[] {7}, 7, 5, 4, 11);
    PaimonBucketCompactionUnit byPartition = unit(new byte[] {0}, 6, 5, 4, 10);
    PaimonBucketCompactionUnit byBucketZero = unit(new byte[] {1}, 0, 5, 4, 10);
    PaimonBucketCompactionUnit byBucketOne = unit(new byte[] {1}, 1, 5, 4, 10);
    List<PaimonBucketCompactionUnit> candidates = new ArrayList<>();
    candidates.add(byBucketOne);
    candidates.add(byPartition);
    candidates.add(bySize);
    candidates.add(byRuns);
    candidates.add(byBucketZero);
    candidates.add(byFiles);

    List<PaimonBucketCompactionUnit> sorted =
        PaimonPrimaryKeyOptimizingEvaluator.selectMajorCandidates(
            candidates, 6, new BigDecimal("1.00"));

    assertEquals(byRuns, sorted.get(0));
    assertEquals(byFiles, sorted.get(1));
    assertEquals(bySize, sorted.get(2));
    assertEquals(byPartition, sorted.get(3));
    assertEquals(byBucketZero, sorted.get(4));
    assertEquals(byBucketOne, sorted.get(5));

    List<PaimonBucketCompactionUnit> capped =
        PaimonPrimaryKeyOptimizingEvaluator.selectMajorCandidates(
            candidates.subList(0, 5), 10, new BigDecimal("0.33"));
    assertEquals(4, capped.size());

    PaimonBucketCompactionUnit unsignedLow = unit(new byte[] {0x7f}, 0, 1, 1, 1);
    PaimonBucketCompactionUnit unsignedHigh = unit(new byte[] {(byte) 0x80}, 0, 1, 1, 1);
    List<PaimonBucketCompactionUnit> unsignedOrder =
        PaimonPrimaryKeyOptimizingEvaluator.selectMajorCandidates(
            java.util.Arrays.asList(unsignedHigh, unsignedLow), 2, BigDecimal.ONE);
    assertEquals(unsignedLow, unsignedOrder.get(0));
    assertEquals(unsignedHigh, unsignedOrder.get(1));
  }

  @Test
  @DisplayName("evaluator wires active bucket denominator ratio and MAJOR-only Top-B")
  void evaluatorWiresActiveBucketDenominatorRatioAndMajorOnlyTopB(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    BinaryRow partition = BinaryRow.singleColumn(BinaryString.fromString("p"));
    List<ManifestEntry> entries = new ArrayList<>();
    long sequence = 1L;
    for (int bucket = 0; bucket < 10; bucket++) {
      int runs = bucket < 5 ? 4 : bucket < 8 ? 2 : 1;
      long fileSize = bucket < 5 ? (bucket + 1L) * 100L : 50L;
      for (int run = 0; run < runs; run++) {
        int key = bucket * 10 + run;
        entries.add(
            entry(
                partition,
                bucket,
                file("bucket-" + bucket + "-run-" + run, fileSize, 1L, 0, key, sequence++)));
      }
    }
    Collections.reverse(entries);

    Map<String, String> defaultRatioOptions = primaryKeyOptions();
    defaultRatioOptions.put("bucket", "10");
    defaultRatioOptions.put("num-sorted-run.compaction-trigger", "2");
    defaultRatioOptions.put("num-sorted-run.stop-trigger", "3");
    SnapshotFixture defaultRatio =
        fixedSnapshotFixture(
            catalog, "t_major_cap_default_ratio", defaultRatioOptions, entries, true);

    PaimonPrimaryKeyOptimizingEvaluation defaultEvaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            defaultRatio.table,
            defaultRatio.id.getObjectName(),
            defaultConfig(),
            0L,
            0L,
            null,
            now());

    assertEquals(OptimizingType.MAJOR, defaultEvaluation.optimizingType());
    assertFalse(defaultEvaluation.fullCompaction());
    assertEquals(java.util.Arrays.asList(4, 3, 2, 1), buckets(defaultEvaluation.units()));
    verify(defaultRatio.reader, never()).partitionEntries();

    Map<String, String> fullRatioOptions = new HashMap<>(defaultRatioOptions);
    fullRatioOptions.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "1.00");
    SnapshotFixture fullRatio =
        fixedSnapshotFixture(catalog, "t_major_cap_full_ratio", fullRatioOptions, entries, true);

    PaimonPrimaryKeyOptimizingEvaluation fullRatioEvaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            fullRatio.table, fullRatio.id.getObjectName(), defaultConfig(), 0L, 0L, null, now());

    assertEquals(OptimizingType.MAJOR, fullRatioEvaluation.optimizingType());
    assertEquals(java.util.Arrays.asList(4, 3, 2, 1, 0), buckets(fullRatioEvaluation.units()));
    verify(fullRatio.reader, never()).partitionEntries();
  }

  @Test
  @DisplayName("PK clustering override is outside primary-key optimizing capability")
  void pkClusteringOverrideIsRejected(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("deletion-vectors.enabled", "true");
    options.put("clustering.columns", "name");
    options.put("pk-clustering-override", "true");
    Identifier id = createPrimaryKeyTable(catalog, "t_clustering_override", options);
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));

    assertFalse(PaimonPrimaryKeyOptimizingEvaluator.supports(table));
    assertFalse(
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                table, id.getObjectName(), defaultConfig(), 0L, 0L, null, now())
            .necessary());
    verify(table, never()).latestSnapshot();
    verify(table, never()).newSnapshotReader();
  }

  @Test
  @DisplayName("full candidates require explicit partition idle time")
  void fullRequiresPartitionIdleTime(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    Identifier id = createPrimaryKeyTable(catalog, "t_full_no_idle", options);
    writeCommits(catalog.getTable(id), 1);

    OptimizingConfig config = defaultConfig().setMinorLeastFileCount(10).setFullTriggerInterval(1);

    PaimonPrimaryKeyOptimizingEvaluation evaluation = evaluate(catalog, id, config, 0, 0, now());

    assertFalse(evaluation.necessary());
    assertTrue(evaluation.targetSnapshotId() > 0);
  }

  @Test
  @DisplayName("full candidates are planned after interval and idle time")
  void fullPlansAfterIntervalAndIdleTime(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put(PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME, "0s");
    Identifier id = createPrimaryKeyTable(catalog, "t_full_idle", options);
    writeCommits(catalog.getTable(id), 1);

    OptimizingConfig config = defaultConfig().setMinorLeastFileCount(10).setFullTriggerInterval(1);

    PaimonPrimaryKeyOptimizingEvaluation evaluation = evaluate(catalog, id, config, 0, 0, now());

    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    assertTrue(evaluation.fullCompaction());
  }

  @Test
  @DisplayName("non-empty self-optimizing filter is rejected")
  void filterIsRejected(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = primaryKeyOptions();
    options.put("num-sorted-run.compaction-trigger", "2");
    Identifier id = createPrimaryKeyTable(catalog, "t_filter", options);
    writeCommits(catalog.getTable(id), 2);

    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        evaluate(catalog, id, defaultConfig().setFilter("id > 1"), 0, 0, now());

    assertFalse(evaluation.necessary());
  }

  private static long now() {
    return System.currentTimeMillis();
  }

  private static List<String> componentCodes(PaimonPrimaryKeySnapshotAnalysis analysis) {
    List<String> codes = new ArrayList<>();
    analysis.healthDetails().getComponents().forEach(component -> codes.add(component.getCode()));
    return codes;
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluateWithHealth(
      HealthFixture fixture,
      OptimizingConfig config,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime,
      long planTime) {
    return PaimonPrimaryKeyOptimizingEvaluator.evaluate(
        fixture.table,
        fixture.id.getObjectName(),
        fixture.context,
        config,
        lastMinorOptimizingTime,
        lastFullOptimizingTime,
        null,
        planTime);
  }

  private static HealthFixture healthFixture(
      Catalog catalog,
      String tableName,
      Map<String, String> options,
      List<ManifestEntry> entries,
      boolean partitioned,
      BucketMode bucketMode)
      throws Exception {
    Identifier id =
        partitioned
            ? createPartitionedPrimaryKeyTable(catalog, tableName, options)
            : createPrimaryKeyTable(catalog, tableName, options);
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    doReturn(bucketMode).when(table).bucketMode();
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(8_001L + Math.abs(tableName.hashCode()));
    when(snapshot.timeMillis()).thenReturn(1_700_000_000_000L);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    when(snapshotManager.latestSnapshot()).thenReturn(snapshot);
    doReturn(snapshotManager).when(table).snapshotManager();
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenReturn(reader);
    when(reader.readFileIterator()).thenReturn(entries.iterator());
    doReturn(reader).when(table).newSnapshotReader();
    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.capture(table, id.toString(), null);
    return new HealthFixture(id, table, snapshot, reader, context);
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      Catalog catalog,
      Identifier id,
      OptimizingConfig config,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime,
      long planTime)
      throws Exception {
    return PaimonPrimaryKeyOptimizingEvaluator.evaluate(
        (FileStoreTable) catalog.getTable(id),
        id.getObjectName(),
        config,
        lastMinorOptimizingTime,
        lastFullOptimizingTime,
        null,
        planTime);
  }

  private static SnapshotFixture fixedSnapshotFixture(
      Catalog catalog,
      String tableName,
      Map<String, String> options,
      List<ManifestEntry> entries,
      boolean partitioned)
      throws Exception {
    Identifier id =
        partitioned
            ? createPartitionedPrimaryKeyTable(catalog, tableName, options)
            : createPrimaryKeyTable(catalog, tableName, options);
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    long snapshotId = 10_000L + Math.abs(tableName.hashCode());
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(snapshotId);
    doReturn(Optional.of(snapshot)).when(table).latestSnapshot();
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenReturn(reader);
    when(reader.readFileIterator()).thenReturn(entries.iterator());
    doReturn(reader).when(table).newSnapshotReader();
    return new SnapshotFixture(id, table, reader);
  }

  private static List<ManifestEntry> l0Runs(int bucket, int count) {
    List<ManifestEntry> entries = new ArrayList<>();
    for (int run = 0; run < count; run++) {
      entries.add(
          entry(
              BinaryRow.EMPTY_ROW,
              bucket,
              file("l0-" + bucket + '-' + run, 100L, 1L, 0, run, run + 1L)));
    }
    return entries;
  }

  private static List<Integer> buckets(List<PaimonBucketCompactionUnit> units) {
    List<Integer> buckets = new ArrayList<>();
    for (PaimonBucketCompactionUnit unit : units) {
      buckets.add(unit.getBucket());
    }
    return buckets;
  }

  private static List<String> fileNames(PaimonPrimaryKeySnapshotAnalysis.BucketFacts facts) {
    List<String> names = new ArrayList<>();
    facts.files().forEach(file -> names.add(file.fileName()));
    return names;
  }

  private static class SnapshotFixture {

    private final Identifier id;
    private final FileStoreTable table;
    private final SnapshotReader reader;

    private SnapshotFixture(Identifier id, FileStoreTable table, SnapshotReader reader) {
      this.id = id;
      this.table = table;
      this.reader = reader;
    }
  }

  private static class HealthFixture {

    private final Identifier id;
    private final FileStoreTable table;
    private final Snapshot snapshot;
    private final SnapshotReader reader;
    private final PaimonHealthEvaluationContext context;

    private HealthFixture(
        Identifier id,
        FileStoreTable table,
        Snapshot snapshot,
        SnapshotReader reader,
        PaimonHealthEvaluationContext context) {
      this.id = id;
      this.table = table;
      this.snapshot = snapshot;
      this.reader = reader;
      this.context = context;
    }
  }

  private static void assertStrategyPresence(
      Catalog catalog,
      String tableName,
      Map<String, String> options,
      List<ManifestEntry> entries,
      boolean expected)
      throws Exception {
    Identifier id = createPrimaryKeyTable(catalog, tableName, options);
    FileStoreTable table = spy((FileStoreTable) catalog.getTable(id));
    long snapshotId = 1_000L + Math.abs(tableName.hashCode());
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(snapshotId);
    doReturn(Optional.of(snapshot)).when(table).latestSnapshot();
    SnapshotReader reader = mock(SnapshotReader.class);
    when(reader.withSnapshot(snapshot)).thenReturn(reader);
    when(reader.readFileIterator()).thenReturn(entries.iterator());
    doReturn(reader).when(table).newSnapshotReader();

    assertEquals(expected, officialPresence(table, entries));
    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            table, tableName, defaultConfig(), 0L, 0L, null, now());
    assertEquals(expected, evaluation.necessary());
    if (expected) {
      assertEquals(OptimizingType.MINOR, evaluation.optimizingType());
      assertFalse(evaluation.fullCompaction());
    }
  }

  private static boolean officialPresence(FileStoreTable table, List<ManifestEntry> entries) {
    CoreOptions options = CoreOptions.fromMap(table.options());
    List<DataFileMeta> files = new ArrayList<>();
    for (ManifestEntry entry : entries) {
      files.add(entry.file());
    }
    KeyValueFileStore store = (KeyValueFileStore) table.store();
    Levels levels = new Levels(store.newKeyComparator(), files, options.numLevels());
    CompactStrategy strategy = officialStrategy(options);
    for (int pick = 0; pick < 2; pick++) {
      if (strategy.pick(levels.numberOfLevels(), levels.levelSortedRuns()).isPresent()) {
        return true;
      }
    }
    return false;
  }

  private static CompactStrategy officialStrategy(CoreOptions options) {
    UniversalCompaction universal =
        new UniversalCompaction(
            options.maxSizeAmplificationPercent(),
            options.sortedRunSizeRatio(),
            options.numSortedRunCompactionTrigger(),
            EarlyFullCompaction.create(options),
            OffPeakHours.create(options));
    if (options.needLookup()) {
      Integer interval =
          options.lookupCompact() == CoreOptions.LookupCompactMode.GENTLE
              ? options.lookupCompactMaxInterval()
              : null;
      return new ForceUpLevel0Compaction(universal, interval);
    }
    return options.compactionForceUpLevel0()
        ? new ForceUpLevel0Compaction(universal, null)
        : universal;
  }

  private static List<ManifestEntry> twoRuns(long levelZeroSize, long highLevelSize) {
    return java.util.Arrays.asList(
        entry(BinaryRow.EMPTY_ROW, 0, file("l0-" + levelZeroSize, levelZeroSize, 1L, 0, 1, 1L)),
        entry(BinaryRow.EMPTY_ROW, 0, file("l2-" + highLevelSize, highLevelSize, 1L, 2, 2, 2L)));
  }

  private static Map<String, String> strategyOptions() {
    Map<String, String> options = primaryKeyOptions();
    options.put("bucket", "1");
    options.put("num-sorted-run.compaction-trigger", "2");
    options.put("num-sorted-run.stop-trigger", "10");
    return options;
  }

  private static Catalog fsCatalog(Path warehouse) {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    return PaimonCatalogFactory.paimonCatalog(props, new Configuration());
  }

  private static Identifier createPrimaryKeyTable(
      Catalog catalog, String tableName, Map<String, String> extraOptions) throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .primaryKey("id")
            .option("bucket", "2");
    extraOptions.forEach(builder::option);
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return id;
  }

  private static Identifier createPartitionedPrimaryKeyTable(
      Catalog catalog, String tableName, Map<String, String> extraOptions) throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("dt", DataTypes.STRING())
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .partitionKeys("dt")
            .primaryKey("dt", "id")
            .option("bucket", "2");
    extraOptions.forEach(builder::option);
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return id;
  }

  private static void writeCommits(Table table, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      writeRecords(table, GenericRow.of(i, BinaryString.fromString("name-" + i)));
    }
  }

  private static void writeRecords(Table table, GenericRow row) throws Exception {
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit()) {
      write.write(row);
      List<CommitMessage> messages = write.prepareCommit();
      commit.commit(messages);
    }
  }

  private static ManifestEntry entry(BinaryRow partition, int bucket, DataFileMeta file) {
    return ManifestEntry.create(FileKind.ADD, partition, bucket, 1, file);
  }

  private static DataFileMeta file(
      String name, long size, long rows, int level, int key, long sequenceNumber) {
    return file(name, size, rows, level, key, sequenceNumber, now());
  }

  private static DataFileMeta file(
      String name,
      long size,
      long rows,
      int level,
      int key,
      long sequenceNumber,
      long creationTime) {
    BinaryRow binaryKey = BinaryRow.singleColumn(key);
    return DataFileMeta.create(
        name,
        size,
        rows,
        binaryKey,
        binaryKey,
        null,
        null,
        sequenceNumber,
        sequenceNumber,
        0L,
        level,
        Collections.emptyList(),
        Timestamp.fromLocalDateTime(
            LocalDateTime.ofInstant(Instant.ofEpochMilli(creationTime), ZoneId.systemDefault())),
        0L,
        null,
        FileSource.APPEND,
        null,
        null,
        null,
        null);
  }

  private static DataFileMeta fileWithDeleteCount(
      String name, long size, long rows, int level, int key, Long deleteRowCount) {
    DataFileMeta file = spy(file(name, size, rows, level, key, key));
    doReturn(Optional.ofNullable(deleteRowCount)).when(file).deleteRowCount();
    return file;
  }

  private static PaimonBucketCompactionUnit unit(
      byte[] partition, int bucket, long fileCount, long sortedRunCount, long fileSize) {
    return new PaimonBucketCompactionUnit(
        partition, bucket, fileCount, sortedRunCount, fileSize, 1L, 1L);
  }

  private static Map<String, String> primaryKeyOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(CoreOptions.WRITE_ONLY.key(), "true");
    return options;
  }

  private static OptimizingConfig defaultConfig() {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastFileCount(2)
        .setMinorLeastInterval(0)
        .setFullTriggerInterval(-1)
        .setFullRewriteAllFiles(false)
        .setMaxTaskSize(64L * 1024 * 1024);
  }
}
