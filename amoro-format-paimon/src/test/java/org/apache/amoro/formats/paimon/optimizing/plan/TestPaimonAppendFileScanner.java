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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendHealthEvaluator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.ScanTotals;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@DisplayName("PaimonAppendFileScanner")
class TestPaimonAppendFileScanner {

  @Test
  void legacyAppendDeleteCountIsZeroWithoutDeletionVectors() {
    DataFileMeta file = mock(DataFileMeta.class);
    when(file.deleteRowCount()).thenReturn(Optional.empty());

    assertEquals(0L, PaimonAppendFileScanner.deleteCountWithoutDeletionVectors(file));
  }

  @Test
  void explicitAppendDeleteCountIsPreservedWithoutDeletionVectors() {
    DataFileMeta file = mock(DataFileMeta.class);
    when(file.deleteRowCount()).thenReturn(Optional.of(7L));

    assertEquals(7L, PaimonAppendFileScanner.deleteCountWithoutDeletionVectors(file));
  }

  @Test
  void loadsUnawareDeletionMetadataFromOneManifestScan() {
    org.apache.paimon.Snapshot snapshot = mock(org.apache.paimon.Snapshot.class);
    IndexFileHandler handler = mock(IndexFileHandler.class);
    BinaryRow firstPartition = partition("p1");
    BinaryRow secondPartition = partition("p2");
    IndexManifestEntry firstEntry = unawareIndexEntry(firstPartition, "data-1", 11, 22, 3L, "dv-1");
    IndexManifestEntry secondEntry =
        unawareIndexEntry(secondPartition, "data-2", 33, 44, 5L, "dv-2");
    when(handler.scan(snapshot, "DELETION_VECTORS"))
        .thenReturn(java.util.Arrays.asList(firstEntry, secondEntry));
    when(handler.filePath(firstEntry)).thenReturn(new org.apache.paimon.fs.Path("file:/tmp/dv-1"));
    when(handler.filePath(secondEntry)).thenReturn(new org.apache.paimon.fs.Path("file:/tmp/dv-2"));

    PaimonAppendFileScanner.DeletionMetadataLookup lookup =
        PaimonAppendFileScanner.DeletionMetadataLookup.load(
            handler, snapshot, org.apache.paimon.table.BucketMode.BUCKET_UNAWARE);

    DeletionFile first = lookup.unawareDeletionFile(firstPartition, "data-1");
    DeletionFile second = lookup.unawareDeletionFile(secondPartition, "data-2");
    assertNotNull(first);
    assertNotNull(second);
    assertEquals("file:/tmp/dv-1", first.path());
    assertEquals(11, first.offset());
    assertEquals(22, first.length());
    assertEquals(3L, first.cardinality());
    assertEquals(5L, second.cardinality());
    assertFalse(lookup.incomplete(firstPartition));
    assertFalse(lookup.incomplete(secondPartition));
    verify(handler, times(1)).scan(snapshot, "DELETION_VECTORS");
    verify(handler, never())
        .scan(eq(snapshot), eq("DELETION_VECTORS"), any(BinaryRow.class), anyInt());
  }

  @Test
  void groupsBucketedDeletionMetadataWithoutBucketScans() {
    org.apache.paimon.Snapshot snapshot = mock(org.apache.paimon.Snapshot.class);
    IndexFileHandler handler = mock(IndexFileHandler.class);
    BinaryRow partition = partition("p1");
    IndexFileMeta first = indexFile("dv-0", null);
    IndexFileMeta second = indexFile("dv-1", null);
    IndexManifestEntry firstEntry = new IndexManifestEntry(FileKind.ADD, partition, 0, first);
    IndexManifestEntry secondEntry = new IndexManifestEntry(FileKind.ADD, partition, 1, second);
    when(handler.scan(snapshot, "DELETION_VECTORS"))
        .thenReturn(java.util.Arrays.asList(firstEntry, secondEntry));

    PaimonAppendFileScanner.DeletionMetadataLookup lookup =
        PaimonAppendFileScanner.DeletionMetadataLookup.load(
            handler, snapshot, org.apache.paimon.table.BucketMode.HASH_FIXED);

    assertEquals(Collections.singletonList(first), lookup.bucketedIndexFiles(partition, 0));
    assertEquals(Collections.singletonList(second), lookup.bucketedIndexFiles(partition, 1));
    verify(handler, times(1)).scan(snapshot, "DELETION_VECTORS");
    verify(handler, never())
        .scan(eq(snapshot), eq("DELETION_VECTORS"), any(BinaryRow.class), anyInt());
  }

  @Test
  void malformedUnawareMetadataIsIncompleteInsteadOfZero() {
    org.apache.paimon.Snapshot snapshot = mock(org.apache.paimon.Snapshot.class);
    IndexFileHandler handler = mock(IndexFileHandler.class);
    BinaryRow partition = partition("p1");
    IndexManifestEntry entry = unawareIndexEntry(partition, "data", 1, 2, null, "dv");
    when(handler.scan(snapshot, "DELETION_VECTORS")).thenReturn(Collections.singletonList(entry));
    when(handler.filePath(entry)).thenReturn(new org.apache.paimon.fs.Path("file:/tmp/dv"));

    PaimonAppendFileScanner.DeletionMetadataLookup lookup =
        PaimonAppendFileScanner.DeletionMetadataLookup.load(
            handler, snapshot, org.apache.paimon.table.BucketMode.BUCKET_UNAWARE);

    assertTrue(lookup.incomplete(partition));
    assertEquals(null, lookup.unawareDeletionFile(partition, "data"));
  }

  @Test
  void manifestFailureMarksEveryPartitionIncomplete() {
    org.apache.paimon.Snapshot snapshot = mock(org.apache.paimon.Snapshot.class);
    IndexFileHandler handler = mock(IndexFileHandler.class);
    BinaryRow partition = partition("p1");
    when(handler.scan(snapshot, "DELETION_VECTORS"))
        .thenThrow(new IllegalStateException("manifest unavailable"));

    PaimonAppendFileScanner.DeletionMetadataLookup lookup =
        PaimonAppendFileScanner.DeletionMetadataLookup.load(
            handler, snapshot, org.apache.paimon.table.BucketMode.BUCKET_UNAWARE);

    assertTrue(lookup.incomplete(partition));
    verify(handler, times(1)).scan(snapshot, "DELETION_VECTORS");
  }

  @Test
  void fullRewriteSharesOneManifestLookupAcrossPartitions(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("deletion-vectors.enabled", "true");
    Table table = createAppendOnlyTable(catalog, "t_dv_full", options, true);
    writeRecords(
        table,
        Collections.singletonList(
            GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("dt=1"))));
    writeRecords(
        table,
        Collections.singletonList(
            GenericRow.of(2, BinaryString.fromString("b"), BinaryString.fromString("dt=2"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_dv_full"));
    PaimonHealthEvaluationContext captured =
        PaimonHealthEvaluationContext.capture(appendTable, "db1.t_dv_full", null);
    org.apache.paimon.Snapshot snapshot = captured.snapshot().orElseThrow(AssertionError::new);
    IndexFileHandler handler = mock(IndexFileHandler.class);
    when(handler.scan(snapshot, "DELETION_VECTORS")).thenReturn(Collections.emptyList());
    PaimonPlanContext fullRewriteContext =
        context(
            appendTable,
            100_000,
            new OptimizingConfig()
                .setMaxTaskSize(64L * 1024 * 1024)
                .setFullTriggerInterval(1)
                .setFullRewriteAllFiles(true),
            10_000L);

    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, captured, fullRewriteContext, null, handler)
            .scan();

    assertEquals(2, result.files().size());
    assertEquals(2, countFiles(result.files()));
    verify(handler, times(1)).scan(snapshot, "DELETION_VECTORS");
    verify(handler, never())
        .scan(eq(snapshot), eq("DELETION_VECTORS"), any(BinaryRow.class), anyInt());

    IndexFileHandler handoffHandler = mock(IndexFileHandler.class);
    when(handoffHandler.scan(snapshot, "DELETION_VECTORS")).thenReturn(Collections.emptyList());
    Map<BinaryRow, List<PaimonFileCandidate>> handedOffFiles =
        new PaimonAppendFileScanner(appendTable, captured, fullRewriteContext, null, handoffHandler)
            .candidatesFromAnalysis(result.analysis());
    assertEquals(2, countFiles(handedOffFiles));
    verify(handoffHandler, times(1)).scan(snapshot, "DELETION_VECTORS");
    verify(handoffHandler, never())
        .scan(eq(snapshot), eq("DELETION_VECTORS"), any(BinaryRow.class), anyInt());
  }

  @Test
  void manifestFailureKeepsStructuralHealthAndDisablesPlannerFacts(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("deletion-vectors.enabled", "true");
    Table table = createAppendOnlyTable(catalog, "t_dv_failure", options, false);
    writeRecords(table, Collections.singletonList(GenericRow.of(1, BinaryString.fromString("a"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_dv_failure"));
    PaimonHealthEvaluationContext captured =
        PaimonHealthEvaluationContext.capture(appendTable, "db1.t_dv_failure", null);
    org.apache.paimon.Snapshot snapshot = captured.snapshot().orElseThrow(AssertionError::new);
    IndexFileHandler handler = mock(IndexFileHandler.class);
    when(handler.scan(snapshot, "DELETION_VECTORS"))
        .thenThrow(new IllegalStateException("manifest unavailable"));

    PaimonAppendSnapshotAnalysis analysis =
        new PaimonAppendFileScanner(
                appendTable, captured, context(appendTable, 100_000), null, handler)
            .scan()
            .analysis();

    assertEquals(-1, analysis.pendingInput().getHealthScore());
    assertEquals("1", analysis.healthDetails().getMetrics().get("totalFileCount"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("deleteRecordCount"));
    assertTrue(analysis.healthDetails().getReasonCodes().contains("DELETE_METADATA_INCOMPLETE"));
    assertFalse(analysis.plannerFactsAvailable());
  }

  @Test
  void bucketedLookupRestoresActualBitmapCardinality(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    options.put("bucket-key", "id");
    options.put("deletion-vectors.enabled", "true");
    Table table = createAppendOnlyTable(catalog, "t_bucketed_dv", options, false);
    List<GenericRow> rows = new java.util.ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(GenericRow.of(1, BinaryString.fromString("value-" + i)));
    }
    writeRecords(table, rows);

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_bucketed_dv"));
    PaimonHealthEvaluationContext captured =
        PaimonHealthEvaluationContext.capture(appendTable, "db1.t_bucketed_dv", null);
    org.apache.paimon.Snapshot snapshot = captured.snapshot().orElseThrow(AssertionError::new);
    ManifestEntry dataEntry = firstAddEntry(appendTable, snapshot);
    IndexFileMeta indexFile = indexFile("bucket-dv", null);
    IndexManifestEntry indexEntry =
        new IndexManifestEntry(FileKind.ADD, dataEntry.partition(), dataEntry.bucket(), indexFile);
    IndexFileHandler handler = mock(IndexFileHandler.class);
    when(handler.scan(snapshot, "DELETION_VECTORS"))
        .thenReturn(Collections.singletonList(indexEntry));
    BitmapDeletionVector deletionVector = new BitmapDeletionVector();
    deletionVector.delete(1L);
    deletionVector.delete(3L);
    deletionVector.delete(5L);
    Map<String, DeletionVector> deletionVectors = new HashMap<>();
    deletionVectors.put(dataEntry.file().fileName(), deletionVector);
    when(handler.readAllDeletionVectors(any(BinaryRow.class), eq(dataEntry.bucket()), anyList()))
        .thenReturn(deletionVectors);
    DeletionVectorsIndexFile dvIndex = mock(DeletionVectorsIndexFile.class);
    when(handler.dvIndex(any(BinaryRow.class), eq(dataEntry.bucket()))).thenReturn(dvIndex);

    PaimonAppendSnapshotAnalysis analysis =
        new PaimonAppendFileScanner(
                appendTable, captured, context(appendTable, 100_000), null, handler)
            .scan()
            .analysis();

    assertTrue(captured.deletionVectorsEnabled());
    verify(handler, times(1))
        .readAllDeletionVectors(any(BinaryRow.class), eq(dataEntry.bucket()), anyList());
    verify(handler, times(1)).dvIndex(any(BinaryRow.class), eq(dataEntry.bucket()));
    assertEquals("3", analysis.healthDetails().getMetrics().get("deleteRecordCount"));
    assertTrue(analysis.pendingInput().getHealthScore() >= 0);
    verify(handler, times(1)).scan(snapshot, "DELETION_VECTORS");
    verify(handler, never())
        .scan(eq(snapshot), eq("DELETION_VECTORS"), any(BinaryRow.class), anyInt());
  }

  @Test
  void returnsEmptyWhenTableHasNoSnapshot(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    AppendOnlyFileStoreTable table =
        (AppendOnlyFileStoreTable)
            createAppendOnlyTable(catalog, "t_empty", new HashMap<>(), false);

    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(table, context(table, 100_000), null).scan();

    assertEquals(-1L, result.snapshotId());
    assertTrue(result.files().isEmpty());
  }

  @Test
  void scansAddFilesByPartition(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_scan", new HashMap<>(), true);
    writeRecords(
        table,
        Collections.singletonList(
            GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("dt=1"))));
    writeRecords(
        table,
        Collections.singletonList(
            GenericRow.of(2, BinaryString.fromString("b"), BinaryString.fromString("dt=2"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_scan"));
    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, context(appendTable, 100_000), null).scan();

    assertTrue(result.snapshotId() > 0);
    assertFalse(result.files().isEmpty());
    assertEquals(2, result.files().size());
    assertEquals(2, countFiles(result.files()));
  }

  @Test
  void respectsFileNumLimit(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_limit", new HashMap<>(), false);
    for (int i = 0; i < 5; i++) {
      writeRecords(
          table, Collections.singletonList(GenericRow.of(i, BinaryString.fromString("name-" + i))));
    }

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_limit"));
    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, context(appendTable, 2), null).scan();

    assertEquals(2, countFiles(result.files()));
  }

  @Test
  void fileNumLimitDoesNotTruncateHealthAggregation(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_health_limit", new HashMap<>(), false);
    for (int i = 0; i < 5; i++) {
      writeRecords(
          table, Collections.singletonList(GenericRow.of(i, BinaryString.fromString("row-" + i))));
    }

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_health_limit"));
    PaimonHealthEvaluationContext healthContext =
        PaimonHealthEvaluationContext.capture(appendTable, "db1.t_health_limit", null);
    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, healthContext, context(appendTable, 2), null)
            .scan();

    assertEquals(2, countFiles(result.files()));
    assertEquals(5, result.analysis().pendingInput().getDataFileCount());
    assertEquals(5L, result.analysis().fullSnapshotFileCount());
    assertEquals(1, result.analysis().fullSnapshotScanCount());
    assertEquals(3, result.analysis().healthDetails().getComponents().size());
    assertEquals("5", result.analysis().healthDetails().getMetrics().get("totalFileCount"));
    assertEquals("N/A", result.analysis().healthDetails().getMetrics().get("baselineSnapshotId"));
    assertEquals("N/A", result.analysis().healthDetails().getMetrics().get("timeThresholdMillis"));
    assertEquals("N/A", result.analysis().healthDetails().getMetrics().get("snapshotPressure"));
    assertEquals("N/A", result.analysis().healthDetails().getMetrics().get("timePressure"));
    assertEquals("N/A", result.analysis().healthDetails().getMetrics().get("activityPressure"));
    assertTrue(result.analysis().healthDetails().getComponents().get(0).getScore() >= 0);
  }

  @Test
  void capturedSnapshotObjectExcludesLaterCommits(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_fixed_snapshot", new HashMap<>(), false);
    writeRecords(table, Collections.singletonList(GenericRow.of(1, BinaryString.fromString("a"))));
    writeRecords(table, Collections.singletonList(GenericRow.of(2, BinaryString.fromString("b"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_fixed_snapshot"));
    PaimonHealthEvaluationContext captured =
        PaimonHealthEvaluationContext.capture(appendTable, "db1.t_fixed_snapshot", null);
    writeRecords(table, Collections.singletonList(GenericRow.of(3, BinaryString.fromString("c"))));

    PaimonAppendSnapshotAnalysis analysis =
        new PaimonAppendFileScanner(appendTable, captured, context(appendTable, 100_000), null)
            .scan()
            .analysis();

    assertEquals(captured.snapshotId(), analysis.key().getSnapshotId());
    assertEquals(2, analysis.pendingInput().getDataFileCount());
  }

  @Test
  void availableActivityMetricsExposeEffectivePressures(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_activity_metrics", new HashMap<>(), false);
    writeRecords(table, Collections.singletonList(GenericRow.of(1, BinaryString.fromString("a"))));
    writeRecords(table, Collections.singletonList(GenericRow.of(2, BinaryString.fromString("b"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_activity_metrics"));
    long snapshotId = appendTable.snapshotManager().latestSnapshot().id();
    OptimizingConfig optimizingConfig =
        new OptimizingConfig().setFullTriggerInterval(1_000).setMaxTaskSize(64L * 1024 * 1024);
    OptimizationContext optimizationContext = mock(OptimizationContext.class);
    when(optimizationContext.getOptimizingConfig()).thenReturn(optimizingConfig);
    when(optimizationContext.getLastOptimizedSnapshotId()).thenReturn(snapshotId - 1L);
    PaimonHealthEvaluationContext healthContext =
        PaimonHealthEvaluationContext.capture(
            appendTable, "db1.t_activity_metrics", optimizationContext);

    PaimonAppendSnapshotAnalysis analysis =
        new PaimonAppendFileScanner(
                appendTable,
                healthContext,
                context(appendTable, 100_000, optimizingConfig, System.currentTimeMillis()),
                null)
            .scan()
            .analysis();

    assertEquals("N/A", analysis.healthDetails().getMetrics().get("timeThresholdMillis"));
    assertEquals("0.1", analysis.healthDetails().getMetrics().get("snapshotPressure"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("timePressure"));
    assertEquals("0.1", analysis.healthDetails().getMetrics().get("activityPressure"));
  }

  @Test
  void bucketedAppendTableIsScoredWithoutPlannerCandidates(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    options.put("bucket-key", "id");
    Table table = createAppendOnlyTable(catalog, "t_bucketed_health", options, false);
    writeRecords(table, Collections.singletonList(GenericRow.of(1, BinaryString.fromString("a"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_bucketed_health"));
    PaimonHealthEvaluationContext captured =
        PaimonHealthEvaluationContext.capture(appendTable, "db1.t_bucketed_health", null);
    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, captured, context(appendTable, 100_000), null)
            .scan();

    assertTrue(result.files().isEmpty());
    assertEquals(1, result.analysis().pendingInput().getDataFileCount());
    assertFalse(result.analysis().plannerFactsAvailable());
  }

  @Test
  void healthyAddFilesDoNotConsumeFileNumLimit(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("target-file-size", "1 kb");
    Table table = createAppendOnlyTable(catalog, "t_candidate_limit", options, false);
    writeRecords(table, Collections.singletonList(GenericRow.of(1, largeName())));
    writeRecords(
        table, Collections.singletonList(GenericRow.of(2, BinaryString.fromString("small"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_candidate_limit"));
    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, context(appendTable, 1), null).scan();

    assertEquals(1, countFiles(result.files()));
    assertTrue(
        result.files().values().stream()
            .flatMap(List::stream)
            .allMatch(PaimonFileCandidate::isProblemFile));
  }

  @Test
  void fullRewriteAllFilesScansAllFilesInProblemPartition(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("target-file-size", "1 kb");
    Table table = createAppendOnlyTable(catalog, "t_full", options, false);
    writeRecords(table, Collections.singletonList(GenericRow.of(1, largeName())));
    writeRecords(table, Collections.singletonList(GenericRow.of(2, BinaryString.fromString("b"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_full"));
    PaimonPlanContext context =
        context(
            appendTable,
            1,
            new OptimizingConfig()
                .setMaxTaskSize(64L * 1024 * 1024)
                .setFullTriggerInterval(1)
                .setFullRewriteAllFiles(true),
            10_000L);

    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, context, null).scan();

    assertEquals(2, countFiles(result.files()));
    assertTrue(
        result.files().values().stream()
            .flatMap(List::stream)
            .anyMatch(candidate -> !candidate.isProblemFile()));
  }

  @Test
  void scanModeAllDoesNotReturnFilesDeletedByPreviousCompact(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_compacted_scan", new HashMap<>(), false);
    for (int i = 0; i < 5; i++) {
      writeRecords(
          table, Collections.singletonList(GenericRow.of(i, BinaryString.fromString("name-" + i))));
    }

    Identifier id = Identifier.create("db1", "t_compacted_scan");
    AppendOnlyFileStoreTable appendTable = (AppendOnlyFileStoreTable) catalog.getTable(id);
    PaimonAppendFileScanner.ScanResult beforeCompact =
        new PaimonAppendFileScanner(appendTable, context(appendTable, 100_000), null).scan();
    Set<String> compactedFileNames = fileNames(beforeCompact);
    assertTrue(compactedFileNames.size() > 1);

    compactFirstPartition(appendTable, beforeCompact);

    AppendOnlyFileStoreTable afterCompact = (AppendOnlyFileStoreTable) catalog.getTable(id);
    PaimonAppendFileScanner.ScanResult afterCompactScan =
        new PaimonAppendFileScanner(afterCompact, context(afterCompact, 100_000), null).scan();
    Set<String> scannedFileNames = fileNames(afterCompactScan);

    assertTrue(
        Collections.disjoint(compactedFileNames, scannedFileNames),
        "ScanMode.ALL must merge compact DELETE entries before Amoro filters ADD candidates");
  }

  @Test
  void appliesPartitionFilter(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_filter", new HashMap<>(), true);
    writeRecords(
        table,
        Collections.singletonList(
            GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("dt=1"))));
    writeRecords(
        table,
        Collections.singletonList(
            GenericRow.of(2, BinaryString.fromString("b"), BinaryString.fromString("dt=2"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_filter"));
    PredicateBuilder builder = new PredicateBuilder(appendTable.schema().logicalPartitionType());
    Predicate partitionFilter =
        builder.equal(builder.indexOf("dt"), BinaryString.fromString("dt=2"));

    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, context(appendTable, 1), partitionFilter).scan();

    assertEquals(1, result.files().size());
    assertEquals(1, countFiles(result.files()));
    assertEquals(2, result.analysis().pendingInput().getDataFileCount());
    assertEquals("2", result.analysis().healthDetails().getMetrics().get("totalFileCount"));
    BinaryRow partition = result.files().keySet().iterator().next();
    assertEquals("dt=2", partition.getString(0).toString());
  }

  @Test
  void missingDeleteMetadataKeepsStructuralFactsAndUsesNA(@TempDir Path warehouse)
      throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Table table = createAppendOnlyTable(catalog, "t_missing_delete", new HashMap<>(), false);
    writeRecords(table, Collections.singletonList(GenericRow.of(1, BinaryString.fromString("a"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_missing_delete"));
    PaimonHealthEvaluationContext healthContext =
        PaimonHealthEvaluationContext.capture(appendTable, "db1.t_missing_delete", null);
    PaimonAppendSnapshotAnalysis analysis =
        new PaimonAppendFileScanner(appendTable, healthContext, context(appendTable, 100_000), null)
            .scan()
            .analysis();
    DataFileMeta file = analysis.plannerFiles().get(0).file();
    PaimonAppendHealthEvaluator evaluator =
        new PaimonAppendHealthEvaluator(
            healthContext.targetFileSize(), healthContext.smallFileBoundary());
    PaimonAppendHealthEvaluator.UnitAccumulator incomplete = evaluator.newUnitAccumulator();
    incomplete.addFile(file.fileSize(), file.rowCount(), null);
    PaimonAppendHealthEvaluator.UnitAccumulator structural = evaluator.newUnitAccumulator();
    structural.addFile(file.fileSize(), file.rowCount(), 0L);
    ScanTotals totals = new ScanTotals();
    totals.addFile(file, healthContext.smallFileBoundary(), healthContext.targetFileSize(), null);
    totals.setPartitionCount(1);
    totals.setUnitCount(1);
    PaimonAppendSnapshotAnalysis incompleteAnalysis =
        PaimonAppendSnapshotAnalysis.create(
            healthContext,
            evaluator.evaluate(
                Collections.singletonList(incomplete.snapshot()), healthContext.activityInput(0)),
            evaluator.evaluate(
                Collections.singletonList(structural.snapshot()), healthContext.activityInput(0)),
            totals,
            Collections.emptyList(),
            true,
            healthContext.snapshot().orElse(null),
            1);

    assertEquals(1, incompleteAnalysis.pendingInput().getDataFileCount());
    assertEquals(file.fileSize(), incompleteAnalysis.pendingInput().getDataFileSize());
    assertEquals(-1, incompleteAnalysis.pendingInput().getHealthScore());
    assertEquals("1", incompleteAnalysis.healthDetails().getMetrics().get("totalFileCount"));
    assertEquals("N/A", incompleteAnalysis.healthDetails().getMetrics().get("deleteRecordCount"));
    assertTrue(incompleteAnalysis.healthDetails().getComponents().get(0).getScore() >= 0);
    assertEquals(
        "DELETE_METADATA_INCOMPLETE", incompleteAnalysis.healthDetails().getReasonCodes().get(0));
    PaimonAppendSnapshotAnalysis failedAnalysis =
        PaimonAppendSnapshotAnalysis.invalid(
            healthContext, PaimonAppendHealthEvaluator.SNAPSHOT_SCAN_FAILED);
    assertEquals("N/A", failedAnalysis.healthDetails().getMetrics().get("reducibleFileCount"));
    assertEquals("N/A", failedAnalysis.healthDetails().getMetrics().get("expectedOutputFileCount"));
    assertEquals("N/A", failedAnalysis.healthDetails().getMetrics().get("totalFileCount"));
    assertEquals(-1L, failedAnalysis.fullSnapshotFileCount());
    assertEquals(
        Long.toString(healthContext.targetFileSize()),
        failedAnalysis.healthDetails().getMetrics().get("targetFileSize"));
    assertEquals(
        Long.toString(healthContext.smallFileBoundary()),
        failedAnalysis.healthDetails().getMetrics().get("smallFileBoundary"));
  }

  @Test
  void skipsBucketedAppendTable(@TempDir Path warehouse) throws Exception {
    Catalog catalog = fsCatalog(warehouse);
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    options.put("bucket-key", "id");
    Table table = createAppendOnlyTable(catalog, "t_bucketed", options, false);
    writeRecords(table, Collections.singletonList(GenericRow.of(1, BinaryString.fromString("a"))));

    AppendOnlyFileStoreTable appendTable =
        (AppendOnlyFileStoreTable) catalog.getTable(Identifier.create("db1", "t_bucketed"));

    PaimonAppendFileScanner.ScanResult result =
        new PaimonAppendFileScanner(appendTable, context(appendTable, 100_000), null).scan();

    assertNotNull(result);
    assertTrue(result.files().isEmpty());
  }

  private static Catalog fsCatalog(Path warehouse) {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    return PaimonCatalogFactory.paimonCatalog(props, new Configuration());
  }

  private static Table createAppendOnlyTable(
      Catalog catalog, String tableName, Map<String, String> extraOptions, boolean partitioned)
      throws Exception {
    catalog.createDatabase("db1", true);
    Schema.Builder builder =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .option("bucket", "-1")
            .option("target-file-size", "1 kb")
            .option("compaction.min.file-num", "2");
    if (partitioned) {
      builder.column("dt", DataTypes.STRING()).partitionKeys("dt");
    }
    for (Map.Entry<String, String> e : extraOptions.entrySet()) {
      builder.option(e.getKey(), e.getValue());
    }
    Identifier id = Identifier.create("db1", tableName);
    catalog.createTable(id, builder.build(), true);
    return catalog.getTable(id);
  }

  private static PaimonPlanContext context(AppendOnlyFileStoreTable table, int fileNumLimit) {
    return context(
        table,
        fileNumLimit,
        new OptimizingConfig().setMaxTaskSize(64L * 1024 * 1024),
        System.currentTimeMillis());
  }

  private static PaimonPlanContext context(
      AppendOnlyFileStoreTable table,
      int fileNumLimit,
      OptimizingConfig optimizingConfig,
      long planTime) {
    Map<String, String> options = new HashMap<>(table.options());
    options.put("compaction.file-num-limit", String.valueOf(fileNumLimit));
    return PaimonPlanContext.forOptions(
        CoreOptions.fromMap(options),
        optimizingConfig,
        0L,
        0L,
        0L,
        1.0,
        64L * 1024 * 1024,
        planTime);
  }

  private static void writeRecords(Table table, List<GenericRow> rowsInOneCommit) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite()) {
      for (GenericRow row : rowsInOneCommit) {
        write.write(row);
      }
      List<CommitMessage> messages = write.prepareCommit();
      try (BatchTableCommit commit = builder.newCommit()) {
        commit.commit(messages);
      }
    }
  }

  private static void compactFirstPartition(
      AppendOnlyFileStoreTable table, PaimonAppendFileScanner.ScanResult scanResult)
      throws Exception {
    Map.Entry<BinaryRow, List<PaimonFileCandidate>> partitionFiles =
        scanResult.files().entrySet().iterator().next();
    List<DataFileMeta> compactBefore =
        partitionFiles.getValue().stream()
            .map(PaimonFileCandidate::file)
            .collect(Collectors.toList());
    AppendCompactTask task = new AppendCompactTask(partitionFiles.getKey(), compactBefore);
    String commitUser = "test-compact-scan";
    BaseAppendFileStoreWrite write = table.store().newWrite(commitUser);
    CommitMessage message;
    try {
      message = task.doCompact(table, write);
    } finally {
      write.close();
    }
    try (TableCommitImpl commit = table.newCommit(commitUser)) {
      commit.commit(Collections.singletonList(message));
    }
  }

  private static Set<String> fileNames(PaimonAppendFileScanner.ScanResult scanResult) {
    return scanResult.files().values().stream()
        .flatMap(List::stream)
        .map(PaimonFileCandidate::fileName)
        .collect(Collectors.toSet());
  }

  private static int countFiles(Map<BinaryRow, List<PaimonFileCandidate>> files) {
    return files.values().stream().mapToInt(List::size).sum();
  }

  private static BinaryRow partition(String value) {
    return BinaryRow.singleColumn(value);
  }

  private static IndexManifestEntry unawareIndexEntry(
      BinaryRow partition,
      String dataFileName,
      int offset,
      int length,
      Long cardinality,
      String indexFileName) {
    LinkedHashMap<String, DeletionVectorMeta> ranges = new LinkedHashMap<>();
    ranges.put(dataFileName, new DeletionVectorMeta(dataFileName, offset, length, cardinality));
    return new IndexManifestEntry(FileKind.ADD, partition, -1, indexFile(indexFileName, ranges));
  }

  private static IndexFileMeta indexFile(
      String fileName, LinkedHashMap<String, DeletionVectorMeta> ranges) {
    return new IndexFileMeta("DELETION_VECTORS", fileName, 100L, 1L, ranges, null);
  }

  private static ManifestEntry firstAddEntry(
      AppendOnlyFileStoreTable table, org.apache.paimon.Snapshot snapshot) {
    java.util.Iterator<ManifestEntry> entries =
        table
            .store()
            .newScan()
            .withSnapshot(snapshot)
            .withKind(org.apache.paimon.table.source.ScanMode.ALL)
            .readFileIterator();
    while (entries.hasNext()) {
      ManifestEntry entry = entries.next();
      if (entry.kind() == FileKind.ADD) {
        return entry;
      }
    }
    throw new AssertionError("No ADD data file found");
  }

  private static BinaryString largeName() {
    StringBuilder builder = new StringBuilder(512 * 1024);
    for (int i = 0; i < 512 * 1024; i++) {
      int value = i * 1103515245 + 12345;
      builder.append((char) (33 + Math.floorMod(value >>> 16, 94)));
    }
    return BinaryString.fromString(builder.toString());
  }
}
