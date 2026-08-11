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

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendHealthEvaluator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendHealthEvaluator.UnitAccumulator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendHealthEvaluator.UnitStatistics;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.PlannerFile;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.ScanTotals;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.ScanMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class PaimonAppendFileScanner {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonAppendFileScanner.class);

  private final AppendOnlyFileStoreTable table;
  private final PaimonHealthEvaluationContext healthContext;
  private final PaimonPlanContext context;
  @Nullable private final Predicate partitionFilter;
  private final IndexFileHandler indexFileHandler;

  PaimonAppendFileScanner(
      AppendOnlyFileStoreTable table,
      PaimonPlanContext context,
      @Nullable Predicate partitionFilter) {
    this(
        table,
        PaimonHealthEvaluationContext.capture(table, table.fullName(), null),
        context,
        partitionFilter);
  }

  PaimonAppendFileScanner(
      AppendOnlyFileStoreTable table,
      PaimonHealthEvaluationContext healthContext,
      PaimonPlanContext context,
      @Nullable Predicate partitionFilter) {
    this(table, healthContext, context, partitionFilter, table.store().newIndexFileHandler());
  }

  PaimonAppendFileScanner(
      AppendOnlyFileStoreTable table,
      PaimonHealthEvaluationContext healthContext,
      PaimonPlanContext context,
      @Nullable Predicate partitionFilter,
      IndexFileHandler indexFileHandler) {
    this.table = table;
    this.healthContext = healthContext;
    this.context = context;
    this.partitionFilter = partitionFilter;
    this.indexFileHandler = indexFileHandler;
  }

  public static PaimonAppendSnapshotAnalysis analyze(
      AppendOnlyFileStoreTable table,
      PaimonHealthEvaluationContext healthContext,
      @Nullable Predicate partitionFilter) {
    PaimonPlanContext planContext =
        PaimonPlanContext.forOptions(
            CoreOptions.fromMap(table.options()),
            new OptimizingConfig(),
            0L,
            0L,
            0L,
            1.0D,
            Long.MAX_VALUE,
            System.currentTimeMillis());
    return new PaimonAppendFileScanner(table, healthContext, planContext, partitionFilter)
        .scan(false)
        .analysis();
  }

  ScanResult scan() {
    return scan(true);
  }

  private ScanResult scan(boolean applyFullRewrite) {
    if (healthContext.configurationError().isPresent()
        || !healthContext.coreOptions().isPresent()) {
      PaimonAppendSnapshotAnalysis analysis =
          PaimonAppendSnapshotAnalysis.invalid(
              healthContext, PaimonAppendHealthEvaluator.INVALID_SCORING_CONFIG);
      return new ScanResult(healthContext.snapshotId(), Collections.emptyMap(), analysis);
    }

    Optional<Snapshot> optionalSnapshot = healthContext.snapshot();
    if (!optionalSnapshot.isPresent()) {
      PaimonAppendHealthEvaluator evaluator = evaluator();
      PaimonAppendHealthEvaluator.Result health =
          evaluator.evaluate(Collections.emptyList(), healthContext.activityInput(100));
      PaimonAppendSnapshotAnalysis analysis =
          PaimonAppendSnapshotAnalysis.create(
              healthContext,
              health,
              health,
              new ScanTotals(),
              Collections.emptyList(),
              table.bucketMode() == BucketMode.BUCKET_UNAWARE,
              null,
              0);
      return new ScanResult(healthContext.snapshotId(), Collections.emptyMap(), analysis);
    }

    Snapshot snapshot = optionalSnapshot.get();
    try {
      DeletionMetadataLookup deletionMetadataLookup = deletionMetadataLookup(snapshot);
      ScanResult result = scanCompleteSnapshot(snapshot, deletionMetadataLookup);
      if (applyFullRewrite
          && context.reachFullInterval()
          && context.fullRewriteAllFiles()
          && !result.files().isEmpty()) {
        return new ScanResult(
            result.snapshotId(),
            scanAllFilesInPartitions(snapshot, result.files(), deletionMetadataLookup),
            result.analysis());
      }
      return result;
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to scan Paimon APPEND table [{}] at snapshot [{}] for health evaluation.",
          healthContext.key().getTableId(),
          snapshot.id(),
          e);
      PaimonAppendSnapshotAnalysis analysis =
          PaimonAppendSnapshotAnalysis.invalid(
              healthContext, PaimonAppendHealthEvaluator.SNAPSHOT_SCAN_FAILED);
      return new ScanResult(snapshot.id(), Collections.emptyMap(), analysis);
    }
  }

  private DeletionMetadataLookup deletionMetadataLookup(Snapshot snapshot) {
    return healthContext.deletionVectorsEnabled()
        ? DeletionMetadataLookup.load(indexFileHandler, snapshot, table.bucketMode())
        : DeletionMetadataLookup.empty();
  }

  private ScanResult scanCompleteSnapshot(
      Snapshot snapshot, DeletionMetadataLookup deletionMetadataLookup) {
    PaimonAppendHealthEvaluator evaluator = evaluator();
    Map<UnitKey, UnitAccumulator> units = new LinkedHashMap<>();
    Map<UnitKey, UnitAccumulator> structuralUnits = new LinkedHashMap<>();
    Set<BinaryRow> partitions = new HashSet<>();
    Map<UnitKey, BucketedDvMaintainer> bucketedMaintainers = new HashMap<>();
    Set<UnitKey> failedDvUnits = new HashSet<>();
    Map<BinaryRow, List<PaimonFileCandidate>> candidates = new LinkedHashMap<>();
    List<PlannerFile> plannerFiles = new ArrayList<>();
    ScanTotals totals = new ScanTotals();
    PartitionPredicate plannerPartitionPredicate = plannerPartitionPredicate();
    int candidateCount = 0;

    Iterator<ManifestEntry> iterator = addFileIterator(snapshot, null, false);
    while (iterator.hasNext()) {
      ManifestEntry entry = iterator.next();
      if (entry.kind() != FileKind.ADD) {
        continue;
      }
      BinaryRow partition = entry.partition().copy();
      partitions.add(partition);
      UnitKey unitKey = new UnitKey(partition, entry.bucket());
      DeleteLookup delete =
          deletionMetadata(
              entry, unitKey, deletionMetadataLookup, bucketedMaintainers, failedDvUnits);
      UnitAccumulator accumulator =
          units.computeIfAbsent(unitKey, ignored -> evaluator.newUnitAccumulator());
      accumulator.addFile(entry.file().fileSize(), entry.file().rowCount(), delete.healthCount);
      structuralUnits
          .computeIfAbsent(unitKey, ignored -> evaluator.newUnitAccumulator())
          .addFile(entry.file().fileSize(), entry.file().rowCount(), 0L);
      totals.addFile(
          entry.file(),
          healthContext.smallFileBoundary(),
          healthContext.targetFileSize(),
          delete.healthCount);

      if (table.bucketMode() != BucketMode.BUCKET_UNAWARE
          || candidateCount >= context.fileNumLimit()
          || (plannerPartitionPredicate != null && !plannerPartitionPredicate.test(partition))) {
        continue;
      }
      PaimonFileCandidate candidate =
          PaimonFileCandidate.from(
              partition, entry.file(), context, delete.deletionFile, delete.dvGroupKey);
      if (!candidate.isProblemFile()) {
        continue;
      }
      candidateCount++;
      candidates.computeIfAbsent(partition, ignored -> new ArrayList<>()).add(candidate);
      plannerFiles.add(
          new PlannerFile(partition, entry.file(), delete.deletionFile, delete.dvGroupKey));
    }

    totals.setPartitionCount(partitions.size());
    totals.setUnitCount(units.size());
    List<UnitStatistics> statistics = new ArrayList<>(units.size());
    List<UnitStatistics> structuralStatistics = new ArrayList<>(structuralUnits.size());
    for (UnitAccumulator accumulator : units.values()) {
      statistics.add(accumulator.snapshot());
    }
    for (UnitAccumulator accumulator : structuralUnits.values()) {
      structuralStatistics.add(accumulator.snapshot());
    }
    PaimonAppendHealthEvaluator.Result health =
        evaluator.evaluate(statistics, healthContext.activityInput(0));
    PaimonAppendHealthEvaluator.Result structuralHealth =
        evaluator.evaluate(structuralStatistics, healthContext.activityInput(0));
    boolean plannerFactsAvailable =
        table.bucketMode() == BucketMode.BUCKET_UNAWARE && failedDvUnits.isEmpty();
    if (!plannerFactsAvailable) {
      candidates.clear();
      plannerFiles.clear();
    }
    PaimonAppendSnapshotAnalysis analysis =
        PaimonAppendSnapshotAnalysis.create(
            healthContext,
            health,
            structuralHealth,
            totals,
            plannerFiles,
            plannerFactsAvailable,
            snapshot,
            1);
    return new ScanResult(snapshot.id(), candidates, analysis);
  }

  private PaimonAppendHealthEvaluator evaluator() {
    return new PaimonAppendHealthEvaluator(
        healthContext.targetFileSize(), healthContext.smallFileBoundary());
  }

  @Nullable
  private PartitionPredicate plannerPartitionPredicate() {
    return partitionFilter == null
        ? null
        : PartitionPredicate.fromPredicate(table.schema().logicalPartitionType(), partitionFilter);
  }

  private DeleteLookup deletionMetadata(
      ManifestEntry entry,
      UnitKey unitKey,
      DeletionMetadataLookup deletionMetadataLookup,
      Map<UnitKey, BucketedDvMaintainer> bucketedMaintainers,
      Set<UnitKey> failedDvUnits) {
    if (!healthContext.deletionVectorsEnabled()) {
      return DeleteLookup.withoutDeletionFile(deleteCountWithoutDeletionVectors(entry.file()));
    }
    if (failedDvUnits.contains(unitKey)) {
      return DeleteLookup.incomplete();
    }

    try {
      if (deletionMetadataLookup.incomplete(unitKey.partition)) {
        failedDvUnits.add(unitKey);
        return DeleteLookup.incomplete();
      }
      if (table.bucketMode() == BucketMode.BUCKET_UNAWARE) {
        DeletionFile deletionFile =
            deletionMetadataLookup.unawareDeletionFile(unitKey.partition, entry.file().fileName());
        if (deletionFile == null) {
          return DeleteLookup.withDeletionFile(null, 0L);
        }
        return DeleteLookup.withDeletionFile(deletionFile, deletionFile.cardinality());
      }

      BucketedDvMaintainer maintainer = bucketedMaintainers.get(unitKey);
      if (maintainer == null) {
        maintainer =
            BucketedDvMaintainer.factory(indexFileHandler)
                .create(
                    unitKey.partition,
                    unitKey.bucket,
                    deletionMetadataLookup.bucketedIndexFiles(unitKey.partition, unitKey.bucket));
        bucketedMaintainers.put(unitKey, maintainer);
      }
      Optional<DeletionVector> deletionVector =
          maintainer.deletionVectorOf(entry.file().fileName());
      return DeleteLookup.withoutDeletionFile(
          deletionVector.isPresent() ? deletionVector.get().getCardinality() : 0L);
    } catch (RuntimeException e) {
      failedDvUnits.add(unitKey);
      return DeleteLookup.incomplete();
    }
  }

  /**
   * APPEND files created by Paimon record zero materialized deletes, while older metadata may omit
   * the field. Paimon treats that omission as no delete rows for backward-compatible reads.
   *
   * <p>Sources:
   * https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java#L96-L125
   * and
   * https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/table/source/MergeTreeSplitGenerator.java#L151-L153
   */
  static long deleteCountWithoutDeletionVectors(org.apache.paimon.io.DataFileMeta file) {
    return file.deleteRowCount().orElse(0L);
  }

  Map<BinaryRow, List<PaimonFileCandidate>> candidatesFromAnalysis(
      PaimonAppendSnapshotAnalysis analysis) {
    Map<BinaryRow, List<PaimonFileCandidate>> candidates = new LinkedHashMap<>();
    PartitionPredicate plannerPartitionPredicate = plannerPartitionPredicate();
    int count = 0;
    for (PlannerFile fact : analysis.plannerFiles()) {
      if (count >= context.fileNumLimit()) {
        break;
      }
      BinaryRow partition = fact.partition();
      if (plannerPartitionPredicate != null && !plannerPartitionPredicate.test(partition)) {
        continue;
      }
      PaimonFileCandidate candidate =
          PaimonFileCandidate.from(
              partition, fact.file(), context, fact.deletionFile(), fact.dvGroupKey());
      if (!candidate.isProblemFile()) {
        continue;
      }
      count++;
      candidates.computeIfAbsent(partition, ignored -> new ArrayList<>()).add(candidate);
    }
    if (context.reachFullInterval() && context.fullRewriteAllFiles() && !candidates.isEmpty()) {
      Snapshot snapshot = analysis.snapshot();
      if (snapshot != null) {
        return scanAllFilesInPartitions(snapshot, candidates, deletionMetadataLookup(snapshot));
      }
    }
    return candidates;
  }

  private Map<BinaryRow, List<PaimonFileCandidate>> scanAllFilesInPartitions(
      Snapshot snapshot,
      Map<BinaryRow, List<PaimonFileCandidate>> candidateFiles,
      DeletionMetadataLookup deletionMetadataLookup) {
    Map<UnitKey, BucketedDvMaintainer> bucketedMaintainers = new HashMap<>();
    Set<UnitKey> failedDvUnits = new HashSet<>();
    Map<BinaryRow, List<PaimonFileCandidate>> allFiles = new LinkedHashMap<>();
    Iterator<ManifestEntry> iterator =
        addFileIterator(snapshot, new ArrayList<>(candidateFiles.keySet()), true);
    while (iterator.hasNext()) {
      ManifestEntry entry = iterator.next();
      if (entry.kind() != FileKind.ADD) {
        continue;
      }
      BinaryRow partition = entry.partition().copy();
      UnitKey unitKey = new UnitKey(partition, entry.bucket());
      DeleteLookup delete =
          deletionMetadata(
              entry, unitKey, deletionMetadataLookup, bucketedMaintainers, failedDvUnits);
      if (delete.healthCount == null) {
        return Collections.emptyMap();
      }
      PaimonFileCandidate candidate =
          PaimonFileCandidate.from(
              partition, entry.file(), context, delete.deletionFile, delete.dvGroupKey);
      allFiles.computeIfAbsent(partition, ignored -> new ArrayList<>()).add(candidate);
    }
    return allFiles;
  }

  private Iterator<ManifestEntry> addFileIterator(
      Snapshot snapshot, @Nullable List<BinaryRow> partitions, boolean targetedPlannerScan) {
    FileStoreScan scan = table.store().newScan().withSnapshot(snapshot).withKind(ScanMode.ALL);
    if (partitions != null) {
      scan.withPartitionFilter(partitions);
    } else if (targetedPlannerScan && partitionFilter != null) {
      scan.withPartitionFilter(plannerPartitionPredicate());
    }
    if (context.coreOptions().manifestDeleteFileDropStats()) {
      scan.dropStats();
    }
    return scan.readFileIterator();
  }

  static final class DeletionMetadataLookup {
    private final Map<BinaryRow, Map<String, DeletionFile>> unawareDeletionFiles;
    private final Map<UnitKey, List<IndexFileMeta>> bucketedIndexFiles;
    private final Set<BinaryRow> incompletePartitions;
    private final boolean manifestScanFailed;

    private DeletionMetadataLookup(
        Map<BinaryRow, Map<String, DeletionFile>> unawareDeletionFiles,
        Map<UnitKey, List<IndexFileMeta>> bucketedIndexFiles,
        Set<BinaryRow> incompletePartitions,
        boolean manifestScanFailed) {
      this.unawareDeletionFiles = unawareDeletionFiles;
      this.bucketedIndexFiles = bucketedIndexFiles;
      this.incompletePartitions = incompletePartitions;
      this.manifestScanFailed = manifestScanFailed;
    }

    private static DeletionMetadataLookup empty() {
      return new DeletionMetadataLookup(
          Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), false);
    }

    static DeletionMetadataLookup load(
        IndexFileHandler indexFileHandler, Snapshot snapshot, BucketMode bucketMode) {
      List<IndexManifestEntry> entries;
      try {
        entries = indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX);
      } catch (RuntimeException e) {
        LOG.warn(
            "Failed to load Paimon deletion-vector manifest at snapshot [{}] for bucket mode [{}].",
            snapshot.id(),
            bucketMode,
            e);
        return new DeletionMetadataLookup(
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet(), true);
      }

      Map<BinaryRow, Map<String, DeletionFile>> unawareDeletionFiles = new HashMap<>();
      Map<UnitKey, List<IndexFileMeta>> bucketedIndexFiles = new HashMap<>();
      Set<BinaryRow> incompletePartitions = new HashSet<>();
      for (IndexManifestEntry entry : entries) {
        BinaryRow partition = entry.partition().copy();
        if (bucketMode != BucketMode.BUCKET_UNAWARE) {
          bucketedIndexFiles
              .computeIfAbsent(new UnitKey(partition, entry.bucket()), ignored -> new ArrayList<>())
              .add(entry.indexFile());
          continue;
        }

        LinkedHashMap<String, DeletionVectorMeta> ranges = entry.indexFile().dvRanges();
        if (ranges == null) {
          incompletePartitions.add(partition);
          continue;
        }
        try {
          String deletionFilePath = null;
          Map<String, DeletionFile> partitionFiles =
              unawareDeletionFiles.computeIfAbsent(partition, ignored -> new HashMap<>());
          for (DeletionVectorMeta range : ranges.values()) {
            if (invalid(range)) {
              incompletePartitions.add(partition);
              continue;
            }
            if (deletionFilePath == null) {
              deletionFilePath = indexFileHandler.filePath(entry).toString();
            }
            partitionFiles.put(
                range.dataFileName(),
                new DeletionFile(
                    deletionFilePath, range.offset(), range.length(), range.cardinality()));
          }
        } catch (RuntimeException e) {
          incompletePartitions.add(partition);
        }
      }
      return new DeletionMetadataLookup(
          unawareDeletionFiles, bucketedIndexFiles, incompletePartitions, false);
    }

    private static boolean invalid(DeletionVectorMeta range) {
      return range == null
          || range.dataFileName() == null
          || range.dataFileName().isEmpty()
          || range.offset() < 0
          || range.length() <= 0
          || range.cardinality() == null
          || range.cardinality() < 0;
    }

    boolean incomplete(BinaryRow partition) {
      return manifestScanFailed || incompletePartitions.contains(partition);
    }

    @Nullable
    DeletionFile unawareDeletionFile(BinaryRow partition, String dataFileName) {
      Map<String, DeletionFile> partitionFiles = unawareDeletionFiles.get(partition);
      return partitionFiles == null ? null : partitionFiles.get(dataFileName);
    }

    List<IndexFileMeta> bucketedIndexFiles(BinaryRow partition, int bucket) {
      return bucketedIndexFiles.getOrDefault(
          new UnitKey(partition, bucket), Collections.emptyList());
    }
  }

  static final class ScanResult {
    private final long snapshotId;
    private final Map<BinaryRow, List<PaimonFileCandidate>> files;
    private final PaimonAppendSnapshotAnalysis analysis;

    private ScanResult(
        long snapshotId,
        Map<BinaryRow, List<PaimonFileCandidate>> files,
        PaimonAppendSnapshotAnalysis analysis) {
      this.snapshotId = snapshotId;
      this.files = files;
      this.analysis = analysis;
    }

    long snapshotId() {
      return snapshotId;
    }

    Map<BinaryRow, List<PaimonFileCandidate>> files() {
      return files;
    }

    PaimonAppendSnapshotAnalysis analysis() {
      return analysis;
    }
  }

  private static final class UnitKey {
    private final BinaryRow partition;
    private final int bucket;

    private UnitKey(BinaryRow partition, int bucket) {
      this.partition = partition.copy();
      this.bucket = bucket;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof UnitKey)) {
        return false;
      }
      UnitKey that = (UnitKey) other;
      return bucket == that.bucket && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
      return 31 * partition.hashCode() + bucket;
    }
  }

  private static final class DeleteLookup {
    @Nullable private final DeletionFile deletionFile;
    @Nullable private final String dvGroupKey;
    @Nullable private final Long healthCount;

    private DeleteLookup(
        @Nullable DeletionFile deletionFile,
        @Nullable String dvGroupKey,
        @Nullable Long healthCount) {
      this.deletionFile = deletionFile;
      this.dvGroupKey = dvGroupKey;
      this.healthCount = healthCount;
    }

    private static DeleteLookup withDeletionFile(
        @Nullable DeletionFile deletionFile, @Nullable Long healthCount) {
      return new DeleteLookup(
          deletionFile, deletionFile == null ? null : deletionFile.path(), healthCount);
    }

    private static DeleteLookup withoutDeletionFile(@Nullable Long healthCount) {
      return new DeleteLookup(null, null, healthCount);
    }

    private static DeleteLookup incomplete() {
      return new DeleteLookup(null, null, null);
    }
  }
}
