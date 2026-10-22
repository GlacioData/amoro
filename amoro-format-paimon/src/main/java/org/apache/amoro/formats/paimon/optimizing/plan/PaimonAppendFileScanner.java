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
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendCapabilities;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendHealthEvaluator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendHealthEvaluator.UnitAccumulator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendHealthEvaluator.UnitStatistics;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.PlannerFile;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.ScanTotals;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonDeletionVectorCounts;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthStructureCollector;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
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
  // Keep synchronous planning bounded: three selection rounds, each with at most one proof.
  private static final int MAX_FIXED_WINDOW_ROUNDS = 3;

  private final AppendOnlyFileStoreTable table;
  private final PaimonHealthEvaluationContext healthContext;
  private final PaimonPlanContext context;
  @Nullable private final Predicate partitionFilter;
  private final IndexFileHandler indexFileHandler;
  private int fixedScopeScanCount;

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
    return analyze(table, healthContext, partitionFilter, true);
  }

  /** Evaluate historical health without retaining candidates for a future optimizing plan. */
  public static PaimonAppendSnapshotAnalysis analyzeHealth(
      AppendOnlyFileStoreTable table, PaimonHealthEvaluationContext healthContext) {
    return analyze(table, healthContext, null, false).withoutPlannerFacts();
  }

  private static PaimonAppendSnapshotAnalysis analyze(
      AppendOnlyFileStoreTable table,
      PaimonHealthEvaluationContext healthContext,
      @Nullable Predicate partitionFilter,
      boolean collectPlannerFacts) {
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
        .scan(false, collectPlannerFacts)
        .analysis();
  }

  ScanResult scan() {
    return scan(true, true);
  }

  private ScanResult scan(boolean applyFullRewrite, boolean collectPlannerFacts) {
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
                  collectPlannerFacts && planningShapeSupported(),
                  null,
                  0)
              .withStructure(new PaimonHealthStructureCollector(table, healthContext).finish(true));
      return new ScanResult(healthContext.snapshotId(), Collections.emptyMap(), analysis);
    }

    Snapshot snapshot = optionalSnapshot.get();
    try {
      DeletionMetadataLookup deletionMetadataLookup = deletionMetadataLookup(snapshot);
      ScanResult result =
          scanCompleteSnapshot(snapshot, deletionMetadataLookup, collectPlannerFacts);
      if (applyFullRewrite
          && table.bucketMode() == BucketMode.BUCKET_UNAWARE
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
      Snapshot snapshot,
      DeletionMetadataLookup deletionMetadataLookup,
      boolean collectPlannerFacts) {
    PaimonAppendHealthEvaluator evaluator = evaluator();
    Map<PaimonAppendUnitKey, UnitAccumulator> units = new LinkedHashMap<>();
    Map<PaimonAppendUnitKey, UnitAccumulator> structuralUnits = new LinkedHashMap<>();
    Set<BinaryRow> partitions = new HashSet<>();
    Map<PaimonAppendUnitKey, Map<String, Long>> bucketedCounts = new HashMap<>();
    Set<PaimonAppendUnitKey> failedDvUnits = new HashSet<>();
    Map<BinaryRow, List<PaimonFileCandidate>> candidates = new LinkedHashMap<>();
    List<PlannerFile> plannerFiles = new ArrayList<>();
    ScanTotals totals = new ScanTotals();
    PaimonHealthStructureCollector collector =
        new PaimonHealthStructureCollector(table, healthContext);
    PartitionPredicate plannerPartitionPredicate = plannerPartitionPredicate();
    int candidateCount = 0;
    long eligibleFileCount = 0;
    boolean supported = planningShapeSupported();
    boolean fixed = table.bucketMode() == BucketMode.HASH_FIXED;
    boolean layoutValid = true;

    Iterator<ManifestEntry> iterator = addFileIterator(snapshot, null, false);
    while (iterator.hasNext()) {
      ManifestEntry entry = iterator.next();
      if (entry.kind() != FileKind.ADD) {
        continue;
      }
      BinaryRow partition = entry.partition().copy();
      partitions.add(partition);
      PaimonAppendUnitKey unitKey = new PaimonAppendUnitKey(partition, entry.bucket());
      DeleteLookup delete =
          deletionMetadata(entry, unitKey, deletionMetadataLookup, bucketedCounts, failedDvUnits);
      collector.addFile(partition, entry.bucket(), entry.file(), delete.healthCount);
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

      // Paimon ac223f47, manifest/FileEntry.java:49-55 carries the physical bucket layout.
      // A complete health scan still succeeds when this snapshot cannot safely be planned.
      if (fixed
          && (entry.bucket() < 0
              || entry.bucket() >= table.coreOptions().bucket()
              || entry.totalBuckets() != table.coreOptions().bucket())) {
        layoutValid = false;
      }
      if (!collectPlannerFacts
          || !supported
          || (plannerPartitionPredicate != null && !plannerPartitionPredicate.test(partition))) {
        continue;
      }
      PaimonFileCandidate candidate =
          PaimonFileCandidate.from(
              partition, entry.file(), context, delete.deletionFile, delete.dvGroupKey);
      if (!candidate.isProblemFile()) {
        continue;
      }
      eligibleFileCount++;
      if (candidateCount >= context.fileNumLimit()) {
        continue;
      }
      candidateCount++;
      if (!fixed) {
        candidates.computeIfAbsent(partition, ignored -> new ArrayList<>()).add(candidate);
      }
      plannerFiles.add(
          new PlannerFile(
              partition,
              entry.bucket(),
              entry.totalBuckets(),
              entry.file(),
              delete.deletionFile,
              delete.dvGroupKey));
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
        deletionMetadataLookup.readFailed
            ? PaimonAppendHealthEvaluator.Result.invalid("DELETE_METADATA_READ_FAILED")
            : evaluator.evaluate(statistics, healthContext.activityInput(0));
    PaimonAppendHealthEvaluator.Result structuralHealth =
        evaluator.evaluate(structuralStatistics, healthContext.activityInput(0));
    boolean plannerFactsAvailable =
        collectPlannerFacts && supported && layoutValid && failedDvUnits.isEmpty();
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
                1)
            .withPlannerFilesTruncated(plannerFactsAvailable && eligibleFileCount > candidateCount)
            .withStructure(collector.finish(health.healthScore() >= 0));
    return new ScanResult(snapshot.id(), candidates, analysis);
  }

  private PaimonAppendHealthEvaluator evaluator() {
    return new PaimonAppendHealthEvaluator(
        healthContext.targetFileSize(), healthContext.smallFileBoundary());
  }

  private boolean planningShapeSupported() {
    return !PaimonAppendCapabilities.appendRejection(table).isPresent();
  }

  @Nullable
  private PartitionPredicate plannerPartitionPredicate() {
    return partitionFilter == null
        ? null
        : PartitionPredicate.fromPredicate(table.schema().logicalPartitionType(), partitionFilter);
  }

  private DeleteLookup deletionMetadata(
      ManifestEntry entry,
      PaimonAppendUnitKey unitKey,
      DeletionMetadataLookup deletionMetadataLookup,
      Map<PaimonAppendUnitKey, Map<String, Long>> bucketedCounts,
      Set<PaimonAppendUnitKey> failedDvUnits) {
    if (!healthContext.deletionVectorsEnabled()) {
      return DeleteLookup.withoutDeletionFile(deleteCountWithoutDeletionVectors(entry.file()));
    }
    if (failedDvUnits.contains(unitKey)) {
      return DeleteLookup.incomplete();
    }

    try {
      if (deletionMetadataLookup.incomplete(unitKey.partition())) {
        failedDvUnits.add(unitKey);
        return DeleteLookup.incomplete();
      }
      if (table.bucketMode() == BucketMode.BUCKET_UNAWARE) {
        DeletionFile deletionFile =
            deletionMetadataLookup.unawareDeletionFile(
                unitKey.partition(), entry.file().fileName());
        if (deletionFile == null) {
          return DeleteLookup.withDeletionFile(null, 0L);
        }
        return DeleteLookup.withDeletionFile(deletionFile, deletionFile.cardinality());
      }

      Map<String, Long> counts = bucketedCounts.get(unitKey);
      if (counts == null) {
        counts =
            PaimonDeletionVectorCounts.read(
                indexFileHandler,
                unitKey.partition(),
                unitKey.bucket(),
                deletionMetadataLookup.bucketedIndexFiles(unitKey.partition(), unitKey.bucket()));
        bucketedCounts.put(unitKey, counts);
      }
      return DeleteLookup.withoutDeletionFile(counts.getOrDefault(entry.file().fileName(), 0L));
    } catch (RuntimeException e) {
      deletionMetadataLookup.readFailed = true;
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
    if (table.bucketMode() != BucketMode.BUCKET_UNAWARE) {
      // Fixed buckets require their own range proof; never mix them into partition-only inputs.
      return Collections.emptyMap();
    }
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
    Map<PaimonAppendUnitKey, Map<String, Long>> bucketedCounts = new HashMap<>();
    Set<PaimonAppendUnitKey> failedDvUnits = new HashSet<>();
    Map<BinaryRow, List<PaimonFileCandidate>> allFiles = new LinkedHashMap<>();
    Iterator<ManifestEntry> iterator =
        addFileIterator(snapshot, new ArrayList<>(candidateFiles.keySet()), true);
    while (iterator.hasNext()) {
      ManifestEntry entry = iterator.next();
      if (entry.kind() != FileKind.ADD) {
        continue;
      }
      BinaryRow partition = entry.partition().copy();
      PaimonAppendUnitKey unitKey = new PaimonAppendUnitKey(partition, entry.bucket());
      DeleteLookup delete =
          deletionMetadata(entry, unitKey, deletionMetadataLookup, bucketedCounts, failedDvUnits);
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

  Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> fixedBucketWindows(
      PaimonAppendSnapshotAnalysis analysis) {
    if (PaimonAppendCapabilities.fixedBucketRejection(table).isPresent()
        || !analysis.plannerFactsAvailable()
        || analysis.snapshot() == null
        || !healthContext.key().equals(analysis.key())) {
      return Collections.emptyMap();
    }
    Map<PaimonAppendUnitKey, Integer> budgets = new LinkedHashMap<>();
    PartitionPredicate filter = plannerPartitionPredicate();
    int remaining = context.fileNumLimit();
    for (PlannerFile file : analysis.plannerFiles()) {
      if (remaining <= 0) {
        break;
      }
      if (filter != null && !filter.test(file.partition())) {
        continue;
      }
      PaimonAppendUnitKey unit = new PaimonAppendUnitKey(file.partition(), file.bucket());
      budgets.merge(unit, 1, Integer::sum);
      remaining--;
    }
    if (budgets.isEmpty()) {
      return Collections.emptyMap();
    }
    // Candidate count only selects units. Spend the remaining existing file-count budget on
    // their active files too, so FULL can include large files instead of capping each bucket
    // at its problem-file count. The sum of window capacities is still fileNumLimit.
    int share = remaining / budgets.size();
    int extra = remaining % budgets.size();
    for (Map.Entry<PaimonAppendUnitKey, Integer> entry : budgets.entrySet()) {
      entry.setValue(entry.getValue() + share + (extra-- > 0 ? 1 : 0));
    }
    Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> windows = new LinkedHashMap<>();
    budgets.forEach(
        (unit, budget) -> {
          windows.put(
              unit,
              new PaimonFixedBucketFileWindow(
                  unit,
                  table.coreOptions().bucket(),
                  budget,
                  table.coreOptions().bucketAppendOrdered(),
                  entry ->
                      PaimonFileCandidate.from(entry.partition(), entry.file(), context, null, null)
                          .isProblemFile()));
        });
    Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> pending = windows;
    PaimonPartitionEvaluator evaluator = new PaimonPartitionEvaluator(context);
    PaimonFixedBucketTaskPacker packer = new PaimonFixedBucketTaskPacker(context);
    for (int round = 0; round < MAX_FIXED_WINDOW_ROUNDS && !pending.isEmpty(); round++) {
      scanFixedWindows(analysis, pending);
      Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> next = new LinkedHashMap<>();
      boolean lastRound = round == MAX_FIXED_WINDOW_ROUNDS - 1;
      int exhaustedUnits = 0;
      for (Map.Entry<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> entry : pending.entrySet()) {
        PaimonFixedBucketFileWindow window = entry.getValue();
        if (window.canAdvance() && packer.pack(window, evaluator.evaluate(window)).isEmpty()) {
          if (lastRound) {
            exhaustedUnits++;
          } else {
            next.put(entry.getKey(), window.advance());
          }
        }
      }
      if (exhaustedUnits > 0) {
        LOG.warn(
            "Fixed-bucket candidate search reached {} rounds for {} units of {} at snapshot {}; "
                + "the file-count budget may be too small for this ordered layout",
            MAX_FIXED_WINDOW_ROUNDS,
            exhaustedUnits,
            table.fullName(),
            analysis.snapshot().id());
      }
      windows.putAll(next);
      pending = next;
    }
    return Collections.unmodifiableMap(windows);
  }

  private void scanFixedWindows(
      PaimonAppendSnapshotAnalysis analysis,
      Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> windows) {
    // Paimon ac223f47, BucketedAppendCompactManager.java:204-229 advances its sorted queue.
    // Manifest iteration is not sorted: retry only unproductive units at the same snapshot,
    // with a strictly advancing cursor and unchanged capacity. This trades extra scan I/O
    // for bounded metadata; MAX_FIXED_WINDOW_ROUNDS also bounds synchronous scan work.
    Iterator<ManifestEntry> entries = fixedScopeIterator(analysis, windows);
    while (entries.hasNext()) {
      ManifestEntry entry = entries.next();
      PaimonFixedBucketFileWindow window =
          windows.get(new PaimonAppendUnitKey(entry.partition(), entry.bucket()));
      if (window != null) {
        window.add(entry);
      }
    }
    windows.values().forEach(PaimonFixedBucketFileWindow::finish);
    Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> proofWindows = new LinkedHashMap<>();
    windows.forEach(
        (unit, window) -> {
          if (window.needsScopeProof()) {
            proofWindows.put(unit, window);
          }
        });
    if (!proofWindows.isEmpty()) {
      // Re-scan only the selected units at the SAME snapshot after bounded selection freezes.
      // Omitted metadata stays streaming; complete windows and proven prefixes need no rescan.
      entries = fixedScopeIterator(analysis, proofWindows);
      while (entries.hasNext()) {
        ManifestEntry entry = entries.next();
        PaimonFixedBucketFileWindow window =
            proofWindows.get(new PaimonAppendUnitKey(entry.partition(), entry.bucket()));
        if (window != null) {
          window.verifyScopeEntry(entry);
        }
      }
      proofWindows.values().forEach(PaimonFixedBucketFileWindow::finishScopeProof);
    }
  }

  private Iterator<ManifestEntry> fixedScopeIterator(
      PaimonAppendSnapshotAnalysis analysis,
      Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> windows) {
    Set<BinaryRow> partitions = new HashSet<>();
    windows.keySet().forEach(unit -> partitions.add(unit.partition()));
    // Paimon ac223f47, operation/FileStoreScan.java:54-74 accepts one selected-unit predicate
    // and a fixed Snapshot. This is one logical scope scan, not a full scan per bucket; native
    // manifest reads may still inspect other units and are not claimed as physical I/O pruning.
    fixedScopeScanCount++;
    return table
        .store()
        .newScan()
        .withSnapshot(analysis.snapshot())
        .withKind(ScanMode.ALL)
        .withPartitionFilter(new ArrayList<>(partitions))
        .withTotalAwareBucketFilter(
            (partition, bucket, total) ->
                windows.containsKey(new PaimonAppendUnitKey(partition, bucket)))
        .readFileIterator();
  }

  int fixedScopeScanCount() {
    return fixedScopeScanCount;
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
    private final Map<PaimonAppendUnitKey, List<IndexFileMeta>> bucketedIndexFiles;
    private final Set<BinaryRow> incompletePartitions;
    private final boolean manifestScanFailed;
    private boolean readFailed;

    private DeletionMetadataLookup(
        Map<BinaryRow, Map<String, DeletionFile>> unawareDeletionFiles,
        Map<PaimonAppendUnitKey, List<IndexFileMeta>> bucketedIndexFiles,
        Set<BinaryRow> incompletePartitions,
        boolean manifestScanFailed) {
      this.unawareDeletionFiles = unawareDeletionFiles;
      this.bucketedIndexFiles = bucketedIndexFiles;
      this.incompletePartitions = incompletePartitions;
      this.manifestScanFailed = manifestScanFailed;
      this.readFailed = manifestScanFailed;
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
      Map<PaimonAppendUnitKey, List<IndexFileMeta>> bucketedIndexFiles = new HashMap<>();
      Set<BinaryRow> incompletePartitions = new HashSet<>();
      boolean readFailed = false;
      Map<BinaryRow, Set<String>> resolvedFiles = new HashMap<>();
      for (int i = 0; i < entries.size(); i++) {
        // Match SnapshotReaderImpl: the last range for a data file wins.
        // Keep bucketed index files in native order for PaimonDeletionVectorCounts.
        IndexManifestEntry entry =
            entries.get(bucketMode == BucketMode.BUCKET_UNAWARE ? entries.size() - 1 - i : i);
        BinaryRow partition = entry.partition().copy();
        if (bucketMode != BucketMode.BUCKET_UNAWARE) {
          bucketedIndexFiles
              .computeIfAbsent(
                  new PaimonAppendUnitKey(partition, entry.bucket()), ignored -> new ArrayList<>())
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
          Set<String> resolved =
              resolvedFiles.computeIfAbsent(partition, ignored -> new HashSet<>());
          for (DeletionVectorMeta range : ranges.values()) {
            if (range == null || range.dataFileName() == null || range.dataFileName().isEmpty()) {
              incompletePartitions.add(partition);
              continue;
            }
            if (!resolved.add(range.dataFileName())) {
              continue;
            }
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
          readFailed = true;
          incompletePartitions.add(partition);
        }
      }
      DeletionMetadataLookup result =
          new DeletionMetadataLookup(
              unawareDeletionFiles, bucketedIndexFiles, incompletePartitions, false);
      result.readFailed = readFailed;
      return result;
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
          new PaimonAppendUnitKey(partition, bucket), Collections.emptyList());
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
