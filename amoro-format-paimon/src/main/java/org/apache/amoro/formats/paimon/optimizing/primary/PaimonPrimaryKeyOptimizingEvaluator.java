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

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.EarlyFullCompaction;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.OffPeakHours;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared no-side-effect evaluator for Paimon primary-key HASH table optimizing. */
public class PaimonPrimaryKeyOptimizingEvaluator {

  private static final Logger LOG =
      LoggerFactory.getLogger(PaimonPrimaryKeyOptimizingEvaluator.class);

  private static final Comparator<PaimonBucketCompactionUnit> MAJOR_PRIORITY =
      Comparator.comparingLong(PaimonBucketCompactionUnit::getSortedRunCount)
          .reversed()
          .thenComparing(
              Comparator.comparingLong(PaimonBucketCompactionUnit::getFileCount).reversed())
          .thenComparing(
              Comparator.comparingLong(PaimonBucketCompactionUnit::getFileSizeInBytes).reversed())
          .thenComparing(
              (left, right) ->
                  PaimonPrimaryKeySnapshotAnalysis.compareUnsigned(
                      left.getPartitionBytes(), right.getPartitionBytes()))
          .thenComparingInt(PaimonBucketCompactionUnit::getBucket);

  private PaimonPrimaryKeyOptimizingEvaluator() {}

  public static boolean supports(FileStoreTable table) {
    if (!supportsTableShape(table)) {
      return false;
    }
    try {
      return !CoreOptions.fromMap(table.options()).pkClusteringOverride();
    } catch (RuntimeException e) {
      return false;
    }
  }

  public static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      FileStoreTable table,
      String tableName,
      OptimizingConfig optimizingConfig,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime,
      Predicate partitionFilter,
      long planTime) {
    if (!supportsTableShape(table)) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }

    OptimizingConfig effectiveConfig =
        optimizingConfig == null ? defaultOptimizingConfig() : optimizingConfig;
    if (hasUnsupportedFilter(tableName, effectiveConfig, partitionFilter)) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }

    final PaimonPrimaryKeyOptions primaryKeyOptions;
    final CoreOptions coreOptions;
    try {
      primaryKeyOptions = PaimonPrimaryKeyOptions.from(table.options());
      coreOptions = CoreOptions.fromMap(table.options());
    } catch (RuntimeException e) {
      LOG.warn(
          "Paimon primary-key optimizing options are invalid for table [{}], skip planning.",
          tableName,
          e);
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }
    if (coreOptions.pkClusteringOverride()) {
      LOG.warn(
          "Paimon primary-key table [{}] enables pk-clustering-override; skip primary-key "
              + "self-optimizing planning.",
          tableName);
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }

    final Snapshot targetSnapshot;
    try {
      Optional<Snapshot> latestSnapshot = table.latestSnapshot();
      if (!latestSnapshot.isPresent()) {
        return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
      }
      targetSnapshot = latestSnapshot.get();
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to capture latest snapshot for Paimon primary-key table [{}]; skip planning.",
          tableName,
          e);
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }

    try {
      if (!(table.store() instanceof KeyValueFileStore)) {
        LOG.warn(
            "Paimon primary-key table [{}] does not expose a KeyValueFileStore; skip planning.",
            tableName);
        return PaimonPrimaryKeyOptimizingEvaluation.empty(targetSnapshot.id());
      }
      KeyValueFileStore store = (KeyValueFileStore) table.store();
      SnapshotFiles snapshotFiles = scanSnapshot(table, targetSnapshot);
      List<PaimonBucketCompactionUnit> allUnits = new ArrayList<>();
      List<PaimonBucketCompactionUnit> normalCandidates = new ArrayList<>();
      List<PaimonBucketCompactionUnit> majorCandidates = new ArrayList<>();
      int stopTrigger = coreOptions.numSortedRunStopTrigger();

      for (BucketFiles bucketFiles : snapshotFiles.buckets.values()) {
        Levels levels =
            new Levels(store.newKeyComparator(), bucketFiles.files, coreOptions.numLevels());
        long sortedRunCount = levels.numberOfSortedRuns();
        PaimonBucketCompactionUnit unit = bucketFiles.toUnit(sortedRunCount);
        allUnits.add(unit);
        if (hasNormalCompactionCandidate(coreOptions, levels)) {
          normalCandidates.add(unit);
          if (sortedRunCount > stopTrigger) {
            majorCandidates.add(unit);
          }
        }
      }

      if (!majorCandidates.isEmpty()) {
        List<PaimonBucketCompactionUnit> selected =
            selectMajorCandidates(
                majorCandidates, allUnits.size(), primaryKeyOptions.majorMaxBucketRatio());
        return PaimonPrimaryKeyOptimizingEvaluation.of(
            selected, OptimizingType.MAJOR, false, targetSnapshot.id());
      }
      if (!normalCandidates.isEmpty()
          && reachMinorInterval(effectiveConfig, lastMinorOptimizingTime, planTime)) {
        return PaimonPrimaryKeyOptimizingEvaluation.of(
            normalCandidates, OptimizingType.MINOR, false, targetSnapshot.id());
      }
      return evaluateFull(
          table,
          tableName,
          snapshotFiles,
          allUnits,
          primaryKeyOptions,
          effectiveConfig,
          lastFullOptimizingTime,
          planTime,
          targetSnapshot.id());
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to evaluate fixed snapshot [{}] for Paimon primary-key table [{}]; skip "
              + "planning without falling back to latest snapshot.",
          targetSnapshot.id(),
          tableName,
          e);
      return PaimonPrimaryKeyOptimizingEvaluation.empty(targetSnapshot.id());
    }
  }

  /**
   * Evaluate health and the existing HASH optimizing decision from one fixed-snapshot analysis.
   * KEY_DYNAMIC deliberately returns health only and never exposes planner units.
   */
  public static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      FileStoreTable table,
      String tableName,
      PaimonHealthEvaluationContext healthContext,
      OptimizingConfig optimizingConfig,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime,
      Predicate partitionFilter,
      long planTime) {
    if (!supportsHealthShape(table)) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(healthContext.snapshotId());
    }

    PaimonPrimaryKeySnapshotAnalysis analysis =
        PaimonPrimaryKeySnapshotAnalysis.analyze(table, healthContext);
    return evaluate(
        table,
        tableName,
        healthContext,
        analysis,
        optimizingConfig,
        lastMinorOptimizingTime,
        lastFullOptimizingTime,
        partitionFilter,
        planTime);
  }

  /** Decide HASH optimizing from reusable facts already scanned for the exact health key. */
  public static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      FileStoreTable table,
      String tableName,
      PaimonHealthEvaluationContext healthContext,
      PaimonPrimaryKeySnapshotAnalysis analysis,
      OptimizingConfig optimizingConfig,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime,
      Predicate partitionFilter,
      long planTime) {
    if (!healthContext.key().equals(analysis.key())) {
      throw new IllegalArgumentException(
          "Paimon primary-key analysis key does not match the captured planning context");
    }
    long targetSnapshotId = healthContext.snapshotId();
    if (healthContext.bucketMode() == BucketMode.KEY_DYNAMIC || !analysis.validForPlanning()) {
      return PaimonPrimaryKeyOptimizingEvaluation.healthOnly(targetSnapshotId, analysis);
    }

    OptimizingConfig effectiveConfig =
        optimizingConfig == null ? defaultOptimizingConfig() : optimizingConfig;
    if (hasUnsupportedFilter(tableName, effectiveConfig, partitionFilter)) {
      return PaimonPrimaryKeyOptimizingEvaluation.healthOnly(targetSnapshotId, analysis);
    }

    final PaimonPrimaryKeyOptions primaryKeyOptions;
    final CoreOptions coreOptions;
    try {
      primaryKeyOptions = PaimonPrimaryKeyOptions.from(table.options());
      coreOptions = healthContext.coreOptions().get();
    } catch (RuntimeException e) {
      LOG.warn(
          "Paimon primary-key optimizing options are invalid for table [{}], skip planning.",
          tableName,
          e);
      return PaimonPrimaryKeyOptimizingEvaluation.healthOnly(targetSnapshotId, analysis);
    }
    if (coreOptions.pkClusteringOverride()) {
      return PaimonPrimaryKeyOptimizingEvaluation.healthOnly(targetSnapshotId, analysis);
    }

    try {
      List<PaimonBucketCompactionUnit> allUnits = new ArrayList<>();
      List<PaimonBucketCompactionUnit> normalCandidates = new ArrayList<>();
      List<PaimonBucketCompactionUnit> majorCandidates = new ArrayList<>();
      int stopTrigger = coreOptions.numSortedRunStopTrigger();
      for (PaimonPrimaryKeySnapshotAnalysis.BucketFacts facts : analysis.bucketFacts()) {
        PaimonBucketCompactionUnit unit = facts.unit();
        allUnits.add(unit);
        if (hasNormalCompactionCandidate(coreOptions, facts.levels())) {
          normalCandidates.add(unit);
          if (unit.getSortedRunCount() > stopTrigger) {
            majorCandidates.add(unit);
          }
        }
      }

      if (!majorCandidates.isEmpty()) {
        List<PaimonBucketCompactionUnit> selected =
            selectMajorCandidates(
                majorCandidates, allUnits.size(), primaryKeyOptions.majorMaxBucketRatio());
        return PaimonPrimaryKeyOptimizingEvaluation.of(
            selected, OptimizingType.MAJOR, false, targetSnapshotId, analysis);
      }
      if (!normalCandidates.isEmpty()
          && reachMinorInterval(effectiveConfig, lastMinorOptimizingTime, planTime)) {
        return PaimonPrimaryKeyOptimizingEvaluation.of(
            normalCandidates, OptimizingType.MINOR, false, targetSnapshotId, analysis);
      }
      return evaluateFull(
          table,
          tableName,
          analysis,
          allUnits,
          primaryKeyOptions,
          effectiveConfig,
          lastFullOptimizingTime,
          planTime,
          targetSnapshotId);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to decide optimizing for analyzed Paimon primary-key snapshot [{}] of table "
              + "[{}]; keep health result and skip planning.",
          targetSnapshotId,
          tableName,
          e);
      return PaimonPrimaryKeyOptimizingEvaluation.healthOnly(targetSnapshotId, analysis);
    }
  }

  private static boolean supportsHealthShape(FileStoreTable table) {
    if (table == null || table instanceof AppendOnlyFileStoreTable) {
      return false;
    }
    if (table.primaryKeys() == null || table.primaryKeys().isEmpty()) {
      return false;
    }
    return table.bucketMode() == BucketMode.HASH_FIXED
        || table.bucketMode() == BucketMode.HASH_DYNAMIC
        || table.bucketMode() == BucketMode.KEY_DYNAMIC;
  }

  private static boolean supportsTableShape(FileStoreTable table) {
    if (table == null || table instanceof AppendOnlyFileStoreTable) {
      return false;
    }
    if (table.primaryKeys() == null || table.primaryKeys().isEmpty()) {
      return false;
    }
    return table.bucketMode() == BucketMode.HASH_FIXED
        || table.bucketMode() == BucketMode.HASH_DYNAMIC;
  }

  private static SnapshotFiles scanSnapshot(FileStoreTable table, Snapshot targetSnapshot) {
    SnapshotReader reader = table.newSnapshotReader().withSnapshot(targetSnapshot);
    SnapshotFiles snapshotFiles = new SnapshotFiles(reader);
    Iterator<ManifestEntry> entries = reader.readFileIterator();
    while (entries.hasNext()) {
      ManifestEntry entry = entries.next();
      snapshotFiles.add(entry.partition(), entry.bucket(), entry.file());
    }
    return snapshotFiles;
  }

  private static boolean hasNormalCompactionCandidate(CoreOptions options, Levels levels) {
    CompactStrategy strategy = createCompactStrategy(options);
    for (int pick = 0; pick < 2; pick++) {
      if (strategy.pick(levels.numberOfLevels(), levels.levelSortedRuns()).isPresent()) {
        return true;
      }
    }
    return false;
  }

  private static CompactStrategy createCompactStrategy(CoreOptions options) {
    UniversalCompaction universal =
        new UniversalCompaction(
            options.maxSizeAmplificationPercent(),
            options.sortedRunSizeRatio(),
            options.numSortedRunCompactionTrigger(),
            EarlyFullCompaction.create(options),
            OffPeakHours.create(options));
    if (options.needLookup()) {
      Integer compactMaxInterval = null;
      switch (options.lookupCompact()) {
        case GENTLE:
          compactMaxInterval = options.lookupCompactMaxInterval();
          break;
        case RADICAL:
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported Paimon lookup compact mode: " + options.lookupCompact());
      }
      return new ForceUpLevel0Compaction(universal, compactMaxInterval);
    }
    if (options.compactionForceUpLevel0()) {
      return new ForceUpLevel0Compaction(universal, null);
    }
    return universal;
  }

  static List<PaimonBucketCompactionUnit> selectMajorCandidates(
      List<PaimonBucketCompactionUnit> candidates,
      int activeBucketCount,
      BigDecimal maxBucketRatio) {
    int maximum =
        maxBucketRatio
            .multiply(BigDecimal.valueOf(activeBucketCount))
            .setScale(0, RoundingMode.CEILING)
            .intValueExact();
    List<PaimonBucketCompactionUnit> sorted = new ArrayList<>(candidates);
    sorted.sort(MAJOR_PRIORITY);
    if (sorted.size() > maximum) {
      return new ArrayList<>(sorted.subList(0, maximum));
    }
    return sorted;
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluateFull(
      FileStoreTable table,
      String tableName,
      SnapshotFiles snapshotFiles,
      List<PaimonBucketCompactionUnit> allUnits,
      PaimonPrimaryKeyOptions primaryKeyOptions,
      OptimizingConfig optimizingConfig,
      long lastFullOptimizingTime,
      long planTime,
      long targetSnapshotId) {
    int fullTriggerInterval = optimizingConfig.getFullTriggerInterval();
    if (fullTriggerInterval <= 0
        || planTime - lastFullOptimizingTime < fullTriggerInterval
        || allUnits.isEmpty()) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(targetSnapshotId);
    }
    if (!primaryKeyOptions.partitionIdleTime().isPresent()) {
      LOG.warn(
          "Paimon primary-key table [{}] requires {} for FULL planning, skip planning.",
          tableName,
          PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME);
      return PaimonPrimaryKeyOptimizingEvaluation.empty(targetSnapshotId);
    }

    Duration idleTime = primaryKeyOptions.partitionIdleTime().get();
    if (!table.partitionKeys().isEmpty()) {
      snapshotFiles.loadPartitionWatermarks();
    }
    List<PaimonBucketCompactionUnit> fullCandidates = new ArrayList<>();
    for (PaimonBucketCompactionUnit unit : allUnits) {
      long lastCreationTime = unit.getLastFileCreationTime();
      if (!table.partitionKeys().isEmpty()) {
        lastCreationTime =
            snapshotFiles.partitionLastCreationTime.get(ByteBuffer.wrap(unit.getPartitionBytes()));
      }
      if (isIdle(lastCreationTime, idleTime, planTime)) {
        fullCandidates.add(unit);
      }
    }
    if (fullCandidates.isEmpty()) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(targetSnapshotId);
    }
    return PaimonPrimaryKeyOptimizingEvaluation.of(
        fullCandidates, OptimizingType.FULL, true, targetSnapshotId);
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluateFull(
      FileStoreTable table,
      String tableName,
      PaimonPrimaryKeySnapshotAnalysis analysis,
      List<PaimonBucketCompactionUnit> allUnits,
      PaimonPrimaryKeyOptions primaryKeyOptions,
      OptimizingConfig optimizingConfig,
      long lastFullOptimizingTime,
      long planTime,
      long targetSnapshotId) {
    int fullTriggerInterval = optimizingConfig.getFullTriggerInterval();
    if (fullTriggerInterval <= 0
        || planTime - lastFullOptimizingTime < fullTriggerInterval
        || allUnits.isEmpty()) {
      return PaimonPrimaryKeyOptimizingEvaluation.healthOnly(targetSnapshotId, analysis);
    }
    if (!primaryKeyOptions.partitionIdleTime().isPresent()) {
      LOG.warn(
          "Paimon primary-key table [{}] requires {} for FULL planning, skip planning.",
          tableName,
          PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME);
      return PaimonPrimaryKeyOptimizingEvaluation.healthOnly(targetSnapshotId, analysis);
    }

    Duration idleTime = primaryKeyOptions.partitionIdleTime().get();
    List<PaimonBucketCompactionUnit> fullCandidates = new ArrayList<>();
    for (PaimonBucketCompactionUnit unit : allUnits) {
      long lastCreationTime = unit.getLastFileCreationTime();
      if (!table.partitionKeys().isEmpty()) {
        Long partitionWatermark = analysis.partitionWatermark(unit.getPartitionBytes());
        if (partitionWatermark == null) {
          throw new IllegalStateException("Missing partition watermark for analyzed bucket");
        }
        lastCreationTime = partitionWatermark;
      }
      if (isIdle(lastCreationTime, idleTime, planTime)) {
        fullCandidates.add(unit);
      }
    }
    if (fullCandidates.isEmpty()) {
      return PaimonPrimaryKeyOptimizingEvaluation.healthOnly(targetSnapshotId, analysis);
    }
    return PaimonPrimaryKeyOptimizingEvaluation.of(
        fullCandidates, OptimizingType.FULL, true, targetSnapshotId, analysis);
  }

  private static OptimizingConfig defaultOptimizingConfig() {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastFileCount(1)
        .setMinorLeastInterval(0)
        .setFullTriggerInterval(-1)
        .setFullRewriteAllFiles(false)
        .setMaxTaskSize(Long.MAX_VALUE);
  }

  private static boolean hasUnsupportedFilter(
      String tableName, OptimizingConfig optimizingConfig, Predicate partitionFilter) {
    if (optimizingConfig.getFilter() != null && !optimizingConfig.getFilter().trim().isEmpty()) {
      LOG.warn(
          "Paimon primary-key table [{}] does not support self-optimizing.filter yet, skip "
              + "planning.",
          tableName);
      return true;
    }
    if (partitionFilter != null) {
      LOG.warn(
          "Paimon primary-key table [{}] does not support partition filter yet, skip planning.",
          tableName);
      return true;
    }
    return false;
  }

  private static boolean reachMinorInterval(
      OptimizingConfig optimizingConfig, long lastMinorOptimizingTime, long planTime) {
    return optimizingConfig.getMinorLeastInterval() >= 0
        && planTime - lastMinorOptimizingTime > optimizingConfig.getMinorLeastInterval();
  }

  private static boolean isIdle(long lastFileCreationTime, Duration idleTime, long planTime) {
    long idleMillis = idleTime.toMillis();
    return idleMillis == 0 || planTime - lastFileCreationTime >= idleMillis;
  }

  private static class SnapshotFiles {

    private final SnapshotReader reader;
    private final Map<BucketKey, BucketFiles> buckets = new LinkedHashMap<>();
    private final Map<ByteBuffer, Long> partitionLastCreationTime = new HashMap<>();

    private SnapshotFiles(SnapshotReader reader) {
      this.reader = reader;
    }

    private void add(BinaryRow partition, int bucket, DataFileMeta file) {
      byte[] partitionBytes = SerializationUtils.serializeBinaryRow(partition);
      BucketKey key = new BucketKey(partitionBytes, bucket);
      buckets.computeIfAbsent(key, ignored -> new BucketFiles(partitionBytes, bucket)).add(file);
    }

    private void loadPartitionWatermarks() {
      for (PartitionEntry entry : reader.partitionEntries()) {
        byte[] partitionBytes = SerializationUtils.serializeBinaryRow(entry.partition());
        partitionLastCreationTime.put(
            ByteBuffer.wrap(partitionBytes), entry.lastFileCreationTime());
      }
    }
  }

  private static class BucketFiles {

    private final byte[] partitionBytes;
    private final int bucket;
    private final List<DataFileMeta> files = new ArrayList<>();
    private long fileSizeInBytes;
    private long recordCount;
    private long lastFileCreationTime;

    private BucketFiles(byte[] partitionBytes, int bucket) {
      this.partitionBytes = Arrays.copyOf(partitionBytes, partitionBytes.length);
      this.bucket = bucket;
    }

    private void add(DataFileMeta file) {
      files.add(file);
      fileSizeInBytes += file.fileSize();
      recordCount += file.rowCount();
      lastFileCreationTime = Math.max(lastFileCreationTime, file.creationTimeEpochMillis());
    }

    private PaimonBucketCompactionUnit toUnit(long sortedRunCount) {
      return new PaimonBucketCompactionUnit(
          Arrays.copyOf(partitionBytes, partitionBytes.length),
          bucket,
          files.size(),
          sortedRunCount,
          fileSizeInBytes,
          recordCount,
          lastFileCreationTime);
    }
  }

  private static class BucketKey {

    private final byte[] partitionBytes;
    private final int bucket;

    private BucketKey(byte[] partitionBytes, int bucket) {
      this.partitionBytes = Arrays.copyOf(partitionBytes, partitionBytes.length);
      this.bucket = bucket;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof BucketKey)) {
        return false;
      }
      BucketKey that = (BucketKey) other;
      return bucket == that.bucket && Arrays.equals(partitionBytes, that.partitionBytes);
    }

    @Override
    public int hashCode() {
      return 31 * Arrays.hashCode(partitionBytes) + bucket;
    }
  }
}
