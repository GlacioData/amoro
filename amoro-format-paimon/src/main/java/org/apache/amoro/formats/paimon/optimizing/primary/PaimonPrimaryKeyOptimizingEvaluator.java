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
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Shared no-side-effect evaluator for Paimon primary-key HASH table optimizing. */
public class PaimonPrimaryKeyOptimizingEvaluator {

  private static final Logger LOG =
      LoggerFactory.getLogger(PaimonPrimaryKeyOptimizingEvaluator.class);

  private static final String NUM_SORTED_RUN_COMPACTION_TRIGGER =
      "num-sorted-run.compaction-trigger";
  private static final String NUM_SORTED_RUN_STOP_TRIGGER = "num-sorted-run.stop-trigger";

  private PaimonPrimaryKeyOptimizingEvaluator() {}

  public static boolean supports(FileStoreTable table) {
    if (table == null || table instanceof AppendOnlyFileStoreTable) {
      return false;
    }
    if (table.primaryKeys() == null || table.primaryKeys().isEmpty()) {
      return false;
    }
    return table.bucketMode() == BucketMode.HASH_FIXED
        || table.bucketMode() == BucketMode.HASH_DYNAMIC;
  }

  public static PaimonPrimaryKeyOptimizingEvaluation evaluate(
      FileStoreTable table,
      String tableName,
      OptimizingConfig optimizingConfig,
      long lastMinorOptimizingTime,
      long lastFullOptimizingTime,
      Predicate partitionFilter,
      long planTime) {
    if (!supports(table)) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }

    OptimizingConfig effectiveConfig =
        optimizingConfig == null ? defaultOptimizingConfig() : optimizingConfig;
    if (hasUnsupportedFilter(tableName, effectiveConfig, partitionFilter)) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }

    PaimonPrimaryKeyOptions primaryKeyOptions;
    try {
      primaryKeyOptions = PaimonPrimaryKeyOptions.from(table.options());
    } catch (RuntimeException e) {
      LOG.warn(
          "Paimon primary-key optimizing options are invalid for table [{}], skip planning.",
          tableName,
          e);
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }
    if (!primaryKeyOptions.enabled()) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }

    Optional<Snapshot> latestSnapshot = table.latestSnapshot();
    if (!latestSnapshot.isPresent()) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(-1L);
    }
    long targetSnapshotId = latestSnapshot.get().id();

    CoreOptions coreOptions = CoreOptions.fromMap(table.options());
    int effectiveMinor = effectiveMinor(table, coreOptions, effectiveConfig);
    if (primaryKeyOptions.majorFileCountThreshold().isPresent()
        && primaryKeyOptions.majorFileCountThreshold().get() < effectiveMinor) {
      LOG.warn(
          "Paimon primary-key table [{}] has {}={} smaller than effective minor threshold {}, "
              + "skip planning.",
          tableName,
          PaimonPrimaryKeyOptions.MAJOR_FILE_COUNT_THRESHOLD,
          primaryKeyOptions.majorFileCountThreshold().get(),
          effectiveMinor);
      return PaimonPrimaryKeyOptimizingEvaluation.empty(targetSnapshotId);
    }
    long effectiveMajor = effectiveMajor(table, coreOptions, primaryKeyOptions, effectiveMinor);

    List<PaimonBucketCompactionUnit> allUnits = bucketUnits(table);
    List<PaimonBucketCompactionUnit> minorCandidates = new ArrayList<>();
    List<PaimonBucketCompactionUnit> majorCandidates = new ArrayList<>();
    for (PaimonBucketCompactionUnit unit : allUnits) {
      if (unit.getFileCount() >= effectiveMinor) {
        minorCandidates.add(unit);
        if (unit.getFileCount() >= effectiveMajor) {
          majorCandidates.add(unit);
        }
      }
    }

    if (!majorCandidates.isEmpty()) {
      return PaimonPrimaryKeyOptimizingEvaluation.of(
          majorCandidates, OptimizingType.MAJOR, true, targetSnapshotId);
    }
    if (!minorCandidates.isEmpty()) {
      if (reachMinorInterval(effectiveConfig, lastMinorOptimizingTime, planTime)) {
        return PaimonPrimaryKeyOptimizingEvaluation.of(
            minorCandidates, OptimizingType.MINOR, false, targetSnapshotId);
      }
      return PaimonPrimaryKeyOptimizingEvaluation.empty(targetSnapshotId);
    }
    return evaluateFull(
        table,
        tableName,
        allUnits,
        primaryKeyOptions,
        effectiveConfig,
        lastFullOptimizingTime,
        planTime,
        targetSnapshotId);
  }

  private static PaimonPrimaryKeyOptimizingEvaluation evaluateFull(
      FileStoreTable table,
      String tableName,
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
    List<PaimonBucketCompactionUnit> fullCandidates =
        idleUnits(table, allUnits, idleTime, planTime);
    if (fullCandidates.isEmpty()) {
      return PaimonPrimaryKeyOptimizingEvaluation.empty(targetSnapshotId);
    }
    return PaimonPrimaryKeyOptimizingEvaluation.of(
        fullCandidates, OptimizingType.FULL, true, targetSnapshotId);
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

  private static List<PaimonBucketCompactionUnit> idleUnits(
      FileStoreTable table,
      List<PaimonBucketCompactionUnit> units,
      Duration idleTime,
      long planTime) {
    if (table.partitionKeys().isEmpty()) {
      return idleBucketUnits(units, idleTime, planTime);
    }
    return idlePartitionBucketUnits(table, units, idleTime, planTime);
  }

  private static List<PaimonBucketCompactionUnit> idleBucketUnits(
      List<PaimonBucketCompactionUnit> units, Duration idleTime, long planTime) {
    List<PaimonBucketCompactionUnit> idleUnits = new ArrayList<>();
    for (PaimonBucketCompactionUnit unit : units) {
      if (isIdle(unit.getLastFileCreationTime(), idleTime, planTime)) {
        idleUnits.add(unit);
      }
    }
    return idleUnits;
  }

  private static List<PaimonBucketCompactionUnit> idlePartitionBucketUnits(
      FileStoreTable table,
      List<PaimonBucketCompactionUnit> units,
      Duration idleTime,
      long planTime) {
    Set<ByteBuffer> idlePartitions = new HashSet<>();
    for (PartitionEntry entry : table.newSnapshotReader().partitionEntries()) {
      if (isIdle(entry.lastFileCreationTime(), idleTime, planTime)) {
        idlePartitions.add(partitionKey(entry.partition()));
      }
    }
    List<PaimonBucketCompactionUnit> idleUnits = new ArrayList<>();
    for (PaimonBucketCompactionUnit unit : units) {
      if (idlePartitions.contains(ByteBuffer.wrap(unit.getPartitionBytes()))) {
        idleUnits.add(unit);
      }
    }
    return idleUnits;
  }

  private static boolean isIdle(long lastFileCreationTime, Duration idleTime, long planTime) {
    long idleMillis = idleTime.toMillis();
    return idleMillis == 0 || planTime - lastFileCreationTime >= idleMillis;
  }

  private static List<PaimonBucketCompactionUnit> bucketUnits(FileStoreTable table) {
    List<PaimonBucketCompactionUnit> units = new ArrayList<>();
    for (BucketEntry entry : table.newSnapshotReader().bucketEntries()) {
      BinaryRow partition = entry.partition();
      units.add(
          new PaimonBucketCompactionUnit(
              SerializationUtils.serializeBinaryRow(partition),
              entry.bucket(),
              entry.fileCount(),
              entry.fileSizeInBytes(),
              entry.recordCount(),
              entry.lastFileCreationTime()));
    }
    return units;
  }

  private static int effectiveMinor(
      FileStoreTable table, CoreOptions coreOptions, OptimizingConfig optimizingConfig) {
    int configured =
        table.options().containsKey(NUM_SORTED_RUN_COMPACTION_TRIGGER)
            ? coreOptions.numSortedRunCompactionTrigger()
            : optimizingConfig.getMinorLeastFileCount();
    return Math.max(1, configured);
  }

  private static long effectiveMajor(
      FileStoreTable table,
      CoreOptions coreOptions,
      PaimonPrimaryKeyOptions primaryKeyOptions,
      int effectiveMinor) {
    if (primaryKeyOptions.majorFileCountThreshold().isPresent()) {
      return primaryKeyOptions.majorFileCountThreshold().get();
    }
    if (table.options().containsKey(NUM_SORTED_RUN_STOP_TRIGGER)) {
      return coreOptions.numSortedRunStopTrigger();
    }
    return effectiveMinor + 3L;
  }

  private static ByteBuffer partitionKey(BinaryRow partition) {
    return ByteBuffer.wrap(SerializationUtils.serializeBinaryRow(partition));
  }
}
