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

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonBucketCompactionUnit;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionExecutorFactory;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionTask;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluation;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluator;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptions;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeySnapshotAnalysis;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.optimizing.TaskProperties;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** {@link TableOptimizingPlanner} for Paimon primary-key HASH_FIXED/HASH_DYNAMIC tables. */
public class PaimonPrimaryKeyOptimizingPlanner implements TableOptimizingPlanner {

  private static final Logger LOG =
      LoggerFactory.getLogger(PaimonPrimaryKeyOptimizingPlanner.class);

  private final PaimonTable paimonTable;
  private final long tableId;
  private final long processId;
  private final long planTime;
  private final OptimizingConfig optimizingConfig;
  private final long lastMinorOptimizingTime;
  private final long lastFullOptimizingTime;
  private final Predicate partitionFilter;
  private final OptimizationContext runtimeContext;
  private final FormatTableAnalysis suppliedAnalysis;

  private Boolean necessary;
  private List<PaimonBucketCompactionUnit> cachedUnits;
  private OptimizingType optimizingType = OptimizingType.MINOR;
  private boolean fullCompaction;
  private long targetSnapshotId = -1L;
  private String commitUser;
  private PaimonPrimaryKeySnapshotAnalysis usedAnalysis;
  private int fallbackScanCount;

  public static boolean supports(PaimonTable paimonTable) {
    if (paimonTable == null) {
      return false;
    }
    Object raw = paimonTable.originalTable();
    if (!(raw instanceof PrimaryKeyFileStoreTable)) {
      return false;
    }
    FileStoreTable table = (FileStoreTable) raw;
    if (table.primaryKeys() == null || table.primaryKeys().isEmpty()) {
      return false;
    }
    if (table.bucketMode() != BucketMode.HASH_FIXED
        && table.bucketMode() != BucketMode.HASH_DYNAMIC) {
      return false;
    }
    return PaimonPrimaryKeyOptions.enabled(table.options());
  }

  public PaimonPrimaryKeyOptimizingPlanner(
      PaimonTable paimonTable,
      long tableId,
      long processId,
      double availableCore,
      long maxInputSizePerThread) {
    this(
        paimonTable,
        tableId,
        processId,
        availableCore,
        maxInputSizePerThread,
        defaultOptimizingConfig(),
        0L,
        0L,
        0L,
        null,
        null,
        null);
  }

  public PaimonPrimaryKeyOptimizingPlanner(
      PaimonTable paimonTable,
      long tableId,
      long processId,
      double availableCore,
      long maxInputSizePerThread,
      OptimizingConfig optimizingConfig,
      long lastMinorOptimizingTime,
      long lastMajorOptimizingTime,
      long lastFullOptimizingTime,
      Predicate partitionFilter) {
    this(
        paimonTable,
        tableId,
        processId,
        availableCore,
        maxInputSizePerThread,
        optimizingConfig,
        lastMinorOptimizingTime,
        lastMajorOptimizingTime,
        lastFullOptimizingTime,
        partitionFilter,
        null,
        null);
  }

  public PaimonPrimaryKeyOptimizingPlanner(
      PaimonTable paimonTable,
      long tableId,
      long processId,
      double availableCore,
      long maxInputSizePerThread,
      OptimizingConfig optimizingConfig,
      long lastMinorOptimizingTime,
      long lastMajorOptimizingTime,
      long lastFullOptimizingTime,
      Predicate partitionFilter,
      OptimizationContext runtimeContext,
      FormatTableAnalysis suppliedAnalysis) {
    this.paimonTable = paimonTable;
    this.tableId = tableId;
    this.processId = processId;
    this.planTime = System.currentTimeMillis();
    this.optimizingConfig = optimizingConfig == null ? defaultOptimizingConfig() : optimizingConfig;
    this.lastMinorOptimizingTime = lastMinorOptimizingTime;
    this.lastFullOptimizingTime = lastFullOptimizingTime;
    this.partitionFilter = partitionFilter;
    this.runtimeContext = runtimeContext;
    this.suppliedAnalysis = suppliedAnalysis;
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

  @Override
  public boolean isNecessary() {
    return paimonTable.doAs(this::isNecessaryInternal);
  }

  private boolean isNecessaryInternal() {
    if (necessary != null) {
      return necessary;
    }
    FileStoreTable table = unwrapPrimaryKeyHashTable();
    if (table == null) {
      return cacheEmpty(false);
    }
    PaimonOptimizingPlanner.RuntimeInputs runtimeInputs =
        PaimonOptimizingPlanner.RuntimeInputs.capture(
            runtimeContext, optimizingConfig, lastMinorOptimizingTime, 0L, lastFullOptimizingTime);
    PaimonHealthEvaluationContext healthContext =
        PaimonHealthEvaluationContext.capture(
            table, paimonTable.id().toString(), runtimeInputs.healthContext());
    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        suppliedAnalysis instanceof PaimonPrimaryKeySnapshotAnalysis
                && reusable(healthContext, (PaimonPrimaryKeySnapshotAnalysis) suppliedAnalysis)
            ? PaimonPrimaryKeyOptimizingEvaluator.evaluate(
                table,
                paimonTable.id().getTableName(),
                healthContext,
                (PaimonPrimaryKeySnapshotAnalysis) suppliedAnalysis,
                runtimeInputs.optimizingConfig(),
                runtimeInputs.lastMinorOptimizingTime(),
                runtimeInputs.lastFullOptimizingTime(),
                partitionFilter,
                planTime)
            : fallbackEvaluation(table, healthContext, runtimeInputs);
    usedAnalysis = evaluation.analysis().orElse(null);
    targetSnapshotId = evaluation.targetSnapshotId();
    if (!evaluation.necessary()) {
      return cacheEmpty(evaluation.fullCompaction());
    }
    return cache(evaluation.units(), evaluation.optimizingType(), evaluation.fullCompaction());
  }

  @Override
  public OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> plan() {
    if (!isNecessary()) {
      return emptyResult();
    }
    FileStoreTable table = unwrapPrimaryKeyHashTable();
    if (table == null) {
      return emptyResult();
    }
    if (commitUser == null) {
      commitUser = CoreOptions.createCommitUser(Options.fromMap(table.options()));
    }

    List<PaimonPrimaryKeyCompactionTask> tasks = packTasks(cachedUnits);
    return new OptimizingPlanResult<>(
        processId,
        getOptimizingType(),
        planTime,
        targetSnapshotId,
        -1L,
        tasks,
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  public String getCommitUser() {
    return commitUser;
  }

  @Override
  public Optional<FormatTableAnalysis> tableAnalysis() {
    return Optional.ofNullable(usedAnalysis);
  }

  int fallbackScanCount() {
    return fallbackScanCount;
  }

  @Override
  public OptimizingType getOptimizingType() {
    return optimizingType;
  }

  @Override
  public long getProcessId() {
    return processId;
  }

  @Override
  public long getPlanTime() {
    return planTime;
  }

  @Override
  public long getTargetSnapshotId() {
    return targetSnapshotId;
  }

  @Override
  public long getTargetChangeSnapshotId() {
    return -1L;
  }

  @Override
  public Map<String, Long> getFromSequence() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getToSequence() {
    return Collections.emptyMap();
  }

  private List<PaimonPrimaryKeyCompactionTask> packTasks(List<PaimonBucketCompactionUnit> units) {
    List<PaimonPrimaryKeyCompactionTask> tasks = new ArrayList<>();
    for (PaimonBucketCompactionUnit unit : units) {
      List<PaimonBucketCompactionUnit> taskUnits = new ArrayList<>();
      taskUnits.add(unit);
      PaimonPrimaryKeyCompactionInput input =
          new PaimonPrimaryKeyCompactionInput(
              paimonTable,
              taskUnits,
              getOptimizingType(),
              fullCompaction,
              targetSnapshotId,
              commitUser,
              processId);
      Map<String, String> props = Maps.newHashMap();
      props.put(
          TaskProperties.TASK_EXECUTOR_FACTORY_IMPL,
          PaimonPrimaryKeyCompactionExecutorFactory.class.getName());
      tasks.add(
          PaimonPrimaryKeyCompactionTask.buildTask(tableId, "primary-key-buckets", input, props));
    }
    return tasks;
  }

  private FileStoreTable unwrapPrimaryKeyHashTable() {
    Object raw = paimonTable.originalTable();
    if (!(raw instanceof PrimaryKeyFileStoreTable)) {
      LOG.info(
          "Paimon table [{}] is not PrimaryKeyFileStoreTable; skip primary-key optimizing.",
          paimonTable.id().getTableName());
      return null;
    }
    FileStoreTable table = (FileStoreTable) raw;
    if (table.primaryKeys() == null || table.primaryKeys().isEmpty()) {
      LOG.info(
          "Paimon table [{}] does not have primary key; skip primary-key optimizing.",
          paimonTable.id().getTableName());
      return null;
    }
    if (table.bucketMode() != BucketMode.HASH_FIXED
        && table.bucketMode() != BucketMode.HASH_DYNAMIC) {
      LOG.info(
          "Paimon table [{}] bucketMode={} is not HASH_FIXED/HASH_DYNAMIC; skip.",
          paimonTable.id().getTableName(),
          table.bucketMode());
      return null;
    }
    return table;
  }

  private static boolean reusable(
      PaimonHealthEvaluationContext context, PaimonPrimaryKeySnapshotAnalysis analysis) {
    return analysis.validForPlanning() && context.key().equals(analysis.key());
  }

  private PaimonPrimaryKeyOptimizingEvaluation fallbackEvaluation(
      FileStoreTable table,
      PaimonHealthEvaluationContext healthContext,
      PaimonOptimizingPlanner.RuntimeInputs runtimeInputs) {
    fallbackScanCount++;
    return PaimonPrimaryKeyOptimizingEvaluator.evaluate(
        table,
        paimonTable.id().getTableName(),
        healthContext,
        runtimeInputs.optimizingConfig(),
        runtimeInputs.lastMinorOptimizingTime(),
        runtimeInputs.lastFullOptimizingTime(),
        partitionFilter,
        planTime);
  }

  private boolean cache(
      List<PaimonBucketCompactionUnit> units, OptimizingType type, boolean fullCompaction) {
    this.cachedUnits = units;
    this.optimizingType = type;
    this.fullCompaction = fullCompaction;
    this.necessary = !units.isEmpty();
    return this.necessary;
  }

  private boolean cacheEmpty(boolean fullCompaction) {
    this.cachedUnits = Collections.emptyList();
    this.fullCompaction = fullCompaction;
    this.necessary = false;
    return false;
  }

  private OptimizingPlanResult<PaimonPrimaryKeyCompactionTask> emptyResult() {
    return new OptimizingPlanResult<>(
        processId,
        getOptimizingType(),
        planTime,
        targetSnapshotId,
        -1L,
        Collections.emptyList(),
        Collections.emptyMap(),
        Collections.emptyMap());
  }
}
