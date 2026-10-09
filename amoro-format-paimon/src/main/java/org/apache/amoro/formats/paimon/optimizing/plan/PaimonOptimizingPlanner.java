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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendCapabilities;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendRewriteOptions;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionExecutorFactory;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionTask;
import org.apache.amoro.formats.paimon.optimizing.PaimonFixedBucketTask;
import org.apache.amoro.formats.paimon.optimizing.PaimonFixedBucketTaskSerializer;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.optimizing.TaskProperties;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.AppendCompactTaskSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * {@link TableOptimizingPlanner} for Paimon BUCKET_UNAWARE and supported HASH_FIXED Append tables.
 *
 * <p>Implements the interface directly instead of extending {@code AbstractOptimizingPlanner}
 * because the latter is deeply coupled to Iceberg-specific types ({@code StructLike}, {@code
 * PartitionSpec}, {@code Expression}).
 *
 * <p>Planning flow:
 *
 * <ol>
 *   <li>{@link #isNecessary()} scans a fixed snapshot through {@link PaimonAppendFileScanner},
 *       reuses existing triggers per partition or bucket, and packs exact input groups.
 *   <li>{@link #plan()} publishes the existing unaware encoding or the versioned fixed envelope,
 *       sharing quota, task descriptors, commit identity and target snapshot semantics.
 * </ol>
 *
 * <p>The generated {@link PaimonCompactionTask}'s {@code properties} carry the executor-factory
 * implementation class name under {@link TaskProperties#TASK_EXECUTOR_FACTORY_IMPL} so the
 * Optimizer side can reflectively load {@link PaimonCompactionExecutorFactory}.
 *
 * <p>The {@code commitUser} returned via {@link #getCommitUser()} is generated once per {@link
 * #plan()} call and is carried end-to-end inside each {@link PaimonCompactionInput} (flushed
 * through {@code TaskFilesPersistence}). Retries and AMS restarts therefore re-use the same value
 * automatically — callers do not need to persist it separately to {@code
 * TableProcessStore.properties}.
 */
public class PaimonOptimizingPlanner implements TableOptimizingPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonOptimizingPlanner.class);

  public static final String COMMIT_USER_PROPERTY = "paimon.commit.user";

  private final PaimonTable paimonTable;
  private final long tableId;
  private final long processId;
  private final long planTime;
  private final double availableCore;
  private final long maxInputSizePerThread;
  private final OptimizingConfig optimizingConfig;
  private final long lastMinorOptimizingTime;
  private final long lastMajorOptimizingTime;
  private final long lastFullOptimizingTime;
  private final Predicate partitionFilter;
  private final OptimizationContext runtimeContext;
  private final FormatTableAnalysis suppliedAnalysis;

  // Memoised state built the first time isNecessary() / plan() runs.
  private Boolean necessary;
  private List<PlannedAppendTask> cachedTasks;
  private String commitUser;
  private long targetSnapshotId = -1L;
  private OptimizingType optimizingType = OptimizingType.MINOR;
  private PaimonAppendSnapshotAnalysis usedAnalysis;
  private int fallbackScanCount;

  public PaimonOptimizingPlanner(
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
        null);
  }

  public PaimonOptimizingPlanner(
      PaimonTable paimonTable,
      long tableId,
      long processId,
      double availableCore,
      long maxInputSizePerThread,
      Predicate partitionFilter) {
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
        partitionFilter,
        null);
  }

  public PaimonOptimizingPlanner(
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

  public PaimonOptimizingPlanner(
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
      FormatTableAnalysis suppliedAnalysis) {
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
        suppliedAnalysis);
  }

  public PaimonOptimizingPlanner(
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
    this.availableCore = availableCore;
    this.maxInputSizePerThread = maxInputSizePerThread;
    this.optimizingConfig = optimizingConfig == null ? defaultOptimizingConfig() : optimizingConfig;
    this.lastMinorOptimizingTime = lastMinorOptimizingTime;
    this.lastMajorOptimizingTime = lastMajorOptimizingTime;
    this.lastFullOptimizingTime = lastFullOptimizingTime;
    this.partitionFilter = partitionFilter;
    this.runtimeContext = runtimeContext;
    this.suppliedAnalysis = suppliedAnalysis;
  }

  private static OptimizingConfig defaultOptimizingConfig() {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastInterval(3600000)
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
    long startTime = logPlanStart("isNecessary");
    try {
      AppendOnlyFileStoreTable table = unwrapAppendTable();
      if (table == null) {
        necessary = false;
        logPlanFinish("isNecessary", startTime, "necessary=false,reason=unsupported-table");
        return false;
      }

      RuntimeInputs runtimeInputs =
          RuntimeInputs.capture(
              runtimeContext,
              optimizingConfig,
              lastMinorOptimizingTime,
              lastMajorOptimizingTime,
              lastFullOptimizingTime);
      OptimizingConfig effectiveConfig = runtimeInputs.optimizingConfig();
      Map<String, String> tableProperties = paimonTable.properties();
      if (!PaimonOptimizingEligibility.isEligible(tableProperties)
          || (effectiveConfig != null && !effectiveConfig.isEnabled())) {
        necessary = false;
        cachedTasks = Collections.emptyList();
        logPlanFinish("isNecessary", startTime, "necessary=false,reason=ineligible");
        return false;
      }
      PaimonHealthEvaluationContext healthContext =
          PaimonHealthEvaluationContext.captureAt(
              table,
              paimonTable.id().toString(),
              runtimeInputs.healthContext(),
              tableProperties,
              planTime);
      PaimonPlanContext context =
          PaimonPlanContext.forOptions(
              CoreOptions.fromMap(table.options()),
              runtimeInputs.optimizingConfig(),
              runtimeInputs.lastMinorOptimizingTime(),
              runtimeInputs.lastMajorOptimizingTime(),
              runtimeInputs.lastFullOptimizingTime(),
              availableCore,
              maxInputSizePerThread,
              planTime);
      boolean fixed = table.bucketMode() == BucketMode.HASH_FIXED;
      PaimonAppendRewriteOptions rewriteOptions =
          fixed ? PaimonAppendRewriteOptions.from(tableProperties) : null;
      if (fixed) {
        context = context.withFixedRewriteOptions(rewriteOptions);
      }
      PaimonAppendFileScanner scanner;
      Map<BinaryRow, List<PaimonFileCandidate>> filesByPartition;
      if (suppliedAnalysis instanceof PaimonAppendSnapshotAnalysis
          && matchesCurrentContext(
              healthContext, (PaimonAppendSnapshotAnalysis) suppliedAnalysis)) {
        usedAnalysis = (PaimonAppendSnapshotAnalysis) suppliedAnalysis;
        targetSnapshotId = usedAnalysis.key().getSnapshotId();
        scanner = new PaimonAppendFileScanner(table, healthContext, context, partitionFilter);
        filesByPartition =
            fixed ? Collections.emptyMap() : scanner.candidatesFromAnalysis(usedAnalysis);
      } else {
        fallbackScanCount++;
        scanner = new PaimonAppendFileScanner(table, healthContext, context, partitionFilter);
        PaimonAppendFileScanner.ScanResult scanResult = scanner.scan();
        usedAnalysis = scanResult.analysis();
        targetSnapshotId = scanResult.snapshotId();
        filesByPartition = scanResult.files();
      }
      List<PlannedAppendTask> tasks = new ArrayList<>();
      if (fixed) {
        tasks.addAll(planFixedBuckets(scanner, context, rewriteOptions));
      }
      PaimonPartitionEvaluator evaluator = new PaimonPartitionEvaluator(context);
      PaimonAppendTaskPacker packer = new PaimonAppendTaskPacker(context);
      for (Map.Entry<BinaryRow, List<PaimonFileCandidate>> entry : filesByPartition.entrySet()) {
        PaimonPartitionEvaluation evaluation = evaluator.evaluate(entry.getKey(), entry.getValue());
        if (!evaluation.necessary()) {
          continue;
        }
        List<AppendCompactTask> packed = packer.pack(evaluation);
        if (packed.isEmpty()) {
          continue;
        }
        for (AppendCompactTask task : packed) {
          tasks.add(new PlannedAppendTask(task, evaluation.optimizingType()));
        }
      }
      cachedTasks = tasks;
      optimizingType = highestType(cachedTasks);
      necessary = !cachedTasks.isEmpty();
      if (!necessary) {
        LOG.info(
            "Paimon table [{}] has no eligible optimizing tasks — skip optimizing.",
            paimonTable.id().getTableName());
      }
      logPlanFinish(
          "isNecessary",
          startTime,
          String.format(
              "necessary=%s,cachedTasks=%d,snapshot=%d,type=%s",
              necessary, cachedTasks.size(), targetSnapshotId, optimizingType));
      return necessary;
    } catch (RuntimeException | Error e) {
      logPlanFinish("isNecessary", startTime, "failed:" + e.getClass().getSimpleName());
      throw e;
    }
  }

  @Override
  public OptimizingPlanResult<PaimonCompactionTask> plan() {
    long startTime = logPlanStart("plan");
    try {
      if (!isNecessary()) {
        OptimizingPlanResult<PaimonCompactionTask> result = emptyResult();
        logPlanFinish("plan", startTime, "necessary=false,tasks=0");
        return result;
      }
      AppendOnlyFileStoreTable table = unwrapAppendTable();
      if (table == null) {
        OptimizingPlanResult<PaimonCompactionTask> result = emptyResult();
        logPlanFinish("plan", startTime, "necessary=false,reason=unsupported-table,tasks=0");
        return result;
      }

      // Generate commitUser once per Planner instance — retries within the same plan MUST reuse
      // the same value so Paimon's (user + identifier) idempotency can dedupe stale commits.
      if (commitUser == null) {
        commitUser = CoreOptions.createCommitUser(Options.fromMap(table.options()));
      }

      // Apply plan-tick quota: cap count to ceil(availableCore). Input-size splitting is handled by
      // PaimonAppendTaskPacker before this point.
      // NOTE (§3.4 split contract): this does NOT re-split, merge, or otherwise mutate the
      // Amoro-built AppendCompactTask — we only decide how many tasks get released this tick.
      List<PlannedAppendTask> eligible =
          applyQuota(
              cachedTasks,
              availableCore,
              maxInputSizePerThread,
              PlannedAppendTask::totalInputSize,
              paimonTable.id().getTableName());

      if (eligible.isEmpty()) {
        LOG.info(
            "Paimon table [{}] plan tick produced 0 eligible tasks — "
                + "reset isNecessary() to false for this planner instance.",
            paimonTable.id().getTableName());
        necessary = false;
        OptimizingPlanResult<PaimonCompactionTask> result = emptyResult();
        logPlanFinish("plan", startTime, "tasks=0");
        return result;
      }
      optimizingType = highestType(eligible);

      List<PaimonCompactionTask> wrapped = new ArrayList<>(eligible.size());
      for (PlannedAppendTask plannedTask : eligible) {
        PaimonCompactionInput input =
            plannedTask.input(paimonTable, commitUser, targetSnapshotId, processId);
        Map<String, String> props = Maps.newHashMap();
        props.put(
            TaskProperties.TASK_EXECUTOR_FACTORY_IMPL,
            PaimonCompactionExecutorFactory.class.getName());
        wrapped.add(new PaimonCompactionTask(tableId, input.getPartitionPath(), input, props));
      }

      OptimizingPlanResult<PaimonCompactionTask> result =
          new OptimizingPlanResult<>(
              processId,
              getOptimizingType(),
              planTime,
              targetSnapshotId,
              -1L /* targetChangeSnapshotId — Paimon has no change snapshot */,
              wrapped,
              Collections.emptyMap() /* fromSequence */,
              Collections.emptyMap() /* toSequence */);
      logPlanFinish(
          "plan",
          startTime,
          String.format(
              "tasks=%d,type=%s,snapshot=%d",
              result.getTasks().size(), result.getOptimizingType(), targetSnapshotId));
      return result;
    } catch (RuntimeException | Error e) {
      logPlanFinish("plan", startTime, "failed:" + e.getClass().getSimpleName());
      throw e;
    }
  }

  /**
   * Return the {@code commitUser} generated for this plan, or {@code null} if {@link #plan()} has
   * not yet been invoked or if no compaction was necessary.
   *
   * <p>The value is authoritative only <em>after</em> {@link #plan()} runs: it is propagated into
   * every task via {@link PaimonCompactionInput#getCommitUser()} and survives AMS restarts through
   * {@code TaskFilesPersistence}. External callers do not need to persist it separately — {@link
   * #COMMIT_USER_PROPERTY} is retained as a property-key constant for future integrations that
   * might want to expose the value on {@code TableProcessStore.properties}.
   */
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

  private List<PlannedAppendTask> planFixedBuckets(
      PaimonAppendFileScanner scanner,
      PaimonPlanContext context,
      PaimonAppendRewriteOptions rewriteOptions) {
    List<PlannedAppendTask> tasks = new ArrayList<>();
    PaimonPartitionEvaluator evaluator = new PaimonPartitionEvaluator(context);
    PaimonFixedBucketTaskPacker packer = new PaimonFixedBucketTaskPacker(context);
    PaimonFixedBucketTaskSerializer serializer = new PaimonFixedBucketTaskSerializer();
    for (PaimonFixedBucketFileWindow window : scanner.fixedBucketWindows(usedAnalysis).values()) {
      if (!window.safe()) {
        LOG.warn(
            "Skip unsafe Paimon unit partition={},bucket={},snapshot={},reason={}",
            window.unit().partition(),
            window.unit().bucket(),
            targetSnapshotId,
            window.reason());
        continue;
      }
      PaimonPartitionEvaluation evaluation = evaluator.evaluate(window);
      List<List<DataFileMeta>> groups = packer.pack(window, evaluation);
      long packedFiles = 0L;
      int publishedTasks = 0;
      for (List<DataFileMeta> group : groups) {
        // Paimon ac223f47, manifest/FileEntry.java:49-57: bucket and totalBuckets belong to the
        // native manifest scope. Reuse the proven window identity, never an unaware sentinel.
        List<ManifestEntry> entries =
            group.stream()
                .map(
                    file ->
                        ManifestEntry.create(
                            FileKind.ADD,
                            window.unit().partition(),
                            window.unit().bucket(),
                            window.totalBuckets(),
                            file))
                .collect(Collectors.toList());
        for (PaimonFixedBucketTask bounded :
            serializer.splitForPublication(
                paimonTable,
                entries,
                targetSnapshotId,
                evaluation.optimizingType(),
                rewriteOptions)) {
          // A protocol split must still satisfy MAJOR's existing reduction requirement.
          List<PaimonFileCandidate> candidates =
              bounded.files().stream()
                  .map(
                      file ->
                          PaimonFileCandidate.from(bounded.partition(), file, context, null, null))
                  .collect(Collectors.toList());
          if (PaimonAppendCompactBenefit.shouldKeep(
              bounded.optimizingType(), candidates, context)) {
            tasks.add(new PlannedAppendTask(bounded));
            packedFiles += bounded.files().size();
            publishedTasks++;
          }
        }
      }
      LOG.info(
          "Paimon fixed unit partition={},bucket={},snapshot={},type={},totalFiles={},"
              + "retainedFiles={},selectedFiles={},packedFiles={},tasks={},complete={},"
              + "provenPrefix={},omittedFiles={},reason={}",
          window.unit().partition(),
          window.unit().bucket(),
          targetSnapshotId,
          evaluation.optimizingType(),
          window.totalFileCount(),
          window.files().size(),
          evaluation.selectedFiles().size(),
          packedFiles,
          publishedTasks,
          window.complete(),
          window.provenPrefix(),
          window.omittedFileCount(),
          window.reason());
    }
    if (usedAnalysis.plannerFilesTruncated()) {
      LOG.info(
          "Paimon fixed candidate coverage truncated at snapshot {} by fileNumLimit={}",
          targetSnapshotId,
          context.fileNumLimit());
    }
    return tasks;
  }

  /**
   * Apply the plan-tick quota to a list of {@link AppendCompactTask}s built by the Amoro Paimon
   * planner.
   *
   * <p>Two independent filters are applied:
   *
   * <p>At most {@code ceil(availableCore)} eligible tasks are released per tick. Remaining tasks
   * are not persisted. The next planning tick scans the latest snapshot again and rebuilds eligible
   * tasks from current metadata.
   *
   * <p>Critically, this method NEVER mutates a task's internal {@code compactBefore()} list —
   * Paimon's atomic commit unit is preserved. Merging or splitting is explicitly disallowed per the
   * plan-document §3.4 task-splitting contract.
   *
   * <p>Visible for testing so unit tests can inject a synthetic {@code sizeFn} without standing up
   * a real Paimon file system.
   */
  static <T> List<T> applyQuota(
      List<T> tasks,
      double availableCore,
      long maxInputSizePerThread,
      ToLongFunction<T> sizeFn,
      String tableNameForLog) {
    if (tasks == null || tasks.isEmpty()) {
      return Collections.emptyList();
    }
    // ceil(availableCore), but guarded against non-positive core values — in those degenerate
    // cases we still allow at most one task through so progress is not completely stalled.
    int cap = Math.max(1, (int) Math.ceil(availableCore));
    List<T> out = new ArrayList<>(Math.min(cap, tasks.size()));
    int observedOversized = 0;
    for (T task : tasks) {
      if (out.size() >= cap) {
        break;
      }
      long totalSize = sizeFn.applyAsLong(task);
      if (maxInputSizePerThread > 0 && totalSize > maxInputSizePerThread) {
        observedOversized++;
      }
      out.add(task);
    }
    if (observedOversized > 0) {
      LOG.warn(
          "Paimon table [{}] releases {} task(s) above maxInputSizePerThread={} bytes. "
              + "The packer already applied best-effort splitting; remaining oversized tasks are "
              + "treated as atomic Paimon compact units.",
          tableNameForLog,
          observedOversized,
          maxInputSizePerThread);
    }
    int deferredByCap = Math.max(0, tasks.size() - out.size());
    if (deferredByCap > 0) {
      LOG.info(
          "Paimon table [{}] plan-tick quota: released {} task(s), deferred {} to next tick "
              + "(cap={}, availableCore={}).",
          tableNameForLog,
          out.size(),
          deferredByCap,
          cap,
          availableCore);
    }
    return out;
  }

  /** Sum the file sizes of the {@code compactBefore} list of an {@link AppendCompactTask}. */
  static long totalCompactBeforeSize(AppendCompactTask task) {
    List<DataFileMeta> before = task.compactBefore();
    if (before == null || before.isEmpty()) {
      return 0L;
    }
    long sum = 0L;
    for (DataFileMeta meta : before) {
      sum += meta.fileSize();
    }
    return sum;
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
    // Paimon has no separate change snapshot concept.
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

  private long logPlanStart(String phase) {
    long startTime = System.currentTimeMillis();
    LOG.info(
        "Paimon table [{}] {} planning started at {}.",
        paimonTable.id().getTableName(),
        phase,
        startTime);
    return startTime;
  }

  private void logPlanFinish(String phase, long startTime, String result) {
    long endTime = System.currentTimeMillis();
    LOG.info(
        "Paimon table [{}] {} planning finished at {} with result {} in {} ms.",
        paimonTable.id().getTableName(),
        phase,
        endTime,
        result,
        endTime - startTime);
  }

  private AppendOnlyFileStoreTable unwrapAppendTable() {
    Object raw = paimonTable.originalTable();
    if (!(raw instanceof AppendOnlyFileStoreTable)) {
      LOG.info(
          "Paimon table [{}] is not AppendOnly; skip (got {}).",
          paimonTable.id().getTableName(),
          raw == null ? "null" : raw.getClass().getSimpleName());
      return null;
    }
    AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) raw;
    if (table.bucketMode() != BucketMode.BUCKET_UNAWARE
        && PaimonAppendCapabilities.fixedBucketRejection(table).isPresent()) {
      LOG.info(
          "Paimon table [{}] bucketMode={} is not supported for Append planning: {}; skip.",
          paimonTable.id().getTableName(),
          table.bucketMode(),
          PaimonAppendCapabilities.fixedBucketRejection(table).get());
      return null;
    }
    return table;
  }

  /**
   * Converts Paimon's internal partition row to its canonical, human-readable partition path.
   *
   * <p>Paimon stores the partition as a {@link BinaryRow}; its default {@code toString()} is an
   * object identity rather than the partition values. The generated {@code key=value/} path matches
   * Paimon's partition-directory convention documented at
   * https://paimon.apache.org/docs/1.4/concepts/spec/datafile/.
   */
  static String partitionPath(FileStoreTable table, BinaryRow partition) {
    if (partition == null) {
      return "";
    }

    RowType partitionType = table.schema().logicalPartitionType();
    if (partitionType.getFieldCount() == 0) {
      return "";
    }

    CoreOptions options = new CoreOptions(table.options());
    InternalRowPartitionComputer partitionComputer =
        new InternalRowPartitionComputer(
            options.partitionDefaultName(),
            partitionType,
            partitionType.getFieldNames().toArray(new String[0]),
            false);
    return PartitionPathUtils.generatePartitionPath(
        partitionComputer.generatePartValues(partition));
  }

  private static boolean matchesCurrentContext(
      PaimonHealthEvaluationContext context, PaimonAppendSnapshotAnalysis analysis) {
    if (!analysis.plannerFactsAvailable() || !context.key().equals(analysis.key())) {
      return false;
    }
    return context.snapshotId() < 0 || analysis.snapshot() != null;
  }

  /** Immutable runtime values captured immediately before the planner binds a table Snapshot. */
  static final class RuntimeInputs implements OptimizationContext {
    private final boolean runtimeAvailable;
    private final ServerTableIdentifier tableIdentifier;
    private final OptimizingConfig optimizingConfig;
    private final boolean idle;
    private final long lastPlanTime;
    private final long lastMinorOptimizingTime;
    private final long lastFullOptimizingTime;
    private final long lastMajorOptimizingTime;
    private final long lastOptimizedSnapshotId;
    private final long fullRetryNotBefore;

    private RuntimeInputs(
        boolean runtimeAvailable,
        ServerTableIdentifier tableIdentifier,
        OptimizingConfig optimizingConfig,
        boolean idle,
        long lastPlanTime,
        long lastMinorOptimizingTime,
        long lastFullOptimizingTime,
        long lastMajorOptimizingTime,
        long lastOptimizedSnapshotId,
        long fullRetryNotBefore) {
      this.runtimeAvailable = runtimeAvailable;
      this.tableIdentifier = tableIdentifier;
      this.optimizingConfig = optimizingConfig;
      this.idle = idle;
      this.lastPlanTime = lastPlanTime;
      this.lastMinorOptimizingTime = lastMinorOptimizingTime;
      this.lastFullOptimizingTime = lastFullOptimizingTime;
      this.lastMajorOptimizingTime = lastMajorOptimizingTime;
      this.lastOptimizedSnapshotId = lastOptimizedSnapshotId;
      this.fullRetryNotBefore = fullRetryNotBefore;
    }

    static RuntimeInputs capture(
        OptimizationContext context,
        OptimizingConfig fallbackConfig,
        long fallbackLastMinor,
        long fallbackLastMajor,
        long fallbackLastFull) {
      if (context == null) {
        return new RuntimeInputs(
            false,
            null,
            fallbackConfig,
            false,
            0L,
            fallbackLastMinor,
            fallbackLastFull,
            fallbackLastMajor,
            -1L,
            0L);
      }
      OptimizingConfig currentConfig = context.getOptimizingConfig();
      return new RuntimeInputs(
          true,
          context.getTableIdentifier(),
          currentConfig == null ? fallbackConfig : currentConfig,
          context.isIdle(),
          context.getLastPlanTime(),
          context.getLastMinorOptimizingTime(),
          context.getLastFullOptimizingTime(),
          context.getLastMajorOptimizingTime(),
          context.getLastOptimizedSnapshotId(),
          context.getFullRetryNotBefore());
    }

    @Override
    public long getFullRetryNotBefore() {
      return fullRetryNotBefore;
    }

    OptimizationContext healthContext() {
      return runtimeAvailable ? this : null;
    }

    long lastMinorOptimizingTime() {
      return lastMinorOptimizingTime;
    }

    long lastMajorOptimizingTime() {
      return lastMajorOptimizingTime;
    }

    long lastFullOptimizingTime() {
      return lastFullOptimizingTime;
    }

    @Override
    public ServerTableIdentifier getTableIdentifier() {
      return tableIdentifier;
    }

    @Override
    public OptimizingConfig getOptimizingConfig() {
      return optimizingConfig;
    }

    OptimizingConfig optimizingConfig() {
      return optimizingConfig;
    }

    @Override
    public boolean isIdle() {
      return idle;
    }

    @Override
    public long getLastPlanTime() {
      return lastPlanTime;
    }

    @Override
    public long getLastMinorOptimizingTime() {
      return lastMinorOptimizingTime;
    }

    @Override
    public long getLastFullOptimizingTime() {
      return lastFullOptimizingTime;
    }

    @Override
    public long getLastMajorOptimizingTime() {
      return lastMajorOptimizingTime;
    }

    @Override
    public long getLastOptimizedSnapshotId() {
      return lastOptimizedSnapshotId;
    }
  }

  private OptimizingPlanResult<PaimonCompactionTask> emptyResult() {
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

  private static OptimizingType higherType(OptimizingType current, OptimizingType candidate) {
    if (candidate == null) {
      return current;
    }
    if (current == null || candidate == OptimizingType.FULL) {
      return candidate;
    }
    if (current == OptimizingType.MINOR && candidate == OptimizingType.MAJOR) {
      return candidate;
    }
    return current;
  }

  private static OptimizingType highestType(List<PlannedAppendTask> tasks) {
    OptimizingType type = null;
    for (PlannedAppendTask task : tasks) {
      type = higherType(type, task.optimizingType());
    }
    return type == null ? OptimizingType.MINOR : type;
  }

  private static final class PlannedAppendTask {
    private final AppendCompactTask task;
    private final PaimonFixedBucketTask fixedTask;
    private final OptimizingType optimizingType;

    private PlannedAppendTask(AppendCompactTask task, OptimizingType optimizingType) {
      this.task = task;
      this.fixedTask = null;
      this.optimizingType = optimizingType;
    }

    private PlannedAppendTask(PaimonFixedBucketTask task) {
      this.task = null;
      this.fixedTask = task;
      this.optimizingType = task.optimizingType();
    }

    private long totalInputSize() {
      return fixedTask == null
          ? totalCompactBeforeSize(task)
          : fixedTask.files().stream().mapToLong(DataFileMeta::fileSize).sum();
    }

    private PaimonCompactionInput input(
        PaimonTable table, String commitUser, long snapshotId, long processId) {
      AppendOnlyFileStoreTable nativeTable = (AppendOnlyFileStoreTable) table.originalTable();
      if (fixedTask != null) {
        return PaimonCompactionInput.forFixedBucket(
            table,
            fixedTask,
            commitUser,
            partitionPath(nativeTable, fixedTask.partition()),
            processId);
      }
      AppendCompactTaskSerializer serializer = new AppendCompactTaskSerializer();
      try {
        return new PaimonCompactionInput(
            table,
            serializer.serialize(task),
            serializer.getVersion(),
            commitUser,
            partitionPath(nativeTable, task.partition()),
            snapshotId,
            processId);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to serialize Paimon AppendCompactTask for partition " + task.partition(), e);
      }
    }

    private OptimizingType optimizingType() {
      return optimizingType;
    }
  }
}
