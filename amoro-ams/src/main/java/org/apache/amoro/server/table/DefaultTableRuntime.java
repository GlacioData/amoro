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

package org.apache.amoro.server.table;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.BlockableOperation;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.optimizing.TableRuntimeOptimizingState;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.AmoroServiceConstants;
import org.apache.amoro.server.optimizing.OptimizingProcess;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.optimizing.TaskRuntime;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.server.persistence.mapper.OptimizerMapper;
import org.apache.amoro.server.persistence.mapper.OptimizingProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableBlockerMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.resource.OptimizerInstance;
import org.apache.amoro.server.table.blocker.TableBlocker;
import org.apache.amoro.server.table.cleanup.TableRuntimeCleanupState;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.table.FormatPendingInput;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableRuntimeStore;
import org.apache.amoro.table.TableSummary;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.utils.SnowflakeIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Default table runtime implementation. */
public class DefaultTableRuntime extends AbstractTableRuntime implements OptimizationContext {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTableRuntime.class);

  protected static final StateKey<TableRuntimeOptimizingState> OPTIMIZING_STATE_KEY =
      StateKey.stateKey("optimizing_state")
          .jsonType(TableRuntimeOptimizingState.class)
          .defaultValue(new TableRuntimeOptimizingState());

  /** Default pending-input key for Iceberg-based formats. */
  public static final StateKey<AbstractOptimizingEvaluator.PendingInput> DEFAULT_PENDING_INPUT_KEY =
      StateKey.stateKey("pending_input")
          .jsonType(AbstractOptimizingEvaluator.PendingInput.class)
          .defaultValue(new AbstractOptimizingEvaluator.PendingInput());

  @SuppressWarnings("unchecked")
  private final StateKey<FormatPendingInput> pendingInputKey;

  public static final StateKey<TableRuntimeCleanupState> CLEANUP_STATE_KEY =
      StateKey.stateKey("cleanup_state")
          .jsonType(TableRuntimeCleanupState.class)
          .defaultValue(new TableRuntimeCleanupState());

  protected static final StateKey<Long> PROCESS_ID_KEY =
      StateKey.stateKey("process_id").longType().defaultValue(0L);

  public static final List<StateKey<?>> REQUIRED_STATES =
      Lists.newArrayList(OPTIMIZING_STATE_KEY, PROCESS_ID_KEY, CLEANUP_STATE_KEY);
  private final TableOptimizingMetrics optimizingMetrics;
  private final TableOrphanFilesCleaningMetrics orphanFilesCleaningMetrics;
  protected final TableSummaryMetrics tableSummaryMetrics;
  private volatile long lastPlanTime;
  private volatile long latestRefreshInterval = AmoroServiceConstants.INVALID_TIME;
  private volatile boolean latestEvaluatedNeedOptimizing = true;
  private volatile TableAnalysisKey currentAnalysisKey;
  private final AtomicReference<FormatTableAnalysis> tableAnalysisSlot = new AtomicReference<>();
  private final RuntimeHealthState runtimeHealthState = new RuntimeHealthState();
  protected volatile OptimizingProcess optimizingProcess;
  private final List<TaskRuntime.TaskQuota> taskQuotas = new CopyOnWriteArrayList<>();

  private final Supplier<AmoroTable<?>> loader;

  public DefaultTableRuntime(
      TableRuntimeStore store,
      Supplier<AmoroTable<?>> loader,
      StateKey<? extends FormatPendingInput> pendingInputKey) {
    super(store);
    this.pendingInputKey = (StateKey<FormatPendingInput>) pendingInputKey;
    this.optimizingMetrics =
        new TableOptimizingMetrics(store.getTableIdentifier(), store.getGroupName());
    this.orphanFilesCleaningMetrics =
        new TableOrphanFilesCleaningMetrics(store.getTableIdentifier());
    this.tableSummaryMetrics = new TableSummaryMetrics(store.getTableIdentifier());
    this.loader = loader;
  }

  /** Convenience constructor using the default Iceberg pending-input key. */
  public DefaultTableRuntime(TableRuntimeStore store, Supplier<AmoroTable<?>> loader) {
    this(store, loader, DEFAULT_PENDING_INPUT_KEY);
  }

  public void recover(OptimizingProcess optimizingProcess) {
    if (!Objects.equals(optimizingProcess.getProcessId(), getProcessId())) {
      throw new IllegalStateException("Table runtime and processing are not matched!");
    }
    this.optimizingProcess = optimizingProcess;
    if (this.optimizingProcess.getStatus() == ProcessStatus.SUCCESS) {
      completeProcess(optimizingProcess, true);
    }
  }

  @Override
  public void registerMetric(MetricRegistry metricRegistry) {
    this.optimizingMetrics.register(metricRegistry);
    this.orphanFilesCleaningMetrics.register(metricRegistry);
    this.tableSummaryMetrics.register(metricRegistry);
  }

  public TableOrphanFilesCleaningMetrics getOrphanFilesCleaningMetrics() {
    return orphanFilesCleaningMetrics;
  }

  public long getCurrentSnapshotId() {
    return store().getState(OPTIMIZING_STATE_KEY).getCurrentSnapshotId();
  }

  public long getCurrentChangeSnapshotId() {
    return store().getState(OPTIMIZING_STATE_KEY).getCurrentChangeSnapshotId();
  }

  public long getLastPlanTime() {
    return lastPlanTime;
  }

  public void setLastPlanTime(long lastPlanTime) {
    this.lastPlanTime = lastPlanTime;
  }

  public long getLatestRefreshInterval() {
    return latestRefreshInterval;
  }

  public void setLatestRefreshInterval(long latestRefreshInterval) {
    this.latestRefreshInterval = latestRefreshInterval;
  }

  public boolean getLatestEvaluatedNeedOptimizing() {
    return this.latestEvaluatedNeedOptimizing;
  }

  public void setLatestEvaluatedNeedOptimizing(boolean latestEvaluatedNeedOptimizing) {
    this.latestEvaluatedNeedOptimizing = latestEvaluatedNeedOptimizing;
  }

  public OptimizingStatus getOptimizingStatus() {
    return OptimizingStatus.ofCode(getStatusCode());
  }

  public long getLastMajorOptimizingTime() {
    return store().getState(OPTIMIZING_STATE_KEY).getLastMajorOptimizingTime();
  }

  public long getLastFullOptimizingTime() {
    return store().getState(OPTIMIZING_STATE_KEY).getLastFullOptimizingTime();
  }

  public long getLastMinorOptimizingTime() {
    return store().getState(OPTIMIZING_STATE_KEY).getLastMinorOptimizingTime();
  }

  public long getLastOptimizedChangeSnapshotId() {
    return store().getState(OPTIMIZING_STATE_KEY).getLastOptimizedChangeSnapshotId();
  }

  @Override
  public long getLastOptimizedSnapshotId() {
    return store().getState(OPTIMIZING_STATE_KEY).getLastOptimizedSnapshotId();
  }

  public OptimizingConfig getOptimizingConfig() {
    return getTableConfiguration().getOptimizingConfig();
  }

  public FormatPendingInput getPendingInput() {
    return store().getState(pendingInputKey);
  }

  /** Returns the latest scan-free analysis key observed during {@link #refresh(AmoroTable)}. */
  public Optional<TableAnalysisKey> getCurrentAnalysisKey() {
    return Optional.ofNullable(currentAnalysisKey);
  }

  /**
   * Atomically consumes the in-memory analysis when it belongs to the requested planning key. Every
   * call consumes the one-shot slot; any key mismatch drops the consumed object.
   */
  public Optional<FormatTableAnalysis> takeTableAnalysis(TableAnalysisKey expectedKey) {
    Objects.requireNonNull(expectedKey, "Expected analysis key must not be null");
    FormatTableAnalysis analysis = tableAnalysisSlot.getAndSet(null);
    if (analysis == null
        || currentAnalysisKey == null
        || !expectedKey.equals(currentAnalysisKey)
        || !expectedKey.equals(analysis.key())) {
      return Optional.empty();
    }
    return Optional.of(analysis);
  }

  /** Returns the Paimon health result produced during this runtime process, if any. */
  public Optional<RuntimeHealthSnapshot> getRuntimeHealthSnapshot() {
    return runtimeHealthState.snapshot();
  }

  /**
   * Writes a planner fallback analysis only when its key is still the runtime's current key.
   *
   * @return true if the summary was persisted
   */
  public boolean updateTableSummaryIfCurrent(FormatTableAnalysis analysis) {
    return updateTableSummaryIfCurrent(analysis, false);
  }

  /**
   * Re-checks the table's metadata-only key after a planner fallback scan, then persists the
   * analysis only if that freshly observed key is still identical.
   */
  public boolean updateTableSummaryIfCurrent(AmoroTable<?> table, FormatTableAnalysis analysis) {
    Objects.requireNonNull(table, "Table must not be null");
    refreshCurrentAnalysisKey(table);
    return updateTableSummaryIfCurrent(analysis, false);
  }

  private boolean updateTableSummaryIfCurrent(
      FormatTableAnalysis analysis, boolean clearPendingSummary) {
    Objects.requireNonNull(analysis, "Table analysis must not be null");
    verifyAnalysis(analysis);
    AtomicBoolean updated = new AtomicBoolean(false);
    store()
        .synchronizedInvoke(
            () -> {
              if (!Objects.equals(currentAnalysisKey, analysis.key())) {
                return;
              }
              persistFullTableSummary(
                  analysis.pendingInput(), analysis.healthDetails(), clearPendingSummary);
              if (recordRuntimeHealth(analysis)) {
                tableSummaryMetrics.refresh(analysis.pendingInput());
              }
              updated.set(true);
            });
    return updated.get();
  }

  /** Clears the current key and any unconsumed structural facts. */
  public void invalidateCurrentAnalysisKey() {
    store()
        .synchronizedInvoke(
            () -> {
              currentAnalysisKey = null;
              clearTableAnalysis();
            });
  }

  /** Returns whether the current key has no deterministic in-process result and needs one scan. */
  public boolean shouldEvaluateCurrentAnalysis() {
    TableAnalysisKey key = currentAnalysisKey;
    if (key == null) {
      return false;
    }
    return runtimeHealthState.shouldEvaluate(key);
  }

  // ---- OptimizationContext implementation ----

  @Override
  public boolean isIdle() {
    return getOptimizingStatus() == OptimizingStatus.IDLE;
  }

  @Override
  public void updateNonMaintainedSnapshotTime(long timestampMillis) {
    optimizingMetrics.nonMaintainedSnapshotTime(timestampMillis);
  }

  @Override
  public void updateLastOptimizingSnapshotTime(long timestampMillis) {
    optimizingMetrics.lastOptimizingSnapshotTime(timestampMillis);
  }

  /**
   * Evaluate pending input and transition state if necessary.
   *
   * <p>Called by {@code TableRuntimeRefreshExecutor} when a snapshot change is detected. Uses
   * {@link AmoroTable#evaluatePendingInput} for format-specific evaluation. Each format's
   * AmoroTable implementation provides its own evaluation logic.
   *
   * @param table the current AmoroTable for evaluation
   * @param maxPendingPartitions max partitions to scan when evaluating pending input
   * @return true if optimizing demand exists, false otherwise
   */
  public boolean evaluatePendingInputAndTransition(AmoroTable<?> table, int maxPendingPartitions) {
    return evaluatePendingInputAndTransition(table, maxPendingPartitions, false);
  }

  /**
   * Evaluate pending input and optionally bypass format scheduling shortcuts when the persisted
   * health result for the current analysis key is absent or stale.
   */
  public boolean evaluatePendingInputAndTransition(
      AmoroTable<?> table, int maxPendingPartitions, boolean forceHealthEvaluation) {
    OptimizingConfig config = getOptimizingConfig();

    if (!config.isEnabled() && !config.isTableSummaryEnabled()) {
      clearTableAnalysis();
      clearPendingSummary();
      return false;
    }

    Optional<PendingInputResult> result;
    try {
      result =
          forceHealthEvaluation
              ? table.evaluatePendingInput(this, maxPendingPartitions, true)
              : table.evaluatePendingInput(this, maxPendingPartitions);
    } catch (Throwable throwable) {
      clearTableAnalysis();
      throw throwable;
    }
    if (!result.isPresent()) {
      clearTableAnalysis();
      boolean idle = isIdle();
      if (config.isEnabled() && idle) {
        optimizingNotNecessary();
      } else if (!config.isEnabled()) {
        clearPendingSummary();
      }
      return !idle;
    }

    PendingInputResult evalResult = result.get();
    Optional<FormatTableAnalysis> tableAnalysis = evalResult.tableAnalysis();
    try {
      tableAnalysis.ifPresent(this::verifyAnalysis);
    } catch (Throwable throwable) {
      clearTableAnalysis();
      throw throwable;
    }

    TableAnalysisKey expectedKey = currentAnalysisKey;
    if (expectedKey != null) {
      refreshCurrentAnalysisKey(table);
      if (!tableAnalysis.isPresent()
          || !Objects.equals(expectedKey, currentAnalysisKey)
          || !expectedKey.equals(tableAnalysis.get().key())) {
        clearTableAnalysis();
        return !isIdle();
      }
    }
    if (tableAnalysis.isPresent()
        && tableAnalysis.get().key().getTableFormat() == TableFormat.PAIMON
        && currentAnalysisKey == null) {
      clearTableAnalysis();
      return !isIdle();
    }

    if (!config.isEnabled()) {
      persistEvaluationSummaryAndClear(evalResult, true);
      return false;
    }

    if (!isIdle()) {
      persistEvaluationSummaryAndClear(evalResult);
      return true;
    }

    if (evalResult.optimizingNecessary()) {
      return transitionToPending(evalResult);
    } else {
      persistEvaluationSummaryAndClear(evalResult);
      optimizingNotNecessary();
      return false;
    }
  }

  public long getProcessId() {
    TableRuntimeState state =
        getAs(
            TableRuntimeMapper.class,
            mapper ->
                mapper.getState(
                    getTableIdentifier().getId(), DefaultTableRuntime.PROCESS_ID_KEY.getKey()));
    if (state == null || state.getStateValue() == null) {
      return 0L;
    }
    return Long.parseLong(state.getStateValue());
  }

  public boolean tryAcquireProcessOwner(long processId) {
    return compareAndSetProcessOwner(0L, processId);
  }

  public boolean tryReleaseProcessOwner(long processId) {
    return compareAndSetProcessOwner(processId, 0L);
  }

  public boolean normalizeProcessOwner(long processId) {
    return compareAndSetProcessOwner(processId, 0L);
  }

  private boolean compareAndSetProcessOwner(long expected, long next) {
    TableRuntimeState currentState =
        getAs(
            TableRuntimeMapper.class,
            mapper -> mapper.getState(getTableIdentifier().getId(), PROCESS_ID_KEY.getKey()));
    if (currentState == null || currentState.getStateValue() == null) {
      return expected == 0L && next == 0L;
    }
    if (!String.valueOf(expected).equals(currentState.getStateValue())) {
      return false;
    }
    long updated =
        updateAs(
            TableRuntimeMapper.class,
            mapper ->
                mapper.setStateValueIfVersion(
                    getTableIdentifier().getId(),
                    PROCESS_ID_KEY.getKey(),
                    currentState.getStateVersion(),
                    String.valueOf(next)));
    return updated == 1L;
  }

  public OptimizingProcess getOptimizingProcess() {
    return optimizingProcess;
  }

  public void addTaskQuota(TaskRuntime.TaskQuota taskQuota) {
    doAsIgnoreError(OptimizingProcessMapper.class, mapper -> mapper.insertTaskQuota(taskQuota));
    taskQuotas.add(taskQuota);
    long validTime = System.currentTimeMillis() - AmoroServiceConstants.QUOTA_LOOK_BACK_TIME;
    this.taskQuotas.removeIf(task -> task.checkExpired(validTime));
  }

  /**
   * TODO: this is not final solution
   *
   * @param startTimeMills
   */
  public void resetTaskQuotas(long startTimeMills) {
    store()
        .synchronizedInvoke(
            () -> {
              long minProcessId = SnowflakeIdGenerator.getMinSnowflakeId(startTimeMills);
              taskQuotas.clear();
              taskQuotas.addAll(
                  getAs(
                      OptimizingProcessMapper.class,
                      mapper ->
                          mapper.selectTaskQuotasByTime(
                              getTableIdentifier().getId(), minProcessId)));
            });
  }

  public double calculateQuotaOccupy() {
    double targetQuota = getOptimizingConfig().getTargetQuota();
    int targetQuotaLimit =
        targetQuota > 1 ? (int) targetQuota : (int) Math.ceil(targetQuota * getThreadCount());
    return (double) getQuotaTime() / AmoroServiceConstants.QUOTA_LOOK_BACK_TIME / targetQuotaLimit;
  }

  public boolean isAllowPartialCommit() {
    return getOptimizingConfig().isAllowPartialCommit();
  }

  public void setPendingInput(FormatPendingInput pendingInput) {
    store()
        .begin()
        .updateState(pendingInputKey, i -> pendingInput)
        .updateStatusCode(
            code -> {
              if (code == OptimizingStatus.IDLE.getCode()) {
                LOG.info(
                    "{} status changed from idle to pending with pendingInput {}",
                    getTableIdentifier(),
                    pendingInput);
                return OptimizingStatus.PENDING.getCode();
              }
              return code;
            })
        .updateTableSummary(
            summary -> {
              summary.setTotalFileSize(pendingInput.getTotalFileSize());
              summary.setTotalFileCount(pendingInput.getTotalFileCount());
              summary.setPendingFileSize(pendingInput.getTotalFileSize());
              summary.setPendingFileCount(pendingInput.getTotalFileCount());
            })
        .commit();
  }

  public void setTableSummary(FormatPendingInput tableSummary) {
    persistFullTableSummary(tableSummary, null);
    tableSummaryMetrics.refresh(tableSummary);
  }

  public DefaultTableRuntime refresh(AmoroTable<?> table) {
    Map<String, String> tableConfig = table.properties();
    TableConfiguration newConfiguration = TableConfigurations.parseTableConfig(tableConfig);
    String newGroupName = newConfiguration.getOptimizingConfig().getOptimizerGroup();

    if (!Objects.equals(getGroupName(), newGroupName)) {
      if (optimizingProcess != null) {
        optimizingProcess.close(false);
      }
      this.optimizingMetrics.optimizerGroupChanged(getGroupName());
    }

    store()
        .synchronizedInvoke(
            () -> {
              store()
                  .begin()
                  .updateTableConfig(
                      config -> {
                        config.clear();
                        config.putAll(tableConfig);
                      })
                  .updateGroup(g -> newGroupName)
                  .updateState(
                      OPTIMIZING_STATE_KEY,
                      s -> {
                        refreshSnapshots(table, s);
                        return s;
                      })
                  .commit();
              refreshCurrentAnalysisKey(table);
            });
    return this;
  }

  public void beginPlanning() {
    OptimizingStatus originalStatus = getOptimizingStatus();
    store().begin().updateStatusCode(code -> OptimizingStatus.PLANNING.getCode()).commit();
  }

  public void planFailed() {
    clearTableAnalysis();
    OptimizingStatus originalStatus = getOptimizingStatus();
    store().begin().updateStatusCode(code -> OptimizingStatus.PENDING.getCode()).commit();
  }

  public void beginProcess(OptimizingProcess optimizingProcess) {
    clearTableAnalysis();
    Objects.requireNonNull(optimizingProcess, "optimizingProcess is null when beginning process");
    if (!tryAcquireProcessOwner(optimizingProcess.getProcessId())) {
      throw new OptimizingOwnerConflictException(
          "acquire", getTableIdentifier(), optimizingProcess.getProcessId(), getProcessId());
    }
    this.optimizingProcess = optimizingProcess;

    store()
        .begin()
        .updateStatusCode(
            code ->
                OptimizingStatus.ofOptimizingType(optimizingProcess.getOptimizingType()).getCode())
        .updateState(pendingInputKey, any -> pendingInputKey.getDefaultValue())
        .commit();
  }

  public void completeProcess(boolean success) {
    completeProcess(
        Objects.requireNonNull(
            optimizingProcess, "optimizingProcess is null when completing table process"),
        success);
  }

  public void completeProcess(OptimizingProcess process, boolean success) {
    Objects.requireNonNull(process, "process is null when completing table process");
    if (!tryReleaseProcessOwner(process.getProcessId())) {
      long currentOwner = getProcessId();
      if (currentOwner != process.getProcessId()) {
        LOG.warn(
            "Skip completing process {} for table {} because current owner is {}",
            process.getProcessId(),
            getTableIdentifier(),
            currentOwner);
        return;
      }
      throw new OptimizingOwnerConflictException(
          "release", getTableIdentifier(), process.getProcessId(), currentOwner);
    }
    invalidateCurrentAnalysisKey();
    OptimizingType processType = process.getOptimizingType();
    long planTime = process.getPlanTime();

    store()
        .begin()
        .updateState(
            OPTIMIZING_STATE_KEY,
            state -> {
              if (success) {
                state.setLastOptimizedSnapshotId(process.getTargetSnapshotId());
                state.setLastOptimizedChangeSnapshotId(process.getTargetChangeSnapshotId());
              }
              if (processType == OptimizingType.MINOR) {
                state.setLastMinorOptimizingTime(planTime);
              } else if (processType == OptimizingType.MAJOR) {
                state.setLastMajorOptimizingTime(planTime);
              } else if (processType == OptimizingType.FULL) {
                state.setLastFullOptimizingTime(planTime);
              }
              return state;
            })
        .updateTableSummary(
            summary -> {
              summary.setPendingFileSize(0L);
              summary.setPendingFileCount(0);
            })
        .updateStatusCode(code -> OptimizingStatus.IDLE.getCode())
        .commit();

    optimizingMetrics.processComplete(processType, success, planTime);
    if (optimizingProcess == null || optimizingProcess.getProcessId() == process.getProcessId()) {
      optimizingProcess = null;
    }
  }

  /**
   * Resets the table to IDLE from any non-IDLE state. This is used both when planning determines
   * that optimization is unnecessary (from PLANNING state) and during startup recovery to reset
   * tables with unrecoverable processes (from any processing state).
   */
  public void completeEmptyProcess() {
    invalidateCurrentAnalysisKey();
    OptimizingStatus originalStatus = getOptimizingStatus();
    if (originalStatus == OptimizingStatus.IDLE) {
      return;
    }
    long processId = getProcessId();
    if (processId != 0L && !normalizeProcessOwner(processId)) {
      throw new IllegalStateException(
          String.format(
              "failed to normalize optimizing owner for table %s, expected owner %d, current owner %d",
              getTableIdentifier(), processId, getProcessId()));
    }
    store()
        .begin()
        .updateStatusCode(code -> OptimizingStatus.IDLE.getCode())
        .updateState(
            OPTIMIZING_STATE_KEY,
            state -> {
              state.setLastOptimizedSnapshotId(state.getCurrentSnapshotId());
              state.setLastOptimizedChangeSnapshotId(state.getCurrentChangeSnapshotId());
              return state;
            })
        .updateTableSummary(
            summary -> {
              summary.setPendingFileSize(0L);
              summary.setPendingFileCount(0);
            })
        .updateState(pendingInputKey, any -> pendingInputKey.getDefaultValue())
        .commit();
    optimizingProcess = null;
  }

  public void optimizingNotNecessary() {
    clearTableAnalysis();
    if (getOptimizingStatus() == OptimizingStatus.IDLE) {
      store()
          .begin()
          .updateState(
              OPTIMIZING_STATE_KEY,
              state -> {
                state.setLastOptimizedSnapshotId(state.getCurrentSnapshotId());
                state.setLastOptimizedChangeSnapshotId(state.getCurrentChangeSnapshotId());
                return state;
              })
          .updateTableSummary(
              summary -> {
                summary.setPendingFileSize(0L);
                summary.setPendingFileCount(0);
              })
          .commit();
    }
  }

  public void beginCommitting() {
    OptimizingStatus originalStatus = getOptimizingStatus();
    store().begin().updateStatusCode(code -> OptimizingStatus.COMMITTING.getCode()).commit();
  }

  @Override
  public void unregisterMetric() {
    tableSummaryMetrics.unregister();
    orphanFilesCleaningMetrics.unregister();
    optimizingMetrics.unregister();
  }

  @Override
  public void dispose() {
    runtimeHealthState.clear();
    invalidateCurrentAnalysisKey();
    unregisterMetric();
    store()
        .synchronizedInvoke(
            () -> {
              Optional.ofNullable(optimizingProcess).ifPresent(process -> process.close(false));
            });
    super.dispose();
  }

  @Override
  public AmoroTable<?> loadTable() {
    return loader.get();
  }

  /**
   * Check if operation are blocked now.
   *
   * @param operation - operation to check
   * @return true if blocked
   */
  public boolean isBlocked(BlockableOperation operation) {
    List<TableBlocker> tableBlockers =
        getAs(
            TableBlockerMapper.class,
            mapper ->
                mapper.selectBlockers(
                    getTableIdentifier().getCatalog(),
                    getTableIdentifier().getDatabase(),
                    getTableIdentifier().getTableName(),
                    System.currentTimeMillis()));
    return TableBlocker.conflict(operation, tableBlockers);
  }

  private long getQuotaTime() {
    long calculatingEndTime = System.currentTimeMillis();
    long calculatingStartTime = calculatingEndTime - AmoroServiceConstants.QUOTA_LOOK_BACK_TIME;
    taskQuotas.removeIf(task -> task.checkExpired(calculatingStartTime));
    long finishedTaskQuotaTime =
        taskQuotas.stream()
            .mapToLong(taskQuota -> taskQuota.getQuotaTime(calculatingStartTime))
            .sum();
    return optimizingProcess == null
        ? finishedTaskQuotaTime
        : finishedTaskQuotaTime
            + optimizingProcess.getRunningQuotaTime(calculatingStartTime, calculatingEndTime);
  }

  private int getThreadCount() {
    List<OptimizerInstance> instances = getAs(OptimizerMapper.class, OptimizerMapper::selectAll);
    if (instances == null || instances.isEmpty()) {
      return 1;
    }
    String groupName = getGroupName();
    return Math.max(
        instances.stream()
            .filter(instance -> Objects.equals(groupName, instance.getGroupName()))
            .mapToInt(OptimizerInstance::getThreadCount)
            .sum(),
        1);
  }

  private boolean refreshSnapshots(AmoroTable<?> amoroTable, TableRuntimeOptimizingState state) {
    tableSummaryMetrics.refreshSnapshots(amoroTable);
    amoroTable.refreshOptimizingMetrics(this);
    return amoroTable.refreshOptimizingState(state);
  }

  private boolean transitionToPending(PendingInputResult evalResult) {
    AtomicBoolean transitioned = new AtomicBoolean(false);
    FormatTableAnalysis analysis = evalResult.tableAnalysis().orElse(null);
    try {
      store()
          .synchronizedInvoke(
              () -> {
                if (!isIdle()) {
                  if (!analysisMatchesCurrentOrLegacy(analysis)) {
                    return;
                  }
                  persistFullTableSummary(
                      evalResult.pendingInput(),
                      analysis == null ? null : analysis.healthDetails());
                  if (recordRuntimeHealth(analysis)) {
                    tableSummaryMetrics.refresh(evalResult.pendingInput());
                  }
                  return;
                }
                if (!analysisMatchesCurrentOrLegacy(analysis)) {
                  return;
                }
                if (analysis != null && currentAnalysisKey != null) {
                  tableAnalysisSlot.set(analysis);
                }
                try {
                  store()
                      .begin()
                      .updateState(pendingInputKey, ignored -> evalResult.optimizingPendingInput())
                      .updateStatusCode(ignored -> OptimizingStatus.PENDING.getCode())
                      .updateTableSummary(
                          summary -> {
                            updateFullSummary(
                                summary,
                                evalResult.pendingInput(),
                                analysis == null ? null : analysis.healthDetails());
                            summary.setPendingFileSize(
                                evalResult.optimizingPendingInput().getTotalFileSize());
                            summary.setPendingFileCount(
                                evalResult.optimizingPendingInput().getTotalFileCount());
                          })
                      .commit();
                  if (recordRuntimeHealth(analysis)) {
                    tableSummaryMetrics.refresh(evalResult.pendingInput());
                  }
                  transitioned.set(true);
                } catch (Throwable throwable) {
                  if (analysis != null) {
                    tableAnalysisSlot.compareAndSet(analysis, null);
                  }
                  throw throwable;
                }
              });
    } catch (Throwable throwable) {
      clearTableAnalysis();
      throw throwable;
    }
    if (!transitioned.get()) {
      clearTableAnalysis();
      return !isIdle();
    }
    return true;
  }

  private void persistEvaluationSummary(
      PendingInputResult evalResult, boolean clearPendingSummary) {
    Optional<FormatTableAnalysis> analysis = evalResult.tableAnalysis();
    if (analysis.isPresent()
        && (analysis.get().key().getTableFormat() == TableFormat.PAIMON
            || currentAnalysisKey != null)) {
      boolean summaryUpdated = updateTableSummaryIfCurrent(analysis.get(), clearPendingSummary);
      if (!summaryUpdated && clearPendingSummary) {
        // Disabling optimization must retain the legacy unconditional pending-summary clear even
        // when a concurrent metadata change makes the scanned health analysis stale.
        clearPendingSummary();
      }
    } else {
      TableHealthDetails healthDetails =
          analysis.map(FormatTableAnalysis::healthDetails).orElse(null);
      persistFullTableSummary(evalResult.pendingInput(), healthDetails, clearPendingSummary);
      tableSummaryMetrics.refresh(evalResult.pendingInput());
    }
  }

  private boolean analysisMatchesCurrentOrLegacy(FormatTableAnalysis analysis) {
    TableAnalysisKey key = currentAnalysisKey;
    if (key != null) {
      return analysis != null && key.equals(analysis.key());
    }
    return analysis == null || analysis.key().getTableFormat() != TableFormat.PAIMON;
  }

  private boolean recordRuntimeHealth(FormatTableAnalysis analysis) {
    if (analysis == null || analysis.key().getTableFormat() != TableFormat.PAIMON) {
      return true;
    }

    if (runtimeHealthState.update(analysis)) {
      return true;
    }

    TableHealthDetails details = analysis.healthDetails();
    LOG.warn(
        "Retain previous successful Paimon health result for {} after evaluation {} failed with {}",
        getTableIdentifier(),
        details.getEvaluationKey(),
        details.getReasonCodes());
    return false;
  }

  private void persistEvaluationSummaryAndClear(PendingInputResult evalResult) {
    persistEvaluationSummaryAndClear(evalResult, false);
  }

  private void persistEvaluationSummaryAndClear(
      PendingInputResult evalResult, boolean clearPendingSummary) {
    try {
      persistEvaluationSummary(evalResult, clearPendingSummary);
    } finally {
      clearTableAnalysis();
    }
  }

  private void persistFullTableSummary(
      FormatPendingInput tableSummary, TableHealthDetails healthDetails) {
    persistFullTableSummary(tableSummary, healthDetails, false);
  }

  private void persistFullTableSummary(
      FormatPendingInput tableSummary,
      TableHealthDetails healthDetails,
      boolean clearPendingSummary) {
    store()
        .begin()
        .updateTableSummary(
            summary -> {
              updateFullSummary(summary, tableSummary, healthDetails);
              if (clearPendingSummary) {
                summary.setPendingFileSize(0L);
                summary.setPendingFileCount(0);
              }
            })
        .commit();
  }

  private void clearPendingSummary() {
    store()
        .begin()
        .updateTableSummary(
            summary -> {
              summary.setPendingFileSize(0L);
              summary.setPendingFileCount(0);
            })
        .commit();
  }

  private void updateFullSummary(
      TableSummary summary, FormatPendingInput tableSummary, TableHealthDetails healthDetails) {
    summary.setHealthScore(tableSummary.getHealthScore());
    summary.setTotalFileCount(tableSummary.getTotalFileCount());
    summary.setTotalFileSize(tableSummary.getTotalFileSize());
    summary.setHealthDetails(healthDetails);
    if (tableSummary instanceof AbstractOptimizingEvaluator.PendingInput) {
      AbstractOptimizingEvaluator.PendingInput iceInput =
          (AbstractOptimizingEvaluator.PendingInput) tableSummary;
      summary.setSmallFileScore(iceInput.getSmallFileScore());
      summary.setEqualityDeleteScore(iceInput.getEqualityDeleteScore());
      summary.setPositionalDeleteScore(iceInput.getPositionalDeleteScore());
    }
  }

  private void refreshCurrentAnalysisKey(AmoroTable<?> table) {
    store()
        .synchronizedInvoke(
            () -> {
              try {
                Optional<TableAnalysisKey> analysisKey = table.currentAnalysisKey(this);
                updateCurrentAnalysisKey(analysisKey == null ? null : analysisKey.orElse(null));
              } catch (RuntimeException throwable) {
                LOG.warn(
                    "Failed to determine the current analysis key for {}",
                    getTableIdentifier(),
                    throwable);
                updateCurrentAnalysisKey(null);
                clearTableAnalysis();
              }
            });
  }

  private void updateCurrentAnalysisKey(TableAnalysisKey analysisKey) {
    if (!Objects.equals(currentAnalysisKey, analysisKey)) {
      currentAnalysisKey = analysisKey;
      clearTableAnalysis();
    }
  }

  private void clearTableAnalysis() {
    tableAnalysisSlot.set(null);
  }

  private void verifyAnalysis(FormatTableAnalysis analysis) {
    Objects.requireNonNull(analysis.key(), "Table analysis key must not be null");
    Objects.requireNonNull(
        analysis.pendingInput(), "Table analysis pending input must not be null");
    TableHealthDetails healthDetails =
        Objects.requireNonNull(
            analysis.healthDetails(), "Table analysis health details must not be null");
    if (!analysis.key().encoded().equals(healthDetails.getEvaluationKey())) {
      throw new IllegalArgumentException("Table analysis key and health details are inconsistent");
    }
  }
}
