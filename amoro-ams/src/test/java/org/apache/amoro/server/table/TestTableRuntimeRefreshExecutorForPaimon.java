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

import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_HEALTH_SCORE;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_TOTAL_FILES;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_TOTAL_FILES_SIZE;
import static org.apache.amoro.server.table.TableSummaryMetrics.TABLE_SUMMARY_TOTAL_RECORDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.TableSnapshot;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.formats.paimon.PaimonHadoopCatalogTestHelper;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.Metric;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.AMSServiceTestBase;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.optimizing.OptimizingProcess;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.scheduler.inline.TableRuntimeRefreshExecutor;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Verifies that {@link TableRuntimeRefreshExecutor} transitions Paimon tables from IDLE to PENDING
 * when a new snapshot exists, and stays IDLE when no snapshot change or optimizing is disabled.
 *
 * <p>Uses a real H2-backed {@link DefaultTableRuntime} with Paimon-typed pending input key (so
 * state transitions actually persist) but overrides {@code loadTable()} to return a Mockito mock
 * instead of hitting a real filesystem catalog.
 */
public class TestTableRuntimeRefreshExecutorForPaimon extends AMSServiceTestBase {

  private static final int MAX_PENDING_PARTITIONS = 1;
  private static final long INTERVAL = 60_000L;
  private static final String CATALOG_NAME = "test_paimon_catalog";
  private static final String DB_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final StateKey<PaimonPendingInput> PAIMON_PENDING_INPUT_KEY =
      StateKey.stateKey("pending_input")
          .jsonType(PaimonPendingInput.class)
          .defaultValue(new PaimonPendingInput());

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private PaimonHadoopCatalogTestHelper catalogHelper;
  private DefaultTableRuntime paimonRuntime;

  @Before
  public void setUp() throws Exception {
    String warehouse = temp.newFolder("warehouse").getAbsolutePath();
    catalogHelper = new PaimonHadoopCatalogTestHelper(CATALOG_NAME, new java.util.HashMap<>());
    catalogHelper.initWarehouse(warehouse);

    CATALOG_MANAGER.createCatalog(catalogHelper.getCatalogMeta());
    catalogHelper.createDatabase(DB_NAME);
    catalogHelper.createTable(DB_NAME, TABLE_NAME);

    tableService().exploreTableRuntimes();

    ServerTableIdentifier id =
        tableManager().listManagedTables().stream()
            .filter(s -> TABLE_NAME.equals(s.getTableName()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Paimon table not registered"));

    DefaultTableRuntime runtime = (DefaultTableRuntime) tableService().getRuntime(id.getId());
    if (!(runtime instanceof DefaultTableRuntime)) {
      throw new IllegalStateException(
          "Expected DefaultTableRuntime, got " + runtime.getClass().getName());
    }
    paimonRuntime = runtime;
    // This class verifies the inline refresh executor in isolation. Config lifecycle reconciliation
    // belongs to DefaultOptimizingService/OptimizingQueue and has its own persisted-process tests.
    ((DefaultTableRuntimeStore) paimonRuntime.store()).setRuntimeHandler(null);

    // The optimizer handler chain may have already transitioned the runtime to PENDING
    // during exploreTableRuntimes(). Reset to IDLE so each test starts clean.
    paimonRuntime.completeEmptyProcess();
  }

  @After
  public void tearDown() throws Exception {
    try {
      catalogHelper.clean();
    } catch (Exception ignored) {
    }
    CATALOG_MANAGER.dropCatalog(CATALOG_NAME);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private TableSnapshot snapshotWithId(long id) {
    TableSnapshot s = mock(TableSnapshot.class);
    when(s.id()).thenReturn(String.valueOf(id));
    return s;
  }

  private AmoroTable<?> paimonAmoroTable(TableSnapshot snapshot) {
    return paimonAmoroTable(snapshot, true);
  }

  private AmoroTable<?> paimonAmoroTable(TableSnapshot snapshot, boolean optimizingNecessary) {
    AmoroTable<?> t = mock(AmoroTable.class);
    when(t.format()).thenReturn(TableFormat.PAIMON);
    when(t.originalTable())
        .thenReturn(mock(org.apache.paimon.table.AppendOnlyFileStoreTable.class));
    when(t.currentSnapshot()).thenReturn(snapshot);
    when(t.refreshOptimizingState(any())).thenCallRealMethod();
    when(t.properties()).thenReturn(new java.util.HashMap<>());
    // Mock evaluatePendingInput to return PaimonPendingInput when called
    PaimonPendingInput pendingInput = new PaimonPendingInput();
    when(t.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(pendingInput, optimizingNecessary)));
    return t;
  }

  private TableRuntimeRefreshExecutor executorWith(AmoroTable<?> table) {
    return new TableRuntimeRefreshExecutor(tableService(), 1, INTERVAL, MAX_PENDING_PARTITIONS) {
      @Override
      protected AmoroTable<?> loadTable(TableRuntime tableRuntime) {
        return table;
      }
    };
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  public void newSnapshotTransitionsIdleToPending() throws Exception {
    AmoroTable<?> mockAmoroTable = paimonAmoroTable(snapshotWithId(100L));
    TableRuntimeRefreshExecutor executor = executorWith(mockAmoroTable);

    assertEquals(OptimizingStatus.IDLE, paimonRuntime.getOptimizingStatus());

    executor.execute(paimonRuntime);

    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());
  }

  @Test
  public void noNewSnapshotKeepsIdle() throws Exception {
    AmoroTable<?> mockAmoroTable = paimonAmoroTable(null);
    TableRuntimeRefreshExecutor executor = executorWith(mockAmoroTable);
    // Normalize state left by a previous refresh of the reused runtime before asserting no change.
    paimonRuntime.refresh(mockAmoroTable);
    paimonRuntime.optimizingNotNecessary();

    executor.execute(paimonRuntime);

    assertEquals(OptimizingStatus.IDLE, paimonRuntime.getOptimizingStatus());
  }

  @Test
  public void newSnapshotWithNoOptimizingDemandKeepsIdle() throws Exception {
    AmoroTable<?> mockAmoroTable = paimonAmoroTable(snapshotWithId(150L), false);
    TableRuntimeRefreshExecutor executor = executorWith(mockAmoroTable);

    executor.execute(paimonRuntime);

    assertEquals(OptimizingStatus.IDLE, paimonRuntime.getOptimizingStatus());
  }

  @Test
  public void optimizingDisabledKeepsIdle() throws Exception {
    AmoroTable<?> mockAmoroTable = paimonAmoroTable(snapshotWithId(200L));
    java.util.Map<String, String> disabledProps = new java.util.HashMap<>();
    disabledProps.put("self-optimizing.enabled", "false");
    when(mockAmoroTable.properties()).thenReturn(disabledProps);

    TableRuntimeRefreshExecutor executor = executorWith(mockAmoroTable);

    // Refresh so the runtime picks up disabled config
    paimonRuntime.refresh(mockAmoroTable);
    executor.execute(paimonRuntime);

    assertEquals(OptimizingStatus.IDLE, paimonRuntime.getOptimizingStatus());
  }

  @Test
  public void sameSnapshotEligibilityChangesAreEvaluatedInBothDirections() {
    long snapshotId = 210L;
    TableAnalysisKey eligibleKey =
        analysisKey(snapshotId, 1L, "writeOnly=true,selfOptimizing=true");
    TableAnalysisKey ineligibleKey =
        analysisKey(snapshotId, 2L, "writeOnly=true,selfOptimizing=null");
    TableAnalysisKey restoredEligibleKey =
        analysisKey(snapshotId, 3L, "writeOnly=true,selfOptimizing=true");
    PaimonPendingInput input = pendingInput(4, 400L, 82);
    AmoroTable<?> eligibleTable =
        keyedPaimonTable(
            snapshotWithId(snapshotId),
            eligibleKey,
            input,
            true,
            analysis(eligibleKey, input, Collections.emptyList()));
    AmoroTable<?> ineligibleTable =
        keyedPaimonTable(
            snapshotWithId(snapshotId),
            ineligibleKey,
            input,
            false,
            analysis(ineligibleKey, input, Collections.emptyList()));
    AmoroTable<?> restoredEligibleTable =
        keyedPaimonTable(
            snapshotWithId(snapshotId),
            restoredEligibleKey,
            input,
            true,
            analysis(restoredEligibleKey, input, Collections.emptyList()));
    java.util.Map<String, String> eligibleProperties = new java.util.HashMap<>();
    eligibleProperties.put("write-only", "true");
    eligibleProperties.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");
    when(eligibleTable.properties()).thenReturn(eligibleProperties);
    when(restoredEligibleTable.properties()).thenReturn(eligibleProperties);
    when(ineligibleTable.properties()).thenReturn(Collections.singletonMap("write-only", "true"));
    when(ineligibleTable.evaluatePendingInput(any(), anyInt())).thenReturn(Optional.empty());
    when(ineligibleTable.evaluatePendingInput(any(), anyInt(), anyBoolean()))
        .thenReturn(Optional.empty());

    executorWith(eligibleTable).execute(paimonRuntime);
    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());

    executorWith(ineligibleTable).execute(paimonRuntime);
    verify(ineligibleTable, times(1)).evaluatePendingInput(any(), anyInt(), eq(true));
    assertEquals(Optional.of(ineligibleKey), paimonRuntime.getCurrentAnalysisKey());

    // Model the existing empty-plan closure for the PENDING table that lost eligibility.
    paimonRuntime.completeEmptyProcess();
    assertEquals(OptimizingStatus.IDLE, paimonRuntime.getOptimizingStatus());

    executorWith(restoredEligibleTable).execute(paimonRuntime);
    verify(eligibleTable, times(1)).evaluatePendingInput(any(), anyInt(), eq(true));
    verify(restoredEligibleTable, times(1)).evaluatePendingInput(any(), anyInt(), eq(true));
    assertEquals(Optional.of(restoredEligibleKey), paimonRuntime.getCurrentAnalysisKey());
    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());
  }

  @Test
  public void disablingSelfOptimizingDoesNotCloseProcessInRefreshExecutor() {
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    OptimizingProcess process = mock(OptimizingProcess.class);
    TableConfiguration originalConfig =
        new TableConfiguration().setOptimizingConfig(new OptimizingConfig().setEnabled(true));
    TableConfiguration disabledConfig =
        new TableConfiguration().setOptimizingConfig(new OptimizingConfig().setEnabled(false));
    when(runtime.getTableConfiguration()).thenReturn(disabledConfig);
    when(runtime.getOptimizingProcess()).thenReturn(process);
    when(process.getStatus()).thenReturn(ProcessStatus.RUNNING);

    executorWith(paimonAmoroTable(null)).handleConfigChanged(runtime, originalConfig);

    verify(process, never()).close(anyBoolean());
  }

  @Test
  public void removingSelfOptimizingPropertyDoesNotCloseRunningProcess() {
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    OptimizingProcess process = mock(OptimizingProcess.class);
    TableConfiguration originalConfig =
        new TableConfiguration().setOptimizingConfig(new OptimizingConfig().setEnabled(true));
    TableConfiguration missingPropertyConfig =
        TableConfigurations.parseTableConfig(Collections.singletonMap("write-only", "true"));
    assertTrue(missingPropertyConfig.getOptimizingConfig().isEnabled());
    when(runtime.getTableConfiguration()).thenReturn(missingPropertyConfig);
    when(runtime.getOptimizingProcess()).thenReturn(process);
    when(process.getStatus()).thenReturn(ProcessStatus.RUNNING);

    executorWith(paimonAmoroTable(null)).handleConfigChanged(runtime, originalConfig);

    verify(process, never()).close(anyBoolean());
  }

  @Test
  public void alreadyPendingDoesNotDoubleTrigger() throws Exception {
    AmoroTable<?> mockAmoroTable = paimonAmoroTable(snapshotWithId(300L));
    TableRuntimeRefreshExecutor executor = executorWith(mockAmoroTable);
    executor.execute(paimonRuntime);
    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());

    // Second execute with same snapshot
    executor.execute(paimonRuntime);

    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());
  }

  @Test
  public void planningStatusDoesNotReTrigger() throws Exception {
    AmoroTable<?> mockAmoroTable = paimonAmoroTable(snapshotWithId(400L));
    TableRuntimeRefreshExecutor executor = executorWith(mockAmoroTable);
    executor.execute(paimonRuntime);
    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());
    paimonRuntime.beginPlanning();
    assertEquals(OptimizingStatus.PLANNING, paimonRuntime.getOptimizingStatus());

    // Execute again while PLANNING
    executor.execute(paimonRuntime);

    assertEquals(OptimizingStatus.PLANNING, paimonRuntime.getOptimizingStatus());
  }

  @Test
  public void sameKeyWithSuccessfulHealthDoesNotScanAgain() {
    TableAnalysisKey key = analysisKey(500L);
    PaimonPendingInput input = pendingInput(4, 400L, 82);
    FormatTableAnalysis analysis = analysis(key, input, Collections.emptyList());
    AmoroTable<?> table = keyedPaimonTable(snapshotWithId(500L), key, input, false, analysis);
    java.util.Map<String, String> adaptiveProperties = new java.util.HashMap<>();
    adaptiveProperties.put("self-optimizing.refresh-table.adaptive.max-interval-ms", "120000");
    when(table.properties()).thenReturn(adaptiveProperties);
    TableRuntimeRefreshExecutor executor = executorWith(table);

    executor.execute(paimonRuntime);
    executor.execute(paimonRuntime);

    verify(table, times(1)).evaluatePendingInput(any(), anyInt(), eq(true));
    assertFalse(paimonRuntime.getLatestEvaluatedNeedOptimizing());
    assertEquals(120_000L, paimonRuntime.getLatestRefreshInterval());
    assertEquals(82, paimonRuntime.getRuntimeHealthSnapshot().get().getHealthScore());
    assertEquals(
        82,
        tableManager()
            .getTableRuntimeMata(paimonRuntime.getTableIdentifier())
            .getTableSummary()
            .getHealthScore());
  }

  @Test
  public void successfulHealthSnapshotAndGaugesSurviveLaterInvalidEvaluation() {
    TableAnalysisKey successfulKey = analysisKey(520L);
    PaimonPendingInput successfulInput = pendingInput(4, 400L, 82);
    FormatTableAnalysis successfulAnalysis =
        analysis(successfulKey, successfulInput, Collections.emptyList());
    executorWith(
            keyedPaimonTable(
                snapshotWithId(520L), successfulKey, successfulInput, false, successfulAnalysis))
        .execute(paimonRuntime);

    RuntimeHealthSnapshot successfulSnapshot = paimonRuntime.getRuntimeHealthSnapshot().get();
    assertEquals(82, successfulSnapshot.getHealthScore());
    assertEquals(successfulKey.encoded(), successfulSnapshot.getHealthDetails().getEvaluationKey());
    assertEquals(82L, metricValue(TABLE_SUMMARY_HEALTH_SCORE));
    assertEquals(4L, metricValue(TABLE_SUMMARY_TOTAL_FILES));
    assertEquals(400L, metricValue(TABLE_SUMMARY_TOTAL_FILES_SIZE));
    assertEquals(40L, metricValue(TABLE_SUMMARY_TOTAL_RECORDS));

    TableAnalysisKey invalidKey = analysisKey(521L);
    PaimonPendingInput invalidInput = pendingInput(9, 900L, -1);
    FormatTableAnalysis invalidAnalysis =
        analysis(invalidKey, invalidInput, Collections.singletonList("INVALID_SCORING_CONFIG"));
    executorWith(
            keyedPaimonTable(snapshotWithId(521L), invalidKey, invalidInput, true, invalidAnalysis))
        .execute(paimonRuntime);

    assertEquals(
        "An invalid health result may still carry valid optimization planning facts",
        OptimizingStatus.PENDING,
        paimonRuntime.getOptimizingStatus());
    RuntimeHealthSnapshot retainedSnapshot = paimonRuntime.getRuntimeHealthSnapshot().get();
    assertEquals(82, retainedSnapshot.getHealthScore());
    assertEquals(successfulKey.encoded(), retainedSnapshot.getHealthDetails().getEvaluationKey());
    assertEquals(82L, metricValue(TABLE_SUMMARY_HEALTH_SCORE));
    assertEquals(4L, metricValue(TABLE_SUMMARY_TOTAL_FILES));
    assertEquals(400L, metricValue(TABLE_SUMMARY_TOTAL_FILES_SIZE));
    assertEquals(40L, metricValue(TABLE_SUMMARY_TOTAL_RECORDS));
  }

  @Test
  public void firstInvalidEvaluationPublishesUnavailableSnapshotWithReason() {
    TableAnalysisKey key = analysisKey(530L);
    PaimonPendingInput input = pendingInput(1, 10L, -1);
    FormatTableAnalysis analysis =
        analysis(key, input, Collections.singletonList("SNAPSHOT_SCAN_FAILED"));
    AmoroTable<?> table = keyedPaimonTable(snapshotWithId(530L), key, input, false, analysis);
    DefaultTableRuntime freshRuntime =
        new DefaultTableRuntime(paimonRuntime.store(), () -> table, PAIMON_PENDING_INPUT_KEY);
    freshRuntime.refresh(table);

    freshRuntime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS, true);

    RuntimeHealthSnapshot snapshot = freshRuntime.getRuntimeHealthSnapshot().get();
    assertEquals(-1, snapshot.getHealthScore());
    assertEquals(
        Collections.singletonList("SNAPSHOT_SCAN_FAILED"),
        snapshot.getHealthDetails().getReasonCodes());
  }

  @Test
  public void invalidatingAnalysisKeyDoesNotClearSuccessfulRuntimeHealth() {
    TableAnalysisKey key = analysisKey(540L);
    PaimonPendingInput input = pendingInput(2, 200L, 76);
    FormatTableAnalysis analysis = analysis(key, input, Collections.emptyList());
    executorWith(keyedPaimonTable(snapshotWithId(540L), key, input, false, analysis))
        .execute(paimonRuntime);

    paimonRuntime.invalidateCurrentAnalysisKey();

    RuntimeHealthSnapshot snapshot = paimonRuntime.getRuntimeHealthSnapshot().get();
    assertEquals(76, snapshot.getHealthScore());
    assertEquals(key.encoded(), snapshot.getHealthDetails().getEvaluationKey());
  }

  @Test
  public void newRuntimeDoesNotRestorePersistedHealthAndMustEvaluateCurrentKey() {
    TableAnalysisKey key = analysisKey(550L);
    TableHealthDetails persistedDetails =
        analysis(key, pendingInput(5, 500L, 88), Collections.emptyList()).healthDetails();
    paimonRuntime
        .store()
        .begin()
        .updateTableSummary(
            summary -> {
              summary.setHealthScore(88);
              summary.setHealthDetails(persistedDetails);
            })
        .commit();
    AmoroTable<?> table =
        keyedPaimonTable(
            snapshotWithId(550L),
            key,
            pendingInput(5, 500L, 88),
            false,
            analysis(key, pendingInput(5, 500L, 88), Collections.emptyList()));

    DefaultTableRuntime restartedRuntime =
        new DefaultTableRuntime(paimonRuntime.store(), () -> table, PAIMON_PENDING_INPUT_KEY);
    restartedRuntime.refresh(table);

    assertFalse(restartedRuntime.getRuntimeHealthSnapshot().isPresent());
    assertTrue(restartedRuntime.shouldEvaluateCurrentAnalysis());
  }

  @Test
  public void idleUnoptimizedSnapshotRetriesWithoutWaitingForHealthKeyChange() {
    TableAnalysisKey key = analysisKey(505L);
    PaimonPendingInput input = pendingInput(4, 400L, 82);
    FormatTableAnalysis analysis = analysis(key, input, Collections.emptyList());
    AmoroTable<?> table = keyedPaimonTable(snapshotWithId(505L), key, input, true, analysis);
    TableRuntimeRefreshExecutor executor = executorWith(table);

    executor.execute(paimonRuntime);
    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());

    // Model completeProcess(false): the failed process returns to IDLE without advancing the last
    // optimized snapshot, while its same-key health summary remains persisted.
    paimonRuntime.invalidateCurrentAnalysisKey();
    paimonRuntime
        .store()
        .begin()
        .updateStatusCode(ignored -> OptimizingStatus.IDLE.getCode())
        .commit();

    executor.execute(paimonRuntime);

    verify(table, times(1)).evaluatePendingInput(any(), anyInt(), eq(true));
    verify(table, times(1)).evaluatePendingInput(any(), anyInt());
    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());
  }

  @Test
  public void pendingSameKeyPreservesAdaptiveDemandWithoutRescan() {
    TableAnalysisKey key = analysisKey(506L);
    PaimonPendingInput input = pendingInput(4, 400L, 82);
    FormatTableAnalysis analysis = analysis(key, input, Collections.emptyList());
    AmoroTable<?> table = keyedPaimonTable(snapshotWithId(506L), key, input, true, analysis);
    java.util.Map<String, String> adaptiveProperties = new java.util.HashMap<>();
    adaptiveProperties.put("self-optimizing.refresh-table.adaptive.max-interval-ms", "120000");
    when(table.properties()).thenReturn(adaptiveProperties);
    TableRuntimeRefreshExecutor executor = executorWith(table);

    executor.execute(paimonRuntime);
    assertTrue(paimonRuntime.getLatestEvaluatedNeedOptimizing());

    executor.execute(paimonRuntime);

    verify(table, times(1)).evaluatePendingInput(any(), anyInt(), eq(true));
    assertTrue(paimonRuntime.getLatestEvaluatedNeedOptimizing());
    assertEquals(INTERVAL, paimonRuntime.getLatestRefreshInterval());
  }

  @Test
  public void legacyFormatWithoutAnalysisKeyKeepsFalseOnSkippedEvaluation() {
    AmoroTable<?> table = paimonAmoroTable(null, false);
    when(table.format()).thenReturn(TableFormat.ICEBERG);
    java.util.Map<String, String> adaptiveProperties = new java.util.HashMap<>();
    adaptiveProperties.put("self-optimizing.refresh-table.adaptive.max-interval-ms", "120000");
    when(table.properties()).thenReturn(adaptiveProperties);
    paimonRuntime.refresh(table);
    paimonRuntime.optimizingNotNecessary();
    paimonRuntime.setLatestEvaluatedNeedOptimizing(true);
    paimonRuntime.setLatestRefreshInterval(INTERVAL);

    executorWith(table).execute(paimonRuntime);

    verify(table, never()).evaluatePendingInput(any(), anyInt());
    assertFalse(paimonRuntime.getLatestEvaluatedNeedOptimizing());
    assertEquals(90_000L, paimonRuntime.getLatestRefreshInterval());
  }

  @Test
  public void missingHealthSummaryForKeyForcesEvaluationWithoutSnapshotChange() {
    TableAnalysisKey key =
        new TableAnalysisKey(
            String.valueOf(paimonRuntime.getTableIdentifier().getId()),
            TableFormat.ICEBERG,
            TableAnalysisKey.NO_SNAPSHOT,
            TableAnalysisKey.NO_CHANGE_SNAPSHOT,
            1L,
            "fingerprint",
            "iceberg-legacy-v1",
            TableAnalysisKey.NO_BASELINE,
            TableAnalysisKey.NO_BASELINE_TIME);
    PaimonPendingInput input = pendingInput(0, 0L, 100);
    FormatTableAnalysis analysis = analysis(key, input, Collections.emptyList());
    AmoroTable<?> table = mock(AmoroTable.class);
    when(table.format()).thenReturn(TableFormat.ICEBERG);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentSnapshot()).thenReturn(null);
    when(table.refreshOptimizingState(any())).thenCallRealMethod();
    when(table.currentAnalysisKey(any())).thenReturn(Optional.of(key));
    when(table.evaluatePendingInput(any(), anyInt(), eq(true)))
        .thenReturn(Optional.of(new PendingInputResult(input, false, analysis)));

    executorWith(table).execute(paimonRuntime);

    verify(table, times(1)).evaluatePendingInput(any(), anyInt(), eq(true));
    verify(table, never()).evaluatePendingInput(any(), anyInt());
    assertEquals(
        100,
        tableManager()
            .getTableRuntimeMata(paimonRuntime.getTableIdentifier())
            .getTableSummary()
            .getHealthScore());
  }

  @Test
  public void sameKeyRetriesOnlySnapshotScanFailure() {
    TableAnalysisKey retryKey = analysisKey(510L);
    PaimonPendingInput retryInput = pendingInput(1, 10L, -1);
    FormatTableAnalysis retryAnalysis =
        analysis(retryKey, retryInput, Collections.singletonList("SNAPSHOT_SCAN_FAILED"));
    AmoroTable<?> retryTable =
        keyedPaimonTable(snapshotWithId(510L), retryKey, retryInput, false, retryAnalysis);
    TableRuntimeRefreshExecutor retryExecutor = executorWith(retryTable);

    retryExecutor.execute(paimonRuntime);
    retryExecutor.execute(paimonRuntime);

    verify(retryTable, times(2)).evaluatePendingInput(any(), anyInt(), eq(true));

    TableAnalysisKey deterministicKey = analysisKey(511L);
    PaimonPendingInput deterministicInput = pendingInput(1, 10L, -1);
    FormatTableAnalysis deterministicAnalysis =
        analysis(
            deterministicKey,
            deterministicInput,
            Collections.singletonList("INVALID_SCORING_CONFIG"));
    AmoroTable<?> deterministicTable =
        keyedPaimonTable(
            snapshotWithId(511L),
            deterministicKey,
            deterministicInput,
            false,
            deterministicAnalysis);
    TableRuntimeRefreshExecutor deterministicExecutor = executorWith(deterministicTable);

    deterministicExecutor.execute(paimonRuntime);
    deterministicExecutor.execute(paimonRuntime);

    verify(deterministicTable, times(1)).evaluatePendingInput(any(), anyInt(), eq(true));
  }

  @Test
  public void changedKeyWhilePendingOnlyRefreshesFullSummary() {
    TableAnalysisKey firstKey = analysisKey(600L);
    TableAnalysisKey secondKey = analysisKey(601L);
    PaimonPendingInput firstInput = pendingInput(3, 300L, 70);
    PaimonPendingInput secondInput = pendingInput(9, 900L, 40);
    FormatTableAnalysis firstAnalysis = analysis(firstKey, firstInput, Collections.emptyList());
    FormatTableAnalysis secondAnalysis = analysis(secondKey, secondInput, Collections.emptyList());
    TableSnapshot firstSnapshot = snapshotWithId(600L);
    TableSnapshot secondSnapshot = snapshotWithId(601L);

    AmoroTable<?> table = mock(AmoroTable.class);
    when(table.format()).thenReturn(TableFormat.PAIMON);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentSnapshot()).thenReturn(firstSnapshot, secondSnapshot);
    when(table.refreshOptimizingState(any())).thenCallRealMethod();
    when(table.currentAnalysisKey(any()))
        .thenReturn(
            Optional.of(firstKey),
            Optional.of(firstKey),
            Optional.of(secondKey),
            Optional.of(secondKey));
    when(table.evaluatePendingInput(any(), anyInt(), anyBoolean()))
        .thenReturn(
            Optional.of(new PendingInputResult(firstInput, true, firstAnalysis)),
            Optional.of(new PendingInputResult(secondInput, true, secondAnalysis)));
    TableRuntimeRefreshExecutor executor = executorWith(table);

    executor.execute(paimonRuntime);
    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());
    executor.execute(paimonRuntime);

    assertEquals(OptimizingStatus.PENDING, paimonRuntime.getOptimizingStatus());
    assertEquals(3, paimonRuntime.getPendingInput().getTotalFileCount());
    assertEquals(
        9,
        tableManager()
            .getTableRuntimeMata(paimonRuntime.getTableIdentifier())
            .getTableSummary()
            .getTotalFileCount());
    assertEquals(
        40,
        tableManager()
            .getTableRuntimeMata(paimonRuntime.getTableIdentifier())
            .getTableSummary()
            .getHealthScore());
    assertEquals(40, paimonRuntime.getRuntimeHealthSnapshot().get().getHealthScore());
    assertEquals(
        secondKey.encoded(),
        paimonRuntime.getRuntimeHealthSnapshot().get().getHealthDetails().getEvaluationKey());
    assertFalse(paimonRuntime.takeTableAnalysis(firstKey).isPresent());
  }

  @Test
  public void concurrentKeyChangeDropsStaleEvaluation() {
    TableAnalysisKey evaluatedKey = analysisKey(700L);
    TableAnalysisKey currentKey = analysisKey(701L);
    PaimonPendingInput input = pendingInput(7, 700L, 55);
    FormatTableAnalysis analysis = analysis(evaluatedKey, input, Collections.emptyList());
    TableSnapshot snapshot = snapshotWithId(700L);
    AmoroTable<?> table = mock(AmoroTable.class);
    when(table.format()).thenReturn(TableFormat.PAIMON);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentSnapshot()).thenReturn(snapshot);
    when(table.refreshOptimizingState(any())).thenCallRealMethod();
    when(table.currentAnalysisKey(any()))
        .thenReturn(Optional.of(evaluatedKey), Optional.of(currentKey));
    when(table.evaluatePendingInput(any(), anyInt(), anyBoolean()))
        .thenReturn(Optional.of(new PendingInputResult(input, false, analysis)));
    TableHealthDetails summaryBefore =
        tableManager()
            .getTableRuntimeMata(paimonRuntime.getTableIdentifier())
            .getTableSummary()
            .getHealthDetails();

    executorWith(table).execute(paimonRuntime);

    assertEquals(Optional.of(currentKey), paimonRuntime.getCurrentAnalysisKey());
    assertEquals(
        summaryBefore,
        tableManager()
            .getTableRuntimeMata(paimonRuntime.getTableIdentifier())
            .getTableSummary()
            .getHealthDetails());
  }

  @Test
  public void tableSummaryOnlyNeverPublishesPending() {
    TableAnalysisKey key = analysisKey(800L);
    PaimonPendingInput input = pendingInput(8, 800L, 66);
    FormatTableAnalysis analysis = analysis(key, input, Collections.emptyList());
    AmoroTable<?> table = keyedPaimonTable(snapshotWithId(800L), key, input, true, analysis);
    java.util.Map<String, String> properties = new java.util.HashMap<>();
    properties.put("self-optimizing.enabled", "false");
    properties.put("table-summary.enabled", "true");
    when(table.properties()).thenReturn(properties);

    executorWith(table).execute(paimonRuntime);

    assertEquals(OptimizingStatus.IDLE, paimonRuntime.getOptimizingStatus());
    assertEquals(
        0,
        tableManager()
            .getTableRuntimeMata(paimonRuntime.getTableIdentifier())
            .getTableSummary()
            .getPendingFileCount());
    assertEquals(
        66,
        tableManager()
            .getTableRuntimeMata(paimonRuntime.getTableIdentifier())
            .getTableSummary()
            .getHealthScore());
  }

  private AmoroTable<?> keyedPaimonTable(
      TableSnapshot snapshot,
      TableAnalysisKey key,
      PaimonPendingInput input,
      boolean optimizingNecessary,
      FormatTableAnalysis analysis) {
    AmoroTable<?> table = paimonAmoroTable(snapshot, optimizingNecessary);
    when(table.currentAnalysisKey(any())).thenReturn(Optional.of(key));
    Optional<PendingInputResult> result =
        Optional.of(new PendingInputResult(input, optimizingNecessary, analysis));
    when(table.evaluatePendingInput(any(), anyInt())).thenReturn(result);
    when(table.evaluatePendingInput(any(), anyInt(), anyBoolean())).thenReturn(result);
    return table;
  }

  private TableAnalysisKey analysisKey(long snapshotId) {
    return analysisKey(snapshotId, "fingerprint");
  }

  private TableAnalysisKey analysisKey(long snapshotId, String fingerprint) {
    return analysisKey(snapshotId, 1L, fingerprint);
  }

  private TableAnalysisKey analysisKey(long snapshotId, long schemaId, String fingerprint) {
    return new TableAnalysisKey(
        String.valueOf(paimonRuntime.getTableIdentifier().getId()),
        TableFormat.PAIMON,
        snapshotId,
        TableAnalysisKey.NO_CHANGE_SNAPSHOT,
        schemaId,
        fingerprint,
        "test-paimon-health-v1",
        TableAnalysisKey.NO_BASELINE,
        TableAnalysisKey.NO_BASELINE_TIME);
  }

  private PaimonPendingInput pendingInput(int fileCount, long fileSize, int healthScore) {
    PaimonPendingInput input = new PaimonPendingInput();
    input.setDataFileCount(fileCount);
    input.setDataFileSize(fileSize);
    input.setDataRecordCount(fileCount * 10L);
    input.setHealthScore(healthScore);
    return input;
  }

  @SuppressWarnings("unchecked")
  private long metricValue(MetricDefine metricDefine) {
    Map<MetricKey, Metric> metrics = MetricManager.getInstance().getGlobalRegistry().getMetrics();
    MetricKey key =
        new MetricKey(
            metricDefine,
            ImmutableMap.of(
                "catalog",
                paimonRuntime.getTableIdentifier().getCatalog(),
                "database",
                paimonRuntime.getTableIdentifier().getDatabase(),
                "table",
                paimonRuntime.getTableIdentifier().getTableName()));
    return ((Gauge<Long>) metrics.get(key)).getValue();
  }

  private FormatTableAnalysis analysis(
      TableAnalysisKey key, PaimonPendingInput input, java.util.List<String> reasonCodes) {
    TableHealthDetails healthDetails =
        new TableHealthDetails(
            key.getFormulaVersion(),
            key.getSnapshotId(),
            null,
            key.getSchemaId(),
            key.getScoringConfigFingerprint(),
            key.encoded(),
            Collections.emptyList(),
            Collections.emptyMap(),
            reasonCodes);
    return new FormatTableAnalysis() {
      @Override
      public TableAnalysisKey key() {
        return key;
      }

      @Override
      public PaimonPendingInput pendingInput() {
        return input;
      }

      @Override
      public TableHealthDetails healthDetails() {
        return healthDetails;
      }
    };
  }
}
