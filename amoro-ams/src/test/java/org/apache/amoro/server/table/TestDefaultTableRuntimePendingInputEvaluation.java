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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.TableFormat;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.optimizing.OptimizingProcess;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TestDefaultTableRuntimePendingInputEvaluation extends AMSTableTestBase {

  private static final int MAX_PENDING_PARTITIONS = 7;

  public TestDefaultTableRuntimePendingInputEvaluation() {
    super(
        new BasicCatalogTestHelper(TableFormat.ICEBERG),
        new BasicTableTestHelper(true, false),
        true);
  }

  @Test
  public void testEvaluatePendingInputReceivesConfiguredMaxPendingPartitions() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    AbstractOptimizingEvaluator.PendingInput pendingInput =
        new AbstractOptimizingEvaluator.PendingInput();
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(pendingInput, true)));

    boolean optimizingNeeded =
        runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS);

    Assert.assertTrue(optimizingNeeded);
    Assert.assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
    verify(table).evaluatePendingInput(any(), eq(MAX_PENDING_PARTITIONS));
  }

  @Test
  public void testEvaluatePendingInputFalseKeepsRuntimeIdle() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    AbstractOptimizingEvaluator.PendingInput pendingInput =
        new AbstractOptimizingEvaluator.PendingInput();
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(pendingInput, false)));

    boolean optimizingNeeded =
        runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS);

    Assert.assertFalse(optimizingNeeded);
    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    verify(table).evaluatePendingInput(any(), eq(MAX_PENDING_PARTITIONS));
  }

  @Test
  public void testDisabledSummaryEvaluationClearsPendingSummary() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    seedPendingSummary(runtime);

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(disabledProperties(true));
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(
            Optional.of(
                new PendingInputResult(new AbstractOptimizingEvaluator.PendingInput(), false)));

    runtime.refresh(table);
    Assert.assertFalse(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));

    assertPendingSummaryCleared();
    verify(table).evaluatePendingInput(any(), eq(MAX_PENDING_PARTITIONS));
  }

  @Test
  public void testDisabledSummaryWithoutEvaluationStillClearsPendingSummary() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    seedPendingSummary(runtime);

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(disabledProperties(false));

    runtime.refresh(table);
    Assert.assertFalse(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));

    assertPendingSummaryCleared();
    verify(table, never()).evaluatePendingInput(any(), anyInt());
  }

  @Test
  public void testDisabledEmptyEvaluationClearsPendingSummary() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    seedPendingSummary(runtime);

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(disabledProperties(true));
    when(table.evaluatePendingInput(any(), anyInt())).thenReturn(Optional.empty());

    runtime.refresh(table);
    Assert.assertFalse(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));

    assertPendingSummaryCleared();
  }

  @Test
  public void testLegacySummaryUpdateClearsStaleHealthDetails() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();
    publishAnalysis(runtime, analysisKey(9L));
    runtime.completeEmptyProcess();

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(
            Optional.of(
                new PendingInputResult(new AbstractOptimizingEvaluator.PendingInput(), false)));

    Assert.assertFalse(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));

    Assert.assertNull(
        tableManager()
            .getTableRuntimeMata(serverTableIdentifier())
            .getTableSummary()
            .getHealthDetails());
  }

  @Test
  public void testAnalysisSlotIsKeyCheckedAndConsumedOnce() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();

    TableAnalysisKey key = analysisKey(10L);
    FormatTableAnalysis analysis = analysis(key, new AbstractOptimizingEvaluator.PendingInput());
    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentAnalysisKey(any())).thenReturn(Optional.of(key));
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(analysis.pendingInput(), true, analysis)));

    runtime.refresh(table);
    Assert.assertTrue(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));
    Assert.assertEquals(Optional.of(key), runtime.getCurrentAnalysisKey());
    Assert.assertSame(analysis, runtime.takeTableAnalysis(key).orElse(null));
    Assert.assertFalse(runtime.takeTableAnalysis(key).isPresent());
  }

  @Test
  public void testMismatchedTakeStillConsumesAnalysis() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();

    TableAnalysisKey key = analysisKey(12L);
    FormatTableAnalysis analysis = analysis(key, new AbstractOptimizingEvaluator.PendingInput());
    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentAnalysisKey(any())).thenReturn(Optional.of(key));
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(analysis.pendingInput(), true, analysis)));

    runtime.refresh(table);
    Assert.assertTrue(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));

    Assert.assertFalse(runtime.takeTableAnalysis(analysisKey(13L)).isPresent());
    Assert.assertFalse(runtime.takeTableAnalysis(key).isPresent());
  }

  @Test
  public void testBeginPlanningDoesNotClearAnalysisSlot() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();

    TableAnalysisKey key = analysisKey(20L);
    FormatTableAnalysis analysis = analysis(key, new AbstractOptimizingEvaluator.PendingInput());
    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentAnalysisKey(any())).thenReturn(Optional.of(key));
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(analysis.pendingInput(), true, analysis)));

    runtime.refresh(table);
    Assert.assertTrue(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));
    runtime.beginPlanning();

    Assert.assertSame(analysis, runtime.takeTableAnalysis(key).orElse(null));
  }

  @Test
  public void testCurrentOnlySummaryWriteRejectsStaleAnalysis() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    TableAnalysisKey oldKey = analysisKey(30L);
    TableAnalysisKey currentKey = analysisKey(31L);
    FormatTableAnalysis stale = analysis(oldKey, new AbstractOptimizingEvaluator.PendingInput());

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentAnalysisKey(any())).thenReturn(Optional.of(currentKey));
    runtime.refresh(table);

    Assert.assertFalse(runtime.updateTableSummaryIfCurrent(stale));
    Assert.assertNull(
        tableManager()
            .getTableRuntimeMata(serverTableIdentifier())
            .getTableSummary()
            .getHealthDetails());
  }

  @Test
  public void testPlannerFallbackRechecksMetadataKeyBeforeSummaryWrite() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    TableAnalysisKey scannedKey = analysisKey(32L);
    TableAnalysisKey advancedKey = analysisKey(33L);
    FormatTableAnalysis fallback =
        analysis(scannedKey, new AbstractOptimizingEvaluator.PendingInput());

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentAnalysisKey(any()))
        .thenReturn(Optional.of(scannedKey), Optional.of(advancedKey));

    runtime.refresh(table);

    Assert.assertFalse(runtime.updateTableSummaryIfCurrent(table, fallback));
    Assert.assertEquals(Optional.of(advancedKey), runtime.getCurrentAnalysisKey());
    Assert.assertNull(
        tableManager()
            .getTableRuntimeMata(serverTableIdentifier())
            .getTableSummary()
            .getHealthDetails());
  }

  @Test
  public void testNonIdleEmptyEvaluationKeepsAdaptiveDemand() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();

    @SuppressWarnings("unchecked")
    AmoroTable<Object> pendingTable = mock(AmoroTable.class);
    when(pendingTable.evaluatePendingInput(any(), anyInt()))
        .thenReturn(
            Optional.of(
                new PendingInputResult(new AbstractOptimizingEvaluator.PendingInput(), true)));
    Assert.assertTrue(
        runtime.evaluatePendingInputAndTransition(pendingTable, MAX_PENDING_PARTITIONS));
    Assert.assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());

    @SuppressWarnings("unchecked")
    AmoroTable<Object> emptyTable = mock(AmoroTable.class);
    when(emptyTable.evaluatePendingInput(any(), anyInt())).thenReturn(Optional.empty());

    Assert.assertTrue(
        runtime.evaluatePendingInputAndTransition(emptyTable, MAX_PENDING_PARTITIONS));
    Assert.assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
  }

  @Test
  public void testPaimonAnalysisWithoutCurrentPreflightCannotPublishPending() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();
    TableAnalysisKey key = analysisKey(40L, TableFormat.PAIMON);
    FormatTableAnalysis analysis = analysis(key, new AbstractOptimizingEvaluator.PendingInput());

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.currentAnalysisKey(any())).thenReturn(Optional.empty());
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(analysis.pendingInput(), true, analysis)));

    Assert.assertFalse(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));

    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    Assert.assertFalse(runtime.takeTableAnalysis(key).isPresent());
  }

  @Test
  public void testLegacyFormatAnalysisWithoutPreflightKeepsPendingCompatibility() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();
    TableAnalysisKey key = analysisKey(41L);
    FormatTableAnalysis analysis = analysis(key, new AbstractOptimizingEvaluator.PendingInput());

    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.currentAnalysisKey(any())).thenReturn(Optional.empty());
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(analysis.pendingInput(), true, analysis)));

    Assert.assertTrue(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));

    Assert.assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
    Assert.assertFalse(runtime.takeTableAnalysis(key).isPresent());
    Assert.assertEquals(
        key.encoded(),
        tableManager()
            .getTableRuntimeMata(serverTableIdentifier())
            .getTableSummary()
            .getHealthDetails()
            .getEvaluationKey());
  }

  @Test
  public void testPreflightFailureInvalidatesCurrentKey() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    TableAnalysisKey key = analysisKey(50L);

    @SuppressWarnings("unchecked")
    AmoroTable<Object> currentTable = mock(AmoroTable.class);
    when(currentTable.properties()).thenReturn(Collections.emptyMap());
    when(currentTable.currentAnalysisKey(any())).thenReturn(Optional.of(key));
    runtime.refresh(currentTable);
    Assert.assertEquals(Optional.of(key), runtime.getCurrentAnalysisKey());

    @SuppressWarnings("unchecked")
    AmoroTable<Object> failingTable = mock(AmoroTable.class);
    when(failingTable.properties()).thenReturn(Collections.emptyMap());
    when(failingTable.currentAnalysisKey(any())).thenThrow(new IllegalStateException("preflight"));

    runtime.refresh(failingTable);

    Assert.assertFalse(runtime.getCurrentAnalysisKey().isPresent());
  }

  @Test
  public void testSkippedCompletionDoesNotInvalidateCurrentAnalysis() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();
    TableAnalysisKey key = analysisKey(60L);
    FormatTableAnalysis analysis = publishAnalysis(runtime, key);
    OptimizingProcess owner = process(921L, 161L, 162L, 1021L);
    OptimizingProcess stale = process(922L, 171L, 172L, 1022L);
    Assert.assertTrue(runtime.tryAcquireProcessOwner(owner.getProcessId()));

    runtime.completeProcess(stale, true);

    Assert.assertEquals(owner.getProcessId(), runtime.getProcessId());
    Assert.assertEquals(Optional.of(key), runtime.getCurrentAnalysisKey());
    Assert.assertSame(analysis, runtime.takeTableAnalysis(key).orElse(null));
  }

  @Test
  public void testNullCompletionArgumentsDoNotInvalidateCurrentAnalysis() {
    DefaultTableRuntime runtime = getDefaultTableRuntime(serverTableIdentifier().getId());
    runtime.completeEmptyProcess();
    TableAnalysisKey key = analysisKey(61L);
    FormatTableAnalysis analysis = publishAnalysis(runtime, key);

    Assert.assertThrows(NullPointerException.class, () -> runtime.completeProcess(null, true));

    Assert.assertEquals(Optional.of(key), runtime.getCurrentAnalysisKey());
    Assert.assertSame(analysis, runtime.takeTableAnalysis(key).orElse(null));
  }

  private static OptimizingProcess process(
      long processId, long targetSnapshotId, long targetChangeSnapshotId, long planTime) {
    OptimizingProcess process = mock(OptimizingProcess.class);
    when(process.getProcessId()).thenReturn(processId);
    when(process.getTargetSnapshotId()).thenReturn(targetSnapshotId);
    when(process.getTargetChangeSnapshotId()).thenReturn(targetChangeSnapshotId);
    when(process.getOptimizingType()).thenReturn(OptimizingType.MINOR);
    when(process.getPlanTime()).thenReturn(planTime);
    when(process.getStatus()).thenReturn(ProcessStatus.SUCCESS);
    return process;
  }

  private FormatTableAnalysis publishAnalysis(DefaultTableRuntime runtime, TableAnalysisKey key) {
    FormatTableAnalysis analysis = analysis(key, new AbstractOptimizingEvaluator.PendingInput());
    @SuppressWarnings("unchecked")
    AmoroTable<Object> table = mock(AmoroTable.class);
    when(table.properties()).thenReturn(Collections.emptyMap());
    when(table.currentAnalysisKey(any())).thenReturn(Optional.of(key));
    when(table.evaluatePendingInput(any(), anyInt()))
        .thenReturn(Optional.of(new PendingInputResult(analysis.pendingInput(), true, analysis)));
    runtime.refresh(table);
    Assert.assertTrue(runtime.evaluatePendingInputAndTransition(table, MAX_PENDING_PARTITIONS));
    return analysis;
  }

  private void seedPendingSummary(DefaultTableRuntime runtime) {
    runtime
        .store()
        .begin()
        .updateTableSummary(
            summary -> {
              summary.setPendingFileSize(123L);
              summary.setPendingFileCount(4);
            })
        .commit();
  }

  private void assertPendingSummaryCleared() {
    org.apache.amoro.table.TableSummary summary =
        tableManager().getTableRuntimeMata(serverTableIdentifier()).getTableSummary();
    Assert.assertEquals(0L, summary.getPendingFileSize());
    Assert.assertEquals(0, summary.getPendingFileCount());
  }

  private Map<String, String> disabledProperties(boolean summaryEnabled) {
    Map<String, String> properties = new HashMap<>();
    properties.put("self-optimizing.enabled", "false");
    properties.put("table-summary.enabled", String.valueOf(summaryEnabled));
    return properties;
  }

  private TableAnalysisKey analysisKey(long snapshotId) {
    return analysisKey(snapshotId, TableFormat.ICEBERG);
  }

  private TableAnalysisKey analysisKey(long snapshotId, TableFormat format) {
    return new TableAnalysisKey(
        String.valueOf(serverTableIdentifier().getId()),
        format,
        snapshotId,
        TableAnalysisKey.NO_CHANGE_SNAPSHOT,
        1L,
        "fingerprint",
        "test-health-v1",
        TableAnalysisKey.NO_BASELINE,
        TableAnalysisKey.NO_BASELINE_TIME);
  }

  private FormatTableAnalysis analysis(
      TableAnalysisKey key, AbstractOptimizingEvaluator.PendingInput pendingInput) {
    TableHealthDetails details =
        new TableHealthDetails(
            key.getFormulaVersion(),
            key.getSnapshotId(),
            null,
            key.getSchemaId(),
            key.getScoringConfigFingerprint(),
            key.encoded(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyList());
    return new FormatTableAnalysis() {
      @Override
      public TableAnalysisKey key() {
        return key;
      }

      @Override
      public AbstractOptimizingEvaluator.PendingInput pendingInput() {
        return pendingInput;
      }

      @Override
      public TableHealthDetails healthDetails() {
        return details;
      }
    };
  }
}
