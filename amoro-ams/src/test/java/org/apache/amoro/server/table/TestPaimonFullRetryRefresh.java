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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.scheduler.inline.TableRuntimeRefreshExecutor;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class TestPaimonFullRetryRefresh {
  @Test
  void dueFullIsEvaluatedEvenWhenSnapshotsAndHealthAreUnchanged() {
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    OptimizingConfig config = new OptimizingConfig().setEnabled(true).setFullTriggerInterval(1);
    when(runtime.getOptimizingConfig()).thenReturn(config);
    when(runtime.isIdle()).thenReturn(true);
    Map<String, String> options = new HashMap<>();
    options.put("write-only", "true");
    options.put("self-optimizing.enabled", "true");
    when(runtime.getTableConfig()).thenReturn(options);
    when(runtime.getLastOptimizedSnapshotId()).thenReturn(10L);
    when(runtime.getCurrentSnapshotId()).thenReturn(10L);
    when(runtime.getLastOptimizedChangeSnapshotId()).thenReturn(6L);
    when(runtime.getCurrentChangeSnapshotId()).thenReturn(6L);
    when(runtime.getCurrentAnalysisKey()).thenReturn(Optional.of(mock(TableAnalysisKey.class)));
    PaimonTable table = mock(PaimonTable.class);
    org.mockito.Mockito.when(table.supportsPendingInputRefresh()).thenCallRealMethod();
    org.mockito.Mockito.when(
            table.shouldEvaluatePendingInput(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.anyLong()))
        .thenCallRealMethod();
    PrimaryKeyFileStoreTable nativeTable = mock(PrimaryKeyFileStoreTable.class);
    when(nativeTable.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    when(nativeTable.primaryKeys()).thenReturn(java.util.Collections.singletonList("id"));
    doReturn(nativeTable).when(table).originalTable();
    TableService tables = mock(TableService.class);
    doReturn(table).when(tables).loadTable(any());
    TableRuntimeRefreshExecutor refresh = new TableRuntimeRefreshExecutor(tables, 1, 60000, 20);
    when(runtime.getFullRetryNotBefore()).thenReturn(Long.MAX_VALUE);
    refresh.execute(runtime);
    verify(runtime, never()).evaluatePendingInputAndTransition(any(), anyInt(), anyBoolean());
    when(runtime.getFullRetryNotBefore()).thenReturn(1L);
    refresh.execute(runtime);
    verify(runtime).evaluatePendingInputAndTransition(table, 20, false);
    assertTrue(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 1L));
    assertFalse(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 0L));
    doReturn(mock(AppendOnlyFileStoreTable.class)).when(table).originalTable();
    assertFalse(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 100L));
    doReturn(nativeTable).when(table).originalTable();
    config.setEnabled(false);
    assertFalse(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 100L));
    config.setEnabled(true).setFullTriggerInterval(-1);
    assertFalse(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 100L));
    config.setFullTriggerInterval(1);
    when(runtime.isIdle()).thenReturn(false);
    assertFalse(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 100L));
    when(runtime.isIdle()).thenReturn(true);
    when(runtime.getFullRetryNotBefore()).thenReturn(0L);
    refresh.execute(runtime);
    verify(runtime, times(2)).evaluatePendingInputAndTransition(table, 20, false);
    assertTrue(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 100L));
    when(runtime.getLastFullOptimizingTime()).thenReturn(100L);
    assertFalse(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 100L));
    assertTrue(
        table.shouldEvaluatePendingInput(
            runtime, runtime.getOptimizingStatus() == OptimizingStatus.PENDING, 101L));
    refresh.gracefulShutdown();
  }
}
