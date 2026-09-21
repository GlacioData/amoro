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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.TableFormat;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.table.FormatPendingInput;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

class TestRuntimeHealthState {
  @Test
  void failedLatestAttemptKeepsSuccessButExposesStalenessAndReason() {
    AtomicLong clock = new AtomicLong(1000);
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON), clock::get);
    state.update(analysis(1, 90, null));
    state.update(analysis(2, -1, "DELETE_METADATA_READ_FAILED"));
    RuntimeHealthSnapshot snapshot = state.snapshot(key(2)).get();
    assertEquals(90, snapshot.getHealthScore());
    assertEquals(Long.valueOf(1), snapshot.getHealthDetails().getSnapshotId());
    assertTrue(snapshot.getEvaluation().isStale());
    assertEquals("2", snapshot.getEvaluation().getAttemptedSnapshotId());
    assertEquals("FAILED", snapshot.getEvaluation().getStatus());
    assertTrue(snapshot.getEvaluation().isRetryable());
    assertFalse(state.shouldEvaluate(key(2)));
    clock.set(6000);
    assertTrue(state.shouldEvaluate(key(2)));
    state.update(analysis(2, 75, null));
    assertFalse(state.snapshot(key(2)).get().getEvaluation().isStale());
    assertTrue(state.snapshot(key(3)).get().getEvaluation().isStale());
  }

  @Test
  void backoffIsBoundedAndNewInputsBypassHealthRetryDelay() {
    AtomicLong clock = new AtomicLong(1000);
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON), clock::get);
    for (int i = 0; i < 12; i++) {
      state.update(analysis(1, -1, "SNAPSHOT_SCAN_FAILED"));
      assertFalse(state.shouldEvaluate(key(1)));
      assertTrue(state.shouldEvaluate(key(2)));
      clock.addAndGet(60000);
      assertTrue(state.shouldEvaluate(key(1)));
    }
    state.update(analysis(1, -1, "DELETE_METADATA_INCOMPLETE"));
    clock.addAndGet(600000);
    assertFalse(state.shouldEvaluate(key(1)));
    state.clear();
    assertTrue(state.shouldEvaluate(key(1)));
    assertFalse(state.snapshot(key(1)).isPresent());
  }

  @Test
  void lateCompletionCannotReplaceNewAttemptOrClearedGeneration() {
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON));
    long old = state.beginAttempt();
    long newer = state.beginAttempt();
    assertTrue(state.update(analysis(2, 80, null), newer));
    assertFalse(state.update(analysis(1, 40, null), old));
    assertEquals(80, state.snapshot().get().getHealthScore());
    long beforeClear = state.beginAttempt();
    state.clear();
    assertFalse(state.update(analysis(3, 90, null), beforeClear));
    assertFalse(state.snapshot().isPresent());
    assertTrue(state.update(analysis(3, 90, null), state.beginAttempt()));
  }

  @Test
  void completeOldSnapshotRemainsVisibleAsStaleUntilLatestInputFinishes() {
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON));
    state.update(analysis(1, 75, null), state.beginAttempt());
    assertEquals("STALE", state.snapshot(key(2)).get().getEvaluation().getStatus());
    assertEquals(75, state.snapshot(key(2)).get().getHealthScore());
  }

  @Test
  void thrownScanFailureKeepsVerifiedSnapshotAndIsRetryable() {
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON));
    state.update(analysis(1, 90, null));
    state.fail(key(2), state.beginAttempt());
    RuntimeHealthSnapshot snapshot = state.snapshot(key(2)).get();
    assertEquals(90, snapshot.getHealthScore());
    assertTrue(snapshot.getEvaluation().isRetryable());
    assertEquals("FAILED", snapshot.getEvaluation().getStatus());
  }

  @Test
  void schedulingBaselineDoesNotInvalidateStructuralHealthButConfigDoes() {
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON));
    state.update(analysis(1, 90, null));
    TableAnalysisKey baseline =
        new TableAnalysisKey("1", TableFormat.PAIMON, 1, -1, 1, "config", "v3", 1, 999);
    assertFalse(state.shouldEvaluate(baseline));
    assertFalse(state.snapshot(baseline).get().getEvaluation().isStale());
    TableAnalysisKey changed =
        new TableAnalysisKey("1", TableFormat.PAIMON, 1, -1, 1, "changed", "v3", 1, 999);
    assertTrue(state.shouldEvaluate(changed));
    assertTrue(state.snapshot(changed).get().getEvaluation().isStale());
  }

  private static TableAnalysisKey key(long id) {
    return new TableAnalysisKey("1", TableFormat.PAIMON, id, -1, 1, "config", "v3", -1, -1);
  }

  private static FormatTableAnalysis analysis(long id, int score, String reason) {
    FormatTableAnalysis analysis = mock(FormatTableAnalysis.class);
    FormatPendingInput input = mock(FormatPendingInput.class);
    when(input.getHealthScore()).thenReturn(score);
    when(analysis.key()).thenReturn(key(id));
    when(analysis.pendingInput()).thenReturn(input);
    when(analysis.healthDetails())
        .thenReturn(
            new TableHealthDetails(
                "v3",
                id,
                null,
                1L,
                "config",
                key(id).encoded(),
                null,
                null,
                reason == null ? Collections.emptyList() : Collections.singletonList(reason)));
    return analysis;
  }
}
