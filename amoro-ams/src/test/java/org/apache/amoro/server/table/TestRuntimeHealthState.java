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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.TableFormat;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.table.FormatPendingInput;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableFileStatisticsSnapshot;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.table.health.TableHealthStructure;
import org.apache.amoro.table.health.TableHealthSupport;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

  @Test
  void partialFileStatisticsSurviveLaterFailureWithoutChangingHistoricalHealth() {
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON));
    assertTrue(state.update(fileAnalysis(100, 90, 3, 30), state.beginAttempt(), key(100)));
    assertFalse(state.update(fileAnalysis(101, -1, 4, 80), state.beginAttempt(), key(101)));
    TableFileStatisticsSnapshot statistics = state.snapshot(key(101)).get().getFileStatistics();
    state.fail(key(102), state.beginAttempt());
    RuntimeHealthSnapshot result = state.snapshot(key(102)).get();
    assertSame(statistics, result.getFileStatistics());
    assertEquals(Long.valueOf(101), statistics.getSnapshotId());
    assertEquals(4, statistics.getFileCount());
    assertEquals(80, statistics.getTotalSizeBytes());
    assertEquals(90, result.getHealthScore());
    assertEquals(Long.valueOf(100), result.getHealthDetails().getSnapshotId());
    assertEquals("102", result.getEvaluation().getAttemptedSnapshotId());
    assertEquals(-1, result.getCurrentHealthScore());
    for (int i = 0; i < 100; i++) {
      assertSame(statistics, state.snapshot(key(102)).get().getFileStatistics());
    }
    assertNull(
        state.snapshot().get().getFileStatistics(),
        "Persisted health must not retain old file summaries");
  }

  @Test
  void fileStatisticsHonorAttemptGenerationAndAllowConfirmedRollbackRecovery() {
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON));
    long early = state.beginAttempt();
    state.update(fileAnalysis(200, 90, 2, 200), state.beginAttempt(), key(200));
    state.update(fileAnalysis(150, 80, 1, 100), early, key(150));
    assertEquals(
        Long.valueOf(200), state.snapshot(key(200)).get().getFileStatistics().getSnapshotId());
    state.update(fileAnalysis(150, 80, 1, 100), state.beginAttempt(), key(200));
    assertEquals(
        Long.valueOf(200), state.snapshot(key(200)).get().getFileStatistics().getSnapshotId());
    state.update(fileAnalysis(150, 80, 1, 100), state.beginAttempt(), key(150));
    assertEquals(
        Long.valueOf(150), state.snapshot(key(150)).get().getFileStatistics().getSnapshotId());
    long beforeClear = state.beginAttempt();
    state.clear();
    assertFalse(state.update(fileAnalysis(200, 90, 2, 200), beforeClear, key(200)));
    assertFalse(state.snapshot(key(200)).isPresent());
  }

  @Test
  void fileExtractionFailureNeverChangesHealthResultOrGaugeUpdateDecision() {
    TableHealthSupport throwing =
        new TableHealthSupport() {
          @Override
          public Optional<TableFileStatisticsSnapshot> extractFileStatistics(
              TableHealthDetails details) {
            throw new IllegalArgumentException("invalid file statistics");
          }
        };
    RuntimeHealthState state = new RuntimeHealthState(throwing);
    assertTrue(state.update(analysis(1, 95, null)));
    RuntimeHealthSnapshot result = state.snapshot(key(1)).get();
    assertEquals(95, result.getCurrentHealthScore());
    assertNull(result.getFileStatistics());
  }

  @Test
  void requestSnapshotDoesNotWaitForAnUnpublishedBackgroundUpdate() throws Exception {
    CountDownLatch extracting = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    TableHealthSupport paimon = TableHealthSupports.forFormat(TableFormat.PAIMON);
    TableHealthSupport blocked =
        new TableHealthSupport() {
          @Override
          public Optional<TableFileStatisticsSnapshot> extractFileStatistics(
              TableHealthDetails details) {
            if (Long.valueOf(2).equals(details.getSnapshotId())) {
              extracting.countDown();
              try {
                assertTrue(
                    release.await(10, TimeUnit.SECONDS), "Background update was not released");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
              }
            }
            return paimon.extractFileStatistics(details);
          }
        };
    RuntimeHealthState state = new RuntimeHealthState(blocked);
    state.update(fileAnalysis(1, 90, 3, 30), state.beginAttempt(), key(1));
    TableFileStatisticsSnapshot previous = state.snapshot(key(1)).get().getFileStatistics();
    long sequence = state.beginAttempt();
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> update =
          workers.submit(() -> state.update(fileAnalysis(2, 75, 4, 80), sequence, key(2)));
      assertTrue(extracting.await(5, TimeUnit.SECONDS));
      Future<RuntimeHealthSnapshot> request = workers.submit(() -> state.snapshot(key(2)).get());
      RuntimeHealthSnapshot during = request.get(2, TimeUnit.SECONDS);
      assertFalse(update.isDone());
      assertEquals(90, during.getHealthScore());
      assertSame(previous, during.getFileStatistics());
      assertEquals("1", during.getEvaluation().getAttemptedSnapshotId());
      assertEquals("STALE", during.getEvaluation().getStatus());
      release.countDown();
      assertTrue(update.get(5, TimeUnit.SECONDS));
      RuntimeHealthSnapshot after = state.snapshot(key(2)).get();
      assertEquals(75, after.getHealthScore());
      assertEquals(Long.valueOf(2), after.getFileStatistics().getSnapshotId());
      assertEquals("2", after.getEvaluation().getAttemptedSnapshotId());
      assertEquals("SUCCESS", after.getEvaluation().getStatus());
      assertSame(previous, during.getFileStatistics());
    } finally {
      release.countDown();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void repeatedResultsRetainOneScalarSummaryAndClearReleasesTheRuntimeReference() throws Exception {
    RuntimeHealthState state =
        new RuntimeHealthState(TableHealthSupports.forFormat(TableFormat.PAIMON));
    RuntimeHealthSnapshot inFlight = null;
    for (long id = 100; id < 160; id += 3) {
      assertTrue(state.update(fileAnalysis(id, 90, 3, 30), state.beginAttempt(), key(id)));
      assertFalse(state.update(fileAnalysis(id + 1, -1, 4, 80), state.beginAttempt(), key(id + 1)));
      RuntimeHealthSnapshot partial = state.snapshot(key(id + 1)).get();
      if (inFlight == null) {
        inFlight = partial;
      }
      state.fail(key(id + 2), state.beginAttempt());
      RuntimeHealthSnapshot failed = state.snapshot(key(id + 2)).get();
      assertSame(partial.getFileStatistics(), failed.getFileStatistics());
      Set<Object> retained = reachableFrom(state);
      assertEquals(
          1, retained.stream().filter(TableFileStatisticsSnapshot.class::isInstance).count());
      assertFalse(retained.stream().anyMatch(FormatTableAnalysis.class::isInstance));
      TableFileStatisticsSnapshot summary = failed.getFileStatistics();
      assertTrue(
          reachableFrom(summary).stream()
              .allMatch(value -> value == summary || value instanceof Number),
          "The retained summary must contain only scalar values");
    }
    assertEquals(Long.valueOf(101), inFlight.getFileStatistics().getSnapshotId());
    long beforeClear = state.beginAttempt();
    state.clear();
    assertFalse(state.snapshot(key(160)).isPresent());
    assertFalse(
        reachableFrom(state).stream().anyMatch(TableFileStatisticsSnapshot.class::isInstance));
    assertFalse(state.update(fileAnalysis(160, 80, 1, 10), beforeClear, key(160)));
    assertFalse(
        reachableFrom(state).stream().anyMatch(TableFileStatisticsSnapshot.class::isInstance));
    assertTrue(state.update(fileAnalysis(161, 80, 1, 10), state.beginAttempt(), key(161)));
    assertEquals(
        Long.valueOf(161), state.snapshot(key(161)).get().getFileStatistics().getSnapshotId());
    assertEquals(
        1,
        reachableFrom(state).stream()
            .filter(TableFileStatisticsSnapshot.class::isInstance)
            .count());
    assertEquals(Long.valueOf(101), inFlight.getFileStatistics().getSnapshotId());
  }

  // Inspect reachability without naming private fields, triggering GC, or following static caches.
  private static Set<Object> reachableFrom(Object root) throws IllegalAccessException {
    Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    ArrayDeque<Object> pending = new ArrayDeque<>();
    pending.add(root);
    while (!pending.isEmpty()) {
      Object value = pending.removeFirst();
      if (!visited.add(value)) {
        continue;
      }
      if (value instanceof AtomicReference) {
        Object target = ((AtomicReference<?>) value).get();
        if (target != null) {
          pending.add(target);
        }
      } else if (value instanceof Map) {
        pending.addAll(((Map<?, ?>) value).keySet());
        pending.addAll(((Map<?, ?>) value).values());
      } else if (value instanceof Iterable) {
        for (Object element : (Iterable<?>) value) {
          if (element != null) {
            pending.add(element);
          }
        }
      } else {
        for (Class<?> type = value.getClass();
            type != null && type.getName().startsWith("org.apache.amoro.");
            type = type.getSuperclass()) {
          for (Field field : type.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) {
              field.setAccessible(true);
              Object target = field.get(value);
              if (target != null) {
                pending.add(target);
              }
            }
          }
        }
      }
    }
    return visited;
  }

  private static FormatTableAnalysis fileAnalysis(long id, int score, long count, long bytes) {
    FormatTableAnalysis analysis =
        analysis(id, score, score < 0 ? "DELETE_METADATA_READ_FAILED" : null);
    Map<String, String> metrics = new HashMap<>();
    metrics.put("totalFileCount", Long.toString(count));
    metrics.put("totalFileSize", Long.toString(bytes));
    TableHealthDetails details =
        new TableHealthDetails(
                "v3",
                id,
                null,
                1L,
                "config",
                key(id).encoded(),
                null,
                metrics,
                score < 0 ? Collections.singletonList("DELETE_METADATA_READ_FAILED") : null)
            .withStructure(
                new TableHealthStructure(
                    "APPEND",
                    score < 0 ? "PARTIAL" : "COMPLETE",
                    1000 + id,
                    1000,
                    "1",
                    "0",
                    "0",
                    null,
                    null));
    when(analysis.healthDetails()).thenReturn(details);
    return analysis;
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
