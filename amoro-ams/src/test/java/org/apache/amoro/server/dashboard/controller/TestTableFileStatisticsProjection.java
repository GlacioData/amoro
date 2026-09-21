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

package org.apache.amoro.server.dashboard.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.server.table.RuntimeHealthSnapshot;
import org.apache.amoro.table.descriptor.TableDetailObservation;
import org.apache.amoro.table.descriptor.TableFileStatisticsView;
import org.apache.amoro.table.descriptor.TableFileStatisticsView.Status;
import org.apache.amoro.table.health.TableFileStatisticsSnapshot;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.table.health.TableHealthEvaluation;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class TestTableFileStatisticsProjection {
  @Test
  void distinguishesCurrentStaleFutureAndSchemaMismatchWithoutChangingNumbers() {
    RuntimeHealthSnapshot health = health(103L, 7, true);
    assertEquals(Status.CURRENT, project(103L, 7, health, true).getStatus());
    TableFileStatisticsView stale = project(105L, 7, health, true);
    assertEquals(Status.STALE, stale.getStatus());
    assertEquals("100", stale.getFileCount());
    assertEquals("1048576", stale.getAverageFileSizeBytes());
    assertEquals(Status.UNKNOWN, project(102L, 7, health, true).getStatus());
    assertEquals(Status.UNKNOWN, project(105L, 8, health, true).getStatus());
    assertEquals(Status.CURRENT, project(103L, 7, health, false).getStatus());
    assertTrue(
        project(103L, 7, health, false)
            .getReasonCodes()
            .contains(TableFileStatisticsView.Reason.COLLECTION_DISABLED));
  }

  @Test
  void distinguishesNoAttemptFromCompletedEvaluationWithoutValidFileStatistics() {
    assertEquals(Status.PENDING, project(1L, 0, null, true).getStatus());
    assertEquals(Status.UNAVAILABLE, project(1L, 0, health(1L, 0, false), true).getStatus());
    assertEquals(Status.UNAVAILABLE, project(1L, 0, null, false).getStatus());
    TableFileStatisticsView unmanaged =
        TableFileStatisticsProjection.project(new TableDetailObservation(1L, 0), null, false, null);
    assertEquals(Status.UNAVAILABLE, unmanaged.getStatus());
    assertNull(unmanaged.getFileCount());
    assertTrue(unmanaged.getReasonCodes().contains(TableFileStatisticsView.Reason.NOT_MANAGED));
  }

  @Test
  void emptyObservationNeverOverwritesConflictingRuntimeEvidenceWithZero() {
    TableFileStatisticsView empty =
        TableFileStatisticsProjection.project(
            new TableDetailObservation(null, 0), null, false, null);
    assertEquals(Status.CURRENT, empty.getStatus());
    assertEquals("0", empty.getFileCount());
    assertNull(empty.getEvaluatedAtMillis());
    TableFileStatisticsView conflict = project(null, 0, health(1L, 0, false), true);
    assertEquals(Status.UNKNOWN, conflict.getStatus());
    assertNull(conflict.getFileCount());
    assertEquals("100", project(null, 0, health(1L, 0, true), true).getFileCount());
    assertEquals(
        Status.UNKNOWN,
        TableFileStatisticsProjection.project(null, health(1L, 0, true), true, true).getStatus());
  }

  @Test
  void emptyBackgroundStatisticsCanBecomeStaleAfterFirstCommit() {
    RuntimeHealthSnapshot health =
        new RuntimeHealthSnapshot(
            100, details(null, 0), null, new TableFileStatisticsSnapshot(null, 0, 0, 0, 1234));
    assertEquals(Status.STALE, project(1L, 0, health, true).getStatus());
    assertEquals("0", project(1L, 0, health, true).getFileCount());
  }

  private static TableFileStatisticsView project(
      Long snapshot, long schema, RuntimeHealthSnapshot health, boolean enabled) {
    return TableFileStatisticsProjection.project(
        new TableDetailObservation(snapshot, schema), health, true, enabled);
  }

  private static RuntimeHealthSnapshot health(Long snapshot, long schema, boolean statistics) {
    return new RuntimeHealthSnapshot(
        -1,
        details(snapshot, schema),
        new TableHealthEvaluation(
            "FAILED",
            true,
            snapshot.toString(),
            snapshot.toString(),
            2000,
            true,
            Collections.singletonList("SNAPSHOT_SCAN_FAILED")),
        statistics
            ? new TableFileStatisticsSnapshot(snapshot, schema, 100, 104857600, 1000)
            : null);
  }

  private static TableHealthDetails details(Long snapshot, long schema) {
    return new TableHealthDetails("v3", snapshot, null, schema, "config", "key", null, null, null);
  }
}
