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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.table.TableSummary;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.table.health.TableHealthEvaluation;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

public class TestTableOptimizingQuery {

  @Test
  void defaultsToDescendingWhenSortFieldIsProvided() {
    TableOptimizingQuery query =
        TableOptimizingQuery.builder(25, 0)
            .sort(TableOptimizingQuery.SortField.DURATION, null)
            .build();

    assertEquals(TableOptimizingQuery.SortOrder.DESC, query.getSortOrder());
  }

  @Test
  void rejectsInvalidHealthScoreRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TableOptimizingQuery.builder(25, 0).healthScoreRange(61, 60).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> TableOptimizingQuery.builder(25, 0).healthScoreRange(-1, null).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> TableOptimizingQuery.builder(25, 0).healthScoreRange(null, 101).build());
  }

  @Test
  void rejectsInvalidDurationAndPaging() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TableOptimizingQuery.builder(25, 0).durationMinMinutes(-1L).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> TableOptimizingQuery.builder(25, 0).durationMinMinutes(Long.MAX_VALUE).build());
    assertThrows(IllegalArgumentException.class, () -> TableOptimizingQuery.builder(0, 0).build());
    assertThrows(
        IllegalArgumentException.class, () -> TableOptimizingQuery.builder(25, -1).build());
  }

  @Test
  void projectsHealthScoreFromTheFormatSpecificSource() {
    TableService tableService = mock(TableService.class);
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    TableHealthDetails details = mock(TableHealthDetails.class);
    when(runtime.getRuntimeHealthSnapshot())
        .thenReturn(
            Optional.of(
                new RuntimeHealthSnapshot(
                    72,
                    details,
                    new TableHealthEvaluation(
                        "SUCCESS", false, "1", "1", 1L, false, Collections.emptyList()))));
    when(tableService.getRuntime(17L)).thenReturn(runtime);

    TableRuntimeMeta runtimeMeta = new TableRuntimeMeta();
    TableSummary summary = new TableSummary();
    summary.setHealthScore(91);
    runtimeMeta.setTableSummary(summary);

    assertEquals(
        72,
        DefaultTableManager.resolveHealthScore(
            tableService,
            ServerTableIdentifier.of(
                17L, "catalog", "database", "paimon_table", TableFormat.PAIMON),
            runtimeMeta));
    assertEquals(
        91,
        DefaultTableManager.resolveHealthScore(
            tableService,
            ServerTableIdentifier.of(
                18L, "catalog", "database", "iceberg_table", TableFormat.ICEBERG),
            runtimeMeta));

    when(runtime.getRuntimeHealthSnapshot()).thenReturn(Optional.empty());
    assertEquals(
        -1,
        DefaultTableManager.resolveHealthScore(
            tableService,
            ServerTableIdentifier.of(
                17L, "catalog", "database", "paimon_table", TableFormat.PAIMON),
            runtimeMeta));
  }

  @Test
  void staleScoresAreUnavailableForOptimizingListsAndScoreRanges() {
    TableService service = mock(TableService.class);
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    when(service.getRuntime(17L)).thenReturn(runtime);
    RuntimeHealthSnapshot historical =
        new RuntimeHealthSnapshot(
            100,
            mock(TableHealthDetails.class),
            new TableHealthEvaluation("FAILED", true, "2", "2", 2L, true, Collections.emptyList()));
    when(runtime.getRuntimeHealthSnapshot()).thenReturn(Optional.of(historical));
    int score =
        DefaultTableManager.resolveHealthScore(
            service,
            ServerTableIdentifier.of(17L, "c", "d", "t", TableFormat.PAIMON),
            new TableRuntimeMeta());
    assertEquals(-1, score);
    assertEquals(100, historical.getHealthScore());
  }
}
