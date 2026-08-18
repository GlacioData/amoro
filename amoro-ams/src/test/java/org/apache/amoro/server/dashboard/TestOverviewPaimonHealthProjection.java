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

package org.apache.amoro.server.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.server.dashboard.model.OverviewTopTableItem;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.RuntimeHealthSnapshot;
import org.apache.amoro.server.table.TableService;
import org.apache.amoro.table.TableSummary;
import org.apache.amoro.table.health.TableHealthDetails;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

class TestOverviewPaimonHealthProjection {

  @Test
  void usesRuntimeHealthForPaimonWhileKeepingPersistedFileStatistics() {
    TableService tableService = mock(TableService.class);
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    TableHealthDetails details = healthDetails("runtime-key");
    when(runtime.getRuntimeHealthSnapshot())
        .thenReturn(Optional.of(new RuntimeHealthSnapshot(72, details)));
    when(tableService.getRuntime(17L)).thenReturn(runtime);
    OverviewManager manager = new OverviewManager(10, Duration.ZERO, () -> tableService);

    OverviewTopTableItem item =
        manager
            .toTopTableItem(
                ServerTableIdentifier.of(17L, "catalog", "database", "table", TableFormat.PAIMON),
                runtimeMeta(91, 7, 700L))
            .get();

    assertEquals(72, item.getHealthScore());
    assertEquals(7, item.getFileCount());
    assertEquals(700L, item.getTableSize());
  }

  @Test
  void returnsUnavailablePaimonHealthWhenRuntimeSnapshotIsMissing() {
    TableService tableService = mock(TableService.class);
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    when(runtime.getRuntimeHealthSnapshot()).thenReturn(Optional.empty());
    when(tableService.getRuntime(17L)).thenReturn(runtime);
    OverviewManager manager = new OverviewManager(10, Duration.ZERO, () -> tableService);

    OverviewTopTableItem item =
        manager
            .toTopTableItem(
                ServerTableIdentifier.of(17L, "catalog", "database", "table", TableFormat.PAIMON),
                runtimeMeta(91, 7, 700L))
            .get();

    assertEquals(-1, item.getHealthScore());
  }

  @Test
  void keepsPersistedHealthForIceberg() {
    TableService tableService = mock(TableService.class);
    OverviewManager manager = new OverviewManager(10, Duration.ZERO, () -> tableService);

    OverviewTopTableItem item =
        manager
            .toTopTableItem(
                ServerTableIdentifier.of(
                    18L, "catalog", "database", "iceberg_table", TableFormat.ICEBERG),
                runtimeMeta(91, 7, 700L))
            .get();

    assertEquals(91, item.getHealthScore());
  }

  private TableRuntimeMeta runtimeMeta(int healthScore, int fileCount, long fileSize) {
    TableSummary summary = new TableSummary();
    summary.setHealthScore(healthScore);
    summary.setTotalFileCount(fileCount);
    summary.setTotalFileSize(fileSize);
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableSummary(summary);
    return meta;
  }

  private TableHealthDetails healthDetails(String evaluationKey) {
    return new TableHealthDetails(
        "paimon-primary-key-health-v2",
        1L,
        null,
        1L,
        "fingerprint",
        evaluationKey,
        Collections.emptyList(),
        Collections.emptyMap(),
        Collections.emptyList());
  }
}
