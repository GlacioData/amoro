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

package org.apache.amoro.table.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.TableFormat;
import org.apache.amoro.utils.JacksonUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestTableHealthModels {

  @Test
  void fileStatisticsPreserveLongValuesAndRejectUnknownOrInconsistentCounts() {
    TableFileStatisticsSnapshot value =
        new TableFileStatisticsSnapshot(9L, 0L, 9007199254740993L, Long.MAX_VALUE, 1000L);
    assertEquals(9007199254740993L, value.getFileCount());
    assertEquals(1023L, value.getAverageFileSizeBytes());
    assertEquals(
        0L, new TableFileStatisticsSnapshot(null, 0L, 0L, 0L, 1L).getAverageFileSizeBytes());
    assertEquals(0L, new TableFileStatisticsSnapshot(1L, 0L, 3L, 0L, 1L).getAverageFileSizeBytes());
    assertThrows(
        IllegalArgumentException.class, () -> new TableFileStatisticsSnapshot(1L, 0, 0, 1, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new TableFileStatisticsSnapshot(null, 0, 1, 1, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new TableFileStatisticsSnapshot(0L, 0, 0, 0, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new TableFileStatisticsSnapshot(1L, -1, 1, 1, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new TableFileStatisticsSnapshot(1L, 0, -1, 1, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new TableFileStatisticsSnapshot(1L, 0, 1, -1, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new TableFileStatisticsSnapshot(1L, 0, 1, 1, 0));
  }

  @Test
  void testAnalysisKeyUsesAllFieldsAndCollisionSafeEncoding() {
    TableAnalysisKey first =
        new TableAnalysisKey(
            "catalog.db.table",
            TableFormat.PAIMON,
            Long.MAX_VALUE,
            TableAnalysisKey.NO_CHANGE_SNAPSHOT,
            Long.MAX_VALUE - 1,
            "ab",
            "c",
            Long.MAX_VALUE - 2,
            Long.MAX_VALUE - 3);
    TableAnalysisKey second =
        new TableAnalysisKey(
            "catalog.db.table",
            TableFormat.PAIMON,
            Long.MAX_VALUE,
            TableAnalysisKey.NO_CHANGE_SNAPSHOT,
            Long.MAX_VALUE - 1,
            "a",
            "bc",
            Long.MAX_VALUE - 2,
            Long.MAX_VALUE - 3);

    assertNotEquals(first, second);
    assertNotEquals(first.encoded(), second.encoded());
    assertEquals(Long.MAX_VALUE, first.getSnapshotId());
    assertEquals(Long.MAX_VALUE - 2, first.getSuccessfulOptimizationBaselineId());
    assertEquals(Long.MAX_VALUE - 3, first.getSuccessfulOptimizationBaselineTimeMillis());
    assertEquals(first, new TableAnalysisKey(first));
    assertEquals(first.hashCode(), new TableAnalysisKey(first).hashCode());
  }

  @Test
  void testDetailsAndComponentsDefensivelyCopyCollections() {
    Map<String, String> componentMetrics = new LinkedHashMap<>();
    componentMetrics.put("fileCount", Long.toString(Long.MAX_VALUE));
    TableHealthComponent component =
        new TableHealthComponent("FILE_ORGANIZATION", 0, 80, "WEIGHTED", componentMetrics);

    List<TableHealthComponent> components = new ArrayList<>();
    components.add(component);
    Map<String, String> metrics = new LinkedHashMap<>();
    metrics.put("snapshotCount", Long.toString(Long.MAX_VALUE));
    List<String> reasons = new ArrayList<>(Arrays.asList("EMPTY_TABLE"));

    TableHealthDetails details =
        new TableHealthDetails(
            "paimon-append-health-v2",
            Long.MAX_VALUE,
            null,
            Long.MAX_VALUE - 1,
            "fingerprint",
            "evaluation-key",
            components,
            metrics,
            reasons);

    componentMetrics.put("unexpected", "1");
    components.clear();
    metrics.clear();
    reasons.clear();

    assertEquals(1, details.getComponents().size());
    assertEquals(1, details.getMetrics().size());
    assertEquals(1, details.getReasonCodes().size());
    assertEquals(1, component.getMetrics().size());
    assertEquals("9223372036854775807", details.getMetrics().get("snapshotCount"));
    assertThrows(UnsupportedOperationException.class, () -> details.getComponents().add(component));
    assertThrows(
        UnsupportedOperationException.class, () -> details.getMetrics().put("another", "2"));
    assertThrows(
        UnsupportedOperationException.class, () -> component.getMetrics().put("another", "2"));
  }

  @Test
  void testStructuredDiagnosticsAreImmutableAndRestSafe() {
    List<TableHealthStructure.Band> bands = new ArrayList<>();
    bands.add(
        new TableHealthStructure.Band(
            "L0",
            null,
            null,
            "9007199254740993",
            Long.toString(Long.MAX_VALUE),
            "9007199254740993"));
    TableHealthStructure structure =
        new TableHealthStructure(
            "PRIMARY_KEY", "COMPLETE", 123, 100, "1", "1", "1", bands, Collections.emptyList());
    TableHealthDetails details =
        new TableHealthDetails("v3", Long.MAX_VALUE, null, 1L, "config", "key", null, null, null)
            .withStructure(structure);
    bands.clear();
    String json = JacksonUtil.toJSONString(details);
    assertEquals(details, JacksonUtil.parseObject(json, TableHealthDetails.class));
    String view =
        JacksonUtil.toJSONString(
            org.apache.amoro.table.descriptor.TableHealthDetailsView.from(details));
    org.junit.jupiter.api.Assertions.assertTrue(
        view.contains("\"fileCount\":\"9007199254740993\""));
    assertEquals(1, structure.getBands().size());
    assertThrows(UnsupportedOperationException.class, () -> structure.getBands().clear());
    TableHealthDetails legacy =
        new TableHealthDetails("v2", 1L, null, 1L, "config", "key", null, null, null);
    org.junit.jupiter.api.Assertions.assertNull(
        JacksonUtil.parseObject(JacksonUtil.toJSONString(legacy), TableHealthDetails.class)
            .getStructure());
  }

  @Test
  void testHealthScoreRangeIncludesUnavailableZeroAndHealthy() {
    new TableHealthComponent("UNAVAILABLE", -1, null, null, null);
    new TableHealthComponent("SEVERE", 0, null, null, null);
    new TableHealthComponent("EMPTY_TABLE", 100, null, null, null);

    assertThrows(
        IllegalArgumentException.class,
        () -> new TableHealthComponent("TOO_LOW", -2, null, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new TableHealthComponent("TOO_HIGH", 101, null, null, null));
  }

  @Test
  void testHealthDetailsJacksonRoundTrip() {
    TableHealthDetails details =
        new TableHealthDetails(
            "iceberg-legacy-v1",
            Long.MAX_VALUE,
            Long.MAX_VALUE - 1,
            null,
            "fingerprint",
            "evaluation-key",
            Collections.singletonList(new TableHealthComponent("SMALL_FILE", 40, 40, "SUM", null)),
            Collections.singletonMap("totalFileSize", Long.toString(Long.MAX_VALUE)),
            Collections.singletonList("EMPTY_TABLE"));

    String json = JacksonUtil.toJSONString(details);
    TableHealthDetails restored = JacksonUtil.parseObject(json, TableHealthDetails.class);

    assertEquals(details, restored);
    assertEquals(Long.MAX_VALUE, restored.getSnapshotId());
    assertEquals("9223372036854775807", restored.getMetrics().get("totalFileSize"));
  }
}
