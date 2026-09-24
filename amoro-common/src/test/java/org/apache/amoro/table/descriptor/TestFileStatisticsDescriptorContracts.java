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

package org.apache.amoro.table.descriptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.amoro.table.health.TableFileStatisticsSnapshot;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class TestFileStatisticsDescriptorContracts {
  @Test
  void exactStatisticsDoNotExposeInternalObservationOrDuplicateHealth() throws Exception {
    ServerTableMeta meta = new ServerTableMeta();
    TableDetailObservation observation = new TableDetailObservation(Long.MAX_VALUE, 0);
    meta.setDetailObservation(observation);
    meta.setFileStatistics(
        new TableFileStatisticsView(
            TableFileStatisticsView.Status.CURRENT,
            TableFileStatisticsView.Source.BACKGROUND,
            new TableFileStatisticsSnapshot(
                Long.MAX_VALUE, 0, 9007199254740993L, Long.MAX_VALUE, 1234),
            observation,
            true,
            Collections.emptyList()));
    JsonNode json = new ObjectMapper().valueToTree(meta);
    assertFalse(json.has("detailObservation"));
    assertFalse(json.get("fileStatistics").has("evaluation"));
    assertEquals("9007199254740993", json.at("/fileStatistics/fileCount").asText());
    assertTrue(json.at("/fileStatistics/fileCount").isTextual());
    assertEquals("9223372036854775807", json.at("/fileStatistics/totalSizeBytes").asText());
    assertEquals("1023", json.at("/fileStatistics/averageFileSizeBytes").asText());
    assertEquals("CURRENT", json.at("/fileStatistics/status").asText());
    assertFalse(new ObjectMapper().valueToTree(new ServerTableMeta()).has("fileStatistics"));
  }

  @Test
  void explicitEmptyObservationHasNoFabricatedStatisticsTime() {
    TableDetailObservation empty = new TableDetailObservation(null, 0);
    TableFileStatisticsView view =
        new TableFileStatisticsView(
            TableFileStatisticsView.Status.CURRENT,
            TableFileStatisticsView.Source.EMPTY_SNAPSHOT,
            null,
            empty,
            null,
            Collections.emptyList());
    assertTrue(empty.isEmptySnapshot());
    assertEquals("0", view.getFileCount());
    assertEquals("0", view.getTotalSizeBytes());
    assertNull(view.getEvaluatedAtMillis());
    assertThrows(IllegalArgumentException.class, () -> new TableDetailObservation(0L, 0));
    assertThrows(IllegalArgumentException.class, () -> new TableDetailObservation(1L, -1));
  }
}
