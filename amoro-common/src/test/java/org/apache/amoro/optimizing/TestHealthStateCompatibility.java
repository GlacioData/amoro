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

package org.apache.amoro.optimizing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableSummary;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.utils.JacksonUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class TestHealthStateCompatibility {

  @Test
  void testLegacyOptimizingStateJsonKeepsExistingCheckpointDefault() {
    TableRuntimeOptimizingState state =
        JacksonUtil.parseObject("{}", TableRuntimeOptimizingState.class);

    assertEquals(-1L, state.getLastOptimizedSnapshotId());
  }

  @Test
  void testRemovedSuccessfulBaselineFieldsAreIgnoredByStateKeyJsonCompatibilityPath() {
    StateKey<TableRuntimeOptimizingState> stateKey =
        StateKey.stateKey("optimizing_state")
            .jsonType(TableRuntimeOptimizingState.class)
            .defaultValue(new TableRuntimeOptimizingState());

    TableRuntimeOptimizingState restored =
        stateKey.deserialize(
            "{\"lastOptimizedSnapshotId\":42,"
                + "\"lastSuccessfulOptimizingSnapshotId\":100,"
                + "\"lastSuccessfulOptimizingSnapshotTimeMillis\":200,"
                + "\"lastSuccessfulOptimizingProcessId\":300}");

    assertEquals(42L, restored.getLastOptimizedSnapshotId());
    String serialized = stateKey.serialize(restored);
    assertFalse(serialized.contains("lastSuccessfulOptimizingSnapshotId"));
    assertFalse(serialized.contains("lastSuccessfulOptimizingSnapshotTimeMillis"));
    assertFalse(serialized.contains("lastSuccessfulOptimizingProcessId"));
  }

  @Test
  void testLegacyOptimizationContextDefaultsToNoOptimizedCheckpoint() {
    OptimizationContext context = new LegacyOptimizationContext();

    assertEquals(-1L, context.getLastOptimizedSnapshotId());
  }

  @Test
  void testLegacyTableSummaryJsonKeepsHealthDetailsEmpty() {
    TableSummary emptyLegacySummary = JacksonUtil.parseObject("{}", TableSummary.class);
    TableSummary scoredLegacySummary =
        JacksonUtil.parseObject("{\"healthScore\":72}", TableSummary.class);

    assertEquals(-1, emptyLegacySummary.getHealthScore());
    assertNull(emptyLegacySummary.getHealthDetails());
    assertEquals(72, scoredLegacySummary.getHealthScore());
    assertNull(scoredLegacySummary.getHealthDetails());
  }

  @Test
  void testTableSummaryHealthDetailsRoundTripsAndCopies() {
    TableHealthDetails details =
        new TableHealthDetails(
            "paimon-append-health-v2",
            Long.MAX_VALUE,
            null,
            7L,
            "config",
            "key",
            Collections.emptyList(),
            Collections.singletonMap("totalFileSize", Long.toString(Long.MAX_VALUE)),
            Collections.singletonList("EMPTY_TABLE"));
    TableSummary summary = new TableSummary();
    summary.setHealthScore(100);
    summary.setHealthDetails(details);

    TableSummary restored =
        JacksonUtil.parseObject(JacksonUtil.toJSONString(summary), TableSummary.class);
    TableSummary copied = summary.copy();

    assertEquals(Long.valueOf(Long.MAX_VALUE), restored.getHealthDetails().getSnapshotId());
    assertEquals(
        "9223372036854775807", restored.getHealthDetails().getMetrics().get("totalFileSize"));
    assertNotSame(summary, copied);
    assertEquals(details, copied.getHealthDetails());
  }

  private static class LegacyOptimizationContext implements OptimizationContext {

    @Override
    public ServerTableIdentifier getTableIdentifier() {
      return null;
    }

    @Override
    public OptimizingConfig getOptimizingConfig() {
      return null;
    }

    @Override
    public boolean isIdle() {
      return true;
    }

    @Override
    public long getLastPlanTime() {
      return 0L;
    }

    @Override
    public long getLastMinorOptimizingTime() {
      return 0L;
    }

    @Override
    public long getLastFullOptimizingTime() {
      return 0L;
    }

    @Override
    public long getLastMajorOptimizingTime() {
      return 0L;
    }
  }
}
