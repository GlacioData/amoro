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

package org.apache.amoro.optimizing.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.amoro.TableFormat;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.health.IcebergLegacyHealthAdapter;
import org.apache.amoro.table.BasicTableSnapshot;
import org.apache.amoro.table.FormatPendingInput;
import org.apache.amoro.table.KeyedTableSnapshot;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthComponent;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestPendingInputFormatContract {

  @Test
  public void icebergPendingInputImplementsFormatPendingInput() {
    AbstractOptimizingEvaluator.PendingInput pendingInput =
        new AbstractOptimizingEvaluator.PendingInput();

    FormatPendingInput formatPendingInput =
        assertInstanceOf(FormatPendingInput.class, pendingInput);

    assertEquals(0, formatPendingInput.getDataFileCount());
    assertEquals(0L, formatPendingInput.getDataFileSize());
    assertEquals(0, formatPendingInput.getTotalFileCount());
    assertEquals(0L, formatPendingInput.getTotalFileSize());
    assertEquals(-1, formatPendingInput.getHealthScore());
  }

  @Test
  public void adaptsLegacyScoresWithoutReplacingPendingInput() {
    AbstractOptimizingEvaluator.PendingInput pendingInput =
        new AbstractOptimizingEvaluator.PendingInput() {
          @Override
          public int getHealthScore() {
            return 66;
          }

          @Override
          public int getSmallFileScore() {
            return 23;
          }

          @Override
          public int getEqualityDeleteScore() {
            return 31;
          }

          @Override
          public int getPositionalDeleteScore() {
            return 12;
          }
        };
    TableAnalysisKey key =
        IcebergLegacyHealthAdapter.createKey(
            TableIdentifier.of("catalog", "database", "table"),
            TableFormat.ICEBERG,
            new BasicTableSnapshot(11L),
            7L,
            new OptimizingConfig());

    FormatTableAnalysis analysis = IcebergLegacyHealthAdapter.adapt(key, pendingInput);

    assertSame(pendingInput, analysis.pendingInput());
    assertEquals(66, analysis.pendingInput().getHealthScore());
    List<TableHealthComponent> components = analysis.healthDetails().getComponents();
    assertEquals(3, components.size());
    assertComponent(components.get(0), "SMALL_FILE", 23, 40);
    assertComponent(components.get(1), "EQUALITY_DELETE", 31, 40);
    assertComponent(components.get(2), "POSITIONAL_DELETE", 12, 20);
  }

  @Test
  public void icebergSnapshotChangeChangesAnalysisKey() {
    TableIdentifier identifier = TableIdentifier.of("catalog", "database", "table");
    OptimizingConfig config = new OptimizingConfig();

    TableAnalysisKey first =
        IcebergLegacyHealthAdapter.createKey(
            identifier, TableFormat.ICEBERG, new BasicTableSnapshot(11L), 7L, config);
    TableAnalysisKey second =
        IcebergLegacyHealthAdapter.createKey(
            identifier, TableFormat.ICEBERG, new BasicTableSnapshot(12L), 7L, config);

    assertNotEquals(first, second);
  }

  @Test
  public void mixedBaseOrChangeSnapshotChangeChangesAnalysisKey() {
    TableIdentifier identifier = TableIdentifier.of("catalog", "database", "table");
    OptimizingConfig config = new OptimizingConfig();
    TableAnalysisKey initial =
        IcebergLegacyHealthAdapter.createKey(
            identifier, TableFormat.MIXED_ICEBERG, new KeyedTableSnapshot(11L, 21L), 7L, config);
    TableAnalysisKey baseChanged =
        IcebergLegacyHealthAdapter.createKey(
            identifier, TableFormat.MIXED_ICEBERG, new KeyedTableSnapshot(12L, 21L), 7L, config);
    TableAnalysisKey changeChanged =
        IcebergLegacyHealthAdapter.createKey(
            identifier, TableFormat.MIXED_ICEBERG, new KeyedTableSnapshot(11L, 22L), 7L, config);

    assertNotEquals(initial, baseChanged);
    assertNotEquals(initial, changeChanged);
  }

  private static void assertComponent(
      TableHealthComponent component, String code, int score, int weight) {
    assertEquals(code, component.getCode());
    assertEquals(score, component.getScore());
    assertEquals(weight, component.getWeight());
    assertEquals("SUM", component.getCombination());
  }
}
