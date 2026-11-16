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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class TestSelfOptimizingTableMetrics {
  @Test
  void countsCurrentLocalConfigurationAndRemovesUnloadedTables() {
    MetricRegistry registry = new MetricRegistry();
    Map<Long, TableRuntime> runtimes = new ConcurrentHashMap<>();
    try (SelfOptimizingTableMetrics metrics =
        new SelfOptimizingTableMetrics(registry, runtimes::values)) {
      assertTrue(registry.getMetrics().isEmpty());
      TableRuntime first = runtime(1, "lake", TableFormat.PAIMON, true);
      TableRuntime second = runtime(2, "lake", TableFormat.PAIMON, false);
      runtimes.put(1L, first);
      runtimes.put(2L, second);
      metrics.register(first);
      metrics.register(second);
      metrics.register(first);
      assertEquals(2, registry.getMetrics().size());
      assertCounts(registry, "lake", "PAIMON", 1, 1);
      when(first.getTableConfiguration()).thenReturn(configuration(false));
      assertCounts(registry, "lake", "PAIMON", 0, 2);
      runtimes.remove(2L);
      assertCounts(registry, "lake", "PAIMON", 0, 1);
      runtimes.clear();
      assertCounts(registry, "lake", "PAIMON", 0, 0);
      verify(first, never()).loadTable();
      verify(second, never()).loadTable();
    }
    assertTrue(registry.getMetrics().isEmpty());
  }

  @Test
  void separatesCatalogAndFormatAndSkipsMissingConfiguration() {
    MetricRegistry registry = new MetricRegistry();
    Map<Long, TableRuntime> runtimes = new ConcurrentHashMap<>();
    SelfOptimizingTableMetrics metrics = new SelfOptimizingTableMetrics(registry, runtimes::values);
    try {
      TableRuntime first = runtime(1, "lake", TableFormat.PAIMON, true);
      TableRuntime second = runtime(2, "lake", TableFormat.ICEBERG, false);
      TableRuntime third = runtime(3, "other", TableFormat.PAIMON, true);
      when(third.getTableConfiguration()).thenReturn(null);
      for (TableRuntime runtime : new TableRuntime[] {first, second, third}) {
        runtimes.put(runtime.getTableIdentifier().getId(), runtime);
        metrics.register(runtime);
      }
      assertCounts(registry, "lake", "PAIMON", 1, 0);
      assertCounts(registry, "lake", "ICEBERG", 0, 1);
      assertCounts(registry, "other", "PAIMON", 0, 0);
      TableRuntime replacement = runtime(1, "lake", TableFormat.PAIMON, false);
      runtimes.put(1L, replacement);
      metrics.register(replacement);
      assertCounts(registry, "lake", "PAIMON", 0, 1);
      assertEquals(6, registry.getMetrics().size());
      metrics.close();
      metrics.register(first);
      assertTrue(registry.getMetrics().isEmpty());
    } finally {
      metrics.close();
    }
  }

  private static TableRuntime runtime(
      long id, String catalog, TableFormat format, boolean enabled) {
    TableRuntime runtime = mock(TableRuntime.class);
    when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of(id, catalog, "db", "table" + id, format));
    when(runtime.getTableConfiguration()).thenReturn(configuration(enabled));
    return runtime;
  }

  private static TableConfiguration configuration(boolean enabled) {
    OptimizingConfig optimizing = new OptimizingConfig();
    optimizing.setEnabled(enabled);
    return new TableConfiguration().setOptimizingConfig(optimizing);
  }

  private static void assertCounts(
      MetricRegistry registry, String catalog, String format, long enabled, long disabled) {
    Map<String, String> tags = new HashMap<>();
    tags.put("catalog", catalog);
    tags.put("format", format);
    tags.put("enabled", "true");
    assertEquals(
        enabled,
        ((Gauge<?>)
                registry.getMetrics().get(new MetricKey(SelfOptimizingTableMetrics.TABLES, tags)))
            .getValue());
    tags.put("enabled", "false");
    assertEquals(
        disabled,
        ((Gauge<?>)
                registry.getMetrics().get(new MetricKey(SelfOptimizingTableMetrics.TABLES, tags)))
            .getValue());
  }
}
