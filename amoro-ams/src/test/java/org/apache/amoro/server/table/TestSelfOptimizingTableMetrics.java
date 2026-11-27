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
import org.apache.amoro.config.Configurations;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.process.PaimonProcessFactory;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.process.iceberg.IcebergProcessFactory;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.table.TableRuntimeFactory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
      when(first.getTableConfig()).thenReturn(optimizingConfig(false));
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
      when(third.getTableConfig()).thenReturn(null);
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

  @Test
  void countsPaimonOptInEligibilityAndIcebergDefaultFromCachedConfiguration() {
    PaimonProcessFactory paimonFactory = new PaimonProcessFactory();
    paimonFactory.open(
        Collections.singletonMap(PaimonProcessFactory.OPTIMIZER_ENABLED.key(), "true"));
    ProcessFactoryRouter router =
        new ProcessFactoryRouter(Arrays.asList(new IcebergProcessFactory(), paimonFactory));
    MetricRegistry registry = new MetricRegistry();
    Map<Long, TableRuntime> runtimes = new ConcurrentHashMap<>();
    try (SelfOptimizingTableMetrics metrics =
        new SelfOptimizingTableMetrics(registry, runtimes::values, router)) {
      TableRuntime paimonDefault =
          runtime(1, "paimon_cat", TableFormat.PAIMON, Collections.emptyMap());
      TableRuntime paimonOnlySwitch =
          runtime(
              2,
              "paimon_cat",
              TableFormat.PAIMON,
              Collections.singletonMap(
                  PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true"));
      TableRuntime paimonOnlyWriteOnly =
          runtime(
              3, "paimon_cat", TableFormat.PAIMON, Collections.singletonMap("write-only", "true"));
      Map<String, String> paimonEligibleProps = new HashMap<>();
      paimonEligibleProps.put("write-only", "true");
      paimonEligibleProps.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");
      TableRuntime paimonEligible =
          runtime(4, "paimon_cat", TableFormat.PAIMON, paimonEligibleProps);

      TableRuntime icebergDefault =
          runtime(5, "iceberg_cat", TableFormat.ICEBERG, Collections.emptyMap());
      TableRuntime icebergDisabled =
          runtime(
              6,
              "iceberg_cat",
              TableFormat.ICEBERG,
              Collections.singletonMap(TableProperties.ENABLE_SELF_OPTIMIZING, "false"));

      for (TableRuntime runtime :
          new TableRuntime[] {
            paimonDefault,
            paimonOnlySwitch,
            paimonOnlyWriteOnly,
            paimonEligible,
            icebergDefault,
            icebergDisabled
          }) {
        runtimes.put(runtime.getTableIdentifier().getId(), runtime);
        metrics.register(runtime);
      }

      assertCounts(registry, "paimon_cat", "PAIMON", 1, 3);
      assertCounts(registry, "iceberg_cat", "ICEBERG", 1, 1);

      when(paimonDefault.getTableConfig()).thenReturn(paimonEligibleProps);
      assertCounts(registry, "paimon_cat", "PAIMON", 2, 2);

      Map<String, String> paimonExplicitFalse = new HashMap<>(paimonEligibleProps);
      paimonExplicitFalse.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "false");
      when(paimonEligible.getTableConfig()).thenReturn(paimonExplicitFalse);
      assertCounts(registry, "paimon_cat", "PAIMON", 1, 3);
      verify(paimonDefault, never()).loadTable();
      verify(paimonEligible, never()).loadTable();
    }
  }

  @Test
  void respectsProcessFactoryRouterFormatSupportAndIsolatesEvaluationFailures() {
    Map<String, String> paimonEligibleProps = new HashMap<>();
    paimonEligibleProps.put("write-only", "true");
    paimonEligibleProps.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");

    PaimonProcessFactory disabledPaimonFactory = new PaimonProcessFactory();
    disabledPaimonFactory.open(
        Collections.singletonMap(PaimonProcessFactory.OPTIMIZER_ENABLED.key(), "false"));
    ProcessFactoryRouter routerWithoutPaimon =
        new ProcessFactoryRouter(Arrays.asList(new IcebergProcessFactory(), disabledPaimonFactory));

    MetricRegistry registry = new MetricRegistry();
    Map<Long, TableRuntime> runtimes = new ConcurrentHashMap<>();
    try (SelfOptimizingTableMetrics metrics =
        new SelfOptimizingTableMetrics(registry, runtimes::values, routerWithoutPaimon)) {
      TableRuntime paimonRuntime = runtime(1, "lake", TableFormat.PAIMON, paimonEligibleProps);
      TableRuntime icebergRuntime = runtime(2, "lake", TableFormat.ICEBERG, Collections.emptyMap());
      runtimes.put(1L, paimonRuntime);
      runtimes.put(2L, icebergRuntime);
      metrics.register(paimonRuntime);
      metrics.register(icebergRuntime);

      assertCounts(registry, "lake", "PAIMON", 0, 1);
      assertCounts(registry, "lake", "ICEBERG", 1, 0);
    }

    ProcessFactory throwingFactory = mock(ProcessFactory.class);
    when(throwingFactory.name()).thenReturn("throwing");
    when(throwingFactory.supportedFormats())
        .thenReturn(Collections.singleton(TableFormat.MIXED_ICEBERG));
    TableRuntime badRuntime =
        runtime(10, "lake", TableFormat.MIXED_ICEBERG, Collections.emptyMap());
    TableRuntime goodRuntime =
        runtime(11, "lake", TableFormat.MIXED_ICEBERG, Collections.emptyMap());
    when(throwingFactory.isOptimizingEligible(badRuntime))
        .thenThrow(new IllegalStateException("corrupt config"));
    when(throwingFactory.isOptimizingEligible(goodRuntime)).thenReturn(true);

    MetricRegistry isolatedRegistry = new MetricRegistry();
    Map<Long, TableRuntime> isolatedRuntimes = new ConcurrentHashMap<>();
    try (SelfOptimizingTableMetrics metrics =
        new SelfOptimizingTableMetrics(
            isolatedRegistry,
            isolatedRuntimes::values,
            new ProcessFactoryRouter(Collections.singletonList(throwingFactory)))) {
      isolatedRuntimes.put(10L, badRuntime);
      isolatedRuntimes.put(11L, goodRuntime);
      metrics.register(badRuntime);
      metrics.register(goodRuntime);

      assertCounts(isolatedRegistry, "lake", "MIXED_ICEBERG", 1, 0);
    }
  }

  @Test
  void unreadableRuntimeConfigurationDoesNotPreventCountingOtherTables() {
    MetricRegistry registry = new MetricRegistry();
    TableRuntime unreadable = runtime(1, "lake", TableFormat.ICEBERG, true);
    when(unreadable.getTableConfig()).thenThrow(new IllegalStateException("unreadable"));
    TableRuntime healthy = runtime(2, "lake", TableFormat.ICEBERG, Collections.emptyMap());
    try (SelfOptimizingTableMetrics metrics =
        new SelfOptimizingTableMetrics(registry, () -> Arrays.asList(unreadable, healthy))) {
      metrics.register(unreadable);
      metrics.register(healthy);
      assertCounts(registry, "lake", "ICEBERG", 1, 0);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void defaultTableServiceWiresOptimizingRouterIntoSelfOptimizingMetrics() throws Exception {
    PaimonProcessFactory paimonFactory = new PaimonProcessFactory();
    paimonFactory.open(
        Collections.singletonMap(PaimonProcessFactory.OPTIMIZER_ENABLED.key(), "true"));
    ProcessFactoryRouter router =
        new ProcessFactoryRouter(Arrays.asList(new IcebergProcessFactory(), paimonFactory));

    DefaultTableService service =
        new DefaultTableService(
            new Configurations(),
            mock(CatalogManager.class),
            mock(TableRuntimeFactory.class),
            null,
            null,
            router);
    try {
      Field initialized = DefaultTableService.class.getDeclaredField("initialized");
      initialized.setAccessible(true);
      ((CompletableFuture<Boolean>) initialized.get(service)).complete(true);

      Map<String, String> eligibleProps = new HashMap<>();
      eligibleProps.put("write-only", "true");
      eligibleProps.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, "true");

      service.setRuntime(runtime(101, "svc_test_cat", TableFormat.PAIMON, Collections.emptyMap()));
      service.setRuntime(runtime(102, "svc_test_cat", TableFormat.PAIMON, eligibleProps));

      assertCounts(MetricManager.getInstance().getGlobalRegistry(), "svc_test_cat", "PAIMON", 1, 1);
    } finally {
      service.dispose();
    }
  }

  private static TableRuntime runtime(
      long id, String catalog, TableFormat format, boolean enabled) {
    return runtime(id, catalog, format, optimizingConfig(enabled));
  }

  private static TableRuntime runtime(
      long id, String catalog, TableFormat format, Map<String, String> config) {
    TableRuntime runtime = mock(TableRuntime.class);
    when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of(id, catalog, "db", "table" + id, format));
    when(runtime.getFormat()).thenReturn(format);
    when(runtime.getTableConfig()).thenReturn(config);
    return runtime;
  }

  private static Map<String, String> optimizingConfig(boolean enabled) {
    return Collections.singletonMap(
        TableProperties.ENABLE_SELF_OPTIMIZING, String.valueOf(enabled));
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
