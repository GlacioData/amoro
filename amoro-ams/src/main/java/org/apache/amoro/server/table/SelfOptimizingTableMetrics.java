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

import org.apache.amoro.TableRuntime;
import org.apache.amoro.metrics.Gauge;
import org.apache.amoro.metrics.MetricDefine;
import org.apache.amoro.metrics.MetricKey;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.utils.CompatiblePropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Counts the runtimes currently loaded by this table service, using ordinary in-memory gauges. */
final class SelfOptimizingTableMetrics implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SelfOptimizingTableMetrics.class);

  static final MetricDefine TABLES =
      MetricDefine.defineGauge("ams_self_optimizing_tables")
          .withDescription("Locally loaded tables by the effective self-optimizing switch")
          .withTags("catalog", "format", "enabled")
          .build();

  private final MetricRegistry registry;
  private final Supplier<Collection<TableRuntime>> runtimes;
  private final ProcessFactoryRouter optimizingRouter;
  private final Map<List<String>, List<MetricKey>> registered = new HashMap<>();
  private boolean closed;

  SelfOptimizingTableMetrics(MetricRegistry registry, Supplier<Collection<TableRuntime>> runtimes) {
    this(registry, runtimes, null);
  }

  SelfOptimizingTableMetrics(
      MetricRegistry registry,
      Supplier<Collection<TableRuntime>> runtimes,
      ProcessFactoryRouter optimizingRouter) {
    this.registry = registry;
    this.runtimes = runtimes;
    this.optimizingRouter = optimizingRouter;
  }

  synchronized void register(TableRuntime runtime) {
    if (closed) {
      return;
    }
    String catalog = runtime.getTableIdentifier().getCatalog();
    String format = runtime.getTableIdentifier().getFormat().name();
    List<String> dimension = Arrays.asList(catalog, format);
    if (registered.containsKey(dimension)) {
      return;
    }
    List<MetricKey> keys = new ArrayList<>();
    try {
      for (boolean enabled : new boolean[] {true, false}) {
        keys.add(
            registry.register(
                TABLES,
                ImmutableMap.of(
                    "catalog", catalog, "format", format, "enabled", Boolean.toString(enabled)),
                (Gauge<Long>) () -> count(catalog, format, enabled)));
      }
      registered.put(dimension, keys);
    } catch (RuntimeException failure) {
      keys.forEach(registry::unregister);
      throw failure;
    }
  }

  private long count(String catalog, String format, boolean enabled) {
    long count = 0;
    for (TableRuntime runtime : runtimes.get()) {
      if (catalog.equals(runtime.getTableIdentifier().getCatalog())
          && format.equals(runtime.getTableIdentifier().getFormat().name())) {
        Boolean effectiveEnabled = isSelfOptimizingEnabled(runtime);
        if (effectiveEnabled != null && effectiveEnabled == enabled) {
          count++;
        }
      }
    }
    return count;
  }

  private Boolean isSelfOptimizingEnabled(TableRuntime runtime) {
    try {
      Map<String, String> tableConfig = runtime.getTableConfig();
      if (tableConfig == null) {
        return null;
      }
      return CompatiblePropertyUtil.propertyAsBoolean(
              tableConfig,
              TableProperties.ENABLE_SELF_OPTIMIZING,
              TableProperties.ENABLE_SELF_OPTIMIZING_DEFAULT)
          && (optimizingRouter == null
              || (optimizingRouter.supportedFormats().contains(runtime.getFormat())
                  && optimizingRouter.isOptimizingEligible(runtime)));
    } catch (RuntimeException e) {
      LOG.debug(
          "Failed to evaluate self-optimizing metric eligibility for table ID {} ({})",
          runtime.getTableIdentifier().getId(),
          e.getClass().getSimpleName());
      return null;
    }
  }

  @Override
  public synchronized void close() {
    closed = true;
    registered.values().forEach(keys -> keys.forEach(registry::unregister));
    registered.clear();
  }
}
