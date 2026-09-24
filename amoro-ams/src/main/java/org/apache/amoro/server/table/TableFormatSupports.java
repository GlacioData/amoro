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

import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.TableFormat;
import org.apache.amoro.table.TableRuntimeSupport;
import org.apache.amoro.table.health.TableHealthSupport;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Cached format capabilities from the existing catalog SPI, without constructing catalogs/tables.
 */
public final class TableFormatSupports {
  // ServiceLoader is not thread-safe: discover once at initialization, then only read the maps.
  // https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html
  private static final TableFormatSupports INSTANCE =
      new TableFormatSupports(ServiceLoader.load(FormatCatalogFactory.class));

  private final Map<TableFormat, TableHealthSupport> health = new HashMap<>();
  private final Map<TableFormat, TableRuntimeSupport> runtime = new HashMap<>();

  TableFormatSupports(Iterable<FormatCatalogFactory> factories) {
    for (FormatCatalogFactory factory : factories) {
      if (health.putIfAbsent(factory.format(), Objects.requireNonNull(factory.healthSupport()))
          != null) {
        throw new IllegalStateException("Duplicate catalog format factory: " + factory.format());
      }
      factory.runtimeSupport().ifPresent(support -> runtime.put(factory.format(), support));
    }
  }

  public static TableFormatSupports instance() {
    return INSTANCE;
  }

  public TableHealthSupport healthSupport(TableFormat format) {
    return health.getOrDefault(format, TableHealthSupport.NONE);
  }

  public Optional<TableRuntimeSupport> runtimeSupport(TableFormat format) {
    return Optional.ofNullable(runtime.get(format));
  }
}
