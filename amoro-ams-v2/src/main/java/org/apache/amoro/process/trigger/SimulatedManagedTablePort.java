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

package org.apache.amoro.process.trigger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Deterministic in-memory table-fact provider for explicit simulation and tests. It never loads an
 * Iceberg/Paimon table and has no adapter to the v1 AMS metadata/runtime stores.
 */
public final class SimulatedManagedTablePort implements ManagedTablePort {

  private final List<TableSnapshot> ordered;

  public SimulatedManagedTablePort(Collection<TableSnapshot> tables) {
    this.ordered = new ArrayList<>(Objects.requireNonNull(tables, "tables"));
    this.ordered.sort(Comparator.comparing(TableSnapshot::tableId));
    for (int i = 1; i < ordered.size(); i++) {
      if (ordered.get(i - 1).tableId().equals(ordered.get(i).tableId())) {
        throw new IllegalArgumentException("duplicate simulated tableId " + ordered.get(i).tableId());
      }
    }
  }

  @Override
  public TablePage scanAfter(String cursor, int batchSize) {
    if (batchSize < 1 || batchSize > 1000) {
      throw new IllegalArgumentException("batchSize must be in [1, 1000]");
    }
    List<TableSnapshot> remaining =
        ordered.stream()
            .filter(table -> cursor == null || table.tableId().compareTo(cursor) > 0)
            .collect(Collectors.toList());
    int end = Math.min(batchSize, remaining.size());
    List<TableSnapshot> page = new ArrayList<>(remaining.subList(0, end));
    String next = end < remaining.size() ? page.get(page.size() - 1).tableId() : null;
    return new TablePage(page, next);
  }
}
