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

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;

/**
 * The v2-scoped read-only view of managed tables (process spec §6.3). The production adapter reads
 * {@code table_identifier INNER JOIN table_metadata}; credentials and raw table objects never cross
 * this port.
 */
public interface ManagedTablePort {

  /** Returns a stable page strictly after {@code cursor}; null starts a new scan. */
  TablePage scanAfter(String cursor, int batchSize);

  /** Immutable cursor page. A null next cursor marks the end of the current stable traversal. */
  final class TablePage {
    private final List<TableSnapshot> tables;
    private final String nextCursor;

    public TablePage(List<TableSnapshot> tables, String nextCursor) {
      this.tables =
          Collections.unmodifiableList(
              new ArrayList<>(Objects.requireNonNull(tables, "tables")));
      this.nextCursor = nextCursor;
      if (tables.isEmpty() && nextCursor != null) {
        throw new IllegalArgumentException("an empty table page cannot advance the cursor");
      }
    }

    public List<TableSnapshot> tables() {
      return tables;
    }

    public String nextCursor() {
      return nextCursor;
    }
  }

  /**
   * Canonical coordinates plus the allowlisted maintenance stamps the gates consume.
   *
   * <p>Deviation note: the spec's ManagedTableSnapshot also carries an allowlisted config view, and
   * the port is cursor-paged and asynchronous; the first version ships the synchronous minimal
   * snapshot the interval gates need. The production adapter over {@code table_identifier ⋈
   * table_metadata} lands with the real format integrations.
   */
  final class TableSnapshot {
    private final String catalog;
    private final String database;
    private final String table;
    private final String tableId;
    private final String tableFormat; // iceberg | paimon | ...
    private final String lastMaintenanceAt; // RFC 3339; epoch when never maintained

    public TableSnapshot(
        String catalog,
        String database,
        String table,
        String tableId,
        String tableFormat,
        String lastMaintenanceAt) {
      this.catalog = catalog;
      this.database = database;
      this.table = table;
      this.tableId = tableId;
      this.tableFormat = tableFormat;
      this.lastMaintenanceAt = lastMaintenanceAt;
    }

    public String catalog() {
      return catalog;
    }

    public String database() {
      return database;
    }

    public String table() {
      return table;
    }

    public String tableId() {
      return tableId;
    }

    public String tableFormat() {
      return tableFormat;
    }

    public String lastMaintenanceAt() {
      return lastMaintenanceAt;
    }
  }
}
