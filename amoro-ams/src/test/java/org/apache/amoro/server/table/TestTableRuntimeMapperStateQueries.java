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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Verifies that the table-id-scoped state query returns exactly the rows {@link
 * TableRuntimeMapper#selectAllStates()} would return for the same table ids, so {@code
 * DefaultTableService#syncBucketTables} can use it as a drop-in replacement for the cluster-wide
 * query.
 */
class TestTableRuntimeMapperStateQueries extends PersistentBase {

  private final DerbyPersistence derby = new DerbyPersistence();

  @AfterEach
  void cleanup() {
    derby.after();
  }

  @Test
  void selectStatesByTableIdsMatchesFilteredSelectAllStates() {
    ServerTableIdentifier table1 = createTable("db", "table1");
    ServerTableIdentifier table2 = createTable("db", "table2");
    ServerTableIdentifier table3 = createTable("db", "table3");
    ServerTableIdentifier table4 = createTable("db", "unqueriedTable");

    saveState(table1.getId(), "key1", "value1");
    saveState(table1.getId(), "key2", "value2");
    saveState(table3.getId(), "key1", "other-value1");
    // table3's second key is re-versioned so rows differ in more than identity columns;
    // table2 deliberately gets no state rows at all.
    saveState(table3.getId(), "key2", "other-value2");
    doAs(
        TableRuntimeMapper.class,
        mapper -> mapper.setStateValue(table3.getId(), "key2", "updated-value2"));
    // This row must be excluded. Without it, removing the scoped query's WHERE clause would
    // leave the comparison green because every state-bearing table is in queriedIds.
    saveState(table4.getId(), "key1", "must-not-be-returned");

    List<Long> queriedIds = Arrays.asList(table1.getId(), table2.getId(), table3.getId());
    List<TableRuntimeState> scoped =
        getAs(TableRuntimeMapper.class, mapper -> mapper.selectStatesByTableIds(queriedIds));

    // The scoped query must return exactly the rows selectAllStates() holds for the same ids,
    // compared row by row (TableRuntimeState has no equals, so compare every field).
    List<TableRuntimeState> allStates =
        getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates);
    Assertions.assertEquals(5, allStates.size());
    Map<Long, TableRuntimeState> expected =
        allStates.stream()
            .filter(state -> queriedIds.contains(state.getTableId()))
            .collect(Collectors.toMap(TableRuntimeState::getStateId, state -> state));

    Assertions.assertEquals(4, expected.size());
    Assertions.assertEquals(expected.size(), scoped.size());

    Map<Long, TableRuntimeState> scopedById = new HashMap<>();
    for (TableRuntimeState state : scoped) {
      Assertions.assertTrue(
          queriedIds.contains(state.getTableId()),
          "scoped query leaked state of an un-queried table: " + state);
      Assertions.assertNull(scopedById.put(state.getStateId(), state), "duplicate state row");
    }

    expected.forEach(
        (stateId, expectedState) -> {
          TableRuntimeState actual = scopedById.get(stateId);
          Assertions.assertNotNull(actual, "missing state row " + stateId);
          Assertions.assertEquals(expectedState.getStateId(), actual.getStateId());
          Assertions.assertEquals(expectedState.getTableId(), actual.getTableId());
          Assertions.assertEquals(expectedState.getStateKey(), actual.getStateKey());
          Assertions.assertEquals(expectedState.getStateValue(), actual.getStateValue());
          Assertions.assertEquals(expectedState.getStateVersion(), actual.getStateVersion());
        });

    // table2 has no state rows; the scoped query must not fail or invent rows for it.
    Assertions.assertEquals(
        0, scoped.stream().filter(s -> s.getTableId() == table2.getId()).count());
  }

  /**
   * The {@code InListExtendedLanguageDriver} renders an empty list as {@code IN ()}, which is
   * invalid SQL. Callers (e.g. {@code DefaultTableService#syncBucketTables}) must therefore
   * short-circuit before invoking the scoped query when they hold no table ids; this test records
   * that mapper-level contract.
   */
  @Test
  void emptyTableIdsIsRejectedByTheDatabase() {
    ServerTableIdentifier table1 = createTable("db", "table1");
    saveState(table1.getId(), "key1", "value1");

    Assertions.assertThrows(
        PersistenceException.class,
        () ->
            getAs(
                TableRuntimeMapper.class,
                mapper -> mapper.selectStatesByTableIds(Collections.emptyList())));
  }

  private ServerTableIdentifier createTable(String database, String tableName) {
    ServerTableIdentifier identifier =
        ServerTableIdentifier.of("catalog", database, tableName, TableFormat.ICEBERG);
    doAs(TableMetaMapper.class, mapper -> mapper.insertTable(identifier));
    return identifier;
  }

  private void saveState(long tableId, String key, String value) {
    doAs(TableRuntimeMapper.class, mapper -> mapper.saveState(tableId, key, value));
  }
}
