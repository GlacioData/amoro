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

package org.apache.amoro.server.persistence;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.Action;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.server.process.DefaultTableProcessStore;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.server.table.DefaultTableRuntimeStore;
import org.apache.amoro.server.table.FullCompactionCompletion;
import org.apache.amoro.table.TableSummary;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

class TestStoreTransactionFailure {
  @ParameterizedTest
  @ValueSource(strings = {"runtime", "process", "full-compaction"})
  void rollbackFailureCannotReplaceOriginalStoreFailure(String entryPoint) {
    NestedSqlSession session = mock(NestedSqlSession.class);
    SqlSession sql = mock(SqlSession.class);
    PersistenceException original =
        new PersistenceException(new SQLException("operation failed", "08006"));
    IllegalStateException rollbackFailure = new IllegalStateException("rollback failed");
    when(session.getSqlSession()).thenReturn(sql);
    when(sql.getMapper(any())).thenThrow(original);
    doThrow(rollbackFailure).when(session).rollback();

    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(1L);
    meta.setTableConfig(Collections.emptyMap());
    meta.setTableSummary(new TableSummary());
    DefaultTableRuntimeStore runtime =
        new DefaultTableRuntimeStore(
            ServerTableIdentifier.of(1L, "catalog", "db", "table", TableFormat.PAIMON),
            meta,
            Collections.emptyList(),
            Collections.emptyList()) {
          @Override
          protected NestedSqlSession beginSession() {
            return session;
          }
        };
    Runnable operation;
    if ("runtime".equals(entryPoint)) {
      operation = () -> runtime.begin().updateTableSummary(ignored -> {}).commit();
    } else if ("process".equals(entryPoint)) {
      DefaultTableProcessStore process =
          new DefaultTableProcessStore(
              mock(TableRuntime.class), new TableProcessMeta(), Action.register("FAILURE_TEST")) {
            @Override
            protected NestedSqlSession beginSession() {
              return session;
            }
          };
      operation = () -> process.begin().updateRetryNumber(1).commit();
    } else {
      FullCompactionCompletion completion =
          new FullCompactionCompletion() {
            @Override
            protected NestedSqlSession beginSession() {
              return session;
            }
          };
      operation =
          () ->
              completion.complete(
                  runtime,
                  1L,
                  new FullCompactionCompletion.Decision(1L, 1L, 1L),
                  Collections.emptyMap());
    }

    PersistenceException thrown = assertThrows(PersistenceException.class, operation::run);
    assertSame(original, thrown);
    assertTrue(Arrays.asList(thrown.getSuppressed()).contains(rollbackFailure));
    verify(session, atLeastOnce()).rollback();
    verify(session, atLeastOnce()).close();
  }
}
