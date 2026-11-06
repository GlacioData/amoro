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

package org.apache.amoro.server.process;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.Action;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.process.ActionCoordinator;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.TableProcess;
import org.apache.amoro.server.persistence.NestedSqlSession;
import org.apache.amoro.server.persistence.PersistenceFailureReporter;
import org.apache.amoro.server.persistence.PersistenceUnavailableDuringStartupException;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

class TestProcessStartupDatabaseFailure {
  private final Action action = Action.register("database-failure-review");
  private final SQLException failure = new SQLException("connection lost", "08006");
  private final SqlSession sql = mock(SqlSession.class);
  private final TableProcessMapper mapper = mock(TableProcessMapper.class);
  private final ActionCoordinator coordinator = mock(ActionCoordinator.class);
  private final TableRuntime runtime = mock(TableRuntime.class);
  private ProcessService service;

  @BeforeEach
  void setup() {
    when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of(1L, "catalog", "db", "table", TableFormat.PAIMON));
    when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);
    when(coordinator.action()).thenReturn(action);
    when(coordinator.formatSupported(TableFormat.PAIMON)).thenReturn(true);
    when(coordinator.parallelism()).thenReturn(1);
    when(sql.getMapper(TableProcessMapper.class)).thenReturn(mapper);
    when(mapper.selectActiveProcesses(anyCollection())).thenReturn(Arrays.asList(meta(1), meta(2)));
    service =
        new ProcessService(null) {
          @Override
          protected NestedSqlSession beginSession() {
            return NestedSqlSession.openSession(() -> sql);
          }
        };
    service.installActionCoordinator(coordinator);
  }

  @AfterEach
  void cleanup() {
    service.dispose();
  }

  @Test
  void failedSelectionCleanupStopsBeforeAnyProcessIsRecovered() {
    doAnswer(
            invocation -> {
              PersistenceFailureReporter.record(
                  PersistenceFailureReporter.Phase.POOL_CLEANUP, failure);
              return null;
            })
        .when(sql)
        .close();
    try (PersistenceFailureReporter.StartupScope ignored =
        PersistenceFailureReporter.startupScope()) {
      PersistenceUnavailableDuringStartupException thrown =
          assertThrows(
              PersistenceUnavailableDuringStartupException.class,
              () -> service.recoverProcesses(List.of(runtime)));
      assertSame(failure, thrown.getCause());
      verify(coordinator, never()).recoverTableProcess(any(), any());
      assertTrue(service.getActiveTableProcess().isEmpty());
    }
  }

  @Test
  void swallowedFailureDuringRecoveryStopsBeforeTrackingAndSubmission() {
    when(coordinator.recoverTableProcess(any(), any()))
        .thenAnswer(
            invocation -> {
              PersistenceFailureReporter.record(
                  PersistenceFailureReporter.Phase.POOL_CLEANUP, failure);
              return mock(TableProcess.class);
            });
    try (PersistenceFailureReporter.StartupScope ignored =
        PersistenceFailureReporter.startupScope()) {
      assertThrows(
          PersistenceUnavailableDuringStartupException.class,
          () -> service.recoverProcesses(List.of(runtime)));
      verify(coordinator).recoverTableProcess(any(), any());
      assertTrue(service.getActiveTableProcess().isEmpty());
    }
  }

  @Test
  void databaseFailureDuringRecoveryEscapesPerProcessFailureHandling() {
    when(coordinator.recoverTableProcess(any(), any()))
        .thenThrow(new IllegalStateException(failure));
    try (PersistenceFailureReporter.StartupScope ignored =
        PersistenceFailureReporter.startupScope()) {
      PersistenceUnavailableDuringStartupException thrown =
          assertThrows(
              PersistenceUnavailableDuringStartupException.class,
              () -> service.recoverProcesses(List.of(runtime)));
      assertSame(failure, thrown.getCause().getCause());
      verify(coordinator).recoverTableProcess(any(), any());
      assertTrue(service.getActiveTableProcess().isEmpty());
    }
  }

  @Test
  void previouslyFailedStartupDoesNotQueryProcessesAgain() {
    try (PersistenceFailureReporter.StartupScope ignored =
        PersistenceFailureReporter.startupScope()) {
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.ACQUIRE, failure);
      assertThrows(
          PersistenceUnavailableDuringStartupException.class,
          () -> service.recoverProcesses(List.of(runtime)));
      verify(mapper, never()).selectActiveProcesses(anyCollection());
    }
  }

  private TableProcessMeta meta(long id) {
    TableProcessMeta meta = new TableProcessMeta();
    meta.setProcessId(id);
    meta.setTableId(1L);
    meta.setProcessType(action.getName());
    meta.setStatus(ProcessStatus.RUNNING);
    return meta;
  }
}
