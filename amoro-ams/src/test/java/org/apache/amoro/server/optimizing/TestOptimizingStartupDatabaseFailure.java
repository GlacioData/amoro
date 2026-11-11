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

package org.apache.amoro.server.optimizing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.persistence.PersistenceFailureReporter;
import org.apache.amoro.server.persistence.PersistenceUnavailableDuringStartupException;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.iceberg.common.DynFields;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

class TestOptimizingStartupDatabaseFailure {
  private static final DynFields.UnboundField<MetricManager> MANAGER =
      DynFields.builder().hiddenImpl(MetricManager.class, "INSTANCE").build();
  private MetricManager previous;
  private MetricRegistry registry;

  @BeforeEach
  void setup() {
    previous = MANAGER.asStatic().get();
    MetricManager manager = mock(MetricManager.class);
    registry = new MetricRegistry();
    when(manager.getGlobalRegistry()).thenReturn(registry);
    MANAGER.asStatic().set(manager);
  }

  @AfterEach
  void cleanup() {
    MANAGER.asStatic().set(previous);
  }

  @Test
  void startupStopsAfterFirstDatabaseFailureAndUnregistersMetrics() {
    DefaultTableRuntime first = runtime(1);
    DefaultTableRuntime next = runtime(2);
    when(first.getProcessId())
        .thenThrow(new PersistenceException(new SQLException("lost", "08006")));
    // A large recovery queue must not incur one connection timeout per remaining table.
    List<DefaultTableRuntime> tables = new java.util.ArrayList<>(Collections.nCopies(1780, next));
    tables.set(0, first);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      assertThrows(
          PersistenceUnavailableDuringStartupException.class,
          () -> queue(tables, ignored -> OptimizingOwnership.NOT_OWNED));
      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
    }
    verify(next, never()).getProcessId();
    assertTrue(registry.getMetrics().isEmpty(), "Failed constructor must release its gauges");
  }

  @Test
  void swallowedCleanupFailureOnLastTableAlsoAborts() {
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      assertThrows(
          PersistenceUnavailableDuringStartupException.class,
          () ->
              queue(
                  Collections.singletonList(runtime(1)),
                  ignored -> {
                    PersistenceFailureReporter.record(
                        PersistenceFailureReporter.Phase.POOL_CLEANUP,
                        new SQLException("rollback failed", "08006"));
                    return OptimizingOwnership.NOT_OWNED;
                  }));
    }
    assertTrue(registry.getMetrics().isEmpty());
  }

  @Test
  void failedScopeIsCheckedBeforeAnyTableIsRecovered() {
    DefaultTableRuntime runtime = runtime(1);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      PersistenceFailureReporter.record(
          PersistenceFailureReporter.Phase.ACQUIRE, new SQLException("pool unavailable"));
      assertThrows(
          PersistenceUnavailableDuringStartupException.class,
          () ->
              queue(Collections.singletonList(runtime), ignored -> OptimizingOwnership.NOT_OWNED));
    }
    verify(runtime, never()).getProcessId();
    assertTrue(registry.getMetrics().isEmpty());
  }

  @Test
  void catalogTimeoutRemainsPerTableRecoveryFailure() {
    DefaultTableRuntime first = runtime(1);
    DefaultTableRuntime second = runtime(2);
    when(first.getProcessId())
        .thenThrow(new IllegalStateException(new SocketTimeoutException("HDFS")));
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      OptimizingQueue queue =
          queue(Arrays.asList(first, second), ignored -> OptimizingOwnership.NOT_OWNED);
      try {
        scope.check();
        verify(second).getProcessId();
        assertNotNull(queue);
      } finally {
        queue.dispose();
      }
    }
    assertEquals(0, registry.getMetrics().size());
  }

  @Test
  void runtimeOwnershipRecoveryKeepsExistingRetryBehavior() {
    OptimizingQueue queue =
        queue(Collections.emptyList(), ignored -> OptimizingOwnership.NOT_OWNED);
    DefaultTableRuntime runtime = runtime(1);
    when(runtime.getProcessId())
        .thenThrow(new PersistenceException(new SQLException("lost", "08006")));
    try {
      queue.recoverOwnedTable(runtime);
    } finally {
      queue.dispose();
    }
    PersistenceFailureReporter.throwIfStartupFailed();
  }

  private DefaultTableRuntime runtime(long id) {
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of(id, "catalog", "db", "t" + id, TableFormat.PAIMON));
    return runtime;
  }

  private OptimizingQueue queue(
      List<DefaultTableRuntime> tables,
      Function<DefaultTableRuntime, OptimizingOwnership> ownership) {
    return new OptimizingQueue(
        mock(CatalogManager.class),
        new ResourceGroup.Builder("startup-failure", "local").build(),
        ignored -> 1,
        Runnable::run,
        tables,
        1,
        mock(ProcessFactoryRouter.class),
        ownership);
  }
}
