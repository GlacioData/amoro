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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.ibatis.transaction.Transaction;
import org.junit.jupiter.api.Test;

import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class TestPersistenceFailureReporter {
  @Test
  void findsConnectionFailureInWrappedNextAndSuppressedExceptions() {
    SQLException original = new SQLException("statement failed", "40001");
    original.setNextException(new SQLException("connection lost", "08006"));
    RuntimeException wrapper = new IllegalStateException("mapper failed", original);
    assertTrue(PersistenceFailureReporter.isConnectionFailure(wrapper));
    RuntimeException suppressed = new IllegalStateException("operation failed");
    suppressed.addSuppressed(
        new SQLException("read failed", new SocketTimeoutException("read timed out")));
    assertTrue(PersistenceFailureReporter.isConnectionFailure(suppressed));
    assertTrue(PersistenceFailureReporter.isConnectionFailure(new SQLRecoverableException()));
  }

  @Test
  void ordinaryQueryTimeoutAndDeadlockDoNotMeanConnectionLoss() {
    assertFalse(
        PersistenceFailureReporter.isConnectionFailure(
            new SQLTimeoutException("query timeout", "HYT00")));
    assertFalse(
        PersistenceFailureReporter.isConnectionFailure(new SQLException("deadlock", "40001")));
    SQLException cyclic = new SQLException("ordinary failure", "42000");
    cyclic.setNextException(cyclic);
    assertFalse(PersistenceFailureReporter.isConnectionFailure(cyclic));
  }

  @Test
  void startupKeepsFirstFailureAndDoesNotLeakIntoNextScope() {
    SQLException first = new SQLException("first", "08006");
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.STATEMENT, first);
      PersistenceFailureReporter.record(
          PersistenceFailureReporter.Phase.POOL_CLEANUP, new SQLException("second", "08006"));
      PersistenceUnavailableDuringStartupException failure =
          assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
      assertSame(first, failure.getCause());
    }
    assertDoesNotThrow(PersistenceFailureReporter::throwIfStartupFailed);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      assertDoesNotThrow(scope::check);
    }
  }

  @Test
  void failedAcquisitionIsFatalOnlyInsideStartupScope() {
    SQLException exhausted = new SQLException("pool exhausted");
    assertDoesNotThrow(
        () ->
            PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.ACQUIRE, exhausted));
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.ACQUIRE, exhausted);
      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
    }
  }

  @Test
  void catalogSocketTimeoutIsNotMistakenForDatabaseFailure() {
    RuntimeException failure =
        new IllegalStateException("catalog unavailable", new SocketTimeoutException());
    assertFalse(PersistenceFailureReporter.isConnectionFailure(failure));
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.STATEMENT, failure);
      assertDoesNotThrow(scope::check);
    }
  }

  @Test
  void ordinaryStatementFailureDoesNotAbortStartup() {
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      PersistenceFailureReporter.record(
          PersistenceFailureReporter.Phase.STATEMENT,
          new SQLTimeoutException("lock wait", "HYT00"));
      assertDoesNotThrow(scope::check);
    }
  }

  @Test
  void simultaneousStartupScopesKeepFailuresOnTheirOwningThreads() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch scopesOpen = new CountDownLatch(2);
    CountDownLatch failed = new CountDownLatch(1);
    SQLException failure = new SQLException("thread-local connection lost", "08006");
    try {
      Future<?> failing =
          executor.submit(
              () -> {
                try (PersistenceFailureReporter.StartupScope scope =
                    PersistenceFailureReporter.startupScope()) {
                  scopesOpen.countDown();
                  assertTrue(scopesOpen.await(5, TimeUnit.SECONDS));
                  PersistenceFailureReporter.record(
                      PersistenceFailureReporter.Phase.ACQUIRE, failure);
                  failed.countDown();
                  assertSame(
                      failure,
                      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check)
                          .getCause());
                }
                assertDoesNotThrow(PersistenceFailureReporter::throwIfStartupFailed);
                return null;
              });
      Future<?> healthy =
          executor.submit(
              () -> {
                try (PersistenceFailureReporter.StartupScope scope =
                    PersistenceFailureReporter.startupScope()) {
                  scopesOpen.countDown();
                  assertTrue(failed.await(5, TimeUnit.SECONDS));
                  assertDoesNotThrow(scope::check);
                }
                assertDoesNotThrow(PersistenceFailureReporter::throwIfStartupFailed);
                return null;
              });
      failing.get(10, TimeUnit.SECONDS);
      healthy.get(10, TimeUnit.SECONDS);
      assertDoesNotThrow(PersistenceFailureReporter::throwIfStartupFailed);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void observesResetFailureForExternallySuppliedConnection() throws Exception {
    Connection connection = mock(Connection.class);
    when(connection.getAutoCommit()).thenReturn(false);
    SQLException failure = new SQLException("reset failed", "08006");
    doThrow(failure).when(connection).setAutoCommit(true);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      Transaction transaction = new ReportingJdbcTransactionFactory().newTransaction(connection);
      transaction.close();
      verify(connection).close();
      assertSame(
          failure,
          assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check)
              .getCause());
    }
  }
}
