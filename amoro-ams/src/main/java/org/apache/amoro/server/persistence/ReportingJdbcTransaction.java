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

import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.transaction.jdbc.JdbcTransaction;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;

/** Reports failures that MyBatis' executor would otherwise consume during close. */
class ReportingJdbcTransaction extends JdbcTransaction {
  private boolean lastCommitAcknowledged;

  ReportingJdbcTransaction(Connection connection) {
    super(connection);
  }

  ReportingJdbcTransaction(
      DataSource dataSource,
      TransactionIsolationLevel level,
      boolean autoCommit,
      boolean skipSetAutoCommitOnClose) {
    super(dataSource, level, autoCommit, skipSetAutoCommitOnClose);
  }

  @Override
  public Connection getConnection() throws SQLException {
    Connection result = super.getConnection();
    lastCommitAcknowledged = false;
    return result;
  }

  @Override
  protected void openConnection() throws SQLException {
    long start = System.nanoTime();
    try {
      super.openConnection();
    } catch (SQLException | RuntimeException failure) {
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.ACQUIRE, failure, start);
      throw failure;
    }
  }

  @Override
  public void commit() throws SQLException {
    long start = System.nanoTime();
    lastCommitAcknowledged = false;
    try {
      boolean manualCommit = connection != null && !connection.getAutoCommit();
      super.commit();
      lastCommitAcknowledged = manualCommit;
    } catch (SQLException | RuntimeException failure) {
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.COMMIT, failure, start);
      throw failure;
    }
  }

  @Override
  public void rollback() throws SQLException {
    long start = System.nanoTime();
    try {
      super.rollback();
    } catch (SQLException | RuntimeException failure) {
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.ROLLBACK, failure, start);
      throw failure;
    }
  }

  @Override
  public void close() throws SQLException {
    long start = System.nanoTime();
    try (PersistenceFailureReporter.CleanupScope ignored =
        PersistenceFailureReporter.closingTransaction(lastCommitAcknowledged)) {
      try {
        super.close();
      } catch (SQLException | RuntimeException failure) {
        PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.CLOSE, failure, start);
        throw failure;
      }
    }
  }

  @Override
  protected void resetAutoCommit() {
    long start = System.nanoTime();
    try {
      if (!skipSetAutoCommitOnClose && !connection.getAutoCommit()) {
        connection.setAutoCommit(true);
      }
    } catch (SQLException failure) {
      // Preserve MyBatis' behavior for external connections, but make the failure observable.
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.CLOSE, failure, start);
    }
  }
}
