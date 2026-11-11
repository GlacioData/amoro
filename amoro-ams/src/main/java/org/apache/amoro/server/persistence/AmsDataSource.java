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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.pool2.SwallowedExceptionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/** Connection pool that owns cleanup of transactions returned by AMS sessions. */
class AmsDataSource extends BasicDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(AmsDataSource.class);

  @Override
  public Connection getConnection() throws SQLException {
    long start = System.nanoTime();
    try {
      return super.getConnection();
    } catch (SQLException | RuntimeException failure) {
      PersistenceFailureReporter.record(PersistenceFailureReporter.Phase.ACQUIRE, failure, start);
      throw failure;
    }
  }

  @Override
  protected void createConnectionPool(PoolableConnectionFactory factory) {
    super.createConnectionPool(new TransactionCleaningConnectionFactory(factory));
    SwallowedExceptionListener previous = getConnectionPool().getSwallowedExceptionListener();
    getConnectionPool()
        .setSwallowedExceptionListener(
            failure -> {
              try {
                PersistenceFailureReporter.record(
                    PersistenceFailureReporter.Phase.POOL_CLEANUP, failure);
                if (previous != null) {
                  previous.onSwallowException(failure);
                }
              } catch (RuntimeException reportingFailure) {
                LOG.warn(
                    "Unable to report pool cleanup failure ({})",
                    reportingFailure.getClass().getName());
              }
            });
  }

  boolean cleansTransactionsOnReturn() {
    return Boolean.FALSE.equals(getDefaultAutoCommit())
        && !getAutoCommitOnReturn()
        && getRollbackOnReturn();
  }
}
