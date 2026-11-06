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
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.TransactionFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.util.Properties;

/** Keeps MyBatis JDBC transaction semantics while observing failures before executor cleanup. */
class ReportingJdbcTransactionFactory implements TransactionFactory {
  private boolean skipSetAutoCommitOnClose;

  @Override
  public void setProperties(Properties properties) {
    skipSetAutoCommitOnClose =
        properties != null
            && Boolean.parseBoolean(properties.getProperty("skipSetAutoCommitOnClose"));
  }

  @Override
  public Transaction newTransaction(Connection connection) {
    // A caller-supplied connection may not belong to our transaction-cleaning pool.
    return new ReportingJdbcTransaction(connection);
  }

  @Override
  public Transaction newTransaction(
      DataSource dataSource, TransactionIsolationLevel level, boolean autoCommit) {
    boolean poolCleansTransactions =
        dataSource instanceof AmsDataSource
            && ((AmsDataSource) dataSource).cleansTransactionsOnReturn();
    return new ReportingJdbcTransaction(
        dataSource, level, autoCommit, skipSetAutoCommitOnClose && poolCleansTransactions);
  }
}
