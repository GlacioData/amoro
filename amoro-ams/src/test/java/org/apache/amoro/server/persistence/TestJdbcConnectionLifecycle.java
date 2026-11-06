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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class TestJdbcConnectionLifecycle {
  private BasicDataSource dataSource;
  private SqlSessionFactory sessions;

  @BeforeEach
  void setup() throws Exception {
    Configurations config = new Configurations();
    config.set(
        AmoroManagementConf.DB_CONNECTION_URL,
        "jdbc:derby:memory:lifecycle"
            + UUID.randomUUID().toString().replace("-", "")
            + ";create=true");
    config.set(AmoroManagementConf.DB_DRIVER_CLASS_NAME, TrackingDriver.class.getName());
    config.set(AmoroManagementConf.DB_AUTO_CREATE_TABLES, false);
    dataSource = (BasicDataSource) DataSourceFactory.createDataSource(config);
    dataSource.setMaxTotal(1);
    dataSource.setMaxIdle(1);
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement()) {
      st.executeUpdate("CREATE TABLE lifecycle_rows (id INT PRIMARY KEY)");
      c.commit();
    }
    SqlSessionFactoryProvider provider = new SqlSessionFactoryProvider();
    provider.init(dataSource);
    sessions = provider.get();
    sessions.getConfiguration().addMapper(Rows.class);
    TrackingDriver.events.clear();
  }

  @AfterEach
  void close() throws Exception {
    TrackingDriver.failNextRollback.set(false);
    if (dataSource != null) {
      dataSource.close();
    }
  }

  @Test
  void readSessionsKeepManualCommitAndEndReadTransactions() {
    for (int i = 0; i < 3; i++) {
      assertEquals(0, count());
    }
    assertFalse(
        TrackingDriver.events.stream().anyMatch(e -> e.startsWith("setAutoCommit")),
        TrackingDriver.events.toString());
    assertEquals(3, TrackingDriver.events.stream().filter("rollback"::equals).count());
    assertEquals(0, dataSource.getNumActive());
    assertEquals(1, dataSource.getNumIdle());
  }

  @Test
  void uncommittedWritesRollbackAndExplicitCommitsPersist() {
    try (SqlSession session = sessions.openSession()) {
      session.getMapper(Rows.class).insert(1);
    }
    assertEquals(0, count());
    try (SqlSession session = sessions.openSession()) {
      session.getMapper(Rows.class).insert(2);
      session.commit(true);
    }
    assertEquals(1, count());
  }

  @Test
  void reactivatesConnectionUsedByAutoCommitCaller() throws Exception {
    try (Connection c = dataSource.getConnection()) {
      c.setAutoCommit(true);
    }
    TrackingDriver.events.clear();
    assertEquals(0, count());
    assertTrue(TrackingDriver.events.contains("setAutoCommit=false"));
    assertFalse(TrackingDriver.events.contains("setAutoCommit=true"));
  }

  @Test
  void readOnlyDirectConnectionEndsItsTransactionOnReturn() throws Exception {
    try (Connection connection = dataSource.getConnection()) {
      connection.setReadOnly(true);
      try (Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM lifecycle_rows")) {
        assertTrue(rows.next());
      }
      TrackingDriver.events.clear();
    }
    assertEquals(1, TrackingDriver.events.stream().filter("rollback"::equals).count());
    assertFalse(TrackingDriver.events.contains("setAutoCommit=true"));
  }

  @Test
  void readOnlyMyBatisConnectionEndsItsReadTransactionOnReturn() throws Exception {
    try (Connection connection = dataSource.getConnection()) {
      connection.setReadOnly(true);
    }
    TrackingDriver.events.clear();
    assertEquals(0, count());
    assertEquals(1, TrackingDriver.events.stream().filter("rollback"::equals).count());
    assertFalse(TrackingDriver.events.contains("setAutoCommit=true"));
  }

  @Test
  void externalDataSourceRetainsMyBatisReset() throws Exception {
    try (BasicDataSource external = new BasicDataSource()) {
      external.setDriverClassName(TrackingDriver.class.getName());
      external.setUrl(dataSource.getUrl());
      external.setDefaultAutoCommit(false);
      external.setAutoCommitOnReturn(false);
      SqlSessionFactoryProvider provider = new SqlSessionFactoryProvider();
      provider.init(external);
      TrackingDriver.events.clear();
      try (SqlSession session = provider.get().openSession()) {
        assertFalse(session.getConnection().getAutoCommit());
      }
      assertTrue(TrackingDriver.events.contains("setAutoCommit=true"));
    }
  }

  @Test
  void observesSwallowedCleanupFailureAndDiscardsConnectionAfterSuccessfulCommit() {
    int previousConnections = TrackingDriver.connections.get();
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      try (SqlSession session = sessions.openSession()) {
        session.getMapper(Rows.class).insert(3);
        session.commit(true);
        TrackingDriver.failNextRollback.set(true);
      }
      assertEquals(0, dataSource.getNumActive());
      assertEquals(0, dataSource.getNumIdle());
      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
    }
    assertEquals(1, count(), "A cleanup failure must not replay or undo a successful commit");
    assertTrue(TrackingDriver.connections.get() > previousConnections);
  }

  @Test
  void readOnlyRollbackFailureDiscardsTheReturnedConnection() throws Exception {
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      try (Connection connection = dataSource.getConnection()) {
        connection.setReadOnly(true);
        TrackingDriver.failNextRollback.set(true);
      }
      assertEquals(0, dataSource.getNumActive());
      assertEquals(0, dataSource.getNumIdle());
      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
    }
    assertEquals(0, count());
  }

  private int count() {
    try (SqlSession session = sessions.openSession()) {
      return session.getMapper(Rows.class).count();
    }
  }

  public interface Rows {
    @Select("SELECT COUNT(*) FROM lifecycle_rows")
    int count();

    @Insert("INSERT INTO lifecycle_rows VALUES (#{id})")
    void insert(@Param("id") int id);
  }

  public static class TrackingDriver extends EmbeddedDriver {
    private static final List<String> events = new ArrayList<>();
    private static final AtomicBoolean failNextRollback = new AtomicBoolean();
    private static final AtomicInteger connections = new AtomicInteger();

    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
      Connection raw = super.connect(url, properties);
      if (raw == null) {
        return null;
      }
      connections.incrementAndGet();
      return (Connection)
          Proxy.newProxyInstance(
              getClass().getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (method.getName().equals("setAutoCommit")) {
                  events.add("setAutoCommit=" + args[0]);
                }
                if (method.getName().equals("rollback")) {
                  events.add("rollback");
                  if (failNextRollback.compareAndSet(true, false)) {
                    raw.rollback();
                    throw new SQLTimeoutException("injected cleanup timeout", "08006");
                  }
                }
                try {
                  return method.invoke(raw, args);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    }
  }
}
