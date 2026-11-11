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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.JdbcConnection;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.metrics.MetricRegistry;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.manager.MetricManager;
import org.apache.amoro.server.optimizing.OptimizingOwnership;
import org.apache.amoro.server.optimizing.OptimizingQueue;
import org.apache.amoro.server.process.ProcessFactoryRouter;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.iceberg.common.DynFields;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Real driver/pool/session tests: the proxy drops bytes without closing the TCP connection. */
@Testcontainers
@Timeout(30)
class TestJdbcConnectionTimeoutWithMySQL {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestJdbcConnectionTimeoutWithMySQL.class);

  @Container
  private static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>(System.getProperty("amoro.test.mysql.image", "mysql:8.0"))
          .withDatabaseName("amoro")
          .withUsername("amoro")
          .withPassword("amoro")
          .withUrlParam("useSSL", "false")
          .withUrlParam("connectTimeout", "5000")
          .withUrlParam("socketTimeout", "5000");

  private MySqlFaultProxy proxy;
  private BasicDataSource dataSource;
  private SqlSessionFactory sessions;

  @BeforeAll
  static void initializeTable() throws Exception {
    try (Connection c = backend();
        Statement st = c.createStatement()) {
      st.executeUpdate("CREATE TABLE jdbc_fault_rows (id INT PRIMARY KEY)");
      try (ResultSet rs = st.executeQuery("SELECT VERSION()")) {
        assertTrue(rs.next());
        LOG.info("MySQL fault-injection server version={}", rs.getString(1));
      }
    }
  }

  @BeforeEach
  void setup() throws Exception {
    try (Connection c = backend();
        Statement st = c.createStatement()) {
      st.executeUpdate("TRUNCATE TABLE jdbc_fault_rows");
    }
    proxy = new MySqlFaultProxy(MYSQL.getHost(), MYSQL.getMappedPort(3306));
    Configurations config = new Configurations();
    config.set(AmoroManagementConf.DB_TYPE, AmoroManagementConf.DB_TYPE_MYSQL);
    config.set(AmoroManagementConf.DB_CONNECTION_URL, proxyUrl());
    config.set(AmoroManagementConf.DB_DRIVER_CLASS_NAME, "com.mysql.cj.jdbc.Driver");
    config.set(AmoroManagementConf.DB_USER_NAME, MYSQL.getUsername());
    config.set(AmoroManagementConf.DB_PASSWORD, MYSQL.getPassword());
    config.set(AmoroManagementConf.DB_AUTO_CREATE_TABLES, false);
    dataSource = (BasicDataSource) DataSourceFactory.createDataSource(config);
    dataSource.setMaxTotal(1);
    dataSource.setMaxIdle(1);
    dataSource.setMaxWaitMillis(1000);
  }

  @AfterEach
  void cleanup() throws Exception {
    try {
      if (dataSource != null) {
        dataSource.close();
      }
    } finally {
      if (proxy != null) {
        proxy.close();
        assertEquals(0, proxy.openConnections(), "All frontend/backend sockets must close");
        assertTrue(proxy.terminated(), "Both proxy forwarding workers must actually exit");
        assertTrue(proxy.unexpectedFailures().isEmpty(), proxy.unexpectedFailures().toString());
      }
    }
    assertDoesNotThrow(PersistenceFailureReporter::throwIfStartupFailed);
  }

  @ParameterizedTest
  @EnumSource(MySqlFaultProxy.Mode.class)
  void activationAutoCommitZeroTimesOutBeforeBorrowValidation(MySqlFaultProxy.Mode mode)
      throws Exception {
    MySqlFaultProxy.Fault fault = proxy.arm("SET autocommit=0", mode);
    long start = System.nanoTime();
    SQLException failure = assertThrows(SQLException.class, dataSource::getConnection);
    assertSocketTimeout("activation-" + mode, start, 5000);
    assertTrue(PersistenceFailureReporter.isConnectionFailure(failure));
    assertEquals(1, fault.hits());
    assertEquals(1, proxy.commandCount("SET autocommit=0"), "No driver reconnect/replay");
    assertPoolDiscarded();
    proxy.clearFault();
    assertEquals(0, count(), "A fresh connection must work after removing the fault");
  }

  @ParameterizedTest
  @EnumSource(MySqlFaultProxy.Mode.class)
  void idleConnectionActivationFailureDiscardsAndReplacesBeforeValidation(MySqlFaultProxy.Mode mode)
      throws Exception {
    long originalId;
    try (Connection c = dataSource.getConnection()) {
      originalId = connectionId(c);
      c.setAutoCommit(true);
    }
    int createdBefore = proxy.acceptedConnections();
    MySqlFaultProxy.Fault fault = proxy.arm("SET autocommit=0", mode);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      long start = System.nanoTime();
      try (Connection c = dataSource.getConnection()) {
        assertTrue(connectionId(c) != originalId);
        assertFalse(c.getAutoCommit());
      }
      assertSocketTimeout("idle-activation-" + mode, start, 5000);
      assertDoesNotThrow(scope::check, "The pool may replace a failed idle connection");
    }
    assertEquals(1, fault.hits());
    assertEquals(createdBefore + 1, proxy.acceptedConnections());
    assertEquals(0, dataSource.getNumActive());
    assertEquals(1, dataSource.getNumIdle());
    assertTrue(proxy.awaitOpenConnections(1, 2000), "The failed idle connection must close");
  }

  @Test
  void readOnlyReturnRollsBackTheActualMySqlTransaction() throws Exception {
    JdbcConnection physical;
    long rollbackBefore;
    try (Connection connection = dataSource.getConnection()) {
      connection.setReadOnly(true);
      try (Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM jdbc_fault_rows")) {
        assertTrue(rows.next());
      }
      physical = connection.unwrap(JdbcConnection.class);
      assertTrue(physical.getSession().getServerSession().inTransactionOnServer());
      rollbackBefore = proxy.commandCount("ROLLBACK");
    }
    assertEquals(rollbackBefore + 1, proxy.commandCount("ROLLBACK"));
    assertFalse(physical.getSession().getServerSession().inTransactionOnServer());
    assertEquals(0, dataSource.getNumActive());
    assertEquals(1, dataSource.getNumIdle());
  }

  @Test
  void readSessionsCloseWithoutAutoCommitOneAndStillRollback() throws Exception {
    assertEquals(0, count());
    long resetBefore = proxy.commandCount("SET autocommit=1");
    long rollbackBefore = proxy.commandCount("ROLLBACK");
    MySqlFaultProxy.Fault fault = proxy.arm("SET autocommit=1", MySqlFaultProxy.Mode.DROP_REQUEST);
    for (int i = 0; i < 4; i++) {
      assertEquals(0, count());
    }
    assertEquals(0, fault.hits());
    assertEquals(resetBefore, proxy.commandCount("SET autocommit=1"));
    assertEquals(rollbackBefore + 4, proxy.commandCount("ROLLBACK"));
    assertEquals(0, dataSource.getNumActive());
    assertEquals(1, dataSource.getNumIdle());
    try (Connection c = dataSource.getConnection()) {
      JdbcConnection raw = c.unwrap(JdbcConnection.class);
      assertFalse(c.isReadOnly(), "Ordinary pool connections use writable JDBC mode");
      assertEquals(1000, c.getNetworkTimeout());
      assertEquals(
          1000, raw.getPropertySet().getIntegerProperty(PropertyKey.socketTimeout).getValue());
      assertEquals(
          1000, raw.getPropertySet().getIntegerProperty(PropertyKey.connectTimeout).getValue());
      assertFalse(raw.getPropertySet().getBooleanProperty(PropertyKey.autoReconnect).getValue());
      assertFalse(
          raw.getPropertySet().getBooleanProperty(PropertyKey.autoReconnectForPools).getValue());
    }
  }

  @Test
  void explicitAutoCommitOneHasTheSameSocketTimeoutBound() throws Exception {
    try (Connection c = dataSource.getConnection()) {
      MySqlFaultProxy.Fault fault =
          proxy.arm("SET autocommit=1", MySqlFaultProxy.Mode.DROP_RESPONSE);
      long start = System.nanoTime();
      SQLException failure = assertThrows(SQLException.class, () -> c.setAutoCommit(true));
      assertSocketTimeout("explicit-set-autocommit-one", start, 5000);
      assertTrue(PersistenceFailureReporter.isConnectionFailure(failure));
      assertEquals(1, fault.hits());
    }
    assertPoolDiscarded();
    proxy.clearFault();
    assertEquals(0, count());
  }

  @ParameterizedTest
  @EnumSource(MySqlFaultProxy.Mode.class)
  void externalSessionResetFailureIsBoundedAndReported(MySqlFaultProxy.Mode mode) throws Exception {
    dataSource.close();
    dataSource = new BasicDataSource();
    dataSource.setUrl(proxyUrl());
    dataSource.setUsername(MYSQL.getUsername());
    dataSource.setPassword(MYSQL.getPassword());
    dataSource.setDefaultAutoCommit(false);
    dataSource.setAutoCommitOnReturn(false);
    dataSource.setMaxTotal(1);
    dataSource.setMaxIdle(1);
    SqlSession session = sessions().openSession();
    assertEquals(0, session.getMapper(Rows.class).count());
    MySqlFaultProxy.Fault fault = proxy.arm("SET autocommit=1", mode);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      long start = System.nanoTime();
      // MyBatis consumes resetAutoCommit's SQLException; our observer must still see it.
      assertDoesNotThrow(session::close);
      assertSocketTimeout("external-close-reset-" + mode, start, 5000);
      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
    }
    assertEquals(1, fault.hits());
    assertPoolDiscarded();
    proxy.clearFault();
    assertEquals(0, count());
  }

  @ParameterizedTest
  @EnumSource(MySqlFaultProxy.Mode.class)
  void rollbackCleanupTimeoutCannotSilentlySucceedDuringStartup(MySqlFaultProxy.Mode mode)
      throws Exception {
    SqlSession session = sessions().openSession();
    assertEquals(0, session.getMapper(Rows.class).count());
    MySqlFaultProxy.Fault fault = proxy.arm("ROLLBACK", mode);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      long start = System.nanoTime();
      // For a read-only MyBatis session the pool, not the executor, rolls back the transaction.
      assertDoesNotThrow(session::close);
      assertSocketTimeout("rollback-cleanup-" + mode, start, 5000);
      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
    }
    assertEquals(1, fault.hits());
    assertPoolDiscarded();
    proxy.clearFault();
    assertEquals(0, count());
  }

  @Test
  void acknowledgedCommitIsNotReplayedWhenPoolCleanupTimesOut() throws Exception {
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      SqlSession session = sessions().openSession();
      session.getMapper(Rows.class).insert(7);
      session.commit(true);
      MySqlFaultProxy.Fault fault = proxy.arm("ROLLBACK", MySqlFaultProxy.Mode.DROP_RESPONSE);
      long start = System.nanoTime();
      assertDoesNotThrow(session::close);
      assertSocketTimeout("acknowledged-commit-cleanup", start, 5000);
      assertEquals(1, fault.hits());
      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
    }
    assertPoolDiscarded();
    assertEquals(1, backendCount(), "The acknowledged commit remains durable");
    assertEquals(1, proxy.commandCount("INSERT INTO jdbc_fault_rows"));
    proxy.clearFault();
    assertEquals(1, count());
  }

  @Test
  void missingCommitResponsePropagatesUnknownOutcomeWithoutReplayingWrite() throws Exception {
    try (SqlSession session = sessions().openSession()) {
      session.getMapper(Rows.class).insert(9);
      MySqlFaultProxy.Fault fault = proxy.arm("COMMIT", MySqlFaultProxy.Mode.DROP_RESPONSE);
      long start = System.nanoTime();
      RuntimeException failure = assertThrows(RuntimeException.class, () -> session.commit(true));
      assertSocketTimeout("commit-response-lost", start, 5000);
      assertTrue(PersistenceFailureReporter.isConnectionFailure(failure));
      assertEquals(1, fault.hits());
    }
    assertPoolDiscarded();
    assertEquals(
        1, backendCount(), "The server committed even though the client never received OK");
    assertEquals(1, proxy.commandCount("INSERT INTO jdbc_fault_rows"));
    assertEquals(1, proxy.commandCount("COMMIT"), "The driver/application must not replay COMMIT");
    proxy.clearFault();
    assertEquals(1, count());
  }

  @Test
  void validationPingDiscardsTheDeadConnectionAndBorrowsAFreshOne() throws Exception {
    long originalId;
    try (Connection c = dataSource.getConnection()) {
      originalId = connectionId(c);
    }
    int createdBefore = proxy.acceptedConnections();
    MySqlFaultProxy.Fault fault = proxy.arm("PING", MySqlFaultProxy.Mode.DROP_RESPONSE);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      long start = System.nanoTime();
      try (Connection c = dataSource.getConnection()) {
        assertTrue(connectionId(c) != originalId);
      }
      assertSocketTimeout("validation-ping", start, 6000);
      assertDoesNotThrow(scope::check, "Successful replacement is not a startup outage");
    }
    assertEquals(1, fault.hits());
    assertEquals(createdBefore + 1, proxy.acceptedConnections());
    assertEquals(0, dataSource.getNumActive());
    assertEquals(1, dataSource.getNumIdle());
    assertTrue(proxy.awaitOpenConnections(1, 2000), "The old physical socket must close");
  }

  @Test
  void persistentActivationBlackholeAborts1780TableStartupAfterFirstTable() throws Exception {
    // Exercise both the existing idle connection and the pool's single replacement attempt.
    try (Connection c = dataSource.getConnection()) {
      c.setAutoCommit(true);
    }
    DynFields.UnboundField<MetricManager> managerField =
        DynFields.builder().hiddenImpl(MetricManager.class, "INSTANCE").build();
    MetricManager previous = managerField.asStatic().get();
    MetricManager manager = mock(MetricManager.class);
    MetricRegistry registry = new MetricRegistry();
    when(manager.getGlobalRegistry()).thenReturn(registry);
    DefaultTableRuntime first = mock(DefaultTableRuntime.class);
    DefaultTableRuntime remaining = mock(DefaultTableRuntime.class);
    when(first.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of(1L, "catalog", "db", "first", TableFormat.PAIMON));
    when(first.getProcessId())
        .thenAnswer(
            invocation -> {
              try (Connection ignored = dataSource.getConnection()) {
                return 1L;
              } catch (SQLException failure) {
                throw new org.apache.ibatis.exceptions.PersistenceException(failure);
              }
            });
    List<DefaultTableRuntime> tables = new ArrayList<>(Collections.nCopies(1780, remaining));
    tables.set(0, first);
    MySqlFaultProxy.Fault fault =
        proxy.armPersistent("SET autocommit=0", MySqlFaultProxy.Mode.DROP_REQUEST);
    managerField.asStatic().set(manager);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      long start = System.nanoTime();
      assertThrows(
          PersistenceUnavailableDuringStartupException.class,
          () ->
              new OptimizingQueue(
                  mock(CatalogManager.class),
                  new ResourceGroup.Builder("mysql-blackhole-startup", "local").build(),
                  ignored -> 1,
                  Runnable::run,
                  tables,
                  1,
                  mock(ProcessFactoryRouter.class),
                  ignored -> OptimizingOwnership.NOT_OWNED));
      assertSocketTimeout("persistent-activation-1780-tables", start, 7000);
      assertThrows(PersistenceUnavailableDuringStartupException.class, scope::check);
      verify(remaining, never()).getProcessId();
      verify(remaining, never()).getTableIdentifier();
      assertTrue(
          registry.getMetrics().isEmpty(), "A failed queue must not retain its metric gauges");
      assertEquals(
          2, fault.hits(), "Only the failed idle connection and one replacement may time out");
    } finally {
      managerField.asStatic().set(previous);
    }
    assertPoolDiscarded();
    proxy.clearFault();
    assertEquals(0, count(), "Fault removal and scope exit must allow a clean startup attempt");
  }

  private SqlSessionFactory sessions() throws SQLException {
    if (sessions == null) {
      SqlSessionFactoryProvider provider = new SqlSessionFactoryProvider();
      provider.init(dataSource);
      sessions = provider.get();
      sessions.getConfiguration().addMapper(Rows.class);
    }
    return sessions;
  }

  private int count() throws SQLException {
    try (SqlSession session = sessions().openSession()) {
      return session.getMapper(Rows.class).count();
    }
  }

  private String proxyUrl() {
    return "jdbc:mysql://127.0.0.1:"
        + proxy.port()
        + "/amoro"
        + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=1000&socketTimeout=1000"
        + "&autoReconnect=false&autoReconnectForPools=false";
  }

  private void assertPoolDiscarded() throws InterruptedException {
    assertEquals(0, dataSource.getNumActive());
    assertEquals(
        0, dataSource.getNumIdle(), "A failed physical connection must not return to idle");
    assertTrue(proxy.awaitOpenConnections(0, 2000), "The actual JDBC socket must close");
  }

  private static Connection backend() throws SQLException {
    return DriverManager.getConnection(
        MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
  }

  private static int backendCount() throws SQLException {
    try (Connection c = backend();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jdbc_fault_rows")) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }

  private static long connectionId(Connection c) throws SQLException {
    try (Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT CONNECTION_ID()")) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }

  private static void assertSocketTimeout(String scenario, long start, long upperBoundMillis) {
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    LOG.info(
        "MySQL fault scenario={} elapsedMs={} upperBoundMs={}",
        scenario,
        elapsed,
        upperBoundMillis);
    assertTrue(
        elapsed >= 700, "The injected silent read must actually wait for its timeout: " + elapsed);
    assertTrue(
        elapsed < upperBoundMillis, "Call failed to return within timeout tolerance: " + elapsed);
  }

  public interface Rows {
    @Select("SELECT COUNT(*) FROM jdbc_fault_rows")
    int count();

    @Insert("INSERT INTO jdbc_fault_rows VALUES (#{id})")
    void insert(@Param("id") int id);
  }
}
