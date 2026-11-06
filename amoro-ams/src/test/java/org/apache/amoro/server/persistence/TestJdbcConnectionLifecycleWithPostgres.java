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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.eclipse.jetty.server.session.SessionContext;
import org.eclipse.jetty.server.session.SessionData;
import org.eclipse.jetty.server.session.SessionDataStore;
import org.eclipse.jetty.server.session.SessionHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/** Exercises PostgreSQL and the actual Jetty session store on the shared AMS pool. */
@Testcontainers
class TestJdbcConnectionLifecycleWithPostgres {
  @Container
  private static final GenericContainer<?> POSTGRES =
      new GenericContainer<>("postgres:16")
          .withEnv("POSTGRES_USER", "amoro")
          .withEnv("POSTGRES_PASSWORD", "amoro")
          .withEnv("POSTGRES_DB", "amoro")
          .withExposedPorts(5432)
          .waitingFor(
              Wait.forLogMessage(".*database system is ready to accept connections.*\n", 2));

  private BasicDataSource dataSource;
  private SqlSessionFactory sessions;
  private Configurations config;

  @BeforeEach
  void setup() throws Exception {
    config = new Configurations();
    config.set(AmoroManagementConf.DB_TYPE, AmoroManagementConf.DB_TYPE_POSTGRES);
    config.set(AmoroManagementConf.DB_CONNECTION_URL, url());
    config.set(AmoroManagementConf.DB_DRIVER_CLASS_NAME, "org.postgresql.Driver");
    config.set(AmoroManagementConf.DB_USER_NAME, "amoro");
    config.set(AmoroManagementConf.DB_PASSWORD, "amoro");
    config.set(AmoroManagementConf.DB_AUTO_CREATE_TABLES, true);
    dataSource = (BasicDataSource) DataSourceFactory.createDataSource(config);
    dataSource.setMaxTotal(1);
    dataSource.setMaxIdle(1);
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement()) {
      st.executeUpdate("CREATE TABLE IF NOT EXISTS lifecycle_rows (id INT PRIMARY KEY)");
      st.executeUpdate("DELETE FROM lifecycle_rows");
      c.commit();
    }
    SqlSessionFactoryProvider provider = new SqlSessionFactoryProvider();
    provider.init(dataSource);
    sessions = provider.get();
    sessions.getConfiguration().addMapper(Rows.class);
  }

  @AfterEach
  void cleanup() throws Exception {
    if (dataSource != null) {
      dataSource.close();
    }
  }

  @Test
  void commitAndRollbackRemainIsolatedAcrossSessionReuse() {
    try (SqlSession session = sessions.openSession()) {
      session.getMapper(Rows.class).insert(1);
    }
    assertEquals(0, count());
    try (SqlSession session = sessions.openSession()) {
      session.getMapper(Rows.class).insert(2);
      session.commit();
    }
    assertEquals(1, count());
    assertEquals(0, dataSource.getNumActive());
    assertEquals(1, dataSource.getNumIdle());
  }

  @Test
  void readSessionReturnEndsTheBackendTransaction() throws Exception {
    int backendPid;
    try (SqlSession session = sessions.openSession()) {
      backendPid = session.getMapper(Rows.class).backendPid();
      assertFalse(session.getConnection().isReadOnly());
      assertFalse(session.getConnection().getAutoCommit());
    }
    assertBackendIdle(backendPid);
  }

  @Test
  void readOnlyUrlDoesNotLeaveTransactionsInTheIdlePool() throws Exception {
    dataSource.close();
    config.set(AmoroManagementConf.DB_CONNECTION_URL, url() + "&readOnly=true&readOnlyMode=ignore");
    dataSource = (BasicDataSource) DataSourceFactory.createDataSource(config);
    dataSource.setMaxTotal(1);
    dataSource.setMaxIdle(1);
    SqlSessionFactoryProvider provider = new SqlSessionFactoryProvider();
    provider.init(dataSource);
    sessions = provider.get();
    sessions.getConfiguration().addMapper(Rows.class);

    int backendPid;
    try (SqlSession session = sessions.openSession()) {
      backendPid = session.getMapper(Rows.class).backendPid();
      assertTrue(session.getConnection().isReadOnly());
      assertFalse(session.getConnection().getAutoCommit());
    }
    assertBackendIdle(backendPid);
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      // readOnlyMode=ignore is a supported hint: writes still require rollback on return.
      assertTrue(connection.isReadOnly());
      statement.executeUpdate("INSERT INTO lifecycle_rows VALUES (100)");
    }
    assertBackendIdle(backendPid);
    assertEquals(0, count());
  }

  @Test
  void directlyBorrowedReadOnlyConnectionEndsItsBackendTransaction() throws Exception {
    int backendPid;
    try (Connection connection = dataSource.getConnection()) {
      connection.setReadOnly(true);
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT pg_backend_pid()")) {
        assertTrue(result.next());
        backendPid = result.getInt(1);
      }
    }
    assertBackendIdle(backendPid);
  }

  private void assertBackendIdle(int backendPid) throws Exception {
    try (Connection observer = DriverManager.getConnection(url(), "amoro", "amoro");
        Statement st = observer.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT state, xact_start FROM pg_stat_activity WHERE pid=" + backendPid)) {
      assertTrue(rs.next());
      assertEquals("idle", rs.getString(1));
      assertNull(rs.getTimestamp(2), "Read transactions must not leak into the idle pool");
    }
  }

  @Test
  void jettySessionStoreAndMyBatisCanShareThePool() throws Exception {
    SessionHandler handler = HttpSessionHandlerFactory.createSessionHandler(dataSource, config);
    SessionDataStore store = handler.getSessionCache().getSessionDataStore();
    store.initialize(new SessionContext("local-test", null));
    store.start();
    try {
      long now = System.currentTimeMillis();
      SessionData data = store.newSessionData("jdbc-shared-session", now, now, now, 60000);
      data.setLastNode("local-test");
      data.setAttribute("user", "local-user");
      store.store(data.getId(), data);
      assertEquals("local-user", store.load(data.getId()).getAttribute("user"));
      // Jetty explicitly uses autoCommit=true; activation must restore MyBatis' manual commit.
      try (SqlSession session = sessions.openSession()) {
        session.getMapper(Rows.class).insert(3);
        assertFalse(session.getConnection().getAutoCommit());
      }
      assertEquals(0, count(), "MyBatis must not accidentally commit after a Jetty operation");
      data.setAttribute("user", "updated-user");
      store.store(data.getId(), data);
      assertEquals("updated-user", store.load(data.getId()).getAttribute("user"));
      assertTrue(store.delete(data.getId()));
      assertNull(store.load(data.getId()));
    } finally {
      store.stop();
    }
    assertEquals(0, dataSource.getNumActive());
  }

  private int count() {
    try (SqlSession session = sessions.openSession()) {
      return session.getMapper(Rows.class).count();
    }
  }

  private static String url() {
    return "jdbc:postgresql://"
        + POSTGRES.getHost()
        + ":"
        + POSTGRES.getMappedPort(5432)
        + "/amoro?connectTimeout=5&socketTimeout=5";
  }

  interface Rows {
    @Insert("INSERT INTO lifecycle_rows(id) VALUES(#{id})")
    int insert(int id);

    @Select("SELECT COUNT(*) FROM lifecycle_rows")
    int count();

    @Select("SELECT pg_backend_pid()")
    int backendPid();
  }
}
