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

package org.apache.amoro.persistence.blob;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.persistence.PersistenceDomain;
import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.persistence.exception.ResourceAlreadyExists;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Offline durable-store tests against embedded Derby (framework spec §9 / Task 9): the five SQL
 * semantics, metadata-guarded idempotent schema creation (Derby has no IF NOT EXISTS), duplicate
 * translation, restart-equivalence and double-domain table isolation.
 */
@Timeout(60)
public class TestMyBatisBlobStoreDerby {

  private static final String JDBC_URL = "jdbc:derby:memory:amoroV2Test;create=true";

  private Connection connection;
  private MyBatisBlobStore resourceStore;
  private MyBatisBlobStore processStore;

  @BeforeEach
  public void setUp() throws Exception {
    connection = DriverManager.getConnection(JDBC_URL);
    runDerbySchemaWithMetadataGuard(connection);
    // amoro_resource is NOT in the shipped single-table DDL: the dual-domain isolation test
    // creates it inline (framework-generic domains own their table setup)
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE amoro_resource (name VARCHAR(256) NOT NULL, "
              + "collection CHAR(50) NOT NULL, value CLOB NOT NULL, "
              + "last_updated TIMESTAMP NOT NULL, PRIMARY KEY (name))");
    } catch (SQLException alreadyExists) {
      if (!"X0Y32".equals(alreadyExists.getSQLState())) {
        throw alreadyExists;
      }
    }
    SqlSessionFactory factory = sqlSessionFactory();
    resourceStore =
        new MyBatisBlobStore(
            new PersistenceDomain("resource", "amoro_resource", SerdeFormat.JSON),
            factory.openSession(true).getMapper(ResourceBlobMapper.class));
    processStore =
        new MyBatisBlobStore(
            new PersistenceDomain("process", "amoro_process_v2", SerdeFormat.YAML),
            factory.openSession(true).getMapper(ResourceBlobMapper.class));
    clearTables();
  }

  @AfterEach
  public void tearDown() throws Exception {
    clearTables();
    try (Statement statement = connection.createStatement()) {
      for (String table : new String[] {"amoro_process_v2", "amoro_resource"}) {
        statement.execute("DROP TABLE " + table);
      }
    }
    connection.close();
    try {
      DriverManager.getConnection("jdbc:derby:memory:amoroV2Test;drop=true").close();
    } catch (SQLException ignored) {
      // already dropped
    }
  }

  private void clearTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String table : new String[] {"amoro_process_v2", "amoro_resource"}) {
        statement.execute("DELETE FROM " + table);
      }
    }
  }

  private SqlSessionFactory sqlSessionFactory() {
    // Derby 10.15+ removed the explicit EmbeddedDriver class: let DriverManager discover the
    // ServiceLoader-registered AutoloadedDriver instead of naming a driver class
    // Derby 10.15+ removed the explicit EmbeddedDriver class: let DriverManager discover the
    // ServiceLoader-registered AutoloadedDriver instead of naming a driver class
    // Derby 10.15+ removed EmbeddedDriver; AutoloadedDriver is the ServiceLoader entry
    // point MyBatis can still reference by name
    org.apache.ibatis.datasource.unpooled.UnpooledDataSource dataSource =
        new org.apache.ibatis.datasource.unpooled.UnpooledDataSource(
            "org.apache.derby.iapi.jdbc.AutoloadedDriver", JDBC_URL, "", "");
    Environment environment =
        new Environment("derby-test", new JdbcTransactionFactory(), dataSource);
    Configuration configuration = new Configuration(environment);
    configuration.addMapper(ResourceBlobMapper.class);
    return new SqlSessionFactoryBuilder().build(configuration);
  }

  /**
   * Derby 10.14 lacks CREATE TABLE IF NOT EXISTS: check the metadata first and run the plain script
   * only when a table is missing — exactly the initializer contract of the spec.
   */
  static void runDerbySchemaWithMetadataGuard(Connection connection) throws Exception {
    DatabaseMetaData metadata = connection.getMetaData();
    boolean missing = false;
    try (ResultSet tables = metadata.getTables(null, null, "AMORO_%", null)) {
      List<String> found = new ArrayList<String>();
      while (tables.next()) {
        found.add(tables.getString("TABLE_NAME"));
      }
      missing = found.size() < 2; // any of the two test tables absent -> run the script
    }
    if (!missing) {
      return; // second initialization is a no-op: idempotent by metadata guard
    }
    String script =
        new String(
            Files.readAllBytes(Path.of("src/main/resources/schema-derby.sql")),
            StandardCharsets.UTF_8);
    // strip line comments BEFORE splitting: the license header contains ';' inside quotes
    String withoutComments = script.replaceAll("--[^\n]*", "");
    try (Statement statement = connection.createStatement()) {
      for (String sqlPiece : withoutComments.split(";")) {
        String sql = sqlPiece.trim();
        if (!sql.isEmpty()) {
          statement.execute(sql);
        }
      }
    }
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  // ------------------------------------------------------------------ five SQL semantics

  @Test
  public void insertFindUpdateDeleteRoundTrip() {
    resourceStore.insert("fake", "r1", bytes("{\"doc\":\"one\"}"));
    assertArrayEquals(bytes("{\"doc\":\"one\"}"), resourceStore.find("fake", "r1").orElse(null));

    assertTrue(resourceStore.update("fake", "r1", bytes("{\"doc\":\"two\"}")));
    assertArrayEquals(bytes("{\"doc\":\"two\"}"), resourceStore.find("fake", "r1").orElse(null));

    assertTrue(resourceStore.delete("fake", "r1"));
    assertEquals(Optional.empty(), resourceStore.find("fake", "r1"));
    assertFalse(resourceStore.delete("fake", "r1"), "second delete reports absence");
    assertFalse(
        resourceStore.update("fake", "ghost", bytes("x")),
        "update of an absent name reports false");
  }

  @Test
  public void duplicateInsertTranslatesToResourceAlreadyExists() {
    resourceStore.insert("fake", "dup", bytes("a"));
    assertThrows(
        ResourceAlreadyExists.class, () -> resourceStore.insert("fake", "dup", bytes("b")));
  }

  @Test
  public void forEachScansTheWholeCollection() {
    resourceStore.insert("fake", "a", bytes("1"));
    resourceStore.insert("fake", "b", bytes("2"));
    resourceStore.insert("other", "c", bytes("3")); // different collection

    AtomicInteger seen = new AtomicInteger();
    List<String> names = new ArrayList<String>();
    resourceStore.forEach(
        "fake",
        (name, value) -> {
          names.add(name);
          seen.incrementAndGet();
        });
    assertEquals(2, seen.get(), "only the requested collection is scanned");
    assertTrue(names.contains("a") && names.contains("b"));
  }

  @Test
  public void restartRebuildsExactlyWhatWasPersisted() throws Exception {
    resourceStore.insert("fake", "persisted", bytes("{\"v\":1}"));
    // a fresh store instance over the same database sees exactly the durable bytes
    MyBatisBlobStore restarted =
        new MyBatisBlobStore(
            new PersistenceDomain("resource", "amoro_resource", SerdeFormat.JSON),
            sqlSessionFactory().openSession(true).getMapper(ResourceBlobMapper.class));
    assertArrayEquals(bytes("{\"v\":1}"), restarted.find("fake", "persisted").orElse(null));
  }

  @Test
  public void doubleDomainDoubleTableIsolation() {
    // the same row name in two domain tables must not interfere
    resourceStore.insert("fake", "same-name", bytes("from-resource-table"));
    processStore.insert("process", "same-name", bytes("from-process-table"));

    assertArrayEquals(
        bytes("from-resource-table"), resourceStore.find("fake", "same-name").orElse(null));
    assertArrayEquals(
        bytes("from-process-table"), processStore.find("process", "same-name").orElse(null));

    assertTrue(processStore.delete("process", "same-name"));
    assertNotNull(
        resourceStore.find("fake", "same-name").orElse(null),
        "deleting in one domain table leaves the other untouched");
  }

  @Test
  public void schemaReinitializationIsIdempotentUnderMetadataGuard() throws Exception {
    // tables already exist from setUp: the guard must skip the script entirely
    runDerbySchemaWithMetadataGuard(connection);
    runDerbySchemaWithMetadataGuard(connection);
    resourceStore.insert("fake", "after-reinit", bytes("still-works"));
    assertArrayEquals(
        bytes("still-works"), resourceStore.find("fake", "after-reinit").orElse(null));
  }

  // ------------------------------------------------------------------ DDL dialect contracts

  @Test
  public void dialectDdlContractsAreLocked() throws Exception {
    String mysql = Files.readString(Path.of("src/main/resources/schema-mysql.sql"));
    String postgres = Files.readString(Path.of("src/main/resources/schema-postgres.sql"));
    String derby = Files.readString(Path.of("src/main/resources/schema-derby.sql"));

    // MySQL 5.7: MEDIUMTEXT/DATETIME; the SHIPPED DDL creates exactly one table
    assertTrue(mysql.contains("MEDIUMTEXT   NOT NULL"));
    assertTrue(mysql.contains("last_updated DATETIME     NOT NULL"));
    assertTrue(mysql.contains("CREATE TABLE IF NOT EXISTS amoro_process_v2"));
    assertFalse(mysql.contains("amoro_resource"), "generic-domain tables are not shipped");

    // PostgreSQL: TEXT/TIMESTAMP(3) WITHOUT TIME ZONE (static contract only — no runtime test)
    assertTrue(postgres.contains("TEXT         NOT NULL"));
    assertTrue(postgres.contains("last_updated TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL"));

    // Derby: CLOB/TIMESTAMP, plain CREATE (10.14 has no IF NOT EXISTS); strip the explanatory
    // header comment first, which mentions the missing syntax in prose
    assertTrue(derby.contains("CLOB         NOT NULL"));
    assertTrue(derby.contains("last_updated TIMESTAMP    NOT NULL"));
    assertTrue(derby.contains("CREATE TABLE amoro_process_v2"));
    assertFalse(
        derby.replaceAll("--[^\\n]*", "").contains("CREATE TABLE IF NOT EXISTS"),
        "the shipped Derby script must stay 10.14-compatible");

    for (String ddl : new String[] {mysql, postgres, derby}) {
      assertTrue(
          ddl.contains("Licensed to the Apache Software Foundation"),
          "every DDL carries the Apache license header (rat)");
      // the shipped DDL defines exactly the single deployed table
      assertTrue(ddl.contains("amoro_process_v2"), "each dialect defines the process table");
      assertTrue(ddl.contains("PRIMARY KEY (name)"), "name is the primary key everywhere");
    }
  }
}
