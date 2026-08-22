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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real-MySQL integration (framework spec §8/§9, tag {@code docker-mysql}, activated by {@code
 * -Pdocker-it}). Runs against a reachable MySQL 5.7 instance — by default the local Docker
 * container on localhost:3306, database {@code amoro_v2}; override with the {@code
 * AMORO_V2_MYSQL_*} environment variables. The port probe is the second safety net: without a
 * reachable database the whole group skips explicitly (never silently green).
 */
@Tag("docker-mysql")
@Timeout(120)
public class TestMyBatisBlobStoreMysql {

  private static final String JDBC_URL =
      System.getenv()
          .getOrDefault(
              "AMORO_V2_MYSQL_URL",
              "jdbc:mysql://localhost:3306/amoro_v2"
                  + "?useSSL=false&characterEncoding=utf8&allowPublicKeyRetrieval=true");
  private static final String JDBC_USER =
      System.getenv().getOrDefault("AMORO_V2_MYSQL_USER", "root");
  private static final String JDBC_PASSWORD =
      System.getenv().getOrDefault("AMORO_V2_MYSQL_PASSWORD", "");

  private static Connection connection;
  private static MyBatisBlobStore resourceStore;
  private static MyBatisBlobStore processStore;

  @BeforeAll
  public static void probeAndSetUp() throws Exception {
    try {
      connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    } catch (SQLException unreachable) {
      Assumptions.assumeTrue(
          false, "no reachable MySQL at " + JDBC_URL + " — docker-mysql group skips explicitly");
      return;
    }
    runMysqlSchema(connection); // IF NOT EXISTS: idempotent on every rerun
    SqlSessionFactory factory = sqlSessionFactory();
    resourceStore =
        new MyBatisBlobStore(
            new PersistenceDomain("resource", "amoro_resource", SerdeFormat.JSON),
            factory.openSession(true).getMapper(ResourceBlobMapper.class));
    processStore =
        new MyBatisBlobStore(
            new PersistenceDomain("process", "amoro_process", SerdeFormat.YAML),
            factory.openSession(true).getMapper(ResourceBlobMapper.class));
    clearTables();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (connection != null) {
      clearTables();
      connection.close();
    }
  }

  private static void clearTables() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String table :
          new String[] {"amoro_resource", "amoro_process", "amoro_process_trigger"}) {
        statement.execute("DELETE FROM " + table);
      }
    }
  }

  static void runMysqlSchema(Connection connection) throws Exception {
    // classpath load: main resources sit on the test classpath, so IDE runs work too
    String script =
        new String(
            TestMyBatisBlobStoreMysql.class.getResourceAsStream("/schema-mysql.sql").readAllBytes(),
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

  private static SqlSessionFactory sqlSessionFactory() {
    org.apache.ibatis.datasource.unpooled.UnpooledDataSource dataSource =
        new org.apache.ibatis.datasource.unpooled.UnpooledDataSource(
            "com.mysql.cj.jdbc.Driver", JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    Environment environment = new Environment("mysql-it", new JdbcTransactionFactory(), dataSource);
    Configuration configuration = new Configuration(environment);
    configuration.addMapper(ResourceBlobMapper.class);
    return new SqlSessionFactoryBuilder().build(configuration);
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  // ------------------------------------------------------------------ five SQL semantics

  @Test
  public void insertFindUpdateDeleteOnRealMysql() {
    resourceStore.insert("fake", "r1", bytes("{\"doc\":\"one\"}"));
    assertArrayEquals(bytes("{\"doc\":\"one\"}"), resourceStore.find("fake", "r1").orElse(null));

    assertTrue(resourceStore.update("fake", "r1", bytes("{\"doc\":\"two\"}")));
    assertArrayEquals(bytes("{\"doc\":\"two\"}"), resourceStore.find("fake", "r1").orElse(null));

    assertTrue(resourceStore.delete("fake", "r1"));
    assertEquals(Optional.empty(), resourceStore.find("fake", "r1"));
    assertFalse(resourceStore.delete("fake", "r1"));
    assertFalse(resourceStore.update("fake", "ghost", bytes("x")));
  }

  @Test
  public void duplicateInsertTranslatesToResourceAlreadyExists() {
    resourceStore.insert("fake", "dup", bytes("a"));
    assertThrows(
        ResourceAlreadyExists.class, () -> resourceStore.insert("fake", "dup", bytes("b")));
  }

  @Test
  public void mediumTextSurvivesBase64OfAFullSizeDocument() {
    // a 64KiB raw JSON document Base64-encodes to ~87KB: proves MEDIUMTEXT (TEXT would truncate)
    StringBuilder json = new StringBuilder("{\"padding\":\"");
    while (json.length() < 64 * 1024) {
      json.append("abcdefghijklmnopqrstuvwxyz0123456789");
    }
    json.append("\"}");
    byte[] document = bytes(json.toString());
    assertTrue(
        Base64.getEncoder().encodeToString(document).length() > 65_535,
        "the encoded value must exceed the TEXT limit for the test to mean anything");

    resourceStore.insert("fake", "big", document);
    assertArrayEquals(document, resourceStore.find("fake", "big").orElse(null));
  }

  @Test
  public void forEachScansAndRestartReloadsExactlyTheDurableBytes() throws Exception {
    // a dedicated collection keeps the scan independent of rows left by sibling tests
    final String collection = "scan-it";
    resourceStore.insert(collection, "reload", bytes("{\"v\":7}"));

    AtomicInteger seen = new AtomicInteger();
    resourceStore.forEach(collection, (name, value) -> seen.incrementAndGet());
    assertEquals(1, seen.get());

    MyBatisBlobStore restarted =
        new MyBatisBlobStore(
            new PersistenceDomain("resource", "amoro_resource", SerdeFormat.JSON),
            sqlSessionFactory().openSession(true).getMapper(ResourceBlobMapper.class));
    assertArrayEquals(bytes("{\"v\":7}"), restarted.find(collection, "reload").orElse(null));
  }

  @Test
  public void doubleDomainDoubleTableIsolationOnRealMysql() {
    resourceStore.insert("fake", "same-name", bytes("from-resource"));
    processStore.insert("process", "same-name", bytes("from-process"));

    assertArrayEquals(bytes("from-resource"), resourceStore.find("fake", "same-name").orElse(null));
    assertArrayEquals(
        bytes("from-process"), processStore.find("process", "same-name").orElse(null));
    assertTrue(processStore.delete("process", "same-name"));
    assertTrue(resourceStore.find("fake", "same-name").isPresent());
  }

  @Test
  public void schemaInitializationIsIdempotent() throws Exception {
    runMysqlSchema(connection); // CREATE TABLE IF NOT EXISTS: rerunning must not fail
    resourceStore.insert("fake", "after-rerun", bytes("ok"));
    assertArrayEquals(bytes("ok"), resourceStore.find("fake", "after-rerun").orElse(null));
  }
}
