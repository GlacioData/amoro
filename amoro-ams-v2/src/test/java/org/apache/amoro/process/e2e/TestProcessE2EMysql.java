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

package org.apache.amoro.process.e2e;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.persistence.blob.MyBatisBlobStore;
import org.apache.amoro.persistence.blob.ResourceBlobMapper;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.ProcessFinality;
import org.apache.amoro.process.ProcessReconciler;
import org.apache.amoro.process.ProcessTtlCleaner;
import org.apache.amoro.process.engine.LocalEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.apache.amoro.process.rest.ProcessRestSupport;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * P8 end-to-end over REAL MySQL 5.7 (tag docker-mysql, -Pdocker-it): REST create → local engine
 * execution through the reconciler → terminal → TTL cleaning → full context rebuild → postStart
 * replay. The database is the source of truth at every step.
 */
@Tag("docker-mysql")
@Timeout(180)
public class TestProcessE2EMysql {

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

  private static Connection admin;
  private static SqlSessionFactory sqlFactory;

  /** One full control-plane context over the durable store. */
  private static final class Stack implements AutoCloseable {
    final DefaultScheduler scheduler = DefaultScheduler.create(2, 50L);
    final LocalEngineAdapter engine =
        new LocalEngineAdapter(2, LocalEngineAdapter.simulatedAction());
    final ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(engine, 5_000L);
    final ProcessDomainAssembly assembly;
    final ProcessRestSupport rest;
    final ProcessTtlCleaner cleaner;

    Stack() {
      scheduler.start();
      assembly =
          new ProcessDomainAssembly(
              new MyBatisBlobStore(
                  ProcessDomainAssembly.DOMAIN,
                  sqlFactory.openSession(true).getMapper(ResourceBlobMapper.class)),
              event -> HandoffResult.ACCEPTED,
              scheduler,
              128,
              10_000L,
              65536);
      rest = new ProcessRestSupport(assembly);
      cleaner = new ProcessTtlCleaner(assembly);
    }

    void replayAndSchedule(String name) {
      assembly.persistence().postStart();
      ProcessReconciler reconciler =
          new ProcessReconciler(
              name,
              assembly.repository(),
              dispatcher,
              scheduler,
              ProcessReconciler.Clock.systemUtc(),
              100L);
      scheduler.schedule(reconciler);
    }

    @Override
    public void close() {
      scheduler.shutdown(Duration.ofSeconds(5));
      assembly.persistence().shutdown(Duration.ofSeconds(5));
      engine.shutdown(5_000L);
    }
  }

  @BeforeAll
  public static void probeAndSetUp() throws Exception {
    try {
      admin = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    } catch (SQLException unreachable) {
      Assumptions.assumeTrue(
          false, "no reachable MySQL at " + JDBC_URL + " — docker-mysql group skips explicitly");
    }
    try (Statement statement = admin.createStatement()) {
      // the process E2E owns its table exclusively for a clean lifecycle from scratch
      statement.execute("DROP TABLE IF EXISTS amoro_process");
    }
    org.apache.ibatis.datasource.unpooled.UnpooledDataSource dataSource =
        new org.apache.ibatis.datasource.unpooled.UnpooledDataSource(
            "com.mysql.cj.jdbc.Driver", JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    Environment environment =
        new Environment("process-e2e", new JdbcTransactionFactory(), dataSource);
    Configuration configuration = new Configuration(environment);
    configuration.addMapper(ResourceBlobMapper.class);
    sqlFactory = new SqlSessionFactoryBuilder().build(configuration);
    // recreate the process table from the shipped MySQL dialect DDL
    String script =
        new String(
                java.util.Objects.requireNonNull(
                        TestProcessE2EMysql.class.getResourceAsStream("/schema-mysql.sql"))
                    .readAllBytes(),
                java.nio.charset.StandardCharsets.UTF_8)
            .replaceAll("--[^\n]*", "");
    try (Statement statement = admin.createStatement()) {
      for (String piece : script.split(";")) {
        String sql = piece.trim();
        if (sql.toUpperCase().contains("AMORO_PROCESS")) {
          statement.execute(sql);
        }
      }
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (admin != null) {
      try (Statement statement = admin.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS amoro_process");
      }
      admin.close();
    }
  }

  @Test
  public void fullLifecycleOnRealMysqlWithRestartReplay() throws Exception {
    String createdName;
    try (Stack stack = new Stack()) {
      // 1. REST create with idempotency (durable amoro_process row)
      org.apache.amoro.process.ProcessResource created =
          stack.rest.create("prod", "db1", "orders", "e2e-key-1", "expire-snapshots", "local", null)
              .resource;
      createdName = created.name();
      assertEquals(1, created.resourceVersion());
      assertEquals("PENDING", created.status().phase());

      // same key replays to the same resource (no duplicate row)
      org.apache.amoro.process.ProcessResource replayed =
          stack.rest.create("prod", "db1", "orders", "e2e-key-1", "expire-snapshots", "local", null)
              .resource;
      assertEquals(createdName, replayed.name());

      // 2. schedule the reconciler: local engine ACK → observe → SUCCESS, all durable
      ProcessReconciler reconciler =
          new ProcessReconciler(
              createdName,
              stack.assembly.repository(),
              stack.dispatcher,
              stack.scheduler,
              ProcessReconciler.Clock.systemUtc(),
              100L);
      stack.scheduler.schedule(reconciler);
      await()
          .atMost(30, TimeUnit.SECONDS)
          .until(
              () ->
                  ProcessFinality.isFixedTerminal(
                      stack.assembly.repository().get(createdName).status().phase()));
      assertEquals("SUCCESS", stack.assembly.repository().get(createdName).status().phase());
      assertTrue(stack.assembly.repository().get(createdName).status().finishedAt() != null);
      await().atMost(10, TimeUnit.SECONDS).until(() -> stack.scheduler.registrySize() == 0);
    } // full context destruction

    // 3. rebuild over the same MySQL: postStart replays the durable row
    try (Stack rebuilt = new Stack()) {
      rebuilt.assembly.persistence().postStart();
      org.apache.amoro.process.ProcessResource reloaded =
          rebuilt.assembly.repository().get(createdName);
      assertEquals("SUCCESS", reloaded.status().phase());
      assertTrue(
          rebuilt.assembly.indexProjection().current().find(createdName).isPresent(),
          "the restart rebuilt the index from the durable row");

      // 4. TTL: at clock+40d the terminal row is deleted; the active-slot is free again
      int deleted = rebuilt.cleaner.cleanOnce(Instant.now().plus(Duration.ofDays(40)), 30, 10);
      assertEquals(1, deleted);
      assertFalse(rebuilt.assembly.indexProjection().current().find(createdName).isPresent());
      // and the table admits a fresh process for the same table/action
      org.apache.amoro.process.ProcessResource fresh =
          rebuilt.rest.create(
                  "prod", "db1", "orders", "e2e-key-2", "expire-snapshots", "local", null)
              .resource;
      assertEquals("PENDING", fresh.status().phase());
    }
  }
}
