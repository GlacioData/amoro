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

package org.apache.amoro.server.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

class TestOptimizingCommitGuard extends PersistentBase {
  private static final AtomicLong IDS = new AtomicLong(950000);
  private final List<ServerTableIdentifier> tables = new ArrayList<>();
  private final ExecutorService executor = Executors.newFixedThreadPool(3);

  @BeforeAll
  static void database() {
    FullSkipTestDatabase.initialize();
  }

  private ServerTableIdentifier create(long processId) {
    ServerTableIdentifier id =
        ServerTableIdentifier.of("guard", "db", "t" + processId, TableFormat.PAIMON);
    doAs(TableMetaMapper.class, m -> m.insertTable(id));
    tables.add(id);
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("guard");
    meta.setStatusCode(OptimizingStatus.COMMITTING.getCode());
    meta.setTableConfig(Collections.emptyMap());
    doAs(TableRuntimeMapper.class, m -> m.insertRuntime(meta));
    new DefaultTableRuntimeStore(
        id, meta, DefaultTableRuntime.REQUIRED_STATES, Collections.emptyList());
    doAs(
        TableRuntimeMapper.class,
        m ->
            m.setStateValue(
                id.getId(), DefaultTableRuntime.PROCESS_ID_KEY.getKey(), Long.toString(processId)));
    doAs(
        TableProcessMapper.class,
        m ->
            m.insertProcess(
                id.getId(),
                processId,
                "",
                ProcessStatus.RUNNING,
                "FULL",
                "committing",
                "AMORO",
                0,
                1L,
                Collections.emptyMap(),
                Collections.emptyMap()));
    return id;
  }

  @AfterEach
  void clean() throws Exception {
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    for (ServerTableIdentifier id : tables) {
      doAs(TableProcessMapper.class, m -> m.deleteBefore(id.getId(), Long.MAX_VALUE));
      doAs(TableRuntimeMapper.class, m -> m.removeAllTableStates(id.getId()));
      doAs(TableRuntimeMapper.class, m -> m.deleteRuntime(id.getId()));
      doAs(TableMetaMapper.class, m -> m.deleteTableIdById(id.getId()));
    }
  }

  @Test
  void handoffWaitsForSameTableWhileOtherTableCanCommit() throws Exception {
    long first = IDS.incrementAndGet();
    long second = IDS.incrementAndGet();
    ServerTableIdentifier a = create(first);
    ServerTableIdentifier b = create(second);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    Future<?> commit =
        executor.submit(
            () -> {
              OptimizingCommitGuard.commit(
                  a.getId(),
                  first,
                  () -> {
                    entered.countDown();
                    assertTrue(finish.await(10, TimeUnit.SECONDS));
                  });
              return null;
            });
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      Future<?> handoff = executor.submit(() -> OptimizingCommitGuard.seal(a.getId(), first));
      assertThrows(TimeoutException.class, () -> handoff.get(200, TimeUnit.MILLISECONDS));
      executor
          .submit(
              () -> {
                OptimizingCommitGuard.commit(b.getId(), second, () -> {});
                return null;
              })
          .get(5, TimeUnit.SECONDS);
      finish.countDown();
      commit.get(5, TimeUnit.SECONDS);
      handoff.get(5, TimeUnit.SECONDS);
      assertThrows(
          PersistenceException.class,
          () ->
              OptimizingCommitGuard.commit(
                  a.getId(), first, () -> fail("The old process must not publish again")));
    } finally {
      finish.countDown();
    }
  }

  @Test
  void abandonedNativeClaimBlocksHandoffAndPublication() {
    long processId = IDS.incrementAndGet();
    ServerTableIdentifier id = create(processId);
    // Simulate a durable marker left by an AMS crash or database disconnect during native commit.
    doAs(
        TableRuntimeMapper.class,
        m ->
            m.setStateValue(
                id.getId(), OptimizingCommitGuard.STATE_KEY.getKey(), Long.toString(processId)));
    assertThrows(
        PersistenceException.class, () -> OptimizingCommitGuard.seal(id.getId(), processId));
    assertThrows(
        PersistenceException.class,
        () ->
            OptimizingCommitGuard.commit(
                id.getId(), processId, () -> fail("An unresolved call must not be replayed")));
    assertEquals(
        Long.toString(processId),
        getAs(
            TableRuntimeMapper.class,
            m ->
                m.getState(id.getId(), DefaultTableRuntime.PROCESS_ID_KEY.getKey())
                    .getStateValue()));
  }

  @Test
  void uncertainNativeFailureKeepsOwnerBlocked() {
    long processId = IDS.incrementAndGet();
    ServerTableIdentifier id = create(processId);
    assertThrows(
        IllegalStateException.class,
        () ->
            OptimizingCommitGuard.commit(
                id.getId(),
                processId,
                () -> {
                  throw new IllegalStateException("native returned");
                }));
    assertThrows(
        PersistenceException.class, () -> OptimizingCommitGuard.seal(id.getId(), processId));
    assertThrows(
        PersistenceException.class,
        () ->
            OptimizingCommitGuard.commit(
                id.getId(), processId, () -> fail("The old process is fenced")));
  }

  @Test
  void guardWorksWithOneDatabaseConnection() throws Exception {
    long processId = IDS.incrementAndGet();
    ServerTableIdentifier id = create(processId);
    BasicDataSource pool =
        (BasicDataSource)
            SqlSessionFactoryProvider.getInstance()
                .get()
                .getConfiguration()
                .getEnvironment()
                .getDataSource();
    int previous = pool.getMaxTotal();
    pool.setMaxTotal(1);
    try {
      OptimizingCommitGuard.commit(
          id.getId(), processId, () -> assertEquals(1, pool.getNumActive()));
      OptimizingCommitGuard.seal(id.getId(), processId);
    } finally {
      pool.setMaxTotal(previous);
    }
  }

  @Test
  void newGuardInitializationAcceptsConcurrentPersistedRow() {
    long processId = IDS.incrementAndGet();
    ServerTableIdentifier id = create(processId);
    TableRuntimeMeta meta = getAs(TableRuntimeMapper.class, m -> m.selectRuntime(id.getId()));
    List<TableRuntimeState> stale =
        getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates).stream()
            .filter(
                state ->
                    state.getTableId() == id.getId()
                        && !state.getStateKey().equals(OptimizingCommitGuard.STATE_KEY.getKey()))
            .collect(Collectors.toList());
    new DefaultTableRuntimeStore(id, meta, DefaultTableRuntime.REQUIRED_STATES, stale);
    OptimizingCommitGuard.seal(id.getId(), processId);
  }

  @Test
  @EnabledIfSystemProperty(named = "jspt2717.test.db", matches = "mysql")
  void disconnectedMySqlCommitRetainsClaimWhileNativeCallCanContinue() throws Exception {
    long processId = IDS.incrementAndGet();
    ServerTableIdentifier id = create(processId);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    Future<?> nativeCall =
        executor.submit(
            () -> {
              OptimizingCommitGuard.commit(
                  id.getId(),
                  processId,
                  () -> {
                    entered.countDown();
                    assertTrue(finish.await(30, TimeUnit.SECONDS));
                  });
              return null;
            });
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      // Use a disposable, isolated MySQL database and PROCESS privilege for this fault injection.
      try (Connection observer =
              SqlSessionFactoryProvider.getInstance()
                  .get()
                  .getConfiguration()
                  .getEnvironment()
                  .getDataSource()
                  .getConnection();
          Statement statement = observer.createStatement()) {
        List<Long> holders = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        do {
          holders.clear();
          try (ResultSet rows =
              statement.executeQuery(
                  "SELECT t.trx_mysql_thread_id FROM information_schema.innodb_trx t "
                      + "JOIN information_schema.processlist p ON p.ID = t.trx_mysql_thread_id "
                      + "WHERE t.trx_rows_locked > 0 AND p.DB = 'jspt2717' AND p.USER = 'amoro' "
                      + "AND p.ID <> CONNECTION_ID()")) {
            while (rows.next()) {
              holders.add(rows.getLong(1));
            }
          }
          if (!holders.isEmpty()) {
            break;
          }
          Thread.sleep(50L);
        } while (System.nanoTime() < deadline);
        assertEquals(1, holders.size(), "Only the isolated native commit may hold a transaction");
        statement.execute("KILL CONNECTION " + holders.get(0));
      }
      assertThrows(
          PersistenceException.class, () -> OptimizingCommitGuard.seal(id.getId(), processId));
      assertFalse(
          nativeCall.isDone(), "Disconnecting SQL must not be mistaken for stopping the lake call");
      finish.countDown();
      assertThrows(ExecutionException.class, () -> nativeCall.get(5, TimeUnit.SECONDS));
      assertEquals(
          Long.toString(processId),
          getAs(
              TableRuntimeMapper.class,
              m ->
                  m.getState(id.getId(), OptimizingCommitGuard.STATE_KEY.getKey())
                      .getStateValue()));
      assertEquals(
          Long.toString(processId),
          getAs(
              TableRuntimeMapper.class,
              m ->
                  m.getState(id.getId(), DefaultTableRuntime.PROCESS_ID_KEY.getKey())
                      .getStateValue()));
    } finally {
      finish.countDown();
    }
  }

  @Test
  void staleOwnerCannotPublish() {
    long processId = IDS.incrementAndGet();
    ServerTableIdentifier id = create(processId);
    doAs(
        TableRuntimeMapper.class,
        m -> m.setStateValue(id.getId(), DefaultTableRuntime.PROCESS_ID_KEY.getKey(), "0"));
    assertThrows(
        PersistenceException.class,
        () ->
            OptimizingCommitGuard.commit(
                id.getId(), processId, () -> fail("Ownership has been transferred")));
  }
}
