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

import org.apache.amoro.exception.OptimizingCommitSkippedException;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.persistence.NestedSqlSession;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.table.StateKey;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.TransactionIsolationLevel;

/**
 * Serialize commits per table. Zero means no unresolved call; a positive processId marks an
 * unresolved call, and a negative processId fences that process. Under the lock, run only lake
 * operations and SQL on the same connection. Complete the runtime in a separate transaction after
 * durably fencing old commits. A disconnected database session may release the lock, but the
 * durable positive marker still blocks takeover.
 */
public final class OptimizingCommitGuard {
  public static final StateKey<Long> STATE_KEY =
      StateKey.stateKey("optimizing_commit_guard").longType().defaultValue(0L);

  @FunctionalInterface
  public interface CommitAction {
    void run() throws Exception;
  }

  private OptimizingCommitGuard() {}

  /**
   * Persist the unresolved marker before calling the lake; retain it on process failure or a failed
   * cleanup transaction.
   */
  public static void commit(long tableId, long processId, CommitAction action) throws Exception {
    try (SqlSession session = lock(tableId)) {
      verifyOwner(session, tableId, processId);
      long value = value(session, tableId);
      require(value <= 0 && value != -processId, "Commit is unresolved or fenced: " + processId);
      set(session, tableId, processId);
      session.commit(true);
    }
    try (SqlSession session = lock(tableId)) {
      verifyOwner(session, tableId, processId);
      require(value(session, tableId) == processId, "Commit claim changed: " + processId);
      try {
        action.run();
      } catch (OptimizingCommitSkippedException skipped) {
        if (!skipped.isPublicationUncertain()) {
          set(session, tableId, 0L);
          session.commit(true);
        }
        throw skipped;
      }
      // An exception may leave publication pending in storage. Roll back and retain the positive
      // marker unless the result is definitive.
      set(session, tableId, 0L);
      session.commit(true);
    }
  }

  /**
   * Fence old commits before read-only reconciliation and closure; unresolved calls must retain
   * their owner.
   */
  public static void seal(long tableId, long processId) {
    try (SqlSession session = lock(tableId)) {
      verifyOwner(session, tableId, processId);
      require(value(session, tableId) <= 0, "Native commit is unresolved: " + processId);
      set(session, tableId, -processId);
      session.commit(true);
    }
  }

  private static SqlSession lock(long tableId) {
    require(
        !NestedSqlSession.hasActiveSession(), "Commit guard requires an independent transaction");
    SqlSession session =
        SqlSessionFactoryProvider.getInstance()
            .get()
            .openSession(TransactionIsolationLevel.READ_COMMITTED);
    try {
      require(
          session.getMapper(TableRuntimeMapper.class).lockCommitState(tableId, STATE_KEY.getKey())
              == 1,
          "Missing commit guard for table " + tableId);
      return session;
    } catch (RuntimeException failure) {
      session.close();
      throw failure;
    }
  }

  private static long value(SqlSession session, long tableId) {
    return Long.parseLong(
        session
            .getMapper(TableRuntimeMapper.class)
            .getState(tableId, STATE_KEY.getKey())
            .getStateValue());
  }

  private static void set(SqlSession session, long tableId, long value) {
    session
        .getMapper(TableRuntimeMapper.class)
        .setStateValue(tableId, STATE_KEY.getKey(), Long.toString(value));
  }

  private static void verifyOwner(SqlSession session, long tableId, long processId) {
    TableRuntimeState owner =
        session
            .getMapper(TableRuntimeMapper.class)
            .getState(tableId, DefaultTableRuntime.PROCESS_ID_KEY.getKey());
    TableProcessMeta process =
        session.getMapper(TableProcessMapper.class).getProcessMeta(processId);
    require(
        processId > 0
            && owner != null
            && Long.toString(processId).equals(owner.getStateValue())
            && process != null
            && process.getTableId() == tableId
            && process.getStatus() == ProcessStatus.RUNNING,
        "Optimizing process owner changed: " + processId);
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new PersistenceException(message);
    }
  }
}
