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

import com.mysql.cj.exceptions.CJCommunicationsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Observes JDBC failures before transaction/pool cleanup can consume them. Never retries SQL. */
public final class PersistenceFailureReporter {
  private static final Logger LOG = LoggerFactory.getLogger(PersistenceFailureReporter.class);
  private static final ThreadLocal<StartupScope> STARTUP = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> COMMIT_ACKNOWLEDGED = new ThreadLocal<>();

  public enum Phase {
    ACQUIRE,
    STATEMENT,
    COMMIT,
    ROLLBACK,
    CLOSE,
    POOL_CLEANUP
  }

  private PersistenceFailureReporter() {}

  public static StartupScope startupScope() {
    if (STARTUP.get() != null) {
      throw new IllegalStateException("A database startup scope is already active");
    }
    StartupScope scope = new StartupScope();
    STARTUP.set(scope);
    return scope;
  }

  public static void throwIfStartupFailed() {
    StartupScope scope = STARTUP.get();
    if (scope != null) {
      scope.check();
    }
  }

  public static boolean isConnectionFailure(Throwable failure) {
    return details(failure).connectionFailure();
  }

  public static void record(Phase phase, Throwable failure) {
    record(phase, failure, -1);
  }

  static void record(Phase phase, Throwable failure, long startedAtNanos) {
    FailureDetails details = details(failure);
    boolean unavailable =
        phase == Phase.ACQUIRE
            || details.connectionFailure()
            || (phase != Phase.STATEMENT && details.networkFailure);
    StartupScope scope = STARTUP.get();
    if (scope != null && unavailable && scope.failure == null) {
      scope.failure = failure;
      scope.phase = phase;
    }
    long elapsedMillis =
        startedAtNanos < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
    // SQL/URL exception text may contain parameters or credentials. Keep the original throwable
    // for propagation, while logging only connection diagnostics here.
    LOG.warn(
        "JDBC failure phase={} elapsedMs={} sqlState={} exception={} unavailable={} lastCommitAcknowledged={}",
        phase,
        elapsedMillis,
        details.sqlState,
        failure.getClass().getName(),
        unavailable,
        COMMIT_ACKNOWLEDGED.get());
  }

  static CleanupScope closingTransaction(boolean commitAcknowledged) {
    return new CleanupScope(commitAcknowledged);
  }

  private static FailureDetails details(Throwable root) {
    FailureDetails result = new FailureDetails();
    if (root == null) {
      return result;
    }
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    pending.add(root);
    while (!pending.isEmpty()) {
      Throwable failure = pending.removeFirst();
      if (!visited.add(failure)) {
        continue;
      }
      if (failure instanceof SQLException) {
        result.jdbcFailure = true;
        SQLException sql = (SQLException) failure;
        String state = sql.getSQLState();
        if (state != null && state.matches("[A-Z0-9]{5}")) {
          result.sqlState = state;
          result.disconnected |=
              state.startsWith("08")
                  || state.equals("57P01")
                  || state.equals("57P02")
                  || state.equals("57P03");
        }
        if (sql.getNextException() != null) {
          pending.add(sql.getNextException());
        }
      }
      result.disconnected |=
          failure instanceof SQLRecoverableException
              || failure instanceof SQLNonTransientConnectionException
              || failure instanceof SQLTransientConnectionException
              || failure instanceof CJCommunicationsException;
      result.networkFailure |=
          failure instanceof SocketTimeoutException
              || failure instanceof SocketException
              || failure instanceof EOFException;
      if (failure.getCause() != null) {
        pending.add(failure.getCause());
      }
      Collections.addAll(pending, failure.getSuppressed());
    }
    return result;
  }

  private static final class FailureDetails {
    private String sqlState = "unknown";
    private boolean jdbcFailure;
    private boolean disconnected;
    private boolean networkFailure;

    private boolean connectionFailure() {
      // A catalog/HDFS timeout during table recovery is not itself a JDBC outage.
      return disconnected || (jdbcFailure && networkFailure);
    }
  }

  public static final class StartupScope implements AutoCloseable {
    private Throwable failure;
    private Phase phase;

    private StartupScope() {}

    public void check() {
      if (failure != null) {
        throw new PersistenceUnavailableDuringStartupException(phase.name(), failure);
      }
    }

    @Override
    public void close() {
      if (STARTUP.get() == this) {
        STARTUP.remove();
      }
    }
  }

  static final class CleanupScope implements AutoCloseable {
    private final Boolean previous;

    private CleanupScope(boolean acknowledged) {
      previous = COMMIT_ACKNOWLEDGED.get();
      COMMIT_ACKNOWLEDGED.set(acknowledged);
    }

    @Override
    public void close() {
      if (previous == null) {
        COMMIT_ACKNOWLEDGED.remove();
      } else {
        COMMIT_ACKNOWLEDGED.set(previous);
      }
    }
  }
}
