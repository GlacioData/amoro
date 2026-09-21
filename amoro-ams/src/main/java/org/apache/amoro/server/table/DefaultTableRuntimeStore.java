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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.exception.AmoroRuntimeException;
import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.exception.RuntimeStateConflictException;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.NestedSqlSession;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableList;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableRuntimeStore;
import org.apache.amoro.table.TableSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Default table runtime store implementation. */
public class DefaultTableRuntimeStore extends PersistentBase implements TableRuntimeStore {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTableRuntimeStore.class);

  // An operation may reenter this lock from synchronizedInvoke. Releasing only the operation's
  // hold does not make handler invocation safe: handlers enter OptimizingQueue/process locks,
  // while process paths acquire the locks in the opposite process-lock -> table-lock direction.
  // Keep committed callbacks synchronous, but defer them until the outermost table-lock hold is
  // fully released so the reverse table-lock -> process-lock edge cannot be formed.
  private final ReentrantLock tableLock = new ReentrantLock();
  private final ThreadLocal<Deque<Runnable>> deferredRuntimeHandlerCallbacks =
      ThreadLocal.withInitial(ArrayDeque::new);
  private final ServerTableIdentifier tableIdentifier;
  private final TableRuntimeMeta meta;
  private final Map<String, TableRuntimeState> states = new ConcurrentHashMap<>();

  private TableRuntimeHandler runtimeHandler;
  private TableRuntime tableRuntime;

  public DefaultTableRuntimeStore(
      ServerTableIdentifier tableIdentifier,
      TableRuntimeMeta meta,
      List<StateKey<?>> requiredStates,
      List<TableRuntimeState> restoredStates) {
    Preconditions.checkNotNull(tableIdentifier, "ServerTableIdentifier must not be null.");
    Preconditions.checkNotNull(meta, "TableRuntimeMeta must not be null.");
    Preconditions.checkNotNull(requiredStates, "requiredStates must not be null.");
    Preconditions.checkNotNull(restoredStates, "restoredStates must not be null.");
    this.tableIdentifier = tableIdentifier;
    this.meta = meta;
    restoreStates(requiredStates, restoredStates);
  }

  public void setTableRuntime(TableRuntime tableRuntime) {
    this.tableRuntime = tableRuntime;
  }

  public void setRuntimeHandler(TableRuntimeHandler runtimeHandler) {
    this.runtimeHandler = runtimeHandler;
  }

  @Override
  public ServerTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  @Override
  public String getGroupName() {
    return this.meta.getGroupName();
  }

  @Override
  public Map<String, String> getTableConfig() {
    return this.meta.getTableConfig();
  }

  @Override
  public int getStatusCode() {
    return this.meta.getStatusCode();
  }

  /** @return the persisted timestamp when the status code was last updated. */
  public long getStatusCodeUpdateTime() {
    return this.meta.getStatusCodeUpdateTime();
  }

  @Override
  public <T> T getState(StateKey<T> key) {
    Preconditions.checkNotNull(key, "TableRuntime state key cannot be null");
    Preconditions.checkNotNull(
        states.containsKey(key.getKey()), "TableRuntime state %s not initialized", key);
    TableRuntimeState tableState = states.get(key.getKey());
    return key.deserialize(tableState.getStateValue());
  }

  @Override
  public void synchronizedInvoke(Runnable runnable) {
    Throwable actionFailure = null;
    tableLock.lock();
    try {
      runnable.run();
    } catch (Throwable throwable) {
      actionFailure = throwable;
    } finally {
      tableLock.unlock();
    }

    Throwable callbackFailure = null;
    try {
      drainRuntimeHandlerCallbacksIfUnlocked();
    } catch (Throwable throwable) {
      callbackFailure = throwable;
    }
    if (actionFailure != null) {
      if (callbackFailure != null && callbackFailure != actionFailure) {
        actionFailure.addSuppressed(callbackFailure);
      }
      rethrow(actionFailure);
    }
    if (callbackFailure != null) {
      rethrow(callbackFailure);
    }
  }

  /** Publishes only already committed skip state, while holding the existing runtime lock. */
  void publishSkippedCompletion(
      TableRuntimeMeta committed, List<TableRuntimeState> committedStates) {
    Preconditions.checkState(tableLock.isHeldByCurrentThread(), "runtime lock is required");
    Preconditions.checkState(!NestedSqlSession.hasActiveSession(), "transaction must be closed");
    OptimizingStatus previous = OptimizingStatus.ofCode(meta.getStatusCode());
    meta.setStatusCode(committed.getStatusCode());
    meta.setStatusCodeUpdateTime(committed.getStatusCodeUpdateTime());
    meta.setTableSummary(committed.getTableSummary());
    committedStates.forEach(state -> states.put(state.getStateKey(), state));
    TableRuntimeHandler handler = runtimeHandler;
    TableRuntime runtime = tableRuntime;
    if (handler != null && runtime != null && previous != OptimizingStatus.IDLE) {
      dispatchOrDeferRuntimeHandlerCallback(() -> handler.handleTableChanged(runtime, previous));
    }
  }

  private void dispatchOrDeferRuntimeHandlerCallback(Runnable callback) {
    if (callback != null) {
      deferredRuntimeHandlerCallbacks.get().addLast(callback);
    }
    drainRuntimeHandlerCallbacksIfUnlocked();
  }

  private void drainRuntimeHandlerCallbacksIfUnlocked() {
    if (tableLock.isHeldByCurrentThread()) {
      return;
    }

    Deque<Runnable> callbacks = deferredRuntimeHandlerCallbacks.get();
    // Remove before invoking handlers so a reentrant operation gets an independent queue and so
    // both normal and exceptional callback paths release thread-local references.
    deferredRuntimeHandlerCallbacks.remove();
    Throwable callbackFailure = null;
    while (!callbacks.isEmpty()) {
      try {
        callbacks.removeFirst().run();
      } catch (Throwable throwable) {
        if (callbackFailure == null) {
          callbackFailure = throwable;
        } else if (callbackFailure != throwable) {
          callbackFailure.addSuppressed(throwable);
        }
      }
    }
    if (callbackFailure != null) {
      rethrow(callbackFailure);
    }
  }

  private static void rethrow(Throwable throwable) {
    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    }
    if (throwable instanceof Error) {
      throw (Error) throwable;
    }
    throw new IllegalStateException(throwable);
  }

  @Override
  public void dispose() {
    doAsTransaction(
        () -> doAs(TableRuntimeMapper.class, m -> m.deleteRuntime(tableIdentifier.getId())),
        () -> doAs(TableRuntimeMapper.class, m -> m.removeAllTableStates(tableIdentifier.getId())),
        () -> doAs(TableMetaMapper.class, m -> m.deleteTableIdById(tableIdentifier.getId())));
  }

  @Override
  public TableRuntimeOperation begin() {
    return new TableRuntimeOperationImpl();
  }

  protected void restoreStates(
      List<StateKey<?>> requiredStates, List<TableRuntimeState> restoredStates) {
    Map<String, TableRuntimeState> stateMap =
        restoredStates.stream()
            .collect(Collectors.toMap(TableRuntimeState::getStateKey, Function.identity()));

    requiredStates.forEach(
        key -> {
          if (stateMap.containsKey(key.getKey())) {
            states.put(key.getKey(), stateMap.get(key.getKey()));
            stateMap.remove(key.getKey());
          } else {
            try {
              doAs(
                  TableRuntimeMapper.class,
                  mapper ->
                      mapper.saveState(
                          tableIdentifier.getId(), key.getKey(), key.serializeDefault()));
            } catch (PersistenceException failure) {
              // Concurrent AMS instances may initialize the new commit state; accept only an
              // existing row for this table and key.
              if ((!OptimizingCommitGuard.STATE_KEY.getKey().equals(key.getKey())
                      && !key.usesOptimisticConcurrency())
                  || (key.usesOptimisticConcurrency() && !isDuplicateState(failure))
                  || getAs(
                          TableRuntimeMapper.class,
                          mapper -> mapper.getState(tableIdentifier.getId(), key.getKey()))
                      == null) {
                throw failure;
              }
            }
            TableRuntimeState state =
                getAs(
                    TableRuntimeMapper.class,
                    m -> m.getState(tableIdentifier.getId(), key.getKey()));
            Preconditions.checkNotNull(state, "State %s initialize failed", key.getKey());
            states.put(key.getKey(), state);
          }
        });

    if (!stateMap.isEmpty()) {
      LOG.warn("Found {} useless runtime states for {}", stateMap.size(), tableIdentifier);
      stateMap.forEach(
          (k, s) -> {
            LOG.warn("Remove useless runtime state {} for {}", k, tableIdentifier);
            doAs(TableRuntimeMapper.class, m -> m.removeState(s.getStateId()));
          });
    }
  }

  private static boolean isDuplicateState(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof java.sql.SQLException) {
        java.sql.SQLException sql = (java.sql.SQLException) cause;
        // MySQL duplicate key; Derby's duplicate SQLState is retained for the existing test
        // fixture.
        if (sql.getErrorCode() == 1062 || "23505".equals(sql.getSQLState())) {
          return true;
        }
      }
    }
    return false;
  }

  private class TableRuntimeOperationImpl implements TableRuntimeOperation {

    private TableRuntimeMeta oldMeta;
    private final List<Runnable> operations = Lists.newArrayList();
    private final Set<String> stateKeys = Sets.newHashSet();
    private final Set<String> optimisticKeys = Sets.newHashSet();
    private final Map<String, Long> expectedVersions = new HashMap<>();
    private final Map<String, String> oldStates = new ConcurrentHashMap<>();
    private boolean metaOperation = false;

    private final List<BiConsumer<TableRuntimeHandler, TableRuntime>> handlerCallbacks =
        Lists.newArrayList();

    public TableRuntimeOperationImpl() {}

    @Override
    public <T> TableRuntimeOperation updateState(StateKey<T> key, Function<T, T> updater) {
      stateKeys.add(key.getKey());
      if (key.usesOptimisticConcurrency()) {
        optimisticKeys.add(key.getKey());
      }
      operations.add(
          () -> {
            Preconditions.checkNotNull(key, "TableRuntime state key cannot be null");
            Preconditions.checkNotNull(
                oldStates.containsKey(key.getKey()), "TableRuntime state %s not initialized", key);
            T oldValue = key.deserialize(oldStates.get(key.getKey()));
            T newValue = updater.apply(oldValue);
            String storedValue = key.serialize(newValue);
            oldStates.put(key.getKey(), storedValue);
          });
      return this;
    }

    @Override
    public TableRuntimeOperation updateGroup(Function<String, String> updater) {
      operations.add(
          () -> {
            String newGroupName = updater.apply(oldMeta.getGroupName());
            oldMeta.setGroupName(newGroupName);
          });
      metaOperation = true;
      return this;
    }

    @Override
    public TableRuntimeOperation updateStatusCode(Function<Integer, Integer> updater) {
      operations.add(
          () -> {
            OptimizingStatus originalStatus = OptimizingStatus.ofCode(oldMeta.getStatusCode());
            Integer originalStatusCode = oldMeta.getStatusCode();
            Integer newStatusCode = updater.apply(originalStatusCode);
            if (Objects.equals(originalStatusCode, newStatusCode)) {
              return;
            }
            oldMeta.setStatusCode(newStatusCode);
            handlerCallbacks.add(
                (handler, runtime) -> handler.handleTableChanged(runtime, originalStatus));
          });
      metaOperation = true;
      return this;
    }

    @Override
    public TableRuntimeOperation updateTableConfig(Consumer<Map<String, String>> updater) {
      operations.add(
          () -> {
            TableConfiguration oldConfiguration =
                TableConfigurations.parseTableConfig(oldMeta.getTableConfig());
            Map<String, String> tableConfig = oldMeta.getTableConfig();
            updater.accept(tableConfig);
            oldMeta.setTableConfig(tableConfig);
            handlerCallbacks.add(
                (handler, runtime) -> handler.handleTableChanged(runtime, oldConfiguration));
          });
      metaOperation = true;
      return this;
    }

    @Override
    public TableRuntimeOperation updateTableSummary(Consumer<TableSummary> updater) {
      operations.add(
          () -> {
            TableSummary summary = oldMeta.getTableSummary();
            updater.accept(summary);
            oldMeta.setTableSummary(summary);
          });
      metaOperation = true;
      return this;
    }

    @Override
    public void commit() {
      try (NestedSqlSession session = beginSession()) {
        try {
          tableLock.lock();
          initializeWorkingSnapshot();
          operations.forEach(Runnable::run);
          persist();
          session.commit();
          makeVisible();
        } catch (Throwable t) {
          if (t instanceof RuntimeStateConflictException) {
            LOG.debug("Version-fenced runtime state update conflicted: {}", t.getMessage());
          } else {
            LOG.error("failed to commit operations", t);
          }
          session.rollback();
          throw AmoroRuntimeException.wrap(t, PersistenceException::new);
        } finally {
          tableLock.unlock();
        }
      }
      // A handler may start its own persistence transaction (for example, strict Process close).
      // Invoke it only after the current NestedSqlSession is closed so its commit is not absorbed
      // as a nested no-op after this operation has already committed.
      dispatchOrDeferRuntimeHandlerCallback(runtimeHandlerCallback());
    }

    private void initializeWorkingSnapshot() {
      oldMeta = meta.copy();
      oldStates.clear();
      states.forEach((key, state) -> oldStates.put(key, state.getStateValue()));
      expectedVersions.clear();
      for (String key : optimisticKeys) {
        TableRuntimeState fresh =
            getAs(
                TableRuntimeMapper.class, mapper -> mapper.getState(tableIdentifier.getId(), key));
        Preconditions.checkNotNull(fresh, "TableRuntime state %s not initialized", key);
        oldStates.put(key, fresh.getStateValue());
        expectedVersions.put(key, fresh.getStateVersion());
      }
    }

    private void persist() {
      doAs(
          TableRuntimeMapper.class,
          mapper -> {
            if (metaOperation) {
              mapper.updateRuntime(oldMeta);
            }
            stateKeys.forEach(
                key -> {
                  String storedValue = oldStates.get(key);
                  if (optimisticKeys.contains(key)) {
                    int updated =
                        mapper.setStateValueIfVersion(
                            tableIdentifier.getId(), key, expectedVersions.get(key), storedValue);
                    if (updated != 1) {
                      throw new RuntimeStateConflictException(tableIdentifier.getId(), key);
                    }
                  } else {
                    mapper.setStateValue(tableIdentifier.getId(), key, storedValue);
                  }
                });
          });
    }

    private void makeVisible() {
      if (metaOperation) {
        meta.setGroupName(oldMeta.getGroupName());
        meta.setStatusCode(oldMeta.getStatusCode());
        meta.setStatusCodeUpdateTime(oldMeta.getStatusCodeUpdateTime());
        meta.setTableSummary(oldMeta.getTableSummary());
        meta.setTableConfig(oldMeta.getTableConfig());
      }
      stateKeys.forEach(
          key -> {
            TableRuntimeState state = states.get(key);
            state.setStateValue(oldStates.get(key));
            if (optimisticKeys.contains(key)) {
              state.setStateVersion(expectedVersions.get(key) + 1);
            }
          });
    }

    private Runnable runtimeHandlerCallback() {
      TableRuntimeHandler callbackHandler = runtimeHandler;
      TableRuntime callbackRuntime = tableRuntime;
      if (callbackHandler == null || callbackRuntime == null || handlerCallbacks.isEmpty()) {
        return null;
      }
      List<BiConsumer<TableRuntimeHandler, TableRuntime>> callbacks =
          ImmutableList.copyOf(handlerCallbacks);
      return () -> callbacks.forEach(callback -> callback.accept(callbackHandler, callbackRuntime));
    }
  }
}
