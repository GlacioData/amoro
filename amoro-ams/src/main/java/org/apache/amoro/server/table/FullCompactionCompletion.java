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

import org.apache.amoro.exception.PersistenceException;
import org.apache.amoro.optimizing.TableRuntimeOptimizingState;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.NestedSqlSession;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.server.process.TableProcessMeta;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/** Persists a FULL skip without using the ordinary failed-completion watermarks. */
public class FullCompactionCompletion extends PersistentBase {
  public static final String REASON_KEY = "amoro.commit.skip-reason";
  public static final String TIME_KEY = "amoro.commit.skip-time-ms";
  public static final String REASON = "STALE_FULL";

  private final Consumer<String> checkpoint;

  public FullCompactionCompletion() {
    this(stage -> {});
  }

  @VisibleForTesting
  FullCompactionCompletion(Consumer<String> checkpoint) {
    this.checkpoint = checkpoint;
  }

  /** Frozen once per in-memory skip; persisted decisions are always read back on retry. */
  public static final class Decision {
    private final long time;
    private final long initialDelay;
    private final long maxDelay;
    private final String reason;

    public Decision(long time, long initialDelay, long maxDelay) {
      require(time > 0 && initialDelay > 0 && maxDelay >= initialDelay, "Invalid skip decision");
      this.reason = REASON;
      this.time = time;
      this.initialDelay = initialDelay;
      this.maxDelay = maxDelay;
    }

    private Decision(long time, String reason) {
      require(time > 0, "Invalid completion time");
      this.time = time;
      this.initialDelay = 0;
      this.maxDelay = 0;
      this.reason = reason;
    }

    public static Decision replan(long time, boolean published) {
      return new Decision(time, published ? "REPLAN_PUBLISHED" : "REPLAN");
    }

    public long getTime() {
      return time;
    }
  }

  public static boolean isSkipped(TableProcessMeta process) {
    return process != null
        && process.getStatus() == ProcessStatus.CLOSED
        && "FULL".equals(process.getProcessType())
        && "AMORO".equals(process.getExecutionEngine())
        && "committing".equals(process.getProcessStage())
        && process.getProcessParameters() != null
        && (REASON.equals(process.getProcessParameters().get(REASON_KEY))
            || "REPLAN".equals(process.getProcessParameters().get(REASON_KEY))
            || "REPLAN_PUBLISHED".equals(process.getProcessParameters().get(REASON_KEY)));
  }

  public static boolean isTerminal(TableProcessMeta process) {
    if (process == null
        || !"FULL".equals(process.getProcessType())
        || !"AMORO".equals(process.getExecutionEngine())) {
      return false;
    }
    switch (process.getStatus()) {
      case SUCCESS:
      case FAILED:
      case CLOSED:
      case KILLED:
      case CANCELED:
        return true;
      default:
        return false;
    }
  }

  /** Caller holds the process lock; the store serializes local runtime publication. */
  public void complete(
      DefaultTableRuntimeStore store,
      long processId,
      Decision decision,
      Map<String, String> summary) {
    require(!NestedSqlSession.hasActiveSession(), "FULL skip requires a top-level transaction");
    store.synchronizedInvoke(
        () -> {
          try {
            completeLocked(store, processId, decision, summary);
          } catch (RuntimeException failure) {
            throw failure instanceof PersistenceException
                ? failure
                : new PersistenceException("Failed to complete FULL skip " + processId, failure);
          }
        });
  }

  private void completeLocked(
      DefaultTableRuntimeStore store,
      long processId,
      Decision decision,
      Map<String, String> summary) {
    long tableId = store.getTableIdentifier().getId();
    TableRuntimeMeta committedMeta = null;
    TableRuntimeState committedState = null;
    TableRuntimeState committedOwner = null;
    try (NestedSqlSession session = beginSession()) {
      try {
        TableRuntimeMapper runtimes = getMapper(session, TableRuntimeMapper.class);
        TableProcessMapper processes = getMapper(session, TableProcessMapper.class);
        TableProcessMeta process = processes.getProcessMeta(processId);
        TableRuntimeState owner =
            runtimes.getState(tableId, DefaultTableRuntime.PROCESS_ID_KEY.getKey());
        TableRuntimeState state =
            runtimes.getState(tableId, DefaultTableRuntime.OPTIMIZING_STATE_KEY.getKey());
        TableRuntimeMeta meta = runtimes.selectRuntime(tableId);
        require(
            process != null
                && process.getTableId() == tableId
                && meta != null
                && owner != null
                && state != null,
            "Missing FULL skip persistence state");
        long ownerId = Long.parseLong(owner.getStateValue());
        TableRuntimeOptimizingState optimizing =
            DefaultTableRuntime.OPTIMIZING_STATE_KEY.deserialize(state.getStateValue());
        checkpoint.accept("read");
        if (decision == null && isTerminal(process) && !isSkipped(process)) {
          require(ownerId != processId, "Completed process still owns runtime");
          if (ownerId == 0L) {
            require(
                meta.getStatusCode() == OptimizingStatus.IDLE.getCode(),
                "Completed runtime is not idle");
            committedMeta = meta;
            committedState = state;
            committedOwner = owner;
          }
        } else if (isSkipped(process)) {
          require(
              Long.parseLong(process.getProcessParameters().get(TIME_KEY)) > 0,
              "Invalid persisted FULL skip time");
          require(ownerId != processId, "Closed FULL skip still owns runtime");
          if (ownerId == 0L) {
            require(
                optimizing.getLastFullStaleProcessId() == processId,
                "Closed FULL skip has inconsistent retry state");
            require(
                meta.getStatusCode() == OptimizingStatus.IDLE.getCode(),
                "Closed FULL skip runtime is not idle");
            committedMeta = meta;
            committedState = state;
            committedOwner = owner;
          }
        } else {
          require(decision != null, "Missing frozen FULL skip decision");
          require(ownerId == processId, "FULL skip owner changed");
          require(
              process.getStatus() == ProcessStatus.RUNNING
                  && "FULL".equals(process.getProcessType())
                  && "AMORO".equals(process.getExecutionEngine())
                  && validActiveStage(process.getProcessStage())
                  && (!REASON.equals(decision.reason)
                      || meta.getStatusCode() == OptimizingStatus.COMMITTING.getCode()),
              "FULL skip process is not committing");
          require(
              optimizing.getLastFullStaleProcessId() != processId,
              "Active process has a persisted FULL skip marker");
          if (REASON.equals(decision.reason)) {
            advanceRetry(optimizing, processId, decision);
          } else {
            optimizing.setFullRetryNotBefore(0L);
            optimizing.setFullStaleSkipCount(0L);
            optimizing.setLastFullStaleProcessId(processId);
          }
          Map<String, String> parameters = new HashMap<>();
          if (process.getProcessParameters() != null) {
            parameters.putAll(process.getProcessParameters());
          }
          parameters.put(REASON_KEY, decision.reason);
          parameters.put(TIME_KEY, Long.toString(decision.time));
          Map<String, String> mergedSummary = new HashMap<>();
          if (process.getSummary() != null) {
            mergedSummary.putAll(process.getSummary());
          }
          if (summary != null) {
            mergedSummary.putAll(summary);
          }
          require(
              processes.closeSkippedFull(
                      tableId,
                      processId,
                      process.getProcessStage(),
                      decision.time,
                      decision.reason,
                      parameters,
                      mergedSummary)
                  == 1,
              "FULL skip process changed");
          checkpoint.accept("process");
          String value = DefaultTableRuntime.OPTIMIZING_STATE_KEY.serialize(optimizing);
          require(
              runtimes.setStateValueIfVersion(
                      tableId, state.getStateKey(), state.getStateVersion(), value)
                  == 1,
              "FULL skip state changed");
          state.setStateValue(value);
          state.setStateVersion(state.getStateVersion() + 1);
          checkpoint.accept("state");
          int previous = meta.getStatusCode();
          meta.setStatusCode(OptimizingStatus.IDLE.getCode());
          meta.getTableSummary().setPendingFileCount(0);
          meta.getTableSummary().setPendingFileSize(0L);
          require(
              runtimes.completeSkippedRuntime(meta, previous) == 1, "FULL skip runtime changed");
          checkpoint.accept("runtime");
          require(
              runtimes.setStateValueIfVersion(
                      tableId, owner.getStateKey(), owner.getStateVersion(), "0")
                  == 1,
              "FULL skip owner changed");
          owner.setStateValue("0");
          owner.setStateVersion(owner.getStateVersion() + 1);
          checkpoint.accept("owner");
          committedMeta = meta;
          committedState = state;
          committedOwner = owner;
        }
        session.commit();
      } catch (Throwable failure) {
        session.rollback();
        throw failure;
      }
    }
    // A failure here is an ambiguous response: retry reads CLOSED + the stored deadline above.
    checkpoint.accept("committed");
    if (committedMeta != null) {
      TableRuntimeState currentOwner =
          getAs(
              TableRuntimeMapper.class,
              mapper -> mapper.getState(tableId, DefaultTableRuntime.PROCESS_ID_KEY.getKey()));
      if (currentOwner != null
          && "0".equals(currentOwner.getStateValue())
          && currentOwner.getStateVersion() == committedOwner.getStateVersion()) {
        store.publishSkippedCompletion(
            committedMeta, Arrays.asList(committedState, committedOwner));
      }
    }
  }

  private static boolean validActiveStage(String stage) {
    // Creation persists the pre-beginProcess stage; beginCommitting does not update this row.
    return "pending".equals(stage)
        || "planning".equals(stage)
        || "full_optimizing".equals(stage)
        || "committing".equals(stage);
  }

  @VisibleForTesting
  static void advanceRetry(TableRuntimeOptimizingState state, long processId, Decision decision) {
    require(state.getFullStaleSkipCount() >= 0L, "Negative FULL skip count");
    long count =
        state.getFullStaleSkipCount() == Long.MAX_VALUE
            ? Long.MAX_VALUE
            : state.getFullStaleSkipCount() + 1L;
    long delay = decision.initialDelay;
    for (int i = 1; i < 64 && i < count && delay < decision.maxDelay; i++) {
      delay = delay > decision.maxDelay / 2L ? decision.maxDelay : delay * 2L;
    }
    state.setFullStaleSkipCount(count);
    state.setFullRetryNotBefore(
        decision.time > Long.MAX_VALUE - delay ? Long.MAX_VALUE : decision.time + delay);
    state.setLastFullStaleProcessId(processId);
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new PersistenceException(message);
    }
  }
}
