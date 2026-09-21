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

package org.apache.amoro.formats.paimon.optimizing.health;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.exception.RuntimeStateConflictException;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthComparisonState.Entry;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionInput;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.process.StagedTaskDescriptor;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableRuntimeStore;
import org.apache.amoro.table.descriptor.TableHealthComparisonView;
import org.apache.amoro.table.descriptor.TableHealthDetailsView;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableFileStatisticsSnapshot;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.table.health.TableHealthSupport;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/** Read-only commit observation and background comparison; never invoked from the commit lock. */
public final class PaimonHealthSupport implements TableHealthSupport {
  private static final Logger LOG = LoggerFactory.getLogger(PaimonHealthSupport.class);

  @Override
  public Optional<TableFileStatisticsSnapshot> extractFileStatistics(TableHealthDetails details) {
    if (details == null
        || details.getStructure() == null
        || details.getSchemaId() == null
        || (details.getSnapshotId() == null
            && !"COMPLETE".equals(details.getStructure().getAvailability()))) {
      return Optional.empty();
    }
    try {
      return Optional.of(
          new TableFileStatisticsSnapshot(
              details.getSnapshotId(),
              details.getSchemaId(),
              Long.parseLong(details.getMetrics().get("totalFileCount")),
              Long.parseLong(details.getMetrics().get("totalFileSize")),
              details.getStructure().getEvaluatedAtMillis()));
    } catch (IllegalArgumentException invalidStatistics) {
      return Optional.empty();
    }
  }

  @Override
  public boolean enabled() {
    return true;
  }

  @Override
  public List<StateKey<?>> requiredStates() {
    return Collections.singletonList(PaimonHealthComparisonState.KEY);
  }

  @Override
  public boolean sameInput(TableAnalysisKey key, TableHealthDetails details) {
    // Optimization scheduling baselines do not affect structural scores.
    return Objects.equals(
            details.getSnapshotId() == null ? -1L : details.getSnapshotId(), key.getSnapshotId())
        && Objects.equals(
            details.getChangeSnapshotId() == null ? -1L : details.getChangeSnapshotId(),
            key.getChangeSnapshotId())
        && Objects.equals(
            details.getSchemaId() == null ? -1L : details.getSchemaId(), key.getSchemaId())
        && details.getFormulaVersion().equals(key.getFormulaVersion())
        && PaimonHealthEvaluationContext.healthIdentity(details.getScoringConfigFingerprint())
            .equals(
                PaimonHealthEvaluationContext.healthIdentity(key.getScoringConfigFingerprint()));
  }

  @Override
  public boolean retryable(TableHealthDetails details) {
    return details.getReasonCodes().contains("SNAPSHOT_SCAN_FAILED")
        || details.getReasonCodes().contains("DELETE_METADATA_READ_FAILED");
  }

  @Override
  public String readFailureReason() {
    return "SNAPSHOT_SCAN_FAILED";
  }

  /** Capture only already available plan inputs. Failure cannot change planning or data commits. */
  public void capture(
      TableRuntimeStore store,
      AmoroTable<?> table,
      OptimizingPlanResult<?> plan,
      FormatTableAnalysis analysis) {
    if (!(table instanceof PaimonTable)) {
      return;
    }
    try {
      if (store.getState(PaimonHealthComparisonState.KEY).getVersion() != 1) {
        return;
      }
      Entry entry = new Entry();
      entry.setProcessId(plan.getProcessId());
      entry.setInputSnapshotId(plan.getTargetSnapshotId());
      entry.setOptimizingType(plan.getOptimizingType().name());
      entry.setSchemaId(((FileStoreTable) table.originalTable()).schema().id());
      for (StagedTaskDescriptor<?, ?, ?> task : plan.getTasks()) {
        Object input = task.getInput();
        String user;
        long identifier;
        if (input instanceof PaimonCompactionInput) {
          user = ((PaimonCompactionInput) input).getCommitUser();
          identifier = ((PaimonCompactionInput) input).getCommitIdentifier();
        } else if (input instanceof PaimonPrimaryKeyCompactionInput) {
          user = ((PaimonPrimaryKeyCompactionInput) input).getCommitUser();
          identifier = ((PaimonPrimaryKeyCompactionInput) input).getCommitIdentifier();
        } else {
          throw new IllegalArgumentException("Unsupported comparison task input");
        }
        if (user == null
            || (entry.getCommitUser() != null
                && (!user.equals(entry.getCommitUser())
                    || identifier != entry.getCommitIdentifier()))) {
          throw new IllegalArgumentException("Inconsistent comparison commit identity");
        }
        entry.setCommitUser(user);
        entry.setCommitIdentifier(identifier);
      }
      if (analysis != null && analysis.key().getSnapshotId() == plan.getTargetSnapshotId()) {
        entry.setBefore(analysis.healthDetails());
        entry.setBeforeScore(analysis.pendingInput().getHealthScore());
      }
      savePending(store, entry);
    } catch (RuntimeException e) {
      LOG.warn("Unable to capture Paimon health comparison for process {}", plan.getProcessId(), e);
    }
  }

  void savePending(TableRuntimeStore store, Entry entry) {
    updateState(
        store,
        state -> {
          if (state.getPending() == null
              || state.getPending().getProcessId() < entry.getProcessId()) {
            state.setPending(entry);
          }
        });
  }

  /** Retry only these pure comparison-state merges, never arbitrary runtime operations or scans. */
  private static void updateState(
      TableRuntimeStore store, Consumer<PaimonHealthComparisonState> updater) {
    for (int attempt = 0; ; attempt++) {
      try {
        store
            .begin()
            .updateState(
                PaimonHealthComparisonState.KEY,
                state -> {
                  requireSupportedVersion(state);
                  updater.accept(state);
                  return state;
                })
            .commit();
        return;
      } catch (RuntimeStateConflictException conflict) {
        if (attempt >= 2) {
          throw conflict;
        }
      }
    }
  }

  @Override
  public void refreshComparison(
      TableRuntimeStore store,
      AmoroTable<?> table,
      TableHealthDetails currentDetails,
      int currentScore) {
    PaimonHealthComparisonState state = store.getState(PaimonHealthComparisonState.KEY);
    if (state.getVersion() != 1) {
      return;
    }
    Entry entry = state.getLatest();
    if (entry == null
        || !"PREPARING".equals(entry.getStatus())
        || entry.getRetryAtMillis() > System.currentTimeMillis()) {
      return;
    }
    ((PaimonTable) table)
        .doAs(
            () -> {
              build(
                  entry,
                  (FileStoreTable) table.originalTable(),
                  table.id().toString(),
                  currentDetails,
                  currentScore);
              return null;
            });
    saveLatest(store, entry);
  }

  @Override
  public void observeSuccess(
      TableRuntimeStore store, long id, String processType, long completedAtMillis) {
    PaimonHealthComparisonState current = store.getState(PaimonHealthComparisonState.KEY);
    if (current.getVersion() != 1
        || (current.getLatest() != null && current.getLatest().getProcessId() >= id)) {
      return;
    }
    updateState(
        store,
        state -> {
          if (state.getLatest() != null && state.getLatest().getProcessId() >= id) {
            return;
          }
          // Resolve pending input from the fresh persisted state, not from a previous owner's
          // cache.
          Entry entry = state.getPending();
          if (entry == null || entry.getProcessId() != id) {
            entry = new Entry();
            entry.setProcessId(id);
            entry.setStatus("UNAVAILABLE");
            entry.setReason("MISSING_INPUT");
          }
          entry.setCompletedAtMillis(completedAtMillis);
          entry.setOptimizingType(processType);
          state.setLatest(entry);
        });
  }

  void saveLatest(TableRuntimeStore store, Entry entry) {
    updateState(
        store,
        state -> {
          if (state.getLatest() == null
              || state.getLatest().getProcessId() < entry.getProcessId()
              || (state.getLatest().getProcessId() == entry.getProcessId()
                  && "PREPARING".equals(state.getLatest().getStatus())
                  && state.getLatest().getAttempts() <= entry.getAttempts())) {
            state.setLatest(entry);
          }
        });
  }

  private static void requireSupportedVersion(PaimonHealthComparisonState state) {
    if (state.getVersion() != 1) {
      throw new IllegalStateException(
          "Unsupported health comparison state version: " + state.getVersion());
    }
  }

  static void build(Entry entry, FileStoreTable table, String tableId) {
    build(entry, table, tableId, null, -1);
  }

  static void build(
      Entry entry,
      FileStoreTable table,
      String tableId,
      TableHealthDetails currentDetails,
      int currentScore) {
    try {
      entry.setAttempts(entry.getAttempts() + 1);
      if (entry.getCommitUser() == null) {
        unavailable(entry, "MISSING_INPUT");
        return;
      }
      PaimonHistoricalHealth.CommitMatch match =
          PaimonHistoricalHealth.findCommit(
              table,
              entry.getInputSnapshotId(),
              entry.getCommitUser(),
              entry.getCommitIdentifier());
      if (match.snapshot() == null) {
        unavailable(entry, match.status());
        return;
      }
      Snapshot after = match.snapshot();
      if (!table.snapshotManager().snapshotExists(entry.getInputSnapshotId())) {
        unavailable(entry, "SNAPSHOT_EXPIRED");
        return;
      }
      Snapshot before = table.snapshotManager().snapshot(entry.getInputSnapshotId());
      if (before.schemaId() != entry.getSchemaId() || after.schemaId() != entry.getSchemaId()) {
        unavailable(entry, "CONFIGURATION_CHANGED");
        return;
      }
      // Reuse bounded facts only when snapshot, schema, formula and scoring options all match.
      if (!reusable(entry.getBefore(), entry.getBeforeScore(), table, tableId, before)) {
        FormatTableAnalysis left = PaimonHistoricalHealth.evaluate(table, tableId, before.id());
        entry.setBefore(left.healthDetails());
        entry.setBeforeScore(left.pendingInput().getHealthScore());
      }
      if (currentDetails != null && reusable(currentDetails, currentScore, table, tableId, after)) {
        entry.setAfter(currentDetails);
        entry.setAfterScore(currentScore);
      } else {
        FormatTableAnalysis right = PaimonHistoricalHealth.evaluate(table, tableId, after.id());
        entry.setAfter(right.healthDetails());
        entry.setAfterScore(right.pendingInput().getHealthScore());
      }
      entry.setConcurrentChanges(after.id() != before.id() + 1);
      if (!Objects.equals(
              entry.getBefore().getFormulaVersion(), entry.getAfter().getFormulaVersion())
          || !Objects.equals(
              PaimonHealthEvaluationContext.healthIdentity(
                  entry.getBefore().getScoringConfigFingerprint()),
              PaimonHealthEvaluationContext.healthIdentity(
                  entry.getAfter().getScoringConfigFingerprint()))) {
        unavailable(entry, "CONFIGURATION_CHANGED");
        return;
      }
      if (entry.getBeforeScore() < 0 || entry.getAfterScore() < 0) {
        throw new IllegalStateException("Historical health inputs are incomplete");
      }
      entry.setStatus("READY");
      entry.setReason("OBSERVED_STRUCTURE");
    } catch (RuntimeException e) {
      entry.setReason("READ_FAILED");
      entry.setRetryAtMillis(System.currentTimeMillis() + 30000L);
      if (entry.getAttempts() >= 3) {
        entry.setStatus("UNAVAILABLE");
      }
      LOG.warn(
          "Historical Paimon health evaluation failed for process {}", entry.getProcessId(), e);
    }
  }

  static boolean reusable(
      TableHealthDetails details,
      int score,
      FileStoreTable table,
      String tableId,
      Snapshot snapshot) {
    if (details == null
        || score < 0
        || score > 100
        || details.getStructure() == null
        || !Objects.equals(details.getSnapshotId(), snapshot.id())
        || !Objects.equals(details.getSchemaId(), snapshot.schemaId())) {
      return false;
    }
    FileStoreTable historical = table.copy(table.schemaManager().schema(snapshot.schemaId()));
    TableAnalysisKey key =
        PaimonHealthEvaluationContext.forSnapshot(historical, tableId, snapshot).key();
    return key.getFormulaVersion().equals(details.getFormulaVersion())
        && PaimonHealthEvaluationContext.healthIdentity(key.getScoringConfigFingerprint())
            .equals(
                PaimonHealthEvaluationContext.healthIdentity(
                    details.getScoringConfigFingerprint()));
  }

  private static void unavailable(Entry entry, String reason) {
    entry.setStatus("UNAVAILABLE");
    entry.setReason(reason);
  }

  public TableHealthComparisonView comparisonView(TableRuntimeStore store) {
    try {
      PaimonHealthComparisonState state = store.getState(PaimonHealthComparisonState.KEY);
      if (state.getVersion() != 1) {
        return null;
      }
      Entry entry = state.getLatest();
      if (entry == null) {
        return null;
      }
      return new TableHealthComparisonView(
          Long.toString(entry.getProcessId()),
          entry.getOptimizingType(),
          entry.getStatus(),
          entry.getReason(),
          entry.getConcurrentChanges(),
          entry.getBeforeScore(),
          entry.getAfterScore(),
          TableHealthDetailsView.from(entry.getBefore()),
          TableHealthDetailsView.from(entry.getAfter()),
          entry.getCompletedAtMillis());
    } catch (RuntimeException e) {
      return null;
    }
  }
}
