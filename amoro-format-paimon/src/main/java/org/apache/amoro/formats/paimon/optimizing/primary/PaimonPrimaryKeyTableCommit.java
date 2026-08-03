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

package org.apache.amoro.formats.paimon.optimizing.primary;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingCommitter;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class PaimonPrimaryKeyTableCommit implements TableOptimizingCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonPrimaryKeyTableCommit.class);

  private final PaimonTable paimonTable;
  private final FileStoreTable table;
  private final Collection<PaimonPrimaryKeyCompactionTask> successTasks;

  public PaimonPrimaryKeyTableCommit(
      FileStoreTable table, Collection<PaimonPrimaryKeyCompactionTask> successTasks) {
    this(null, table, successTasks);
  }

  public PaimonPrimaryKeyTableCommit(
      PaimonTable paimonTable,
      FileStoreTable table,
      Collection<PaimonPrimaryKeyCompactionTask> successTasks) {
    this.paimonTable = paimonTable;
    this.table =
        table == null
            ? null
            : table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
    this.successTasks = successTasks;
  }

  @Override
  public void commit() throws OptimizingCommitException {
    commit(CommitMode.NORMAL);
  }

  @Override
  public void commit(CommitMode mode) throws OptimizingCommitException {
    if (successTasks == null || successTasks.isEmpty()) {
      LOG.info("PaimonPrimaryKeyTableCommit: no success tasks for table={} - skip commit.", name());
      return;
    }
    if (table == null) {
      throw new OptimizingCommitException(
          "Paimon primary-key success tasks have no Paimon table", false);
    }

    CommitIdentity identity = extractCommitIdentity();
    List<CommitMessage> messages = collectCommitMessages();
    if (shouldSkipStaleFull(identity)) {
      LOG.info(
          "PaimonPrimaryKeyTableCommit: skip stale FULL commit for table={} targetSnapshotId={}.",
          name(),
          identity.targetSnapshotId);
      return;
    }
    if (messages.isEmpty()) {
      LOG.info(
          "PaimonPrimaryKeyTableCommit: empty CommitMessage list for table={} - skip commit.",
          name());
      return;
    }
    if (mode == null) {
      throw new OptimizingCommitException(
          "Paimon primary-key commit mode must not be null for table=" + name(), false);
    }

    try {
      if (paimonTable == null) {
        commitMessages(messages, identity.commitUser, identity.commitIdentifier, mode);
      } else {
        paimonTable.doAs(
            () -> {
              commitMessages(messages, identity.commitUser, identity.commitIdentifier, mode);
              return null;
            });
      }
    } catch (RuntimeException e) {
      if (e.getCause() instanceof OptimizingCommitException) {
        throw (OptimizingCommitException) e.getCause();
      }
      throw new OptimizingCommitException(
          "Paimon primary-key commit failed for table=" + name(), e);
    }
  }

  private List<CommitMessage> collectCommitMessages() throws OptimizingCommitException {
    List<CommitMessage> messages = new ArrayList<>();
    for (PaimonPrimaryKeyCompactionTask task : successTasks) {
      PaimonPrimaryKeyCompactionOutput output = task == null ? null : task.getOutput();
      if (output == null) {
        throw new OptimizingCommitException(
            "Paimon primary-key success task for partition "
                + partition(task)
                + " has no Paimon CommitMessage list",
            false);
      }
      List<byte[]> bytesList = output.getCommitMessageBytesList();
      if (bytesList.isEmpty()) {
        LOG.info(
            "PaimonPrimaryKeyTableCommit: success task for partition {} has empty CommitMessage "
                + "list - skip task.",
            partition(task));
        continue;
      }
      try {
        messages.addAll(CommitMessageSerializer.deserializeAll(bytesList));
      } catch (Exception e) {
        throw new OptimizingCommitException(
            "Failed to deserialize Paimon primary-key CommitMessage list for partition "
                + partition(task),
            e);
      }
    }
    return messages;
  }

  private CommitIdentity extractCommitIdentity() throws OptimizingCommitException {
    String commitUser = null;
    Long commitIdentifier = null;
    OptimizingType optimizingType = null;
    Long targetSnapshotId = null;
    for (PaimonPrimaryKeyCompactionTask task : successTasks) {
      PaimonPrimaryKeyCompactionInput input = task == null ? null : task.getInput();
      if (input == null) {
        throw new OptimizingCommitException(
            "Paimon primary-key success task for partition "
                + partition(task)
                + " has no PaimonPrimaryKeyCompactionInput",
            false);
      }
      String taskCommitUser = input.getCommitUser();
      if (taskCommitUser == null || taskCommitUser.isEmpty()) {
        throw new OptimizingCommitException(
            "Paimon primary-key success task for partition "
                + partition(task)
                + " has no commitUser",
            false);
      }
      long taskCommitIdentifier = input.getCommitIdentifier();
      if (taskCommitIdentifier <= 0L) {
        throw new OptimizingCommitException(
            "Paimon primary-key success task for partition "
                + partition(task)
                + " has invalid commitIdentifier "
                + taskCommitIdentifier,
            false);
      }
      OptimizingType taskOptimizingType = input.getOptimizingType();
      if (taskOptimizingType == null) {
        throw new OptimizingCommitException(
            "Paimon primary-key success task for partition "
                + partition(task)
                + " has no optimizingType",
            false);
      }
      long taskTargetSnapshotId = input.getTargetSnapshotId();
      if (taskTargetSnapshotId <= 0L) {
        throw new OptimizingCommitException(
            "Paimon primary-key success task for partition "
                + partition(task)
                + " has invalid targetSnapshotId "
                + taskTargetSnapshotId,
            false);
      }
      if (commitUser == null) {
        commitUser = taskCommitUser;
      } else if (!commitUser.equals(taskCommitUser)) {
        throw new OptimizingCommitException(
            "Paimon primary-key success tasks have inconsistent commitUser", false);
      }
      if (commitIdentifier == null) {
        commitIdentifier = taskCommitIdentifier;
      } else if (commitIdentifier.longValue() != taskCommitIdentifier) {
        throw new OptimizingCommitException(
            "Paimon primary-key success tasks have inconsistent commitIdentifier", false);
      }
      if (optimizingType == null) {
        optimizingType = taskOptimizingType;
      } else if (optimizingType != taskOptimizingType) {
        throw new OptimizingCommitException(
            "Paimon primary-key success tasks have inconsistent optimizingType", false);
      }
      if (targetSnapshotId == null) {
        targetSnapshotId = taskTargetSnapshotId;
      } else if (targetSnapshotId.longValue() != taskTargetSnapshotId) {
        throw new OptimizingCommitException(
            "Paimon primary-key success tasks have inconsistent targetSnapshotId", false);
      }
    }
    return new CommitIdentity(commitUser, commitIdentifier, optimizingType, targetSnapshotId);
  }

  private boolean shouldSkipStaleFull(CommitIdentity identity) {
    if (identity.optimizingType != OptimizingType.FULL) {
      return false;
    }
    Optional<Snapshot> latestSnapshot = table.latestSnapshot();
    return !latestSnapshot.isPresent() || latestSnapshot.get().id() != identity.targetSnapshotId;
  }

  void commitMessages(
      List<CommitMessage> messages, String commitUser, long commitIdentifier, CommitMode mode)
      throws OptimizingCommitException {
    if (mode == CommitMode.RECOVERY_REPLAY) {
      try {
        filterAndCommitOnce(messages, commitUser, commitIdentifier, mode, "recovery");
      } catch (Exception failure) {
        throw filterFailure(commitIdentifier, mode, "recovery", failure);
      }
      return;
    }

    long directStartNanos = System.nanoTime();
    LOG.info(
        "PaimonPrimaryKeyTableCommit: commit start mode={} api=commit stage=direct table={} "
            + "commitUser={} identifier={} messageCount={}",
        mode,
        name(),
        commitUser,
        commitIdentifier,
        messages.size());
    Exception directFailure;
    try {
      directCommitOnce(messages, commitUser, commitIdentifier);
      LOG.info(
          "PaimonPrimaryKeyTableCommit: commit success mode={} api=commit stage=direct table={} "
              + "commitUser={} identifier={} messageCount={} elapsedMs={}",
          mode,
          name(),
          commitUser,
          commitIdentifier,
          messages.size(),
          elapsedMillis(directStartNanos));
      return;
    } catch (Exception failure) {
      directFailure = failure;
      LOG.warn(
          "PaimonPrimaryKeyTableCommit: commit failed mode={} api=commit stage=direct table={} "
              + "commitUser={} identifier={} messageCount={} elapsedMs={}, "
              + "trying one idempotent fallback",
          mode,
          name(),
          commitUser,
          commitIdentifier,
          messages.size(),
          elapsedMillis(directStartNanos),
          failure);
    }

    try {
      filterAndCommitOnce(messages, commitUser, commitIdentifier, mode, "fallback");
    } catch (Exception fallbackFailure) {
      OptimizingCommitException finalFailure =
          new OptimizingCommitException(
              "Paimon primary-key direct commit and idempotent fallback failed for table="
                  + name()
                  + " identifier="
                  + commitIdentifier,
              fallbackFailure);
      finalFailure.addSuppressed(directFailure);
      throw finalFailure;
    }
  }

  private void directCommitOnce(
      List<CommitMessage> messages, String commitUser, long commitIdentifier) throws Exception {
    try (StreamTableCommit directCommit = table.newCommit(commitUser)) {
      directCommit.commit(commitIdentifier, messages);
    }
  }

  private int filterAndCommitOnce(
      List<CommitMessage> messages,
      String commitUser,
      long commitIdentifier,
      CommitMode mode,
      String stage)
      throws Exception {
    long startNanos = System.nanoTime();
    LOG.info(
        "PaimonPrimaryKeyTableCommit: commit start mode={} api=filterAndCommit stage={} table={} "
            + "commitUser={} identifier={} messageCount={}",
        mode,
        stage,
        name(),
        commitUser,
        commitIdentifier,
        messages.size());
    int committed;
    try (StreamTableCommit commit = table.newCommit(commitUser)) {
      committed = commit.filterAndCommit(Collections.singletonMap(commitIdentifier, messages));
    } catch (Exception failure) {
      LOG.warn(
          "PaimonPrimaryKeyTableCommit: commit failed mode={} api=filterAndCommit stage={} table={} "
              + "commitUser={} identifier={} messageCount={} elapsedMs={}",
          mode,
          stage,
          name(),
          commitUser,
          commitIdentifier,
          messages.size(),
          elapsedMillis(startNanos),
          failure);
      throw failure;
    }
    if (committed != 0 && committed != 1) {
      IllegalStateException failure =
          new IllegalStateException(
              "filterAndCommit returned "
                  + committed
                  + " for one commit identity; expected 0 or 1");
      LOG.warn(
          "PaimonPrimaryKeyTableCommit: commit failed mode={} api=filterAndCommit stage={} table={} "
              + "commitUser={} identifier={} messageCount={} elapsedMs={}",
          mode,
          stage,
          name(),
          commitUser,
          commitIdentifier,
          messages.size(),
          elapsedMillis(startNanos),
          failure);
      throw failure;
    }
    LOG.info(
        "PaimonPrimaryKeyTableCommit: commit success mode={} api=filterAndCommit stage={} table={} "
            + "commitUser={} identifier={} messageCount={} committedIdentifiers={} elapsedMs={}",
        mode,
        stage,
        name(),
        commitUser,
        commitIdentifier,
        messages.size(),
        committed,
        elapsedMillis(startNanos));
    return committed;
  }

  private OptimizingCommitException filterFailure(
      long commitIdentifier, CommitMode mode, String stage, Exception failure) {
    return new OptimizingCommitException(
        "Paimon primary-key filterAndCommit failed in mode="
            + mode
            + " stage="
            + stage
            + " for table="
            + name()
            + " identifier="
            + commitIdentifier
            + ": "
            + failure.getMessage(),
        failure);
  }

  private static long elapsedMillis(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }

  private String name() {
    return table == null ? "<unknown>" : table.name();
  }

  private static String partition(PaimonPrimaryKeyCompactionTask task) {
    return task == null ? "<null-task>" : task.getPartition();
  }

  private static class CommitIdentity {
    private final String commitUser;
    private final long commitIdentifier;
    private final OptimizingType optimizingType;
    private final long targetSnapshotId;

    private CommitIdentity(
        String commitUser,
        long commitIdentifier,
        OptimizingType optimizingType,
        long targetSnapshotId) {
      this.commitUser = commitUser;
      this.commitIdentifier = commitIdentifier;
      this.optimizingType = optimizingType;
      this.targetSnapshotId = targetSnapshotId;
    }
  }
}
