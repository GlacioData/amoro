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

package org.apache.amoro.formats.paimon.optimizing.commit;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionOutput;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionTask;
import org.apache.amoro.optimizing.TableOptimizingCommitter;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * AMS-side committer for Paimon BUCKET_UNAWARE compaction.
 *
 * <p>Deserialises every {@link CommitMessage} carried by {@link PaimonCompactionTask#getOutput()},
 * then performs a single atomic commit via Paimon's {@link
 * AppendOnlyFileStoreTable#newCommit(String)}. A normal first attempt uses {@link
 * StreamTableCommit#commit(long, List)} without scanning historical snapshots. Recovery replay, and
 * the single fallback after an ambiguous normal failure, use {@link
 * StreamTableCommit#filterAndCommit} to preserve idempotency for the same {@code (commitUser,
 * commitIdentifier)}.
 *
 * <p>The caller is expected to pass the persisted plan commit identifier from {@link
 * org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput#getCommitIdentifier()}.
 *
 * <p>Behaviour:
 *
 * <ul>
 *   <li>Empty task collection → no-op, no snapshot created.
 *   <li>Every success task must carry a {@link PaimonCompactionOutput} with non-null commit message
 *       bytes; missing bytes indicate corrupted task state and fail the commit.
 *   <li>Any runtime exception from Paimon's commit path (conflict, IO, schema drift, …) is wrapped
 *       in {@link OptimizingCommitException} so the AMS optimizer queue marks this process as
 *       failed and re-plans on the next tick.
 * </ul>
 */
public class PaimonTableCommit implements TableOptimizingCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonTableCommit.class);

  private final PaimonTable paimonTable;
  private final AppendOnlyFileStoreTable table;
  private final Collection<PaimonCompactionTask> successTasks;
  private final String commitUser;
  /**
   * Paimon commit identifier; must be monotonic per {@code commitUser} so that {@code
   * FileStoreCommitImpl.filterCommitted} can dedupe replayed commits. The caller (see {@code
   * PaimonProcessFactory.createCommitter}) validates that all success tasks carry the same
   * persisted value.
   */
  private final long commitIdentifier;

  public PaimonTableCommit(
      AppendOnlyFileStoreTable table,
      Collection<PaimonCompactionTask> successTasks,
      String commitUser,
      long commitIdentifier) {
    this(null, table, successTasks, commitUser, commitIdentifier);
  }

  public PaimonTableCommit(
      PaimonTable paimonTable,
      AppendOnlyFileStoreTable table,
      Collection<PaimonCompactionTask> successTasks,
      String commitUser,
      long commitIdentifier) {
    this.paimonTable = paimonTable;
    this.table = table;
    this.successTasks = successTasks;
    this.commitUser = commitUser;
    this.commitIdentifier = commitIdentifier;
  }

  @Override
  public void commit() throws OptimizingCommitException {
    commit(CommitMode.NORMAL);
  }

  @Override
  public void commit(CommitMode mode) throws OptimizingCommitException {
    if (successTasks == null || successTasks.isEmpty()) {
      LOG.info(
          "PaimonTableCommit: no success tasks for table={} commitUser={} - skip commit.",
          table.name(),
          commitUser);
      return;
    }

    List<CommitMessage> messages = new ArrayList<>(successTasks.size());
    CommitMessageSerializer serializer = new CommitMessageSerializer();
    for (PaimonCompactionTask task : successTasks) {
      PaimonCompactionOutput output = task.getOutput();
      if (output == null || output.getCommitMessageBytes() == null) {
        throw new OptimizingCommitException(
            "Paimon success task for partition "
                + task.getPartition()
                + " has no Paimon CommitMessage",
            /* causedByVersionMismatch */ false);
      }
      try {
        messages.add(
            serializer.deserialize(
                output.getCommitMessageVersion(), output.getCommitMessageBytes()));
      } catch (Exception e) {
        throw new OptimizingCommitException(
            "Failed to deserialize Paimon CommitMessage for partition " + task.getPartition(), e);
      }
    }
    if (messages.isEmpty()) {
      LOG.info(
          "PaimonTableCommit: empty CommitMessage list for table={} — skip commit.", table.name());
      return;
    }

    if (commitUser == null || commitUser.isEmpty()) {
      throw new OptimizingCommitException(
          "Paimon commit user must not be empty for table=" + table.name(),
          /* causedByVersionMismatch */ false);
    }
    if (commitIdentifier <= 0L) {
      throw new OptimizingCommitException(
          "Paimon commit identifier must be > 0, got "
              + commitIdentifier
              + " for table="
              + table.name(),
          /* causedByVersionMismatch */ false);
    }
    if (mode == null) {
      throw new OptimizingCommitException(
          "Paimon commit mode must not be null for table=" + table.name(), false);
    }
    try {
      if (paimonTable == null) {
        commitMessages(messages, mode);
      } else {
        paimonTable.doAs(
            () -> {
              commitMessages(messages, mode);
              return null;
            });
      }
    } catch (RuntimeException e) {
      if (e.getCause() instanceof OptimizingCommitException) {
        throw (OptimizingCommitException) e.getCause();
      }
      throw new OptimizingCommitException(
          "Paimon commit failed for table=" + table.name() + " identifier=" + commitIdentifier, e);
    }
  }

  void commitMessages(List<CommitMessage> messages, CommitMode mode)
      throws OptimizingCommitException {
    if (mode == CommitMode.RECOVERY_REPLAY) {
      try {
        filterAndCommitOnce(messages, mode, "recovery");
      } catch (Exception failure) {
        throw filterFailure(mode, "recovery", failure);
      }
      return;
    }

    long directStartNanos = System.nanoTime();
    LOG.info(
        "PaimonTableCommit: commit start mode={} api=commit stage=direct table={} "
            + "commitUser={} identifier={} messageCount={}",
        mode,
        table.name(),
        commitUser,
        commitIdentifier,
        messages.size());
    Exception directFailure;
    try {
      directCommitOnce(messages);
      LOG.info(
          "PaimonTableCommit: commit success mode={} api=commit stage=direct table={} "
              + "commitUser={} identifier={} messageCount={} elapsedMs={}",
          mode,
          table.name(),
          commitUser,
          commitIdentifier,
          messages.size(),
          elapsedMillis(directStartNanos));
      return;
    } catch (Exception failure) {
      directFailure = failure;
      LOG.warn(
          "PaimonTableCommit: commit failed mode={} api=commit stage=direct table={} "
              + "commitUser={} identifier={} messageCount={} elapsedMs={}, "
              + "trying one idempotent fallback",
          mode,
          table.name(),
          commitUser,
          commitIdentifier,
          messages.size(),
          elapsedMillis(directStartNanos),
          failure);
    }

    try {
      filterAndCommitOnce(messages, mode, "fallback");
    } catch (Exception fallbackFailure) {
      OptimizingCommitException finalFailure =
          new OptimizingCommitException(
              "Paimon commit failed: direct commit and idempotent fallback failed for table="
                  + table.name()
                  + " identifier="
                  + commitIdentifier,
              fallbackFailure);
      finalFailure.addSuppressed(directFailure);
      throw finalFailure;
    }
  }

  private void directCommitOnce(List<CommitMessage> messages) throws Exception {
    try (StreamTableCommit directCommit = table.newCommit(commitUser)) {
      directCommit.commit(commitIdentifier, messages);
    }
  }

  private int filterAndCommitOnce(List<CommitMessage> messages, CommitMode mode, String stage)
      throws Exception {
    long startNanos = System.nanoTime();
    LOG.info(
        "PaimonTableCommit: commit start mode={} api=filterAndCommit stage={} table={} "
            + "commitUser={} identifier={} messageCount={}",
        mode,
        stage,
        table.name(),
        commitUser,
        commitIdentifier,
        messages.size());
    int committed;
    try (StreamTableCommit commit = table.newCommit(commitUser)) {
      committed = commit.filterAndCommit(Collections.singletonMap(commitIdentifier, messages));
    } catch (Exception failure) {
      LOG.warn(
          "PaimonTableCommit: commit failed mode={} api=filterAndCommit stage={} table={} "
              + "commitUser={} identifier={} messageCount={} elapsedMs={}",
          mode,
          stage,
          table.name(),
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
          "PaimonTableCommit: commit failed mode={} api=filterAndCommit stage={} table={} "
              + "commitUser={} identifier={} messageCount={} elapsedMs={}",
          mode,
          stage,
          table.name(),
          commitUser,
          commitIdentifier,
          messages.size(),
          elapsedMillis(startNanos),
          failure);
      throw failure;
    }
    LOG.info(
        "PaimonTableCommit: commit success mode={} api=filterAndCommit stage={} table={} "
            + "commitUser={} identifier={} messageCount={} committedIdentifiers={} elapsedMs={}",
        mode,
        stage,
        table.name(),
        commitUser,
        commitIdentifier,
        messages.size(),
        committed,
        elapsedMillis(startNanos));
    return committed;
  }

  private OptimizingCommitException filterFailure(
      CommitMode mode, String stage, Exception failure) {
    return new OptimizingCommitException(
        "Paimon filterAndCommit failed in mode="
            + mode
            + " stage="
            + stage
            + " for table="
            + table.name()
            + " identifier="
            + commitIdentifier
            + ": "
            + failure.getMessage(),
        failure);
  }

  private static long elapsedMillis(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
  }
}
