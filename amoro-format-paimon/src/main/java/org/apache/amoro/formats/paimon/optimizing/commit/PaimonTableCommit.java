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
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendCapabilities;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionOutput;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionTask;
import org.apache.amoro.formats.paimon.optimizing.PaimonFixedBucketCommitGuard;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * AMS-side committer for Paimon Append compaction.
 *
 * <p>Deserialises every {@link CommitMessage} carried by {@link PaimonCompactionTask#getOutput()},
 * then performs a single atomic commit via Paimon's {@link
 * AppendOnlyFileStoreTable#newCommit(String)}. A normal first attempt uses {@link
 * StreamTableCommit#commit(long, List)} without scanning historical snapshots. Recovery replay, and
 * the single fallback after an ambiguous normal failure, use {@link
 * StreamTableCommit#filterAndCommit} to preserve idempotency for the same {@code (commitUser,
 * commitIdentifier)}.
 *
 * <p>This distinction is pinned to Apache Paimon 1.4.2 at immutable commit {@code
 * ac223f47d916f4c8e9c07fd1745db448e4e7d0d8}: {@code StreamTableCommit.commit} does not check
 * whether an identifier was already committed (<a
 * href="https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableCommit.java#L52-L76">source</a>).
 * Paimon's {@code StreamWriteBuilder} requires a stable {@code commitUser}, matching write/commit
 * identifiers, and {@code filterAndCommit} for recovery (<a
 * href="https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilder.java#L27-L37">source</a>).
 * In contrast, {@code filterAndCommit} filters committed identifiers before committing the
 * remainder (<a
 * href="https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L277-L290">source</a>).
 * The filter reads the latest snapshot for the same {@code commitUser} and compares the {@code
 * commitIdentifier} (<a
 * href="https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L246-L284">source</a>).
 * Recheck these contracts before upgrading {@code paimon.version}; a successful normal commit must
 * not be replayed through another direct commit.
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

    // SUCCESS recovery bypasses the executor, so persisted outputs need the same capability
    // fence before either native commit API can publish a snapshot.
    Optional<String> rejection =
        PaimonAppendCapabilities.dataEvolutionRejection(table.coreOptions());
    if (rejection.isPresent()) {
      throw new OptimizingCommitException(rejection.get(), false);
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
    PaimonFixedBucketCommitGuard guard =
        PaimonFixedBucketCommitGuard.validate(
            table, successTasks, messages, commitUser, commitIdentifier);
    if (mode == CommitMode.RECOVERY_REPLAY) {
      try {
        filterAndCommitOnce(messages, mode, "recovery", guard);
      } catch (Exception failure) {
        throw filterFailure(mode, "recovery", failure);
      }
      return;
    }

    if (guard != null) {
      guard.validateUncommitted();
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
      directCommitOnce(messages, guard);
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
      filterAndCommitOnce(messages, mode, "fallback", guard);
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

  private void directCommitOnce(List<CommitMessage> messages, PaimonFixedBucketCommitGuard guard)
      throws Exception {
    AppendOnlyFileStoreTable bound = guard == null ? table : guard.table();
    try (StreamTableCommit directCommit = bound.newCommit(commitUser)) {
      directCommit.commit(commitIdentifier, messages);
    }
  }

  private int filterAndCommitOnce(
      List<CommitMessage> messages,
      CommitMode mode,
      String stage,
      PaimonFixedBucketCommitGuard guard)
      throws Exception {
    boolean alreadyProven = guard != null && guard.validateReplay(commitUser, commitIdentifier);
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
    AppendOnlyFileStoreTable bound = guard == null ? table : guard.table();
    try (StreamTableCommit commit = bound.newCommit(commitUser)) {
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
    // Paimon ac223f47, operation/FileStoreCommitImpl.java:246-284 can filter a concurrent
    // same-identity commit after our preflight. A zero result alone does not prove its scope.
    if (guard != null
        && committed == 0
        && !alreadyProven
        && !guard.validateReplay(commitUser, commitIdentifier)) {
      throw new IllegalStateException("Fixed commit was filtered without matching delta evidence");
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
