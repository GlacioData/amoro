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
import org.apache.amoro.exception.OptimizingCommitSkippedException;
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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * AMS-side committer for Paimon primary-key table compaction.
 *
 * <p>A normal first attempt uses {@link StreamTableCommit#commit(long, List)}. Recovery replay and
 * the single fallback after an ambiguous normal failure use {@link
 * StreamTableCommit#filterAndCommit} with the same persisted {@code (commitUser,
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
 */
public class PaimonPrimaryKeyTableCommit implements TableOptimizingCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonPrimaryKeyTableCommit.class);

  static final int MAX_IDENTITY_SNAPSHOT_PROBES = 256;

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
  public ReconciliationResult reconcile() throws OptimizingCommitException {
    if (successTasks == null || successTasks.isEmpty()) {
      return ReconciliationResult.REPLAN;
    }
    CommitIdentity identity = extractCommitIdentity();
    if (table == null || identity.optimizingType != OptimizingType.FULL) {
      throw new OptimizingCommitException(
          "Read-only reconciliation requires a FULL table process", false);
    }
    // Paimon 1.4.2 StreamTableCommit#filterAndCommit publishes unfiltered messages and is not
    // read-only reconciliation.
    // FileStoreCommitImpl#filterCommitted returns all messages when history expires and cannot
    // prove non-publication.
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L246-L284
    try {
      FullStatus status =
          paimonTable == null
              ? inspectFull(identity)
              : paimonTable.doAs(() -> inspectFull(identity));
      return status == FullStatus.ALREADY_PUBLISHED
          ? ReconciliationResult.PUBLISHED
          : ReconciliationResult.REPLAN;
    } catch (RuntimeException failure) {
      throw new OptimizingCommitException(
          "Failed to reconcile Paimon FULL publication for table=" + name(), failure);
    }
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
    boolean skippedBeforeExecute = hasSkippedOutput(identity);
    if (mode == null) {
      throw new OptimizingCommitException(
          "Paimon primary-key commit mode must not be null for table=" + name(), false);
    }

    try {
      if (paimonTable == null) {
        commitInScope(messages, identity, mode, skippedBeforeExecute);
      } else {
        paimonTable.doAs(
            () -> {
              commitInScope(messages, identity, mode, skippedBeforeExecute);
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

  private void commitInScope(
      List<CommitMessage> messages,
      CommitIdentity identity,
      CommitMode mode,
      boolean skippedBeforeExecute)
      throws OptimizingCommitException {
    FullStatus fullStatus = inspectFull(identity);
    if (skippedBeforeExecute && fullStatus == FullStatus.ALREADY_PUBLISHED) {
      throw new OptimizingCommitException(
          "Paimon FULL output conflicts with its published identity: " + identity.commitIdentifier,
          false);
    }
    if (skippedBeforeExecute
        || (fullStatus != FullStatus.FRESH && fullStatus != FullStatus.ALREADY_PUBLISHED)) {
      throw staleFull(
          identity, skippedBeforeExecute ? "executor skipped a bucket" : fullStatus.name());
    }
    if (fullStatus == FullStatus.ALREADY_PUBLISHED) {
      return;
    }
    if (messages.isEmpty()) {
      LOG.info(
          "PaimonPrimaryKeyTableCommit: empty CommitMessage list for table={} - no-op.", name());
      return;
    }
    commitMessages(messages, identity.commitUser, identity.commitIdentifier, mode, identity);
  }

  private boolean hasSkippedOutput(CommitIdentity identity) throws OptimizingCommitException {
    boolean skipped = false;
    for (PaimonPrimaryKeyCompactionTask task : successTasks) {
      PaimonPrimaryKeyCompactionOutput output = task.getOutput();
      String reason = output.getSkipReason();
      if (reason == null) {
        continue;
      }
      if (!PaimonPrimaryKeyCompactionOutput.STALE_FULL_BEFORE_EXECUTE.equals(reason)
          || identity.optimizingType != OptimizingType.FULL
          || !output.getCommitMessageBytesList().isEmpty()) {
        throw new OptimizingCommitException(
            "Invalid Paimon primary-key skip output: " + reason, false);
      }
      skipped = true;
    }
    return skipped;
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

  /**
   * Paimon 1.4.2 FileStoreCommitImpl#filterCommitted uses the latest user's identifier as a
   * watermark, and returns unfiltered messages when the history has expired. Therefore it cannot
   * prove an old FULL was published. SnapshotManager#latestSnapshotOfUser stops at a missing
   * snapshot and propagates other I/O errors; retain that distinction with a bounded exact search.
   * Source: https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/
   * paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L246-L284 and
   * utils/SnapshotManager.java#L599-L629. No abort: native abort can delete compactAfter files
   * including level-up files (FileStoreCommitImpl#abort, L633), so cleanup belongs elsewhere.
   *
   * @return the freshness or bounded exact-identity result; never publishes messages
   */
  private FullStatus inspectFull(CommitIdentity identity) {
    if (identity == null || identity.optimizingType != OptimizingType.FULL) {
      return FullStatus.FRESH;
    }
    Optional<Snapshot> latest = table.latestSnapshot();
    if (latest.isPresent() && latest.get().id() == identity.targetSnapshotId) {
      return FullStatus.FRESH;
    }
    long latestId = latest.map(Snapshot::id).orElse(0L);
    int probes = 0;
    FullStatus result = FullStatus.NOT_FOUND;
    for (long id = latestId;
        id > identity.targetSnapshotId && probes < MAX_IDENTITY_SNAPSHOT_PROBES;
        id--) {
      probes++;
      Snapshot snapshot;
      try {
        snapshot = id == latestId ? latest.get() : table.snapshotManager().tryGetSnapshot(id);
      } catch (FileNotFoundException expired) {
        result = FullStatus.EXPIRED;
        break;
      }
      if (identity.commitUser.equals(snapshot.commitUser())
          && identity.commitIdentifier == snapshot.commitIdentifier()) {
        LOG.info(
            "Paimon FULL already published: table={} user={} identifier={} snapshot={} probes={}",
            name(),
            identity.commitUser,
            identity.commitIdentifier,
            id,
            probes);
        return FullStatus.ALREADY_PUBLISHED;
      }
    }
    if (probes == MAX_IDENTITY_SNAPSHOT_PROBES) {
      result = FullStatus.BUDGET_EXHAUSTED;
    }
    LOG.info(
        "Paimon FULL stale: table={} user={} identifier={} target={} latest={} probes={} result={}",
        name(),
        identity.commitUser,
        identity.commitIdentifier,
        identity.targetSnapshotId,
        latestId,
        probes,
        result);
    return result;
  }

  private enum FullStatus {
    FRESH,
    ALREADY_PUBLISHED,
    NOT_FOUND,
    EXPIRED,
    BUDGET_EXHAUSTED
  }

  private OptimizingCommitSkippedException staleFull(CommitIdentity identity, String detail) {
    return new OptimizingCommitSkippedException(
        OptimizingCommitSkippedException.Reason.STALE_FULL,
        "Paimon FULL skipped: table="
            + name()
            + " target="
            + identity.targetSnapshotId
            + " identifier="
            + identity.commitIdentifier
            + " reason="
            + detail);
  }

  void commitMessages(
      List<CommitMessage> messages, String commitUser, long commitIdentifier, CommitMode mode)
      throws OptimizingCommitException {
    commitMessages(messages, commitUser, commitIdentifier, mode, null);
  }

  private void commitMessages(
      List<CommitMessage> messages,
      String commitUser,
      long commitIdentifier,
      CommitMode mode,
      CommitIdentity identity)
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

    // A direct attempt may publish and then fail while closing, or race with another writer.
    // Recheck under the same doAs before allowing native fallback to publish anything.
    try {
      FullStatus fullStatus = inspectFull(identity);
      if (fullStatus == FullStatus.ALREADY_PUBLISHED) {
        return;
      }
      if (fullStatus != FullStatus.FRESH) {
        throw staleFull(identity, fullStatus.name());
      }
    } catch (OptimizingCommitSkippedException skipped) {
      OptimizingCommitSkippedException unresolved =
          new OptimizingCommitSkippedException(skipped.getReason(), skipped.getMessage(), true);
      unresolved.addSuppressed(directFailure);
      throw unresolved;
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
