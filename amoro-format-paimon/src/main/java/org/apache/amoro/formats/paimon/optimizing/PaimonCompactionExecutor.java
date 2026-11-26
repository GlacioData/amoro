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

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.amoro.optimizing.OptimizingExecutor;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.AppendCompactTaskSerializer;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Optimizer-side executor for ordinary Paimon Append compaction in either bucket mode.
 *
 * <p>Reuses the Paimon-native path: {@link AppendCompactTask#doCompact} internally calls {@code
 * BaseAppendFileStoreWrite#compactRewrite} and returns a ready-to-commit {@link CommitMessage}.
 * This executor only does the plumbing: deserialize the task, unwrap the table from {@link
 * PaimonCompactionInput#getTable()} (shipped from AMS à la Iceberg {@code RewriteFilesInput}),
 * invoke {@code doCompact}, then serialize the output via {@link CommitMessageSerializer} for the
 * AMS-side Committer to re-deserialize and commit.
 */
public class PaimonCompactionExecutor implements OptimizingExecutor<PaimonCompactionOutput> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(PaimonCompactionExecutor.class);

  private final PaimonCompactionInput input;

  public PaimonCompactionExecutor(PaimonCompactionInput input) {
    this.input = input;
  }

  @Override
  public PaimonCompactionOutput execute() {
    if (input == null || input.getTable() == null || input.getTaskBytes() == null) {
      throw new IllegalStateException(
          "PaimonCompactionInput is missing required fields (table / taskBytes).");
    }
    if (input.getCommitUser() == null || input.getCommitUser().isEmpty()) {
      throw new IllegalStateException("PaimonCompactionInput is missing commitUser.");
    }
    if (input.getCommitIdentifier() <= 0L) {
      throw new IllegalStateException(
          "PaimonCompactionInput has invalid commitIdentifier "
              + input.getCommitIdentifier()
              + ".");
    }

    CommitMessage message =
        input.getSerializerVersion() == PaimonFixedBucketTaskSerializer.VERSION
            ? PaimonFixedBucketRewrite.execute(input)
            : executeUnaware();

    byte[] msgBytes;
    CommitMessageSerializer msgSerializer = new CommitMessageSerializer();
    int msgVersion = msgSerializer.getVersion();
    try {
      msgBytes = msgSerializer.serialize(message);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to serialize Paimon CommitMessage for partition " + input.getPartitionPath(), e);
    }

    PaimonRewriteMetrics metrics = PaimonRewriteMetrics.collect(Collections.singletonList(message));
    long compactedFileCount = metrics.inputFiles();
    long compactedFileSize = metrics.inputBytes();
    long producedFileCount = metrics.outputFiles();
    long producedFileSize = metrics.outputBytes();

    LOG.info(
        "Paimon compaction done: partition={} compactedFiles={} compactedBytes={} producedFiles={} producedBytes={}",
        input.getPartitionPath(),
        compactedFileCount,
        compactedFileSize,
        producedFileCount,
        producedFileSize);

    PaimonCompactionOutput output =
        new PaimonCompactionOutput(
            msgBytes,
            msgVersion,
            compactedFileCount,
            compactedFileSize,
            producedFileCount,
            producedFileSize);
    output.setRewriteRecords(metrics.inputRecords(), metrics.outputRecords());
    return output;
  }

  private CommitMessage executeUnaware() {
    AppendCompactTask task;
    try {
      task =
          new AppendCompactTaskSerializer()
              .deserialize(input.getSerializerVersion(), input.getTaskBytes());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to deserialize AppendCompactTask (version=" + input.getSerializerVersion() + ")",
          e);
    }

    Object raw = input.getTable().originalTable();
    if (!(raw instanceof AppendOnlyFileStoreTable)) {
      throw new IllegalStateException(
          "PaimonCompactionExecutor requires AppendOnlyFileStoreTable, got "
              + (raw == null ? "null" : raw.getClass().getName()));
    }
    AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) raw;
    if (table.bucketMode() != BucketMode.BUCKET_UNAWARE) {
      throw new IllegalStateException("Append compaction requires BUCKET_UNAWARE table");
    }
    // A recovered task can predate the planner's capability gate. Refuse it before opening a
    // writer. PaimonCompactionSchema also preserves and validates these bound execution options.
    PaimonAppendCapabilities.dataEvolutionRejection(table.coreOptions())
        .ifPresent(
            reason -> {
              throw new IllegalStateException(reason);
            });
    // Paimon 1.4.2 AppendCompactTask#doCompact 只重写 compactBefore；不得另扫 latest 扩大输入。
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/append/AppendCompactTask.java
    table =
        (AppendOnlyFileStoreTable)
            PaimonCompactionSchema.refresh(
                table,
                PaimonCompactionSchema.fileSchemaIds(task.compactBefore()),
                input.getTargetSnapshotId());

    CoreOptions coreOptions = table.coreOptions();
    Snapshot boundDvSnapshot =
        coreOptions.deletionVectorsEnabled()
            ? loadBoundSnapshot(table, input.getTargetSnapshotId())
            : null;
    BaseAppendFileStoreWrite write = table.store().newWrite(input.getCommitUser());
    if (coreOptions.rowTrackingEnabled()) {
      write.withWriteType(SpecialFields.rowTypeWithRowTracking(table.rowType()));
    }
    CommitMessage message;
    try {
      message =
          boundDvSnapshot == null
              ? task.doCompact(table, write)
              : compactUnawareAtSnapshot(table, write, task, boundDvSnapshot);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to execute Paimon AppendCompactTask for partition " + input.getPartitionPath(),
          e);
    } finally {
      try {
        write.close();
      } catch (Exception closeErr) {
        LOG.warn(
            "Error closing BaseAppendFileStoreWrite for partition {}: {}",
            input.getPartitionPath(),
            closeErr.getMessage());
      }
    }
    return message;
  }

  private static Snapshot loadBoundSnapshot(AppendOnlyFileStoreTable table, long targetSnapshotId) {
    if (targetSnapshotId < org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID) {
      throw new IllegalStateException(
          "Target snapshot must be >= 1 for unaware deletion-vector compaction, got "
              + targetSnapshotId);
    }
    try {
      org.apache.paimon.Snapshot snapshot =
          table.snapshotManager().tryGetSnapshot(targetSnapshotId);
      if (snapshot == null) {
        throw new IllegalStateException(
            "Target snapshot "
                + targetSnapshotId
                + " does not exist before unaware deletion-vector compaction started");
      }
      return snapshot;
    } catch (java.io.FileNotFoundException expired) {
      throw new IllegalStateException(
          "Target snapshot "
              + targetSnapshotId
              + " expired before unaware deletion-vector compaction started",
          expired);
    }
  }

  private static CommitMessage compactUnawareAtSnapshot(
      AppendOnlyFileStoreTable table,
      BaseAppendFileStoreWrite write,
      AppendCompactTask task,
      org.apache.paimon.Snapshot boundSnapshot)
      throws Exception {
    // Paimon 1.4.2 AppendCompactTask#doCompact reads table.snapshotManager().latestSnapshot(),
    // which causes concurrent unaware tasks in the same partition to emit duplicate DELETE entries
    // when an external DML repacks DV index files. Bind BaseAppendDeleteFileMaintainer to the
    // planned snapshot and pass its getDeletionVector callback to 4-arg compactRewrite:
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/append/AppendCompactTask.java#L75-L115
    org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer dvIndexFileMaintainer =
        org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer.forUnawareAppend(
            table.store().newIndexFileHandler(), boundSnapshot, task.partition());
    List<DataFileMeta> compactBefore = new java.util.ArrayList<>(task.compactBefore());
    List<DataFileMeta> compactAfter =
        new java.util.ArrayList<>(
            write.compactRewrite(
                task.partition(),
                BucketMode.UNAWARE_BUCKET,
                dvIndexFileMaintainer::getDeletionVector,
                compactBefore));
    compactBefore.forEach(
        file -> dvIndexFileMaintainer.notifyRemovedDeletionVector(file.fileName()));
    List<org.apache.paimon.index.IndexFileMeta> newIndexFiles = new java.util.ArrayList<>();
    List<org.apache.paimon.index.IndexFileMeta> deletedIndexFiles = new java.util.ArrayList<>();
    for (org.apache.paimon.manifest.IndexManifestEntry entry : dvIndexFileMaintainer.persist()) {
      if (entry.kind() == org.apache.paimon.manifest.FileKind.ADD) {
        newIndexFiles.add(entry.indexFile());
      } else {
        deletedIndexFiles.add(entry.indexFile());
      }
    }
    org.apache.paimon.io.CompactIncrement compactIncrement =
        new org.apache.paimon.io.CompactIncrement(
            compactBefore,
            compactAfter,
            java.util.Collections.emptyList(),
            newIndexFiles,
            deletedIndexFiles);
    return new org.apache.paimon.table.sink.CommitMessageImpl(
        task.partition(),
        BucketMode.UNAWARE_BUCKET,
        table.coreOptions().bucket(),
        org.apache.paimon.io.DataIncrement.emptyIncrement(),
        compactIncrement);
  }
}
