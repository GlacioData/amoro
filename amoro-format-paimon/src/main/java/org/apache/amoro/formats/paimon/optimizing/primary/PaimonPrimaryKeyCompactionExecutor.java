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

import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionSchema;
import org.apache.amoro.formats.paimon.optimizing.PaimonRewriteMetrics;
import org.apache.amoro.optimizing.OptimizingExecutor;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PaimonPrimaryKeyCompactionExecutor
    implements OptimizingExecutor<PaimonPrimaryKeyCompactionOutput> {

  private static final Logger LOG =
      LoggerFactory.getLogger(PaimonPrimaryKeyCompactionExecutor.class);

  private static final long serialVersionUID = 1L;

  private final PaimonPrimaryKeyCompactionInput input;

  public PaimonPrimaryKeyCompactionExecutor(PaimonPrimaryKeyCompactionInput input) {
    this.input = input;
  }

  @Override
  public PaimonPrimaryKeyCompactionOutput execute() {
    validateInput();

    Object raw = input.getTable().originalTable();
    if (!(raw instanceof FileStoreTable) || raw instanceof AppendOnlyFileStoreTable) {
      throw new IllegalStateException(
          "PaimonPrimaryKeyCompactionExecutor requires non-append FileStoreTable, got "
              + (raw == null ? "null" : raw.getClass().getName()));
    }
    FileStoreTable table = (FileStoreTable) raw;
    if (table.primaryKeys() == null
        || table.primaryKeys().isEmpty()
        || (table.bucketMode() != BucketMode.HASH_FIXED
            && table.bucketMode() != BucketMode.HASH_DYNAMIC)) {
      throw new IllegalStateException(
          "PaimonPrimaryKeyCompactionExecutor requires primary-key HASH_FIXED/HASH_DYNAMIC"
              + " FileStoreTable, got bucketMode="
              + table.bucketMode()
              + ", primaryKeys="
              + table.primaryKeys());
    }
    if (CoreOptions.fromMap(table.options()).pkClusteringOverride()) {
      throw new IllegalStateException(
          "PaimonPrimaryKeyCompactionExecutor does not support pk-clustering-override=true.");
    }

    if (shouldSkipStaleFull(table)) {
      return noOpOutput();
    }

    PaimonPrimaryKeySnapshotWriteRestore restore;
    try (PaimonPrimaryKeyManifestCache.ScanLease scan =
        PaimonPrimaryKeyManifestCache.acquire(input.getTable(), table)) {
      restore =
          new PaimonPrimaryKeySnapshotWriteRestore(
              table, input.getTargetSnapshotId(), input.getUnits(), scan);
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Failed to prepare Paimon compaction inputs: table="
              + input.getTable().id()
              + ", plannedSnapshot="
              + input.getTargetSnapshotId(),
          e);
    }
    FileStoreTable refreshed =
        PaimonCompactionSchema.refresh(table, restore.requiredSchemaId(), restore.snapshotId());

    // Paimon 1.4.2：writer 的 MergeTreeCompactManagerFactory#createCompactStrategy 会重新
    // 从 options 创建 EarlyFullCompaction，只屏蔽 evaluator 无法约束执行端。
    // 源码：paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManagerFactory.java
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManagerFactory.java#L190-L224
    // null 移除的原生依据见 compactionWriteOverrides；原表配置和显式 FULL 参数均保留。
    FileStoreTable compactTable =
        refreshed.copy(PaimonPrimaryKeyOptions.compactionWriteOverrides(table.options()));
    try (IOManager ioManager = IOManager.create(System.getProperty("java.io.tmpdir"));
        BatchTableWrite write =
            compactTable.newBatchWriteBuilder().newWrite().withIOManager(ioManager)) {
      if (!(write instanceof InnerTableWrite)) {
        throw new IllegalStateException(
            "Paimon primary-key compaction requires InnerTableWrite, got "
                + write.getClass().getName());
      }
      ((InnerTableWrite) write).withWriteRestore(restore);
      for (PaimonBucketCompactionUnit unit : input.getUnits()) {
        BinaryRow partition = SerializationUtils.deserializeBinaryRow(unit.getPartitionBytes());
        write.compact(partition, unit.getBucket(), input.isFullCompaction());
      }
      List<CommitMessage> messages = write.prepareCommit();
      PaimonRewriteMetrics metrics = PaimonRewriteMetrics.collect(messages);
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionOutput(
              CommitMessageSerializer.serializeAll(messages),
              input.getUnits().size(),
              metrics.inputFiles(),
              metrics.inputBytes(),
              metrics.inputRecords(),
              metrics.outputFiles(),
              metrics.outputBytes());
      output.setRewriteRecords(metrics.inputRecords(), metrics.outputRecords());
      LOG.info(
          "Paimon primary-key compaction produced commit messages: table={}, type={}, "
              + "identifier={}, bucketUnits={}, commitMessageCount={}, commitMessageBytes={}, "
              + "compactedFiles={}, compactedBytes={}, producedFiles={}, producedBytes={}",
          input.getTable().id(),
          input.getOptimizingType(),
          input.getCommitIdentifier(),
          input.getUnits().size(),
          output.commitMessageCount(),
          output.commitMessageBytesSize(),
          output.getCompactedFileCount(),
          output.getCompactedFileSize(),
          output.getProducedFileCount(),
          output.getProducedFileSize());
      return output;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to execute Paimon primary-key compaction.", e);
    }
  }

  private void validateInput() {
    if (input == null || input.getTable() == null || input.getUnits() == null) {
      throw new IllegalStateException(
          "PaimonPrimaryKeyCompactionInput is missing required fields (table / units).");
    }
    if (input.getUnits().isEmpty()) {
      throw new IllegalStateException("PaimonPrimaryKeyCompactionInput has empty units.");
    }
    if (input.getCommitUser() == null || input.getCommitUser().isEmpty()) {
      throw new IllegalStateException("PaimonPrimaryKeyCompactionInput is missing commitUser.");
    }
    if (input.getCommitIdentifier() <= 0L) {
      throw new IllegalStateException(
          "PaimonPrimaryKeyCompactionInput has invalid commitIdentifier "
              + input.getCommitIdentifier()
              + ".");
    }
    if (input.getTargetSnapshotId() <= 0L) {
      throw new IllegalStateException(
          "PaimonPrimaryKeyCompactionInput has invalid targetSnapshotId "
              + input.getTargetSnapshotId()
              + ".");
    }
    if (input.getOptimizingType() == null) {
      throw new IllegalStateException("PaimonPrimaryKeyCompactionInput is missing optimizingType.");
    }
    switch (input.getOptimizingType()) {
      case MINOR:
      case MAJOR:
        if (input.isFullCompaction()) {
          throw new IllegalStateException(
              "Paimon primary-key "
                  + input.getOptimizingType()
                  + " compaction requires fullCompaction=false.");
        }
        break;
      case FULL:
        if (!input.isFullCompaction()) {
          throw new IllegalStateException(
              "Paimon primary-key FULL compaction requires fullCompaction=true.");
        }
        break;
      default:
        throw new IllegalStateException(
            "Unsupported Paimon primary-key optimizing type: " + input.getOptimizingType());
    }
  }

  private boolean shouldSkipStaleFull(FileStoreTable table) {
    if (input.getOptimizingType() != OptimizingType.FULL) {
      return false;
    }
    Optional<Snapshot> latestSnapshot = table.latestSnapshot();
    return !latestSnapshot.isPresent() || latestSnapshot.get().id() != input.getTargetSnapshotId();
  }

  private PaimonPrimaryKeyCompactionOutput noOpOutput() {
    PaimonPrimaryKeyCompactionOutput output =
        new PaimonPrimaryKeyCompactionOutput(Collections.emptyList(), 0, 0L, 0L, 0L, 0L, 0L);
    output.setRewriteRecords(0L, 0L);
    return output;
  }
}
