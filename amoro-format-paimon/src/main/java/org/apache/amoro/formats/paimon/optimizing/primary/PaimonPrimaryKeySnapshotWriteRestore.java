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

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 为全部合并单元准备同一执行时快照的数据文件与索引，writer 只消费已检查的恢复结果。 快照发现不列举 snapshot 目录；准备完成后不再读取 latest。
 *
 * <p>Paimon's default restore resolves the latest filesystem snapshot for every writer restore and
 * restores its data files, hash index, and deletion vectors. See Paimon 1.4.2 {@code
 * FileSystemWriteRestore}:
 * https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/FileSystemWriteRestore.java#L65-L97.
 * However, its latest-snapshot lookup falls back to listing the snapshot directory when the {@code
 * LATEST} hint is missing or stale. See {@code HintFileUtils}:
 * https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/utils/HintFileUtils.java#L43-L55.
 *
 * <p>Amoro already knows the snapshot used by the optimizing plan. This restore treats that ID as a
 * lower bound, consults the {@code LATEST} hint, and then probes only consecutive snapshot paths.
 * This is valid because Paimon assigns every committed snapshot {@code latestSnapshot.id() + 1}.
 * See {@code FileStoreCommitImpl}:
 * https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L806-L814.
 */
final class PaimonPrimaryKeySnapshotWriteRestore implements WriteRestore {

  private final SnapshotManager snapshotManager;
  private final FileStoreScan scan;
  private final PaimonPrimaryKeyManifestCache.ScanLease scanLease;
  private final IndexFileHandler indexFileHandler;
  private long latestKnownSnapshotId;
  private final Snapshot snapshot;
  private final boolean dynamicIndex;
  private final boolean deletionVectors;
  private final Map<BinaryRow, Map<Integer, RestoreFiles>> prepared = new HashMap<>();
  private long requiredSchemaId = -1;

  long requiredSchemaId() {
    return requiredSchemaId;
  }

  long snapshotId() {
    return snapshot.id();
  }

  PaimonPrimaryKeySnapshotWriteRestore(
      FileStoreTable table, long snapshotId, List<PaimonBucketCompactionUnit> units) {
    this(table, snapshotId, units, null);
  }

  PaimonPrimaryKeySnapshotWriteRestore(
      FileStoreTable table,
      long snapshotId,
      List<PaimonBucketCompactionUnit> units,
      PaimonPrimaryKeyManifestCache.ScanLease scanLease) {
    this.snapshotManager = table.snapshotManager();
    this.latestKnownSnapshotId = snapshotId;
    FileStore<?> store = table.store();
    this.scanLease = scanLease;
    this.scan = scanLease == null ? store.newScan() : scanLease.scan();
    this.indexFileHandler = store.newIndexFileHandler();
    if (CoreOptions.fromMap(table.options()).manifestDeleteFileDropStats()) {
      this.scan.dropStats();
    }
    this.snapshot = latestReachableSnapshot();
    this.dynamicIndex = table.bucketMode() == BucketMode.HASH_DYNAMIC;
    this.deletionVectors = CoreOptions.fromMap(table.options()).deletionVectorsEnabled();
    // Paimon 1.4.2 AbstractFileStoreWrite#createWriterContainer 在恢复后创建 bucket writer。
    // 全部单元必须先成功读取同一 snapshot 的 data/hash/DV，再允许创建任何 writer。
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L446-L504
    for (PaimonBucketCompactionUnit unit : units) {
      if (unit == null || unit.getPartitionBytes() == null || unit.getBucket() < 0) {
        throw new IllegalStateException("Invalid Paimon compaction unit");
      }
      BinaryRow partition =
          SerializationUtils.deserializeBinaryRow(unit.getPartitionBytes()).copy();
      if (partition.getFieldCount() != table.schema().partitionKeys().size()) {
        throw new IllegalStateException("Compaction partition does not match table schema");
      }
      Map<Integer, RestoreFiles> buckets =
          prepared.computeIfAbsent(partition, key -> new HashMap<>());
      if (buckets.containsKey(unit.getBucket())) {
        throw new IllegalStateException("Duplicate Paimon compaction bucket unit");
      }
      RestoreFiles files = scanFiles(partition, unit.getBucket(), dynamicIndex, deletionVectors);
      buckets.put(unit.getBucket(), files);
      for (DataFileMeta file : files.dataFiles()) {
        requiredSchemaId = Math.max(requiredSchemaId, file.schemaId());
      }
    }
  }

  @Override
  public long latestCommittedIdentifier(String user) {
    // The compaction writer is short-lived and prepares exactly one commit. Returning MIN_VALUE
    // conservatively disables writer cleanup if that ever changes; resolving an identifier here
    // would otherwise require another latest-snapshot lookup for each writer.
    return Long.MIN_VALUE;
  }

  @Override
  public RestoreFiles restoreFiles(
      BinaryRow partition,
      int bucket,
      boolean scanDynamicBucketIndex,
      boolean scanDeleteVectorsIndex) {
    if (scanDynamicBucketIndex != dynamicIndex || scanDeleteVectorsIndex != deletionVectors) {
      throw new IllegalStateException(
          "Writer requested indexes inconsistent with prepared compaction");
    }
    Map<Integer, RestoreFiles> buckets = prepared.get(partition);
    RestoreFiles files = buckets == null ? null : buckets.get(bucket);
    if (files == null) {
      throw new IllegalStateException("Writer requested an unprepared compaction bucket");
    }
    return new RestoreFiles(
        files.snapshot(),
        files.totalBuckets(),
        new ArrayList<>(files.dataFiles()),
        files.dynamicBucketIndex(),
        files.deleteVectorsIndex() == null ? null : new ArrayList<>(files.deleteVectorsIndex()));
  }

  private RestoreFiles scanFiles(
      BinaryRow partition,
      int bucket,
      boolean scanDynamicBucketIndex,
      boolean scanDeleteVectorsIndex) {
    List<DataFileMeta> dataFiles = new ArrayList<>();
    // Paimon 1.4.2 iterator 分批读取 ADD manifest，避免 plan 物化全部 ManifestEntry。
    // 此恢复 scan 不设置 value filter/limit，因而没有 KeyValueFileStoreScan 的 plan 后置过滤。
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreScan.java#L390-L433
    Iterator<ManifestEntry> entries =
        scanLease == null
            ? scan.withSnapshot(snapshot).withPartitionBucket(partition, bucket).readFileIterator()
            : scanLease.readFiles(snapshot, partition, bucket);
    Integer totalBuckets = extractDataFiles(entries, dataFiles);

    IndexFileMeta dynamicBucketIndex =
        scanDynamicBucketIndex
            ? indexFileHandler.scanHashIndex(snapshot, partition, bucket).orElse(null)
            : null;
    List<IndexFileMeta> deletionVectorIndexes =
        scanDeleteVectorsIndex
            ? indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partition, bucket)
            : null;
    return new RestoreFiles(
        snapshot,
        totalBuckets,
        Collections.unmodifiableList(dataFiles),
        dynamicBucketIndex,
        deletionVectorIndexes == null
            ? null
            : Collections.unmodifiableList(new ArrayList<>(deletionVectorIndexes)));
  }

  // 与 Paimon 1.4.2 WriteRestore#extractDataFiles 的桶数校验及空结果 null 语义保持一致。
  // 原生 helper 只接收 List；这里接受 iterator，避免重新物化 entry 列表。
  // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/WriteRestore.java#L41-L54
  static Integer extractDataFiles(Iterator<ManifestEntry> entries, List<DataFileMeta> dataFiles) {
    Integer totalBuckets = null;
    while (entries.hasNext()) {
      ManifestEntry entry = entries.next();
      if (totalBuckets != null && totalBuckets != entry.totalBuckets()) {
        throw new RuntimeException(
            String.format(
                "Bucket data files has different total bucket number, %s vs %s, this should be a bug.",
                totalBuckets, entry.totalBuckets()));
      }
      totalBuckets = entry.totalBuckets();
      dataFiles.add(entry.file());
    }
    return totalBuckets;
  }

  private synchronized Snapshot latestReachableSnapshot() {
    Long hintedSnapshotId =
        HintFileUtils.readHint(
            snapshotManager.fileIO(), HintFileUtils.LATEST, snapshotManager.snapshotDirectory());
    if (hintedSnapshotId != null
        && hintedSnapshotId > latestKnownSnapshotId
        && snapshotManager.snapshotExists(hintedSnapshotId)) {
      latestKnownSnapshotId = hintedSnapshotId;
    }

    // Probe exact snapshot paths instead of listing the directory. Besides avoiding the filesystem
    // failure which triggered this restore, resolving at execution time is required for
    // correctness:
    // bucketed index manifests replace deletion-vector indexes by bucket identity, so restoring an
    // old planned snapshot can overwrite a concurrent update's deletion vector. See Paimon 1.4.2
    // IndexManifestFileHandler:
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestFileHandler.java#L171-L198.
    while (latestKnownSnapshotId < Long.MAX_VALUE
        && snapshotManager.snapshotExists(latestKnownSnapshotId + 1)) {
      latestKnownSnapshotId++;
    }
    return snapshotManager.snapshot(latestKnownSnapshotId);
  }
}
