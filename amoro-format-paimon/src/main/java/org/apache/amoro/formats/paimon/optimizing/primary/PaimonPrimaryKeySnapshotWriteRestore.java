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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Restores compaction inputs from the latest snapshot reachable without listing the snapshot
 * directory.
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
  private final IndexFileHandler indexFileHandler;
  private long latestKnownSnapshotId;

  PaimonPrimaryKeySnapshotWriteRestore(FileStoreTable table, long snapshotId) {
    this.snapshotManager = table.snapshotManager();
    this.latestKnownSnapshotId = snapshotId;
    FileStore<?> store = table.store();
    this.scan = store.newScan();
    this.indexFileHandler = store.newIndexFileHandler();
    if (CoreOptions.fromMap(table.options()).manifestDeleteFileDropStats()) {
      this.scan.dropStats();
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
    Snapshot snapshot = latestReachableSnapshot();
    List<DataFileMeta> dataFiles = new ArrayList<>();
    List<ManifestEntry> entries =
        scan.withSnapshot(snapshot).withPartitionBucket(partition, bucket).plan().files();
    Integer totalBuckets = WriteRestore.extractDataFiles(entries, dataFiles);

    IndexFileMeta dynamicBucketIndex =
        scanDynamicBucketIndex
            ? indexFileHandler.scanHashIndex(snapshot, partition, bucket).orElse(null)
            : null;
    List<IndexFileMeta> deletionVectorIndexes =
        scanDeleteVectorsIndex
            ? indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partition, bucket)
            : null;
    return new RestoreFiles(
        snapshot, totalBuckets, dataFiles, dynamicBucketIndex, deletionVectorIndexes);
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
