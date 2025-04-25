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

package org.apache.amoro.optimizing.maintainer;

import static org.apache.amoro.shade.guava32.com.google.common.primitives.Longs.min;

import org.apache.amoro.TableFormat;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.CommitMetaProducer;
import org.apache.amoro.io.AuthenticatedFileIO;
import org.apache.amoro.io.AuthenticatedFileIOs;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.MaintainerType;
import org.apache.amoro.maintainer.output.ExpireSnapshotsOutput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.CatalogUtil;
import org.apache.amoro.utils.TableFileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ExpireSnapshotsExecutor implements MaintainerExecutor<ExpireSnapshotsOutput> {

  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsExecutor.class);

  public static final Set<String> AMORO_MAINTAIN_COMMITS =
      Sets.newHashSet(
          CommitMetaProducer.OPTIMIZE.name(),
          CommitMetaProducer.DATA_EXPIRATION.name(),
          CommitMetaProducer.CLEAN_DANGLING_DELETE.name());

  // same as org.apache.iceberg.flink.sink.IcebergFilesCommitter#MAX_COMMITTED_CHECKPOINT_ID
  public static final String FLINK_MAX_COMMITTED_CHECKPOINT_ID =
      "flink.max-committed-checkpoint-id";

  private final Long olderThan;
  private final Integer minCount;
  private final Set<String> exclude;
  private final CatalogMeta catalogMeta;
  private final String database;
  protected Table table;

  public ExpireSnapshotsExecutor(IcebergExpireSnapshotInput input) {
    this.database = input.getDatabase();
    this.table = input.getTable();
    this.olderThan =
        mustOlderThan(table, input.getSnapshotTTLMinutes(), System.currentTimeMillis());
    this.minCount = input.getMinCount();
    this.exclude = input.getExpireSnapshotNeedToExcludeFiles();
    this.catalogMeta = input.getCatalogMeta();
  }

  @Override
  public ExpireSnapshotsOutput execute() {
    LOG.info(
        "start expire snapshots older than {} and retain last {} snapshots, the exclude is {}",
        olderThan,
        minCount,
        exclude);
    final AtomicLong toDeleteFiles = new AtomicLong(0);
    Set<String> parentDirectories = new HashSet<>();
    Set<String> expiredFiles = new HashSet<>();
    table
        .expireSnapshots()
        .retainLast(Math.max(minCount, 1))
        .expireOlderThan(olderThan)
        .deleteWith(
            file -> {
              if (exclude.isEmpty()) {
                expiredFiles.add(file);
              } else {
                String fileUriPath = TableFileUtil.getUriPath(file);
                if (!exclude.contains(fileUriPath)
                    && !exclude.contains(new Path(fileUriPath).getParent().toString())) {
                  expiredFiles.add(file);
                }
              }

              parentDirectories.add(new Path(file).getParent().toString());
              toDeleteFiles.incrementAndGet();
            })
        .cleanExpiredFiles(
            true) /* enable clean only for collecting the expired files, will delete them later */
        .commit();

    // try to batch delete files
    int deletedFiles =
        TableFileUtil.parallelDeleteFiles(fileIO(), expiredFiles, ThreadPools.getWorkerPool());

    parentDirectories.forEach(
        parent -> {
          try {
            TableFileUtil.deleteEmptyDirectory(fileIO(), parent, exclude);
          } catch (Exception e) {
            // Ignore exceptions to remove as many directories as possible
            LOG.warn("Fail to delete empty directory {}", parent, e);
          }
        });

    LOG.info(
        "To delete {} files in {}, success delete {} files",
        toDeleteFiles.get(),
        table.name(),
        deletedFiles);
    return new ExpireSnapshotsOutput(
        catalogMeta.getCatalogName(),
        database,
        table.name(),
        TableFormat.ICEBERG.name(),
        System.currentTimeMillis(),
        toDeleteFiles.get());
  }

  @Override
  public MaintainerType type() {
    return MaintainerType.EXPIRE_SNAPSHOTS;
  }

  protected AuthenticatedFileIO fileIO() {
    TableMetaStore tableMetaStore = CatalogUtil.buildMetaStore(catalogMeta);
    return AuthenticatedFileIOs.buildAdaptIcebergFileIO(tableMetaStore, table.io());
  }

  protected static long mustOlderThan(Table table, Long snapshotTTLMinutes, long now) {
    return min(
        // The snapshots keep time
        now - snapshotTTLMinutes,
        // The snapshot optimizing plan based should not be expired for committing
        // fetchOptimizingPlanSnapshotTime(table, tableConfiguration),
        // The latest non-optimized snapshot should not be expired for data expiring
        fetchLatestNonOptimizedSnapshotTime(table),
        // The latest flink committed snapshot should not be expired for recovering flink job
        fetchLatestFlinkCommittedSnapshotTime(table));
  }

  public static long fetchLatestNonOptimizedSnapshotTime(Table table) {
    Optional<Snapshot> snapshot =
        IcebergTableUtil.findFirstMatchSnapshot(
            table, s -> s.summary().values().stream().noneMatch(AMORO_MAINTAIN_COMMITS::contains));
    return snapshot.map(Snapshot::timestampMillis).orElse(Long.MAX_VALUE);
  }

  /**
   * When committing a snapshot, Flink will write a checkpoint id into the snapshot summary, which
   * will be used when Flink job recovers from the checkpoint.
   *
   * @param table table
   * @return commit time of snapshot with the latest flink checkpointId in summary, return
   *     Long.MAX_VALUE if not exist
   */
  public static long fetchLatestFlinkCommittedSnapshotTime(Table table) {
    Snapshot snapshot = findLatestSnapshotContainsKey(table, FLINK_MAX_COMMITTED_CHECKPOINT_ID);
    return snapshot == null ? Long.MAX_VALUE : snapshot.timestampMillis();
  }

  public static Snapshot findLatestSnapshotContainsKey(Table table, String summaryKey) {
    Snapshot latestSnapshot = null;
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.summary().containsKey(summaryKey)) {
        latestSnapshot = snapshot;
      }
    }
    return latestSnapshot;
  }
}
