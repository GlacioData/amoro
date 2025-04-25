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

package org.apache.amoro.maintainer.iceberg.producer;

import static org.apache.amoro.shade.guava32.com.google.common.primitives.Longs.min;

import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.CommitMetaProducer;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.MaintainerType;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;
import org.apache.amoro.optimizing.maintainer.IcebergTableUtil;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.ImmutableExpireSnapshots;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.actions.FileInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * An action that performs the same operation as {@link org.apache.iceberg.ExpireSnapshots} but uses
 * Spark to determine the delta in files between the pre and post-expiration table metadata. All of
 * the same restrictions of {@link org.apache.iceberg.ExpireSnapshots} also apply to this action.
 *
 * <p>This action first leverages {@link org.apache.iceberg.ExpireSnapshots} to expire snapshots and
 * then uses metadata tables to find files that can be safely deleted. This is done by anti-joining
 * two Datasets that contain all manifest and content files before and after the expiration. The
 * snapshot expiration will be fully committed before any deletes are issued.
 *
 * <p>This operation performs a shuffle so the parallelism can be controlled through
 * 'spark.sql.shuffle.partitions'.
 *
 * <p>Deletes are still performed locally after retrieving the results from the Spark executors.
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
public class ExpireSnapshotsSparkExecutor extends BaseSparkExecutor<ExpireSnapshotsSparkExecutor>
    implements MaintainerExecutor<IcebergExpireSnapshotsOutput> {

  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsSparkExecutor.class);

  private final Table table;
  private final TableOperations ops;

  private final Set<Long> expiredSnapshotIds = Sets.newHashSet();
  private final String database;
  private final CatalogMeta catalogMeta;
  private Long expireOlderThanValue = null;
  private Integer retainLastValue = null;
  private Consumer<String> deleteFunc = null;
  private ExecutorService deleteExecutorService = null;
  private Dataset<FileInfo> expiredFileDS = null;

  public static final Set<String> AMORO_MAINTAIN_COMMITS =
      org.apache.amoro.shade.guava32.com.google.common.collect.Sets.newHashSet(
          CommitMetaProducer.OPTIMIZE.name(),
          CommitMetaProducer.DATA_EXPIRATION.name(),
          CommitMetaProducer.CLEAN_DANGLING_DELETE.name());

  // same as org.apache.iceberg.flink.sink.IcebergFilesCommitter#MAX_COMMITTED_CHECKPOINT_ID
  public static final String FLINK_MAX_COMMITTED_CHECKPOINT_ID =
      "flink.max-committed-checkpoint-id";

  public ExpireSnapshotsSparkExecutor(SparkSession spark, IcebergExpireSnapshotInput input) {
    super(spark);
    this.database = input.getDatabase();
    this.catalogMeta = input.getCatalogMeta();
    this.table = input.getTable();
    this.ops = ((HasTableOperations) table).operations();
    this.expireOlderThanValue =
        mustOlderThan(table, input.getSnapshotTTLMinutes(), System.currentTimeMillis());
    this.retainLastValue = input.getMinCount();
  }

  @Override
  public ExpireSnapshotsSparkExecutor self() {
    return this;
  }

  public ExpireSnapshotsSparkExecutor executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  public ExpireSnapshotsSparkExecutor expireSnapshotId(long snapshotId) {
    expiredSnapshotIds.add(snapshotId);
    return this;
  }

  public ExpireSnapshotsSparkExecutor expireOlderThan(long timestampMillis) {
    this.expireOlderThanValue = timestampMillis;
    return this;
  }

  public ExpireSnapshotsSparkExecutor retainLast(int numSnapshots) {
    Preconditions.checkArgument(
        1 <= numSnapshots,
        "Number of snapshots to retain must be at least 1, cannot be: %s",
        numSnapshots);
    this.retainLastValue = numSnapshots;
    return this;
  }

  public ExpireSnapshotsSparkExecutor deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  /**
   * Expires snapshots and commits the changes to the table, returning a Dataset of files to delete.
   *
   * <p>This does not delete data files. To delete data files, run {@link #execute()}.
   *
   * <p>This may be called before or after {@link #execute()} to return the expired files.
   *
   * @return a Dataset of files that are no longer referenced by the table
   */
  public Dataset<FileInfo> expireFiles() {
    if (expiredFileDS == null) {
      // fetch metadata before expiration
      TableMetadata originalMetadata = ops.current();

      // perform expiration
      org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots();

      for (long id : expiredSnapshotIds) {
        expireSnapshots = expireSnapshots.expireSnapshotId(id);
      }

      if (expireOlderThanValue != null) {
        expireSnapshots = expireSnapshots.expireOlderThan(expireOlderThanValue);
      }

      if (retainLastValue != null) {
        expireSnapshots = expireSnapshots.retainLast(retainLastValue);
      }

      expireSnapshots.cleanExpiredFiles(false).commit();

      // fetch valid files after expiration
      TableMetadata updatedMetadata = ops.refresh();
      Dataset<FileInfo> validFileDS = fileDS(updatedMetadata);

      // fetch files referenced by expired snapshots
      Set<Long> deletedSnapshotIds = findExpiredSnapshotIds(originalMetadata, updatedMetadata);
      Dataset<FileInfo> deleteCandidateFileDS = fileDS(originalMetadata, deletedSnapshotIds);

      // determine expired files
      this.expiredFileDS = deleteCandidateFileDS.except(validFileDS);
    }

    return expiredFileDS;
  }

  @Override
  public IcebergExpireSnapshotsOutput execute() {
    JobGroupInfo info = newJobGroupInfo("EXPIRE-SNAPSHOTS", jobDesc());
    DeleteSummary summary = doExecute();
    ImmutableExpireSnapshots.Result result =
        ImmutableExpireSnapshots.Result.builder()
            .deletedDataFilesCount(summary.dataFilesCount())
            .deletedPositionDeleteFilesCount(summary.positionDeleteFilesCount())
            .deletedEqualityDeleteFilesCount(summary.equalityDeleteFilesCount())
            .deletedManifestsCount(summary.manifestsCount())
            .deletedManifestListsCount(summary.manifestListsCount())
            .deletedStatisticsFilesCount(summary.statisticsFilesCount())
            .build();
    return new IcebergExpireSnapshotsOutput(
        catalogMeta.getCatalogName(),
        database,
        table.name(),
        type().name(),
        System.currentTimeMillis(),
        summary.totalFilesCount(),
        result);
  }

  @Override
  public MaintainerType type() {
    return MaintainerType.EXPIRE_SNAPSHOTS;
  }

  private String jobDesc() {
    List<String> options = Lists.newArrayList();

    if (expireOlderThanValue != null) {
      options.add("older_than=" + expireOlderThanValue);
    }

    if (retainLastValue != null) {
      options.add("retain_last=" + retainLastValue);
    }

    if (!expiredSnapshotIds.isEmpty()) {
      Long first = expiredSnapshotIds.stream().findFirst().get();
      if (expiredSnapshotIds.size() > 1) {
        options.add(
            String.format("snapshot_ids: %s (%s more...)", first, expiredSnapshotIds.size() - 1));
      } else {
        options.add(String.format("snapshot_id: %s", first));
      }
    }

    return String.format("Expiring snapshots (%s) in %s", COMMA_JOINER.join(options), table.name());
  }

  private DeleteSummary doExecute() {
    return deleteFiles(expireFiles().toLocalIterator());
  }

  private Dataset<FileInfo> fileDS(TableMetadata metadata) {
    return fileDS(metadata, null);
  }

  private Dataset<FileInfo> fileDS(TableMetadata metadata, Set<Long> snapshotIds) {
    Table staticTable = newStaticTable(metadata, table.io());
    return contentFileDS(staticTable, snapshotIds)
        .union(manifestDS(staticTable, snapshotIds))
        .union(manifestListDS(staticTable, snapshotIds))
        .union(statisticsFileDS(staticTable, snapshotIds));
  }

  private Set<Long> findExpiredSnapshotIds(
      TableMetadata originalMetadata, TableMetadata updatedMetadata) {
    Set<Long> retainedSnapshots =
        updatedMetadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
    return originalMetadata.snapshots().stream()
        .map(Snapshot::snapshotId)
        .filter(id -> !retainedSnapshots.contains(id))
        .collect(Collectors.toSet());
  }

  private DeleteSummary deleteFiles(Iterator<FileInfo> files) {
    DeleteSummary summary;
    if (deleteFunc == null && table.io() instanceof SupportsBulkOperations) {
      summary = deleteFiles((SupportsBulkOperations) table.io(), files);
    } else {

      if (deleteFunc == null) {
        LOG.info(
            "Table IO {} does not support bulk operations. Using non-bulk deletes.",
            table.io().getClass().getName());
        summary = deleteFiles(deleteExecutorService, table.io()::deleteFile, files);
      } else {
        LOG.info("Custom delete function provided. Using non-bulk deletes");
        summary = deleteFiles(deleteExecutorService, deleteFunc, files);
      }
    }

    LOG.info("Deleted {} total files", summary.totalFilesCount());
    return summary;
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
