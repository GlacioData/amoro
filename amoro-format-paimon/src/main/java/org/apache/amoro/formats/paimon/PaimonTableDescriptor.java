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

package org.apache.amoro.formats.paimon;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.formats.paimon.tag.PaimonTagReader;
import org.apache.amoro.formats.paimon.utils.SnapShotsScanUtils;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.ProcessTaskStatus;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.AMSColumnInfo;
import org.apache.amoro.table.descriptor.AMSPartitionField;
import org.apache.amoro.table.descriptor.AmoroSnapshotsOfTable;
import org.apache.amoro.table.descriptor.ConsumerInfo;
import org.apache.amoro.table.descriptor.DDLInfo;
import org.apache.amoro.table.descriptor.DDLReverser;
import org.apache.amoro.table.descriptor.FilesStatistics;
import org.apache.amoro.table.descriptor.FilesStatisticsBuilder;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.OptimizingProcessInfo;
import org.apache.amoro.table.descriptor.OptimizingTaskInfo;
import org.apache.amoro.table.descriptor.PartitionBaseInfo;
import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TableDetailObservation;
import org.apache.amoro.table.descriptor.TableModel;
import org.apache.amoro.table.descriptor.TableSummary;
import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Descriptor for Paimon format tables. */
public class PaimonTableDescriptor implements FormatTableDescriptor {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonTableDescriptor.class);

  public static final String PAIMON_MAIN_BRANCH_NAME = "main";

  private static final String PAIMON_OPTIMIZING_TYPE_FULL = "FULL";
  private static final String PAIMON_OPTIMIZING_TYPE_MAJOR = "MAJOR";
  private static final String PAIMON_OPTIMIZING_TYPE_MINOR = "MINOR";

  /**
   * Matches the trailing {@code bucket-<n>} segment of a partition identifier, where {@code <n>} is
   * a possibly-negative integer. Anchored to the end because the bucket is always the leaf segment
   * of a Paimon partition path.
   */
  private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket-(-?\\d+)$");

  @Override
  public void withIoExecutor(ExecutorService ioExecutor) {
    // Snapshot reads stay on the request thread to preserve UGI and avoid queued work.
  }

  @Override
  public List<TableFormat> supportFormat() {
    return Lists.newArrayList(TableFormat.PAIMON);
  }

  @Override
  public boolean supportsTypedSnapshotReferences() {
    return true;
  }

  @Override
  public boolean supportsBoundedSnapshotRead() {
    return true;
  }

  @Override
  public ServerTableMeta getTableDetail(AmoroTable<?> amoroTable) {
    PaimonSnapshotReadDeadline deadline = new PaimonSnapshotReadDeadline();
    return deadline.call(
        () -> doAs(amoroTable, () -> getTableDetailInternal(amoroTable, deadline)));
  }

  private ServerTableMeta getTableDetailInternal(
      AmoroTable<?> amoroTable, PaimonSnapshotReadDeadline deadline) {
    FileStoreTable table = getTable(amoroTable);
    FileStore<?> store = table.store();

    ServerTableMeta serverTableMeta = new ServerTableMeta();
    serverTableMeta.setTableIdentifier(amoroTable.id());
    serverTableMeta.setTableType(amoroTable.format().name());
    String bucketMode = store.bucketMode().name();
    serverTableMeta.setTableModel(new TableModel(TableModel.BUCKET_MODE, bucketMode));
    // Preserve the existing response field for older dashboard clients.
    serverTableMeta.setBucketMode(bucketMode);

    // schema
    serverTableMeta.setSchema(
        table.rowType().getFields().stream()
            .map(
                s ->
                    new AMSColumnInfo(
                        s.name(), s.type().asSQLString(), !s.type().isNullable(), s.description()))
            .collect(Collectors.toList()));

    // primary key
    Set<String> primaryKeyNames = new HashSet<>(table.primaryKeys());
    List<AMSColumnInfo> primaryKeys =
        serverTableMeta.getSchema().stream()
            .filter(s -> primaryKeyNames.contains(s.getField()))
            .collect(Collectors.toList());
    serverTableMeta.setPkList(primaryKeys);

    // partition
    List<AMSPartitionField> partitionFields =
        store.partitionType().getFields().stream()
            .map(f -> new AMSPartitionField(f.name(), null, null, f.id(), null))
            .collect(Collectors.toList());
    serverTableMeta.setPartitionColumnList(partitionFields);

    // properties
    serverTableMeta.setProperties(table.options());

    Map<String, Object> baseMetric = new HashMap<>();
    // table summary
    TableSummary tableSummary;
    if (table.comment().isPresent()) {
      serverTableMeta.setComment(table.comment().get());
    }
    Snapshot snapshot = PaimonSnapshotMetadata.latest(store.snapshotManager(), deadline);
    serverTableMeta.setDetailObservation(
        new TableDetailObservation(snapshot == null ? null : snapshot.id(), table.schema().id()));
    if (snapshot != null) {
      Long totalRecordCount = snapshot.totalRecordCount();
      long records = totalRecordCount == null ? 0L : totalRecordCount;

      tableSummary = new TableSummary(0, null, null, records, "paimon");
      tableSummary.setFile(null);

      baseMetric.put("totalSize", null);
      baseMetric.put("fileCount", null);
      baseMetric.put("averageFileSize", null);
      baseMetric.put("lastCommitTime", snapshot.timeMillis());
      Long watermark = snapshot.watermark();
      if (watermark != null && watermark > 0) {
        baseMetric.put("baseWatermark", watermark);
      }
    } else {
      tableSummary = new TableSummary(0, null, null, 0, "paimon");

      baseMetric.put("totalSize", null);
      baseMetric.put("fileCount", 0);
      baseMetric.put("averageFileSize", null);
    }
    serverTableMeta.setTableSummary(tableSummary);
    serverTableMeta.setBaseMetrics(baseMetric);

    return serverTableMeta;
  }

  @Override
  public Pair<List<AmoroSnapshotsOfTable>, Long> getSnapshots(
      AmoroTable<?> amoroTable,
      String ref,
      OperationType operationType,
      int limit,
      int offset,
      String lastSnapshot) {
    PaimonSnapshotReadDeadline deadline = new PaimonSnapshotReadDeadline();
    String normalizedRef = PaimonSnapshotMetadata.ref(ref);
    return deadline.call(
        () ->
            doAs(
                amoroTable,
                () ->
                    PaimonSnapshotMetadata.page(
                        getTable(amoroTable),
                        normalizedRef,
                        operationType,
                        limit,
                        offset,
                        deadline)));
  }

  @Override
  public List<PartitionFileBaseInfo> getSnapshotDetail(
      AmoroTable<?> amoroTable, String snapshotId, String ref) {
    throw new org.apache.amoro.table.descriptor.SnapshotReadException(
        org.apache.amoro.table.descriptor.SnapshotReadException.Reason.INVALID_REQUEST,
        "Use cursor-based snapshot detail; refresh the dashboard");
  }

  @Override
  public org.apache.amoro.table.descriptor.SnapshotDetailPage getSnapshotDetailPage(
      AmoroTable<?> amoroTable, String snapshotId, String ref, int limit, String cursor) {
    return getSnapshotDetailPage(
        amoroTable,
        snapshotId,
        ref,
        null,
        new org.apache.amoro.table.tag.TagScope(),
        limit,
        cursor);
  }

  @Override
  public org.apache.amoro.table.descriptor.SnapshotDetailPage getSnapshotDetailPage(
      AmoroTable<?> amoroTable,
      String snapshotId,
      String ref,
      String refType,
      org.apache.amoro.table.tag.TagScope scope,
      int limit,
      String cursor) {
    if (refType != null && !"TAG".equals(refType) && !"BRANCH".equals(refType)) {
      throw new org.apache.amoro.table.descriptor.SnapshotReadException(
          org.apache.amoro.table.descriptor.SnapshotReadException.Reason.INVALID_REQUEST,
          "Invalid reference type");
    }
    if ("TAG".equals(refType)) {
      PaimonTagReader.validateScope(scope);
    }
    PaimonSnapshotReadDeadline deadline = new PaimonSnapshotReadDeadline();
    String normalizedRef = PaimonSnapshotMetadata.ref(ref);
    final long id;
    try {
      id = Long.parseLong(snapshotId);
    } catch (NumberFormatException e) {
      throw new org.apache.amoro.table.descriptor.SnapshotReadException(
          org.apache.amoro.table.descriptor.SnapshotReadException.Reason.INVALID_REQUEST,
          "Invalid snapshot ID",
          e);
    }
    return deadline.call(
        () ->
            doAs(
                amoroTable,
                () -> {
                  FileStoreTable loaded = getTable(amoroTable);
                  // Tag coordinates are main-scoped, including when loaded through $branch_*. Use
                  // the native switch so reference, schema and manifest/path context agree.
                  // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java
                  FileStoreTable table =
                      "TAG".equals(refType) ? loaded.switchToBranch("main") : loaded;
                  Snapshot snapshot =
                      "TAG".equals(refType)
                          ? PaimonSnapshotMetadata.resolveTag(table, normalizedRef, id, deadline)
                          : "BRANCH".equals(refType)
                              ? PaimonSnapshotMetadata.resolveBranch(
                                  table, normalizedRef, id, deadline)
                              : PaimonSnapshotMetadata.resolve(table, normalizedRef, id, deadline);
                  FileStore<?> store = table.store();
                  return new PaimonSnapshotDetailReader(deadline)
                      .read(
                          store,
                          snapshot,
                          amoroTable.id().toString() + "|" + table.location(),
                          refType == null ? normalizedRef : refType + "\0main\0" + normalizedRef,
                          limit,
                          cursor,
                          entry ->
                              new PartitionFileBaseInfo(
                                  null,
                                  "BASE_FILE",
                                  getFileCreationTimeMillis(entry),
                                  canonicalPartitionBucket(
                                      entry.partition(), entry.bucket(), store.pathFactory()),
                                  fullFilePath(store, entry),
                                  entry.file().fileSize(),
                                  entry.kind().name()));
                }));
  }

  @Override
  public List<DDLInfo> getTableOperations(AmoroTable<?> amoroTable) {
    return doAs(amoroTable, () -> getTableOperationsInternal(amoroTable));
  }

  private List<DDLInfo> getTableOperationsInternal(AmoroTable<?> amoroTable) {
    DataTable table = getTable(amoroTable);
    PaimonTableMetaExtract extract = new PaimonTableMetaExtract();
    DDLReverser<DataTable> ddlReverser = new DDLReverser<>(extract);
    return ddlReverser.reverse(table, amoroTable.id());
  }

  @Override
  public List<PartitionBaseInfo> getTablePartitions(AmoroTable<?> amoroTable) {
    return doAs(amoroTable, () -> getTablePartitionsInternal(amoroTable));
  }

  private List<PartitionBaseInfo> getTablePartitionsInternal(AmoroTable<?> amoroTable) {
    FileStoreTable table = getTable(amoroTable);
    FileStore<?> store = table.store();
    FileStorePathFactory fileStorePathFactory = store.pathFactory();
    List<ManifestEntry> files = store.newScan().plan().files(FileKind.ADD);

    // Accumulate per partition/bucket in a single pass. The canonical partition string is a stable,
    // unique key for each (partition, bucket) pair, so it doubles as the output identifier and
    // avoids the unreliable BinaryRow-as-Map-key problem. The accumulator keeps only aggregate
    // numbers, never a List<ManifestEntry>, so the extra space scales with partition/bucket count
    // rather than file count.
    Map<String, PartitionStats> statsByPartition = new HashMap<>();
    for (ManifestEntry manifestEntry : files) {
      String partitionSt =
          canonicalPartitionBucket(
              manifestEntry.partition(), manifestEntry.bucket(), fileStorePathFactory);
      PartitionStats stats =
          statsByPartition.computeIfAbsent(partitionSt, k -> new PartitionStats());
      stats.fileCount++;
      stats.fileSize += manifestEntry.file().fileSize();
      stats.lastCommitTime =
          Math.max(stats.lastCommitTime, getFileCreationTimeMillis(manifestEntry));
    }

    List<PartitionBaseInfo> partitionBaseInfoList = new ArrayList<>();
    for (Map.Entry<String, PartitionStats> entry : statsByPartition.entrySet()) {
      PartitionStats stats = entry.getValue();
      partitionBaseInfoList.add(
          new PartitionBaseInfo(
              entry.getKey(), 0, stats.fileCount, stats.fileSize, stats.lastCommitTime));
    }
    return partitionBaseInfoList;
  }

  @Override
  public List<PartitionFileBaseInfo> getTableFiles(
      AmoroTable<?> amoroTable, String partition, Integer specId) {
    return doAs(amoroTable, () -> getTableFilesInternal(amoroTable, partition, specId));
  }

  private List<PartitionFileBaseInfo> getTableFilesInternal(
      AmoroTable<?> amoroTable, String partition, Integer specId) {
    FileStoreTable table = getTable(amoroTable);
    FileStore<?> store = table.store();
    FileStorePathFactory fileStorePathFactory = store.pathFactory();

    // A single current Scan, scoped to the target partition/bucket when the table is partitioned.
    // There is no more Snapshot-history walk, no per-snapshot delta-manifest read and no
    // file-to-snapshot map: commitId is allowed to be null by contract and is not worth a full
    // history scan.
    List<ManifestEntry> files;
    if (table.partitionKeys().isEmpty()) {
      // Non-partitioned table: ignore the partition input (the frontend sends the literal "null"),
      // and return every current ADD file.
      files = store.newScan().plan().files(FileKind.ADD);
    } else {
      PartitionTarget target = parsePartitionTarget(store, partition);
      if (target == null) {
        // Unresolvable input must yield an empty result; never fall back to a full-table scan.
        return Collections.emptyList();
      }
      files =
          store
              .newScan()
              .withPartitionBucket(target.partition, target.bucket)
              .plan()
              .files(FileKind.ADD);
    }

    List<PartitionFileBaseInfo> partitionFileBases = new ArrayList<>();
    for (ManifestEntry manifestEntry : files) {
      partitionFileBases.add(
          new PartitionFileBaseInfo(
              null,
              "INSERT_FILE",
              getFileCreationTimeMillis(manifestEntry),
              canonicalPartitionBucket(
                  manifestEntry.partition(), manifestEntry.bucket(), fileStorePathFactory),
              0,
              fullFilePath(store, manifestEntry),
              manifestEntry.file().fileSize()));
    }

    return partitionFileBases;
  }

  /** Returns a paged view of the table's COMPACT-commit processes. */
  @Override
  public Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfo(
      AmoroTable<?> amoroTable,
      String type,
      ProcessStatus status,
      int limit,
      int offset,
      String lastSnapshot) {
    return doAs(
        amoroTable,
        () ->
            getOptimizingProcessesInfoInternal(
                amoroTable, type, status, limit, offset, lastSnapshot));
  }

  private Pair<List<OptimizingProcessInfo>, Integer> getOptimizingProcessesInfoInternal(
      AmoroTable<?> amoroTable,
      String type,
      ProcessStatus status,
      int limit,
      int offset,
      String lastSnapshot) {
    // Temporary solution for Paimon. TODO: Get compaction info from Paimon compaction task
    TableIdentifier tableIdentifier = amoroTable.id();
    FileStoreTable fileStoreTable = (FileStoreTable) amoroTable.originalTable();
    FileStore<?> store = fileStoreTable.store();
    boolean isPrimaryTable = !fileStoreTable.primaryKeys().isEmpty();
    int maxLevel = CoreOptions.fromMap(fileStoreTable.options()).numLevels() - 1;
    SnapshotManager snapshotManager = store.snapshotManager();

    // Empty-table guard: a freshly-created Paimon table has no snapshots and
    // latestSnapshotId() returns null. The old heuristic total — (latest -
    // earliest) / 2L * 0.6 — unboxed that null and threw NPE before any
    // result could be produced. Short-circuit here keeps the scan off the
    // critical path and returns an empty, stable container.
    if (snapshotManager.latestSnapshotId() == null) {
      return Pair.of(Collections.emptyList(), 0);
    }
    if (status != null && status != ProcessStatus.SUCCESS) {
      return Pair.of(Collections.emptyList(), 0);
    }

    ManifestFile manifestFile = store.manifestFileFactory().create();
    ManifestList manifestList = store.manifestListFactory().create();

    Long lastSnapshotId =
        (lastSnapshot != null && !lastSnapshot.trim().isEmpty())
            ? Long.valueOf(lastSnapshot)
            : null;
    Pair<List<OptimizingProcessInfo>, Integer> page =
        SnapShotsScanUtils.getMappedSnapshotsWithPagination(
            snapshotManager,
            snapshot -> {
              if (snapshot.commitKind() != Snapshot.CommitKind.COMPACT) {
                return Optional.empty();
              }
              OptimizingProcessInfo process =
                  toOptimizingProcessInfo(
                      snapshot,
                      tableIdentifier,
                      manifestFile,
                      manifestList,
                      isPrimaryTable,
                      maxLevel);
              return StringUtils.isBlank(type) || type.equalsIgnoreCase(process.getOptimizingType())
                  ? Optional.of(process)
                  : Optional.empty();
            },
            limit,
            offset,
            lastSnapshotId);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "getOptimizingProcessesInfo for {}: total={}, type={}, status={}, offset={}, limit={}, returned={}",
          tableIdentifier,
          page.getRight(),
          type,
          status,
          offset,
          limit,
          page.getLeft().size());
    }

    return page;
  }

  /**
   * Builds a single {@link OptimizingProcessInfo} from a COMPACT snapshot by walking its delta
   * manifests. Extracted so progressive pagination can reuse the same per-snapshot materialisation
   * logic after selecting compact candidates.
   */
  private OptimizingProcessInfo toOptimizingProcessInfo(
      Snapshot s,
      TableIdentifier tableIdentifier,
      ManifestFile manifestFile,
      ManifestList manifestList,
      boolean isPrimaryTable,
      int maxLevel) {
    OptimizingProcessInfo optimizingProcessInfo = new OptimizingProcessInfo();
    optimizingProcessInfo.setProcessId(String.valueOf(s.id()));
    optimizingProcessInfo.setCatalogName(tableIdentifier.getCatalog());
    optimizingProcessInfo.setDbName(tableIdentifier.getDatabase());
    optimizingProcessInfo.setTableName(tableIdentifier.getTableName());
    optimizingProcessInfo.setStatus(ProcessStatus.SUCCESS);
    optimizingProcessInfo.setFinishTime(s.timeMillis());
    FilesStatisticsBuilder inputBuilder = new FilesStatisticsBuilder();
    FilesStatisticsBuilder outputBuilder = new FilesStatisticsBuilder();
    List<ManifestFileMeta> manifestFileMetas = manifestList.readDeltaManifests(s);
    boolean hasMaxLevels = false;
    long minCreateTime = Long.MAX_VALUE;
    Map<PartitionBucketKey, List<ManifestEntry>> entriesByBucket = new LinkedHashMap<>();
    for (ManifestFileMeta manifestFileMeta : manifestFileMetas) {
      List<ManifestEntry> compactManifestEntries = manifestFile.read(manifestFileMeta.fileName());
      for (ManifestEntry compactManifestEntry : compactManifestEntries) {
        if (compactManifestEntry.file().level() == maxLevel) {
          hasMaxLevels = true;
        }
        PartitionBucketKey key =
            new PartitionBucketKey(compactManifestEntry.partition(), compactManifestEntry.bucket());
        entriesByBucket.computeIfAbsent(key, k -> new ArrayList<>()).add(compactManifestEntry);
      }
    }

    long inputRowCount = 0L;
    long outputRowCount = 0L;
    for (List<ManifestEntry> bucketEntries : entriesByBucket.values()) {
      Set<String> retainedNames = retainedUpgradeFileNames(bucketEntries);
      for (ManifestEntry compactManifestEntry : bucketEntries) {
        String fileName = compactManifestEntry.file().fileName();
        if (fileName != null && retainedNames.contains(fileName)) {
          continue;
        }
        if (compactManifestEntry.kind() == FileKind.DELETE) {
          inputBuilder.addFile(compactManifestEntry.file().fileSize());
          inputRowCount += compactManifestEntry.file().rowCount();
        } else {
          minCreateTime = Math.min(minCreateTime, getFileCreationTimeMillis(compactManifestEntry));
          outputBuilder.addFile(compactManifestEntry.file().fileSize());
          outputRowCount += compactManifestEntry.file().rowCount();
        }
      }
    }

    HashMap<String, String> summary = new HashMap<>(8);
    if (isPrimaryTable && hasMaxLevels) {
      optimizingProcessInfo.setOptimizingType(PAIMON_OPTIMIZING_TYPE_FULL);
    } else {
      optimizingProcessInfo.setOptimizingType(PAIMON_OPTIMIZING_TYPE_MINOR);
    }
    optimizingProcessInfo.setSuccessTasks(entriesByBucket.size());
    optimizingProcessInfo.setTotalTasks(entriesByBucket.size());
    long startTime = minCreateTime == Long.MAX_VALUE ? s.timeMillis() : minCreateTime;
    optimizingProcessInfo.setStartTime(startTime);
    optimizingProcessInfo.setDuration(s.timeMillis() - startTime);
    optimizingProcessInfo.setInputFiles(inputBuilder.build());
    optimizingProcessInfo.setOutputFiles(outputBuilder.build());
    summary.put(
        "input-data-files(rewrite)",
        String.valueOf(optimizingProcessInfo.getInputFiles().getFileCnt()));
    summary.put(
        "input-data-size(rewrite)",
        String.valueOf(optimizingProcessInfo.getInputFiles().getTotalSize()));
    summary.put("input-data-records(rewrite)", String.valueOf(inputRowCount));
    summary.put(
        "output-data-files", String.valueOf(optimizingProcessInfo.getOutputFiles().getFileCnt()));
    summary.put(
        "output-data-size", String.valueOf(optimizingProcessInfo.getOutputFiles().getTotalSize()));
    summary.put("output-data-records", String.valueOf(outputRowCount));
    optimizingProcessInfo.setSummary(summary);
    return optimizingProcessInfo;
  }

  @Override
  public Map<String, String> getTableOptimizingTypes(AmoroTable<?> amoroTable) {
    Map<String, String> types = Maps.newHashMap();
    types.put(PAIMON_OPTIMIZING_TYPE_FULL, PAIMON_OPTIMIZING_TYPE_FULL);
    types.put(PAIMON_OPTIMIZING_TYPE_MAJOR, PAIMON_OPTIMIZING_TYPE_MAJOR);
    types.put(PAIMON_OPTIMIZING_TYPE_MINOR, PAIMON_OPTIMIZING_TYPE_MINOR);
    types.put("INTERNAL", "INTERNAL");
    return types;
  }

  @Override
  public List<OptimizingTaskInfo> getOptimizingTaskInfos(
      AmoroTable<?> amoroTable, String processId) {
    return doAs(amoroTable, () -> getOptimizingTaskInfosInternal(amoroTable, processId));
  }

  private List<OptimizingTaskInfo> getOptimizingTaskInfosInternal(
      AmoroTable<?> amoroTable, String processId) {
    long id = Long.parseLong(processId);
    FileStoreTable table = getTable(amoroTable);
    FileStore<?> store = table.store();
    Snapshot snapshot = null;
    try {
      SnapshotManager snapshotManager = store.snapshotManager();
      ManifestList manifestList = store.manifestListFactory().create();
      ManifestFile manifestFile = store.manifestFileFactory().create();
      try {
        snapshot = snapshotManager.tryGetSnapshot(id);
      } catch (java.io.FileNotFoundException missing) {
        return Collections.emptyList();
      }
      if (snapshot == null) {
        return Collections.emptyList();
      }
      Snapshot.CommitKind commitKind = snapshot.commitKind();
      if (commitKind == Snapshot.CommitKind.COMPACT) {
        List<ManifestFileMeta> manifestFileMetas = manifestList.readDeltaManifests(snapshot);

        // Group manifest entries by (partition, bucket) so distinct partitions sharing bucket-0
        // remain separate task units, and exclude in-place LSM level-upgraded files.
        Map<PartitionBucketKey, List<ManifestEntry>> entriesByBucket = new LinkedHashMap<>();

        for (ManifestFileMeta manifestFileMeta : manifestFileMetas) {
          List<ManifestEntry> compactManifestEntries =
              manifestFile.read(manifestFileMeta.fileName());
          for (ManifestEntry compactManifestEntry : compactManifestEntries) {
            PartitionBucketKey key =
                new PartitionBucketKey(
                    compactManifestEntry.partition(), compactManifestEntry.bucket());
            entriesByBucket.computeIfAbsent(key, k -> new ArrayList<>()).add(compactManifestEntry);
          }
        }

        // Convert (partition, bucket) statistics to OptimizingTaskInfo objects
        List<OptimizingTaskInfo> results = new ArrayList<>(entriesByBucket.size());
        int taskId = 1;
        for (Map.Entry<PartitionBucketKey, List<ManifestEntry>> entry :
            entriesByBucket.entrySet()) {
          PartitionBucketKey key = entry.getKey();
          List<ManifestEntry> bucketEntries = entry.getValue();
          Set<String> retainedNames = retainedUpgradeFileNames(bucketEntries);
          BucketStatistics stats = new BucketStatistics();
          for (ManifestEntry compactManifestEntry : bucketEntries) {
            String fileName = compactManifestEntry.file().fileName();
            if (fileName != null && retainedNames.contains(fileName)) {
              continue;
            }
            if (compactManifestEntry.kind() == FileKind.DELETE) {
              stats.removedFileCount++;
              stats.inputSize += compactManifestEntry.file().fileSize();
            } else {
              stats.addedFileCount++;
              stats.outputSize += compactManifestEntry.file().fileSize();
            }
          }

          // Create FilesStatistics for input and output
          FilesStatistics inputFiles =
              FilesStatistics.builder().addFiles(stats.inputSize, stats.removedFileCount).build();

          FilesStatistics outputFiles =
              FilesStatistics.builder().addFiles(stats.outputSize, stats.addedFileCount).build();

          // Create summary with bucket information
          Map<String, String> summary = new HashMap<>();
          summary.put("bucket", String.valueOf(key.bucket));
          summary.put("removed-files", String.valueOf(stats.removedFileCount));
          summary.put("added-files", String.valueOf(stats.addedFileCount));

          // Create OptimizingTaskInfo
          OptimizingTaskInfo taskInfo =
              new OptimizingTaskInfo(
                  null, // tableId
                  processId, // processId
                  taskId++, // taskId
                  formatTaskPartitionBucket(table, key), // partitionData
                  ProcessTaskStatus.SUCCESS, // status
                  0, // retryNum
                  "paimon-internal-optimizer", // optimizerToken
                  0, // threadId
                  snapshot.timeMillis(), // startTime
                  snapshot.timeMillis(), // endTime
                  0, // costTime (calculated automatically)
                  null, // failReason
                  inputFiles, // inputFiles
                  outputFiles, // outputFiles
                  summary, // summary
                  new HashMap<>() // properties
                  );

          results.add(taskInfo);
        }
        return results;
      }
      return Collections.emptyList();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Helper class to collect bucket-level statistics
  private static class BucketStatistics {
    int removedFileCount = 0;
    int addedFileCount = 0;
    long inputSize = 0;
    long outputSize = 0;

    public BucketStatistics() {}
  }

  /**
   * Identifies filenames retained across {@link FileKind#DELETE} and {@link FileKind#ADD} within a
   * single {@code (partition, bucket)} — i.e., in-place LSM level upgrades produced by {@code
   * AbstractCompactRewriter#upgrade} without physically rewriting data bytes (see Paimon 1.4.2
   * {@code AbstractCompactRewriter.java#L34-L36} and {@code MergeTreeWriter.java#L359-L365}).
   *
   * <p>Reference: <a
   * href="https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L359-L365">
   * MergeTreeWriter.java#L359-L365</a>
   */
  private static Set<String> retainedUpgradeFileNames(List<ManifestEntry> bucketEntries) {
    Set<String> deletedNames = new HashSet<>();
    Set<String> retainedNames = new HashSet<>();
    for (ManifestEntry entry : bucketEntries) {
      if (entry.kind() == FileKind.DELETE) {
        String fileName = entry.file().fileName();
        if (fileName != null) {
          deletedNames.add(fileName);
        }
      }
    }
    if (deletedNames.isEmpty()) {
      return Collections.emptySet();
    }
    for (ManifestEntry entry : bucketEntries) {
      if (entry.kind() != FileKind.DELETE) {
        String fileName = entry.file().fileName();
        if (fileName != null && deletedNames.contains(fileName)) {
          retainedNames.add(fileName);
        }
      }
    }
    return retainedNames;
  }

  private String formatTaskPartitionBucket(FileStoreTable table, PartitionBucketKey key) {
    List<String> partitionKeys = table.partitionKeys();
    if (partitionKeys == null || partitionKeys.isEmpty() || key.partition.getFieldCount() == 0) {
      return "bucket-" + key.bucket;
    }
    FileStore<?> store = table.store();
    FileStorePathFactory pathFactory = store == null ? null : store.pathFactory();
    if (pathFactory != null) {
      String partitionString = pathFactory.getPartitionString(key.partition);
      if (partitionString != null) {
        return canonicalPartitionBucket(key.partition, key.bucket, pathFactory);
      }
    }
    CoreOptions coreOptions = new CoreOptions(table.options());
    InternalRowPartitionComputer computer =
        new InternalRowPartitionComputer(
            coreOptions.partitionDefaultName(),
            table.rowType().project(partitionKeys),
            partitionKeys.toArray(new String[0]),
            coreOptions.legacyPartitionName());
    String generated =
        PartitionPathUtils.generatePartitionPath(computer.generatePartValues(key.partition));
    return generated.isEmpty()
        ? "bucket-" + key.bucket
        : generated.substring(0, generated.length() - 1) + "/bucket-" + key.bucket;
  }

  private static final class PartitionBucketKey {
    final BinaryRow partition;
    final int bucket;

    PartitionBucketKey(BinaryRow partition, int bucket) {
      this.partition = partition == null ? BinaryRow.EMPTY_ROW : partition.copy();
      this.bucket = bucket;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartitionBucketKey)) {
        return false;
      }
      PartitionBucketKey that = (PartitionBucketKey) o;
      return bucket == that.bucket && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partition, bucket);
    }
  }

  /** Resolved partition/bucket target used to push both filters into a single current Scan. */
  private static class PartitionTarget {
    final BinaryRow partition;
    final int bucket;

    PartitionTarget(BinaryRow partition, int bucket) {
      this.partition = partition;
      this.bucket = bucket;
    }
  }

  /** Mutable per partition/bucket accumulator; keeps only numbers, never file references. */
  private static class PartitionStats {
    int fileCount = 0;
    long fileSize = 0;
    long lastCommitTime = 0;
  }

  @Override
  public org.apache.commons.lang3.tuple.Pair<
          java.util.List<org.apache.amoro.table.tag.TableTagInfo>, Long>
      getTableTags(AmoroTable<?> table, org.apache.amoro.table.tag.TagQuery query) {
    return doAs(table, () -> PaimonTagReader.page((PaimonTable) table, getTable(table), query));
  }

  @Override
  public List<TagOrBranchInfo> getTableTags(AmoroTable<?> amoroTable) {
    return doAs(amoroTable, () -> PaimonTagReader.legacy(getTable(amoroTable)));
  }

  @Override
  public List<TagOrBranchInfo> getTableBranches(AmoroTable<?> amoroTable) {
    return doAs(amoroTable, () -> getTableBranchesInternal(amoroTable));
  }

  private List<TagOrBranchInfo> getTableBranchesInternal(AmoroTable<?> amoroTable) {
    FileStoreTable table = getTable(amoroTable);
    List<String> branches = table.branchManager().branches();
    List<TagOrBranchInfo> branchInfos =
        branches.stream()
            .map(name -> new TagOrBranchInfo(name, -1, -1, 0L, 0L, TagOrBranchInfo.BRANCH))
            .collect(Collectors.toList());
    branchInfos.add(TagOrBranchInfo.MAIN_BRANCH);
    return branchInfos;
  }

  @Override
  public List<ConsumerInfo> getTableConsumerInfos(AmoroTable<?> amoroTable) {
    PaimonSnapshotReadDeadline deadline = new PaimonSnapshotReadDeadline();
    return deadline.call(
        () -> doAs(amoroTable, () -> getTableConsumerInfosInternal(amoroTable, deadline)));
  }

  private List<ConsumerInfo> getTableConsumerInfosInternal(
      AmoroTable<?> amoroTable, PaimonSnapshotReadDeadline deadline) {
    FileStoreTable table = getTable(amoroTable);
    ConsumerManager consumerManager = new ConsumerManager(table.fileIO(), table.location());
    List<ConsumerInfo> consumerInfos = new ArrayList<>();
    deadline
        .call(consumerManager::consumers)
        .forEach(
            (consumerId, nextSnapshotId) -> {
              Snapshot snapshot =
                  consumerSnapshot(table.snapshotManager(), nextSnapshotId, deadline);
              if (snapshot == null && nextSnapshotId > Snapshot.FIRST_SNAPSHOT_ID) {
                snapshot = consumerSnapshot(table.snapshotManager(), nextSnapshotId - 1, deadline);
              }
              Snapshot selected = snapshot;
              AmoroSnapshotsOfTable amoroSnapshotsOfTable =
                  deadline.call(
                      () -> selected == null ? null : PaimonSnapshotMetadata.summary(selected));
              consumerInfos.add(
                  new ConsumerInfo(consumerId, nextSnapshotId, amoroSnapshotsOfTable));
            });
    return consumerInfos;
  }

  private Snapshot consumerSnapshot(
      SnapshotManager manager, long snapshotId, PaimonSnapshotReadDeadline deadline) {
    return deadline.call(
        () -> {
          if (snapshotId < Snapshot.FIRST_SNAPSHOT_ID) {
            throw new IllegalArgumentException("Invalid consumer snapshot ID");
          }
          if (!manager.snapshotExists(snapshotId)) {
            return null;
          }
          try {
            return manager.tryGetSnapshot(snapshotId);
          } catch (java.io.FileNotFoundException missing) {
            // Paimon 1.4.2 may lose the snapshot between exists() and tryGetSnapshot().
            // Catch only explicit absence before deadline.call wraps other failures.
            // https://github.com/apache/paimon/blob/release-1.4.2/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java
            return null;
          }
        });
  }

  /**
   * Builds the canonical partition identifier from a Paimon {@link BinaryRow} partition and bucket.
   *
   * <p>{@link FileStorePathFactory#getPartitionString(BinaryRow)} returns the partition path with a
   * trailing {@code /} for non-empty partitions and {@code ""} for an un-partitioned row. This
   * method drops exactly that one trailing slash (never a global replacement, so escaped partition
   * values are untouched) and appends a single {@code /bucket-<n>} separator, or just {@code
   * bucket-<n>} when there are no partition fields.
   *
   * <p>The partition list, file list and snapshot detail views all share this single helper so
   * their output format stays identical.
   */
  private String canonicalPartitionBucket(
      BinaryRow partition, int bucket, FileStorePathFactory fileStorePathFactory) {
    String partitionString = fileStorePathFactory.getPartitionString(partition);
    if (partitionString.isEmpty()) {
      return "bucket-" + bucket;
    }
    // generatePartitionPath always appends exactly one trailing '/', drop only that one.
    return partitionString.substring(0, partitionString.length() - 1) + "/bucket-" + bucket;
  }

  /**
   * Parses a request partition identifier into a {@link BinaryRow} partition plus bucket so the
   * scan can push both down via {@code withPartitionBucket}.
   *
   * <p>The parsing deliberately reuses Paimon's own utilities instead of hand-rolled string
   * splitting: the trailing {@code bucket-<n>} is recognised, the redundant separator before it is
   * collapsed (this is what accepts legacy double-slash input), and the remaining spec is recovered
   * through {@link PartitionPathUtils#extractPartitionSpecFromPath(Path)} and converted to a {@link
   * BinaryRow} with {@link InternalRowPartitionComputer#convertSpecToInternalRow}. Validation is
   * "BinaryRow constructed + spec field set equals the table partition fields" — it does NOT
   * require the regenerated string to match the input character-for-character, otherwise legitimate
   * legacy {@code //} input would be rejected.
   *
   * @return the resolved target, or {@code null} when the input cannot be mapped to a valid
   *     partition/bucket. Callers must return an empty file list in that case and must not fall
   *     back to an unfiltered full-table scan.
   */
  private PartitionTarget parsePartitionTarget(FileStore<?> store, String partition) {
    if (partition == null) {
      return null;
    }
    Matcher matcher = BUCKET_PATTERN.matcher(partition);
    if (!matcher.find()) {
      return null;
    }
    int bucket;
    try {
      bucket = Integer.parseInt(matcher.group(1));
    } catch (NumberFormatException e) {
      // Overflow / non-numeric; the regex already rejected non-digits, this guards huge values.
      return null;
    }
    // Drop the bucket segment and any separator slashes preceding it (handles legacy '//' input).
    String partitionPart = partition.substring(0, matcher.start());
    while (partitionPart.endsWith("/")) {
      partitionPart = partitionPart.substring(0, partitionPart.length() - 1);
    }
    if (partitionPart.isEmpty()) {
      return null;
    }
    try {
      LinkedHashMap<String, String> spec =
          PartitionPathUtils.extractPartitionSpecFromPath(new Path(partitionPart));
      RowType partitionType = store.partitionType();
      if (!spec.keySet().equals(new HashSet<>(partitionType.getFieldNames()))) {
        return null;
      }
      BinaryRow binaryPartition = toBinaryPartition(spec, store);
      return new PartitionTarget(binaryPartition, bucket);
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Converts a partition spec (field name {@code ->} external string value) into the internal
   * {@link BinaryRow} form used by the scan. Mirrors the package-private {@code
   * PartitionPredicate.createBinaryPartitions} using only public Paimon API: {@code
   * convertSpecToInternalRow} casts each value to its partition type and {@link
   * InternalRowSerializer#toBinaryRow} serialises a stable copy.
   */
  private BinaryRow toBinaryPartition(Map<String, String> spec, FileStore<?> store) {
    RowType partitionType = store.partitionType();
    String defaultPartValue = store.options().partitionDefaultName();
    GenericRow row =
        InternalRowPartitionComputer.convertSpecToInternalRow(
            spec, partitionType, defaultPartValue);
    return new InternalRowSerializer(partitionType).toBinaryRow(row).copy();
  }

  private String fullFilePath(FileStore<?> store, ManifestEntry manifestEntry) {
    return store
        .pathFactory()
        .createDataFilePathFactory(manifestEntry.partition(), manifestEntry.bucket())
        .toPath(manifestEntry.file())
        .toString();
  }

  private FileStoreTable getTable(AmoroTable<?> amoroTable) {
    return (FileStoreTable) amoroTable.originalTable();
  }

  private <T> T doAs(AmoroTable<?> amoroTable, Callable<T> callable) {
    if (amoroTable instanceof PaimonTable) {
      return ((PaimonTable) amoroTable).doAs(callable);
    }
    return call(callable);
  }

  private <T> T call(Callable<T> callable) {
    try {
      return callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          "Run with Paimon table descriptor authentication context failed.", e);
    }
  }

  private long getFileCreationTimeMillis(ManifestEntry manifestEntry) {
    // DataFileMeta.creationTimeEpochMillis() may mislead user about the creation time
    // See: https://github.com/apache/paimon/issues/7151
    return manifestEntry.file().creationTime().getMillisecond();
  }
}
