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

import org.apache.amoro.TableFormat;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.io.AuthenticatedFileIO;
import org.apache.amoro.io.AuthenticatedFileIOs;
import org.apache.amoro.io.PathInfo;
import org.apache.amoro.io.SupportsFileSystemOperations;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.MaintainerType;
import org.apache.amoro.maintainer.output.CleanOrphanOutPut;
import org.apache.amoro.optimizing.IcebergCleanOrphanInput;
import org.apache.amoro.shade.guava32.com.google.common.base.Strings;
import org.apache.amoro.shade.guava32.com.google.common.collect.Iterables;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.utils.CatalogUtil;
import org.apache.amoro.utils.TableFileUtil;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CleanOrphanFilesExecutor implements MaintainerExecutor<CleanOrphanOutPut> {

  private static final Logger LOG = LoggerFactory.getLogger(CleanOrphanFilesExecutor.class);
  private final CatalogMeta catalogMeta;
  private final String database;
  protected Table table;
  protected Long orphanExistingMinutes;

  public static final String METADATA_FOLDER_NAME = "metadata";
  public static final String DATA_FOLDER_NAME = "data";
  // same as org.apache.iceberg.flink.sink.IcebergFilesCommitter#FLINK_JOB_ID
  public static final String FLINK_JOB_ID = "flink.job-id";
  // same as org.apache.iceberg.flink.sink.IcebergFilesCommitter#MAX_COMMITTED_CHECKPOINT_ID
  public static final String FLINK_MAX_COMMITTED_CHECKPOINT_ID =
      "flink.max-committed-checkpoint-id";

  public static final String EXPIRE_TIMESTAMP_MS = "TIMESTAMP_MS";
  public static final String EXPIRE_TIMESTAMP_S = "TIMESTAMP_S";

  public CleanOrphanFilesExecutor(IcebergCleanOrphanInput input) {
    this.table = input.getTable();
    this.database = input.getDatabase();
    this.orphanExistingMinutes = input.getOrphanExistingMinutes();
    this.catalogMeta = input.getCatalogMeta();
  }

  @Override
  public CleanOrphanOutPut execute() {
    long keepTime = orphanExistingMinutes * 60 * 1000;

    CleanOrphanOutPut cleanContentFilesOutPut =
        cleanContentFiles(System.currentTimeMillis() - keepTime);

    // refresh
    table.refresh();

    // clear metadata files
    CleanOrphanOutPut cleanMetadataOutPut = cleanMetadata(System.currentTimeMillis() - keepTime);

    return new CleanOrphanOutPut(
        catalogMeta.getCatalogName(),
        database,
        table.name(),
        TableFormat.ICEBERG.name(),
        System.currentTimeMillis(),
        cleanContentFilesOutPut.getExpectedFileCount() + cleanMetadataOutPut.getExpectedFileCount(),
        cleanContentFilesOutPut.getDeletedFileCount() + cleanMetadataOutPut.getDeletedFileCount());
  }

  @Override
  public MaintainerType type() {
    return MaintainerType.CLEAN_ORPHAN_FILES;
  }

  protected CleanOrphanOutPut cleanContentFiles(long lastTime) {
    // For clean data files, should getRuntime valid files in the base store and the change store,
    // so acquire in advance
    // to prevent repeated acquisition
    Set<String> validFiles = orphanFileCleanNeedToExcludeFiles();
    LOG.info("{} start cleaning orphan files in content", table.name());
    return clearInternalTableContentsFiles(lastTime, validFiles);
  }

  protected Set<String> orphanFileCleanNeedToExcludeFiles() {
    return Sets.union(
        IcebergTableUtil.getAllContentFilePath(table),
        IcebergTableUtil.getAllStatisticsFilePath(table));
  }

  private CleanOrphanOutPut clearInternalTableContentsFiles(long lastTime, Set<String> exclude) {
    String dataLocation = table.location() + File.separator + DATA_FOLDER_NAME;
    int expected = 0, deleted = 0;

    try (AuthenticatedFileIO io = fileIO()) {
      // listPrefix will not return the directory and the orphan file clean should clean the empty
      // dir.
      if (io.supportFileSystemOperations()) {
        SupportsFileSystemOperations fio = io.asFileSystemIO();
        Set<PathInfo> directories = new HashSet<>();
        Set<String> filesToDelete =
            deleteInvalidFilesInFs(fio, dataLocation, lastTime, exclude, directories);
        expected = filesToDelete.size();
        deleted = TableFileUtil.deleteFiles(io, filesToDelete);
        /* delete empty directories */
        deleteEmptyDirectories(fio, directories, lastTime, exclude);
      } else if (io.supportPrefixOperations()) {
        SupportsPrefixOperations pio = io.asPrefixFileIO();
        Set<String> filesToDelete =
            deleteInvalidFilesByPrefix(pio, dataLocation, lastTime, exclude);
        expected = filesToDelete.size();
        deleted = TableFileUtil.deleteFiles(io, filesToDelete);
      } else {
        LOG.warn(
            String.format(
                "Table %s doesn't support a fileIo with listDirectory or listPrefix, so skip clear files.",
                table.name()));
      }
    }

    final int finalExpected = expected;
    final int finalDeleted = deleted;
    return new CleanOrphanOutPut(
        catalogMeta.getCatalogName(),
        database,
        table.name(),
        TableFormat.ICEBERG.name(),
        System.currentTimeMillis(),
        finalExpected,
        finalDeleted);
  }

  private Set<String> deleteInvalidFilesInFs(
      SupportsFileSystemOperations fio,
      String location,
      long lastTime,
      Set<String> excludes,
      Set<PathInfo> directories) {
    if (!fio.exists(location)) {
      return Collections.emptySet();
    }

    Set<String> filesToDelete = new HashSet<>();
    for (PathInfo p : fio.listDirectory(location)) {
      String uriPath = TableFileUtil.getUriPath(p.location());
      if (p.isDirectory()) {
        directories.add(p);
        filesToDelete.addAll(
            deleteInvalidFilesInFs(fio, p.location(), lastTime, excludes, directories));
      } else {
        String parentLocation = TableFileUtil.getParent(p.location());
        String parentUriPath = TableFileUtil.getUriPath(parentLocation);
        if (!excludes.contains(uriPath)
            && !excludes.contains(parentUriPath)
            && p.createdAtMillis() < lastTime) {
          filesToDelete.add(p.location());
        }
      }
    }
    return filesToDelete;
  }

  private void deleteEmptyDirectories(
      SupportsFileSystemOperations fio, Set<PathInfo> paths, long lastTime, Set<String> excludes) {
    paths.forEach(
        p -> {
          if (fio.exists(p.location())
              && !p.location().endsWith(METADATA_FOLDER_NAME)
              && !p.location().endsWith(DATA_FOLDER_NAME)
              && p.createdAtMillis() < lastTime
              && fio.isEmptyDirectory(p.location())) {
            TableFileUtil.deleteEmptyDirectory(fio, p.location(), excludes);
          }
        });
  }

  private Set<String> deleteInvalidFilesByPrefix(
      SupportsPrefixOperations pio, String prefix, long lastTime, Set<String> excludes) {
    Set<String> filesToDelete = new HashSet<>();
    for (FileInfo fileInfo : pio.listPrefix(prefix)) {
      String uriPath = TableFileUtil.getUriPath(fileInfo.location());
      if (!excludes.contains(uriPath) && fileInfo.createdAtMillis() < lastTime) {
        filesToDelete.add(fileInfo.location());
      }
    }

    return filesToDelete;
  }

  protected CleanOrphanOutPut cleanMetadata(long lastTime) {
    LOG.info("{} start clean metadata files", table.name());
    return clearInternalTableMetadata(lastTime);
  }

  private CleanOrphanOutPut clearInternalTableMetadata(long lastTime) {
    Set<String> validFiles = getValidMetadataFiles(table);
    LOG.info("{} table getRuntime {} valid files", table.name(), validFiles.size());
    Pattern excludeFileNameRegex = getExcludeFileNameRegex(table);
    LOG.info(
        "{} table getRuntime exclude file name pattern {}", table.name(), excludeFileNameRegex);
    String metadataLocation = table.location() + File.separator + METADATA_FOLDER_NAME;
    LOG.info("start orphan files clean in {}", metadataLocation);
    Integer deleted = 0;
    try (AuthenticatedFileIO io = fileIO()) {
      if (io.supportPrefixOperations()) {
        SupportsPrefixOperations pio = io.asPrefixFileIO();
        Set<String> filesToDelete =
            deleteInvalidMetadataFile(
                pio, metadataLocation, lastTime, validFiles, excludeFileNameRegex);
        deleted += TableFileUtil.deleteFiles(io, filesToDelete);
      }
    }
    return new CleanOrphanOutPut(
        catalogMeta.getCatalogName(),
        database,
        table.name(),
        TableFormat.ICEBERG.name(),
        System.currentTimeMillis(),
        0,
        deleted);
  }

  private static Set<String> getValidMetadataFiles(Table internalTable) {
    String tableName = internalTable.name();
    Set<String> validFiles = new HashSet<>();
    Iterable<Snapshot> snapshots = internalTable.snapshots();
    int size = Iterables.size(snapshots);
    LOG.info("{} getRuntime {} snapshots to scan", tableName, size);
    for (Snapshot snapshot : snapshots) {
      String manifestListLocation = snapshot.manifestListLocation();
      validFiles.add(TableFileUtil.getUriPath(manifestListLocation));
    }
    // valid data files
    Set<String> allManifestFiles = IcebergTableUtil.getAllManifestFiles(internalTable);
    allManifestFiles.forEach(f -> validFiles.add(TableFileUtil.getUriPath(f)));

    Stream.of(
            ReachableFileUtil.metadataFileLocations(internalTable, false).stream(),
            ReachableFileUtil.statisticsFilesLocations(internalTable).stream(),
            Stream.of(ReachableFileUtil.versionHintLocation(internalTable)))
        .reduce(Stream::concat)
        .orElse(Stream.empty())
        .map(TableFileUtil::getUriPath)
        .forEach(validFiles::add);

    return validFiles;
  }

  private Set<String> deleteInvalidMetadataFile(
      SupportsPrefixOperations pio,
      String location,
      long lastTime,
      Set<String> exclude,
      Pattern excludeRegex) {
    Set<String> filesToDelete = new HashSet<>();
    for (FileInfo fileInfo : pio.listPrefix(location)) {
      String uriPath = TableFileUtil.getUriPath(fileInfo.location());
      if (!exclude.contains(uriPath)
          && fileInfo.createdAtMillis() < lastTime
          && (excludeRegex == null
              || !excludeRegex.matcher(TableFileUtil.getFileName(fileInfo.location())).matches())) {
        filesToDelete.add(fileInfo.location());
      }
    }

    return filesToDelete;
  }

  private static Pattern getExcludeFileNameRegex(Table table) {
    String latestFlinkJobId = null;
    for (Snapshot snapshot : table.snapshots()) {
      String flinkJobId = snapshot.summary().get(FLINK_JOB_ID);
      if (!Strings.isNullOrEmpty(flinkJobId)) {
        latestFlinkJobId = flinkJobId;
      }
    }
    if (latestFlinkJobId != null) {
      // file name starting with flink.job-id should not be deleted
      return Pattern.compile(latestFlinkJobId + ".*");
    }
    return null;
  }

  protected AuthenticatedFileIO fileIO() {
    TableMetaStore tableMetaStore = CatalogUtil.buildMetaStore(catalogMeta);
    return AuthenticatedFileIOs.buildAdaptIcebergFileIO(tableMetaStore, table.io());
  }
}
