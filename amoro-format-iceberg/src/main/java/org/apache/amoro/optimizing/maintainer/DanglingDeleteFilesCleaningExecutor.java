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
import org.apache.amoro.api.CommitMetaProducer;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.MaintainerType;
import org.apache.amoro.optimizing.IcebergDanglingDeleteFilesInput;
import org.apache.amoro.optimizing.IcebergDeleteFilesOutput;
import org.apache.amoro.optimizing.OptimizingTaskSummary;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Clean table dangling delete files,copy from ams */
public class DanglingDeleteFilesCleaningExecutor
    implements MaintainerExecutor<IcebergDeleteFilesOutput> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DanglingDeleteFilesCleaningExecutor.class);

  private final String database;
  private final CatalogMeta catalogMeta;

  protected Table table;

  public DanglingDeleteFilesCleaningExecutor(IcebergDanglingDeleteFilesInput input) {
    this.database = input.getDatabase();
    this.catalogMeta = input.getCatalogMeta();
    this.table = input.getTable();
  }

  @Override
  public IcebergDeleteFilesOutput execute() {
    return clearInternalTableDanglingDeleteFiles();
  }

  @Override
  public MaintainerType type() {
    return MaintainerType.DANGLING_DELETE_FILES;
  }

  private IcebergDeleteFilesOutput clearInternalTableDanglingDeleteFiles() {
    long startTime = System.currentTimeMillis();
    Set<DeleteFile> danglingDeleteFiles = IcebergTableUtil.getDanglingDeleteFiles(table);
    if (danglingDeleteFiles.isEmpty()) {
      return new IcebergDeleteFilesOutput(
          catalogMeta.getCatalogName(),
          database,
          table.name(),
          TableFormat.ICEBERG.name(),
          System.currentTimeMillis(),
          new DeleteFile[0],
          new HashMap<>());
    }
    RewriteFiles rewriteFiles = table.newRewrite();
    rewriteFiles.rewriteFiles(
        Collections.emptySet(),
        danglingDeleteFiles,
        Collections.emptySet(),
        Collections.emptySet());
    try {
      rewriteFiles.set(
          org.apache.amoro.op.SnapshotSummary.SNAPSHOT_PRODUCER,
          CommitMetaProducer.CLEAN_DANGLING_DELETE.name());
      rewriteFiles.commit();
    } catch (ValidationException e) {
      LOG.warn("Iceberg RewriteFiles commit failed on clear danglingDeleteFiles, but ignore", e);
      return new IcebergDeleteFilesOutput(
          catalogMeta.getCatalogName(),
          database,
          table.name(),
          TableFormat.ICEBERG.name(),
          System.currentTimeMillis(),
          new DeleteFile[0],
          new HashMap<>());
    }
    long duration = System.currentTimeMillis() - startTime;
    Map<String, String> summary =
        resolverSummary(new ArrayList<>(), new ArrayList<>(danglingDeleteFiles), duration);
    return new IcebergDeleteFilesOutput(
        catalogMeta.getCatalogName(),
        database,
        table.name(),
        TableFormat.ICEBERG.name(),
        System.currentTimeMillis(),
        danglingDeleteFiles.toArray(new DeleteFile[0]),
        summary);
  }

  private Map<String, String> resolverSummary(
      List<DataFile> dataFiles, List<DeleteFile> deleteFiles, long duration) {
    int dataFileCnt = 0;
    long dataFileTotalSize = 0;
    int eqDeleteFileCnt = 0;
    long eqDeleteFileTotalSize = 0;
    int posDeleteFileCnt = 0;
    long posDeleteFileTotalSize = 0;
    if (dataFiles != null) {
      for (DataFile dataFile : dataFiles) {
        dataFileCnt++;
        dataFileTotalSize += dataFile.fileSizeInBytes();
      }
    }
    if (deleteFiles != null) {
      for (DeleteFile deleteFile : deleteFiles) {
        if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
          eqDeleteFileCnt++;
          eqDeleteFileTotalSize += deleteFile.fileSizeInBytes();
        } else {
          posDeleteFileCnt++;
          posDeleteFileTotalSize += deleteFile.fileSizeInBytes();
        }
      }
    }

    OptimizingTaskSummary summary = new OptimizingTaskSummary();
    summary.setDataFileCnt(dataFileCnt);
    summary.setDataFileTotalSize(dataFileTotalSize);
    summary.setEqDeleteFileCnt(eqDeleteFileCnt);
    summary.setEqDeleteFileTotalSize(eqDeleteFileTotalSize);
    summary.setPosDeleteFileCnt(posDeleteFileCnt);
    summary.setPosDeleteFileTotalSize(posDeleteFileTotalSize);
    summary.setExecuteDuration(duration);

    return summary.getSummary();
  }
}
