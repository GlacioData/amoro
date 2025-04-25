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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

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
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.spark.SparkDeleteFile;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Copy form iceberg-spark-connector 1.8.X
 *
 * <p>Remove DV Files Clean
 */
public class RemoveDanglingDeleteFilesSparkExecutor
    implements MaintainerExecutor<IcebergDeleteFilesOutput> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RemoveDanglingDeleteFilesSparkExecutor.class);

  private final String database;
  private final CatalogMeta catalogMeta;

  private final Table table;
  private final SparkSession session;

  public RemoveDanglingDeleteFilesSparkExecutor(
      IcebergDanglingDeleteFilesInput input, SparkSession sparkSession) {
    this.database = input.getDatabase();
    this.catalogMeta = input.getCatalogMeta();
    this.table = input.getTable();
    this.session = sparkSession;
  }

  @Override
  public IcebergDeleteFilesOutput execute() {
    long startTime = System.currentTimeMillis();
    RewriteFiles rewriteFiles = table.newRewrite();
    List<DeleteFile> danglingDeletes = findDanglingDeletes();
    if (danglingDeletes.isEmpty()) {
      return new IcebergDeleteFilesOutput(
          catalogMeta.getCatalogName(),
          database,
          table.name(),
          TableFormat.ICEBERG.name(),
          System.currentTimeMillis(),
          new DeleteFile[0],
          new HashMap<>());
    }
    for (DeleteFile danglingDelete : danglingDeletes) {
      rewriteFiles.deleteFile(danglingDelete);
    }
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
    Map<String, String> summary = resolverSummary(new ArrayList<>(), danglingDeletes, duration);
    return new IcebergDeleteFilesOutput(
        catalogMeta.getCatalogName(),
        database,
        table.name(),
        TableFormat.ICEBERG.name(),
        System.currentTimeMillis(),
        danglingDeletes.toArray(new DeleteFile[0]),
        summary);
  }

  @Override
  public MaintainerType type() {
    return null;
  }

  /**
   * Dangling delete files can be identified with following steps
   *
   * <ol>
   *   <li>Group data files by partition keys and find the minimum data sequence number in each
   *       group.
   *   <li>Left outer join delete files with partition-grouped data files on partition keys.
   *   <li>Find dangling deletes by comparing each delete file's sequence number to its partition's
   *       minimum data sequence number.
   *   <li>Collect results row to driver and use {@link SparkDeleteFile SparkDeleteFile} to wrap
   *       rows to valid delete files
   * </ol>
   */
  private List<DeleteFile> findDanglingDeletes() {
    Dataset<Row> minSequenceNumberByPartition =
        loadMetadataTable(table, MetadataTableType.ENTRIES)
            // find live data files
            .filter("data_file.content == 0 AND status < 2")
            .selectExpr(
                "data_file.partition as partition",
                "data_file.spec_id as spec_id",
                "sequence_number")
            .groupBy("partition", "spec_id")
            .agg(min("sequence_number"))
            .toDF("grouped_partition", "grouped_spec_id", "min_data_sequence_number");

    Dataset<Row> deleteEntries =
        loadMetadataTable(table, MetadataTableType.ENTRIES)
            // find live delete files
            .filter("data_file.content != 0 AND status < 2");

    Column joinOnPartition =
        deleteEntries
            .col("data_file.spec_id")
            .equalTo(minSequenceNumberByPartition.col("grouped_spec_id"))
            .and(
                deleteEntries
                    .col("data_file.partition")
                    .equalTo(minSequenceNumberByPartition.col("grouped_partition")));

    Column filterOnDanglingDeletes =
        col("min_data_sequence_number")
            // delete files without any data files in partition
            .isNull()
            // position delete files without any applicable data files in partition
            .or(
                col("data_file.content")
                    .equalTo("1")
                    .and(col("sequence_number").$less(col("min_data_sequence_number"))))
            // equality delete files without any applicable data files in the partition
            .or(
                col("data_file.content")
                    .equalTo("2")
                    .and(col("sequence_number").$less$eq(col("min_data_sequence_number"))));

    Dataset<Row> danglingDeletes =
        deleteEntries
            .join(minSequenceNumberByPartition, joinOnPartition, "left")
            .filter(filterOnDanglingDeletes)
            .select("data_file.*");
    return danglingDeletes.collectAsList().stream()
        // map on driver because SparkDeleteFile is not serializable
        .map(row -> deleteFileWrapper(danglingDeletes.schema(), row))
        .collect(Collectors.toList());
  }

  protected Dataset<Row> loadMetadataTable(Table table, MetadataTableType type) {
    return SparkTableUtil.loadMetadataTable(session, table, type);
  }

  private DeleteFile deleteFileWrapper(StructType sparkFileType, Row row) {
    int specId = row.getInt(row.fieldIndex("spec_id"));
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));
    // Set correct spec id
    Types.StructType projection = DataFile.getType(table.specs().get(specId).partitionType());
    return new SparkDeleteFile(combinedFileType, projection, sparkFileType).wrap(row);
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
