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

package org.apache.amoro.maintainer;

import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.ExecutorTaskResult;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.TableMaintainerConfig;
import org.apache.amoro.maintainer.api.TableMaintainerDTO;
import org.apache.amoro.maintainer.api.TableMaintainerExecutor;
import org.apache.amoro.maintainer.iceberg.ConfigUtils;
import org.apache.amoro.maintainer.iceberg.SparkDanglingDeleteFilesCleaningFactory;
import org.apache.amoro.maintainer.iceberg.SparkDeleteOrphanFilesFactory;
import org.apache.amoro.maintainer.iceberg.SparkExpireSnapshotsFactory;
import org.apache.amoro.maintainer.output.CleanOrphanOutPut;
import org.apache.amoro.optimizing.IcebergCleanOrphanInput;
import org.apache.amoro.optimizing.IcebergDanglingDeleteFilesInput;
import org.apache.amoro.optimizing.IcebergDeleteFilesOutput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class SparkTableMaintainer {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTableMaintainer.class);

  public static void main(String[] args) throws Exception {
    TableMaintainerConfig config = new TableMaintainerConfig(args);
    TableMaintainerExecutor executor = new TableMaintainerExecutor(config);
    TableMaintainerDTO amoroTable = executor.getTableMaintainer();
    CatalogMeta catalogMeta = amoroTable.getCatalogMeta();
    HashMap<String, String> properties = amoroTable.getProperties();
    TableConfiguration configuration = ConfigUtils.parseTableConfig(properties);

    // Get Model : Native Iceberg / Mixed Iceberg / Paimon

    // TODO SparkContext Load Diff HadoopConfig

    // Get catalogMeta to Set Config with Diff Session Catalog

    SparkSession spark =
        SparkSession.builder()
            .appName(String.format("amoro-maintainer-%s-%s", config.getType(), "tableName"))
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config(
                "spark.sql.catalog.spark_catalog.uri",
                "thrift://txy-hn1-bigdata-hdp11-mhd-prd-11.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-02.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-03.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-04.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-05.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-06.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-07.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-08.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-09.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-10.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-01.myhll.cn:9083")
            .enableHiveSupport()
            .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    if (!jsc.getConf().getBoolean("spark.dynamicAllocation.enabled", false)) {
      LOG.warn(
          "To better utilize computing resources, it is recommended to enable 'spark.dynamicAllocation.enabled' "
              + "and set 'spark.dynamicAllocation.maxExecutors' equal to 'OPTIMIZER_EXECUTION_PARALLEL'");
    }

    Catalog sparkCatalog = Spark3Util.loadIcebergCatalog(spark, "spark_catalog");
    Table originalTable =
        sparkCatalog.loadTable(TableIdentifier.of(config.getDatabase(), config.getTable()));

    // DanglingDelete
    LOG.info(
        "DeleteDanglingDeleteFilesEnabled:{} getSnapshotTTLMinutes:{},",
        configuration.isDeleteDanglingDeleteFilesEnabled(),
        configuration.getSnapshotTTLMinutes());

    IcebergDanglingDeleteFilesInput danglingDeleteFilesInput =
        new IcebergDanglingDeleteFilesInput(
            config.getDatabase(),
            catalogMeta,
            originalTable,
            !configuration.isDeleteDanglingDeleteFilesEnabled());
    SparkDanglingDeleteFilesCleaningFactory factory =
        new SparkDanglingDeleteFilesCleaningFactory(spark);
    MaintainerExecutor<IcebergDeleteFilesOutput> factoryExecutor =
        factory.createExecutor(danglingDeleteFilesInput);
    IcebergDeleteFilesOutput icebergDeleteFilesOutput = factoryExecutor.execute();
    ExecutorTaskResult taskResult = new ExecutorTaskResult();
    taskResult.setSummary(icebergDeleteFilesOutput.summary());
    executor.sendTaskResultToAms(taskResult);

    originalTable.refresh();

    SparkExpireSnapshotsFactory expireSnapshotsFactory = new SparkExpireSnapshotsFactory(spark);

    MaintainerExecutor<IcebergExpireSnapshotsOutput> snapshotsFactoryExecutor =
        expireSnapshotsFactory.createExecutor(
            new IcebergExpireSnapshotInput(
                config.getDatabase(),
                originalTable,
                configuration.getSnapshotTTLMinutes(),
                configuration.getSnapshotMinCount(),
                new HashSet<>(),
                catalogMeta));
    IcebergExpireSnapshotsOutput expireSnapshotsOutput = snapshotsFactoryExecutor.execute();
    taskResult.setSummary(expireSnapshotsOutput.summary());
    executor.sendTaskResultToAms(taskResult);

    originalTable.refresh();

    SparkDeleteOrphanFilesFactory deleteOrphanFilesFactory =
        new SparkDeleteOrphanFilesFactory(spark);
    MaintainerExecutor<CleanOrphanOutPut> deleteOrphanFilesFactoryExecutor =
        deleteOrphanFilesFactory.createExecutor(
            new IcebergCleanOrphanInput(
                config.getDatabase(), originalTable, TimeUnit.DAYS.toMinutes(1), catalogMeta));
    CleanOrphanOutPut cleanOrphanOutPut = deleteOrphanFilesFactoryExecutor.execute();
    LOG.info("cleanOrphanOutPut :{}", cleanOrphanOutPut);
    taskResult.setSummary(cleanOrphanOutPut.summary());
    executor.sendTaskResultToAms(taskResult);

    originalTable.refresh();

    jsc.stop();
  }
}
