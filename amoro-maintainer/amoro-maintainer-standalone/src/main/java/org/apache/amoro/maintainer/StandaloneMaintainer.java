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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.ExecutorTaskResult;
import org.apache.amoro.config.ConfigHelpers;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.maintainer.api.TableMaintainerConfig;
import org.apache.amoro.maintainer.api.TableMaintainerDTO;
import org.apache.amoro.maintainer.api.TableMaintainerExecutor;
import org.apache.amoro.maintainer.iceberg.MaintainerMetricReport;
import org.apache.amoro.maintainer.iceberg.StandaloneMaintainerStarter;
import org.apache.amoro.maintainer.output.CleanOrphanOutPut;
import org.apache.amoro.optimizing.IcebergCleanOrphanInput;
import org.apache.amoro.optimizing.IcebergDanglingDeleteFilesInput;
import org.apache.amoro.optimizing.IcebergDeleteFilesOutput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;
import org.apache.amoro.optimizing.maintainer.IcebergTableMaintainerV2;
import org.apache.amoro.server.catalog.CatalogBuilder;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class StandaloneMaintainer {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneMaintainer.class);

  public static void main(String[] args) throws Exception {

    TableMaintainerConfig config = new TableMaintainerConfig(args);
    TableMaintainerExecutor executor = new TableMaintainerExecutor(config);
    TableMaintainerDTO amoroTable = executor.getTableMaintainer();
    CatalogMeta catalogMeta = amoroTable.getCatalogMeta();
    HashMap<String, String> properties = amoroTable.getProperties();
    Properties properties1 = new Properties();
    properties1.putAll(properties);
    Configurations serverConfig = ConfigHelpers.createConfiguration(properties1);
    LOG.info("GET AMS ServerConfig:{}", serverConfig);
    TableConfiguration configuration = ConfigUtils.parseTableConfig(properties);
    MaintainerMetricReport report = new MaintainerMetricReport(executor);
    ServerCatalog serverCatalog = CatalogBuilder.buildServerCatalog(catalogMeta, serverConfig);
    AmoroTable<?> amoroTableLoader =
        serverCatalog.loadTable(config.getDatabase(), config.getTable());
    IcebergTableMaintainerV2 icebergTableMaintainerV2 =
        StandaloneMaintainerStarter.ofTable(amoroTableLoader);

    IcebergExpireSnapshotsOutput icebergExpireSnapshotsOutput =
        icebergTableMaintainerV2.expireSnapshots(
            new IcebergExpireSnapshotInput(
                config.getDatabase(),
                StandaloneMaintainerStarter.asIcebergTable(amoroTableLoader),
                configuration.getSnapshotTTLMinutes(),
                configuration.getSnapshotMinCount(),
                new HashSet<>(),
                catalogMeta));
    ExecutorTaskResult executorTaskResult = new ExecutorTaskResult();
    executorTaskResult.setCatalog(config.getCatalog());
    executorTaskResult.setDatabase(config.getDatabase());
    executorTaskResult.setTable(config.getTable());
    executorTaskResult.setTableType("ExpireSnapshots");
    executorTaskResult.setStatus(100);
    executorTaskResult.setSummary(icebergExpireSnapshotsOutput.summary());

    report.reportMetrics(executorTaskResult);

    IcebergDeleteFilesOutput icebergDeleteFilesOutput =
        icebergTableMaintainerV2.cleanDanglingDeleteFiles(
            new IcebergDanglingDeleteFilesInput(
                config.getDatabase(),
                catalogMeta,
                StandaloneMaintainerStarter.asIcebergTable(amoroTableLoader),
                configuration.isDeleteDanglingDeleteFilesEnabled()));
    executorTaskResult.setTableType("DanglingDeleteFiles");
    executorTaskResult.setSummary(icebergDeleteFilesOutput.summary());
    report.reportMetrics(executorTaskResult);

    CleanOrphanOutPut cleanOrphanOutPut =
        icebergTableMaintainerV2.cleanOrphanFiles(
            new IcebergCleanOrphanInput(
                config.getDatabase(),
                StandaloneMaintainerStarter.asIcebergTable(amoroTableLoader),
                configuration.getOrphanExistingMinutes(),
                catalogMeta));
    executorTaskResult.setTableType("cleanOrphanFiles");
    executorTaskResult.setSummary(cleanOrphanOutPut.summary());
    report.reportMetrics(executorTaskResult);
  }
}
