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
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.config.ConfigHelpers;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.TableMaintainerConfig;
import org.apache.amoro.maintainer.api.TableMaintainerDTO;
import org.apache.amoro.maintainer.api.TableMaintainerExecutor;
import org.apache.amoro.maintainer.output.CleanOrphanOutPut;
import org.apache.amoro.maintainer.output.ExpireSnapshotsOutput;
import org.apache.amoro.optimizing.IcebergCleanOrphanInput;
import org.apache.amoro.optimizing.IcebergDanglingDeleteFilesInput;
import org.apache.amoro.optimizing.IcebergDeleteFilesOutput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.maintainer.CleanOrphanFilesFactory;
import org.apache.amoro.optimizing.maintainer.DanglingDeleteFilesCleaningFactory;
import org.apache.amoro.optimizing.maintainer.ExpireSnapshotsFactory;
import org.apache.amoro.server.catalog.CatalogBuilder;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.iceberg.Table;
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

    ServerCatalog serverCatalog = CatalogBuilder.buildServerCatalog(catalogMeta, serverConfig);
    AmoroTable<?> amoroTableLoader =
        serverCatalog.loadTable(config.getDatabase(), config.getTable());
    TableFormat format = amoroTableLoader.format();
    if (TableFormat.ICEBERG.equals(format)) {
      Table table = (Table) amoroTableLoader.originalTable();
      ExpireSnapshotsFactory expireSnapshotsFactory = new ExpireSnapshotsFactory();
      MaintainerExecutor<ExpireSnapshotsOutput> snapshotsFactoryExecutor =
          expireSnapshotsFactory.createExecutor(
              new IcebergExpireSnapshotInput(
                  config.getDatabase(),
                  table,
                  configuration.getSnapshotTTLMinutes(),
                  configuration.getSnapshotMinCount(),
                  new HashSet<>(),
                  catalogMeta));
      ExpireSnapshotsOutput snapshotsOutput = snapshotsFactoryExecutor.execute();
      LOG.info("snapshotsOutput : {}", snapshotsOutput);
      table.refresh();

      DanglingDeleteFilesCleaningFactory danglingDeleteFilesCleaningFactory =
          new DanglingDeleteFilesCleaningFactory();
      MaintainerExecutor<IcebergDeleteFilesOutput> cleaningFactoryExecutor =
          danglingDeleteFilesCleaningFactory.createExecutor(
              new IcebergDanglingDeleteFilesInput(
                  config.getDatabase(),
                  catalogMeta,
                  table,
                  configuration.isDeleteDanglingDeleteFilesEnabled(),
                  1L));
      IcebergDeleteFilesOutput icebergDeleteFilesOutput = cleaningFactoryExecutor.execute();
      LOG.info("icebergDeleteFilesOutput : {}", icebergDeleteFilesOutput);
      table.refresh();

      CleanOrphanFilesFactory cleanOrphanFilesFactory = new CleanOrphanFilesFactory();
      MaintainerExecutor<CleanOrphanOutPut> cleanOrphanFilesFactoryExecutor =
          cleanOrphanFilesFactory.createExecutor(
              new IcebergCleanOrphanInput(
                  config.getDatabase(),
                  table,
                  configuration.getOrphanExistingMinutes(),
                  catalogMeta));
      CleanOrphanOutPut cleanOrphanOutPut = cleanOrphanFilesFactoryExecutor.execute();
      LOG.info("cleanOrphanOutPut : {}", cleanOrphanOutPut);
      table.refresh();

    } else {
      throw new RuntimeException("not support format with table maintainer");
    }
  }
}
