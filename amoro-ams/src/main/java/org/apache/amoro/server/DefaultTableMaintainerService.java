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

package org.apache.amoro.server;

import org.apache.amoro.api.AmoroException;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.ExecutorTask;
import org.apache.amoro.api.ExecutorTaskId;
import org.apache.amoro.api.ExecutorTaskResult;
import org.apache.amoro.api.MaintainerService;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.shade.thrift.org.apache.thrift.TException;
import org.apache.amoro.utils.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTableMaintainerService implements MaintainerService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTableMaintainerService.class);

  private final CatalogManager catalogManager;
  private final Configurations serviceConfig;

  public DefaultTableMaintainerService(
      Configurations serviceConfig, CatalogManager catalogManager) {
    this.serviceConfig = serviceConfig;
    this.catalogManager = catalogManager;
  }

  @Override
  public void ping() throws TException {}

  @Override
  public ExecutorTask ackTableMetadata(String catalog, String db, String tableName, String type)
      throws AmoroException, TException {
    LOG.info(
        "Get Table Metadata: catalog:{},db:{},tableName:{},type:{}", catalog, db, tableName, type);
    CatalogMeta catalogMeta = catalogManager.getCatalogMeta(catalog);
    return extractProtocolTask(new ExecutorTaskId(System.currentTimeMillis() + 1, 1), catalogMeta);
  }

  @Override
  public void completeTask(String type, ExecutorTaskResult taskResult)
      throws AmoroException, TException {
    LOG.info("Completing task {} with result {}", taskResult.getTaskId(), taskResult);
  }

  public ExecutorTask extractProtocolTask(ExecutorTaskId taskId, CatalogMeta catalogMeta) {
    ExecutorTask optimizingTask = new ExecutorTask(taskId);
    optimizingTask.setTaskInput(SerializationUtil.simpleSerialize(catalogMeta));
    optimizingTask.setProperties(serviceConfig.toMap());
    return optimizingTask;
  }
}
