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

package org.apache.amoro.maintainer.api;

import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.ExecutorTask;
import org.apache.amoro.api.ExecutorTaskResult;
import org.apache.amoro.utils.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TableMaintainerExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintainerExecutor.class);
  private final AbstractTableMaintainerOperator abstractTableMaintainerOperator;
  private final TableMaintainerConfig config;

  public TableMaintainerExecutor(TableMaintainerConfig config) {
    this.config = config;
    this.abstractTableMaintainerOperator = new AbstractTableMaintainerOperator(config);
  }

  public TableMaintainerDTO getTableMaintainer() throws Exception {
    LOG.info(
        "Get Table Metadata: catalog:{},db:{},tableName:{},type:{}",
        config.getCatalog(),
        config.getDatabase(),
        config.getTable(),
        config.getType());
    ExecutorTask executorTask =
        abstractTableMaintainerOperator.callAms(
            client -> {
              return (client.ackTableMetadata(
                  config.getCatalog(), config.getDatabase(), config.getTable(), config.getType()));
            });
    Map<String, String> properties = executorTask.getServerConfig();
    LOG.info("find ams properties:{}", properties.size());
    byte[] taskInput = executorTask.getTaskInput();
    CatalogMeta catalogMeta = SerializationUtil.simpleDeserialize(taskInput);
    return new TableMaintainerDTO(properties == null ? new HashMap<>() : properties, catalogMeta);
  }

  public void sendTaskResultToAms(ExecutorTaskResult taskResult) throws Exception {
    abstractTableMaintainerOperator.callAms(
        client -> {
          client.completeTask(taskResult);
          return null;
        });
    LOG.info(
        "Send Table Maintainer Result: catalog:{},db:{},tableName:{},type:{},taskResult:{}",
        config.getCatalog(),
        config.getDatabase(),
        config.getTable(),
        config.getType(),
        taskResult.toString());
  }
}
