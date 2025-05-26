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

package org.apache.amoro.optimizer.spark;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.api.ExecutorTask;
import org.apache.amoro.optimizer.common.AbstractTableMaintainerOperator;
import org.apache.amoro.optimizer.common.TableMaintainerConfig;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkTableMaintainerExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTableMaintainerExecutor.class);
  private final JavaSparkContext jsc;
  private final AbstractTableMaintainerOperator abstractTableMaintainerOperator;
  private final TableMaintainerConfig config;

  public SparkTableMaintainerExecutor(JavaSparkContext jsc, TableMaintainerConfig config) {
    this.jsc = jsc;
    this.config = config;
    this.abstractTableMaintainerOperator = new AbstractTableMaintainerOperator(config);
  }

  public AmoroTable<?> getAmoroTable() throws Exception {
    ExecutorTask executorTask =
        abstractTableMaintainerOperator.callAms(
            client -> {
              return (client.ackTableMetadata(
                  config.getDatabase(), config.getCatalog(), config.getTable(), config.getType()));
            });
    AmoroTable<?> amoroTable = SerializationUtil.simpleDeserialize(executorTask.getTaskInput());
    return amoroTable;
  }
}
