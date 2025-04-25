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

import org.apache.amoro.OptimizerTableMaintainerProperties;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.Serializable;

public class TableMaintainerConfig implements Serializable {

  @Option(
      name = "-a",
      aliases = "--" + OptimizerTableMaintainerProperties.AMS_OPTIMIZER_URI,
      usage = "The ams url",
      required = true)
  private String amsUrl;

  @Option(
      name = "-p",
      aliases = "--" + OptimizerTableMaintainerProperties.OPTIMIZER_EXECUTION_PARALLEL,
      usage = "Optimizer execution parallel",
      required = true)
  private int executionParallel;

  @Option(
      name = "-type",
      aliases = "--" + OptimizerTableMaintainerProperties.TABLE_MAINTAINER_TYPE,
      usage = "Table maintainer type,such as ICEBERG/PAIMON",
      required = true)
  private String type;

  @Option(
      name = "-c",
      aliases = "--" + OptimizerTableMaintainerProperties.OPTIMIZER_CATALOG,
      usage = "Ams Table Catalog",
      required = true)
  private String catalog;

  @Option(
      name = "-d",
      aliases = "--" + OptimizerTableMaintainerProperties.OPTIMIZER_DATABASE,
      usage = "Ams Table Database",
      required = true)
  private String database;

  @Option(
      name = "-t",
      aliases = "--" + OptimizerTableMaintainerProperties.OPTIMIZER_TABLE,
      usage = "Ams Table Name",
      required = true)
  private String table;

  public TableMaintainerConfig() {}

  public TableMaintainerConfig(String[] args) throws CmdLineException {
    CmdLineParser parser = new CmdLineParser(this);
    parser.parseArgument(args);
  }

  public String getAmsUrl() {
    return amsUrl;
  }

  public void setAmsUrl(String amsUrl) {
    this.amsUrl = amsUrl;
  }

  public int getExecutionParallel() {
    return executionParallel;
  }

  public void setExecutionParallel(int executionParallel) {
    this.executionParallel = executionParallel;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("amsUrl", amsUrl)
        .append("executionParallel", executionParallel)
        .append("type", type)
        .append("catalog", catalog)
        .append("database", database)
        .append("table", table)
        .toString();
  }
}
