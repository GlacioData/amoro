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

import java.util.HashMap;
import java.util.Map;

public class BaseMaintainerOutput implements TableMaintainerOptimizing.MaintainerOutput {

  private String catalog;
  private String database;
  private String table;
  private String type;
  private Long lastTime;

  public BaseMaintainerOutput(
      String catalog, String database, String table, String type, Long lastTime) {
    this.catalog = catalog;
    this.database = database;
    this.table = table;
    this.type = type;
    this.lastTime = lastTime;
  }

  @Override
  public Map<String, String> summary() {
    HashMap<String, String> summary = new HashMap<>(8);
    summary.put("catalog", catalog);
    summary.put("database", database);
    summary.put("table", table);
    summary.put("type", type);
    return summary;
  }
}
