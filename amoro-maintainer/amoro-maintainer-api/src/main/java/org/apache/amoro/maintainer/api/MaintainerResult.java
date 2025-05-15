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

import org.apache.amoro.StateField;
import org.apache.amoro.TableFormat;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;

public class MaintainerResult {

  public Long id;
  @StateField public String catalogName;
  @StateField public String dbName;
  @StateField public String tableName;
  @StateField public TableFormat tableFormat;
  @StateField public String maintainerType;
  @StateField private TableMaintainer.Status status = TableMaintainer.Status.IDLE;
  @StateField public Map<String, String> summary;
  @StateField private long createdTime;
  @StateField private long updatedTime;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableFormat getTableFormat() {
    return tableFormat;
  }

  public void setTableFormat(TableFormat tableFormat) {
    this.tableFormat = tableFormat;
  }

  public TableMaintainer.Status getStatus() {
    return status;
  }

  public void setStatus(TableMaintainer.Status status) {
    this.status = status;
  }

  public long getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(long createdTime) {
    this.createdTime = createdTime;
  }

  public long getUpdatedTime() {
    return updatedTime;
  }

  public void setUpdatedTime(long updatedTime) {
    this.updatedTime = updatedTime;
  }

  public String getMaintainerType() {
    return maintainerType;
  }

  public void setMaintainerType(String maintainerType) {
    this.maintainerType = maintainerType;
  }

  public Map<String, String> getSummary() {
    return summary;
  }

  public void setSummary(Map<String, String> summary) {
    this.summary = summary;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("catalogName", catalogName)
        .append("dbName", dbName)
        .append("tableName", tableName)
        .append("tableFormat", tableFormat)
        .append("maintainerType", maintainerType)
        .append("status", status)
        .append("summary", summary)
        .append("createdTime", createdTime)
        .append("updatedTime", updatedTime)
        .toString();
  }
}
