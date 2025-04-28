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

public class MaintainerResult {

  public Long id;
  @StateField public String catalog_name;
  @StateField public String db_name;
  @StateField public String table_name;
  @StateField public TableFormat table_format;
  @StateField private TableMaintainer.Status status = TableMaintainer.Status.IDLE;
  @StateField private long createdTime;
  @StateField private long updatedTime;
  @StateField private int threadId = -1;
  @StateField private String failReason;

  public String getCatalog_name() {
    return catalog_name;
  }

  public void setCatalog_name(String catalog_name) {
    this.catalog_name = catalog_name;
  }

  public String getDb_name() {
    return db_name;
  }

  public void setDb_name(String db_name) {
    this.db_name = db_name;
  }

  public String getTable_name() {
    return table_name;
  }

  public void setTable_name(String table_name) {
    this.table_name = table_name;
  }

  public TableFormat getTable_format() {
    return table_format;
  }

  public void setTable_format(TableFormat table_format) {
    this.table_format = table_format;
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

  public int getThreadId() {
    return threadId;
  }

  public void setThreadId(int threadId) {
    this.threadId = threadId;
  }

  public String getFailReason() {
    return failReason;
  }

  public void setFailReason(String failReason) {
    this.failReason = failReason;
  }
}
