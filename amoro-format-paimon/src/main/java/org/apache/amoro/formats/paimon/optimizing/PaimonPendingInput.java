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

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.amoro.table.FormatPendingInput;

/** Paimon-specific pending input metrics collected during the refresh phase. */
public class PaimonPendingInput implements FormatPendingInput {

  // ---- Workload dimension ----
  private int dataFileCount;
  private long dataFileSize;
  private long dataRecordCount;

  // ---- Urgency dimension ----
  private int smallFileCount;
  private long smallFileSize;
  private int partitionCount;

  // ---- Delete vector dimension ----
  private int fileWithDeleteCount;
  private long deleteRecordCount;

  // ---- Health score ----
  private int healthScore = -1;

  public PaimonPendingInput() {}

  public PaimonPendingInput(
      int dataFileCount,
      long dataFileSize,
      long dataRecordCount,
      int smallFileCount,
      long smallFileSize,
      int partitionCount,
      int fileWithDeleteCount,
      long deleteRecordCount,
      int healthScore) {
    this.dataFileCount = dataFileCount;
    this.dataFileSize = dataFileSize;
    this.dataRecordCount = dataRecordCount;
    this.smallFileCount = smallFileCount;
    this.smallFileSize = smallFileSize;
    this.partitionCount = partitionCount;
    this.fileWithDeleteCount = fileWithDeleteCount;
    this.deleteRecordCount = deleteRecordCount;
    this.healthScore = healthScore;
  }

  public int getDataFileCount() {
    return dataFileCount;
  }

  public void setDataFileCount(int dataFileCount) {
    this.dataFileCount = dataFileCount;
  }

  public long getDataFileSize() {
    return dataFileSize;
  }

  public void setDataFileSize(long dataFileSize) {
    this.dataFileSize = dataFileSize;
  }

  public long getDataRecordCount() {
    return dataRecordCount;
  }

  @Override
  public long getTotalFileRecords() {
    return dataRecordCount;
  }

  public void setDataRecordCount(long dataRecordCount) {
    this.dataRecordCount = dataRecordCount;
  }

  public int getSmallFileCount() {
    return smallFileCount;
  }

  public void setSmallFileCount(int smallFileCount) {
    this.smallFileCount = smallFileCount;
  }

  public long getSmallFileSize() {
    return smallFileSize;
  }

  public void setSmallFileSize(long smallFileSize) {
    this.smallFileSize = smallFileSize;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  public int getFileWithDeleteCount() {
    return fileWithDeleteCount;
  }

  public void setFileWithDeleteCount(int fileWithDeleteCount) {
    this.fileWithDeleteCount = fileWithDeleteCount;
  }

  public long getDeleteRecordCount() {
    return deleteRecordCount;
  }

  public void setDeleteRecordCount(long deleteRecordCount) {
    this.deleteRecordCount = deleteRecordCount;
  }

  public int getHealthScore() {
    return healthScore;
  }

  public void setHealthScore(int healthScore) {
    this.healthScore = healthScore;
  }

  @Override
  public int getTotalFileCount() {
    return getDataFileCount();
  }

  @Override
  public long getTotalFileSize() {
    return getDataFileSize();
  }
}
