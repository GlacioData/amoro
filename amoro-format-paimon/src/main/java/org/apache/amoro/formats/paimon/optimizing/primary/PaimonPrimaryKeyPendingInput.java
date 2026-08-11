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

package org.apache.amoro.formats.paimon.optimizing.primary;

import org.apache.amoro.table.FormatPendingInput;

/** Persistable primary-key health summary, separate from APPEND pending-input semantics. */
public class PaimonPrimaryKeyPendingInput implements FormatPendingInput {

  private int dataFileCount;
  private long dataFileSize;
  private long dataRecordCount;
  private int smallFileCount;
  private long smallFileSize;
  private long tombstoneRecordCount;
  private long deletionVectorRecordCount;
  private int effectiveUnitCount;
  private int maxSortedRunCount;
  private int runScore = -1;
  private int materializedDeleteScore = -1;
  private int primaryKeyBaseScore = -1;
  private int healthScore = -1;

  public PaimonPrimaryKeyPendingInput() {}

  public PaimonPrimaryKeyPendingInput(
      int dataFileCount,
      long dataFileSize,
      long dataRecordCount,
      int smallFileCount,
      long smallFileSize,
      long tombstoneRecordCount,
      long deletionVectorRecordCount,
      int effectiveUnitCount,
      int maxSortedRunCount,
      int runScore,
      int materializedDeleteScore,
      int primaryKeyBaseScore,
      int healthScore) {
    this.dataFileCount = dataFileCount;
    this.dataFileSize = dataFileSize;
    this.dataRecordCount = dataRecordCount;
    this.smallFileCount = smallFileCount;
    this.smallFileSize = smallFileSize;
    this.tombstoneRecordCount = tombstoneRecordCount;
    this.deletionVectorRecordCount = deletionVectorRecordCount;
    this.effectiveUnitCount = effectiveUnitCount;
    this.maxSortedRunCount = maxSortedRunCount;
    this.runScore = runScore;
    this.materializedDeleteScore = materializedDeleteScore;
    this.primaryKeyBaseScore = primaryKeyBaseScore;
    this.healthScore = healthScore;
  }

  @Override
  public int getDataFileCount() {
    return dataFileCount;
  }

  public void setDataFileCount(int dataFileCount) {
    this.dataFileCount = dataFileCount;
  }

  @Override
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

  public long getTombstoneRecordCount() {
    return tombstoneRecordCount;
  }

  public void setTombstoneRecordCount(long tombstoneRecordCount) {
    this.tombstoneRecordCount = tombstoneRecordCount;
  }

  public long getDeletionVectorRecordCount() {
    return deletionVectorRecordCount;
  }

  public void setDeletionVectorRecordCount(long deletionVectorRecordCount) {
    this.deletionVectorRecordCount = deletionVectorRecordCount;
  }

  public int getEffectiveUnitCount() {
    return effectiveUnitCount;
  }

  public void setEffectiveUnitCount(int effectiveUnitCount) {
    this.effectiveUnitCount = effectiveUnitCount;
  }

  public int getMaxSortedRunCount() {
    return maxSortedRunCount;
  }

  public void setMaxSortedRunCount(int maxSortedRunCount) {
    this.maxSortedRunCount = maxSortedRunCount;
  }

  public int getRunScore() {
    return runScore;
  }

  public void setRunScore(int runScore) {
    this.runScore = runScore;
  }

  public int getMaterializedDeleteScore() {
    return materializedDeleteScore;
  }

  public void setMaterializedDeleteScore(int materializedDeleteScore) {
    this.materializedDeleteScore = materializedDeleteScore;
  }

  public int getPrimaryKeyBaseScore() {
    return primaryKeyBaseScore;
  }

  public void setPrimaryKeyBaseScore(int primaryKeyBaseScore) {
    this.primaryKeyBaseScore = primaryKeyBaseScore;
  }

  @Override
  public int getHealthScore() {
    return healthScore;
  }

  public void setHealthScore(int healthScore) {
    this.healthScore = healthScore;
  }

  @Override
  public int getTotalFileCount() {
    return dataFileCount;
  }

  @Override
  public long getTotalFileSize() {
    return dataFileSize;
  }
}
