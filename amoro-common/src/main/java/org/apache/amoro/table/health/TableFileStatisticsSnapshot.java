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

package org.apache.amoro.table.health;

/**
 * Complete file aggregates, retained without any reference to scanned files or analysis objects.
 */
public final class TableFileStatisticsSnapshot {
  private final Long snapshotId;
  private final long schemaId;
  private final long fileCount;
  private final long totalSizeBytes;
  private final long evaluatedAtMillis;

  public TableFileStatisticsSnapshot(
      Long snapshotId, long schemaId, long fileCount, long totalSizeBytes, long evaluatedAtMillis) {
    if ((snapshotId != null && snapshotId <= 0)
        || schemaId < 0
        || fileCount < 0
        || totalSizeBytes < 0
        || evaluatedAtMillis <= 0
        || (fileCount == 0 && totalSizeBytes != 0)
        || (snapshotId == null && fileCount != 0)) {
      throw new IllegalArgumentException("Invalid complete file statistics");
    }
    this.snapshotId = snapshotId;
    this.schemaId = schemaId;
    this.fileCount = fileCount;
    this.totalSizeBytes = totalSizeBytes;
    this.evaluatedAtMillis = evaluatedAtMillis;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  public long getSchemaId() {
    return schemaId;
  }

  public long getFileCount() {
    return fileCount;
  }

  public long getTotalSizeBytes() {
    return totalSizeBytes;
  }

  public long getAverageFileSizeBytes() {
    return fileCount == 0 ? 0 : totalSizeBytes / fileCount;
  }

  public long getEvaluatedAtMillis() {
    return evaluatedAtMillis;
  }
}
