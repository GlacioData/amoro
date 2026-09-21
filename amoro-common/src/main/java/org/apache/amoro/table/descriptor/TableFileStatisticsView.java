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

package org.apache.amoro.table.descriptor;

import org.apache.amoro.table.health.TableFileStatisticsSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** File statistics and request-relative freshness. Existing health diagnostics remain separate. */
public final class TableFileStatisticsView {
  public enum Status {
    CURRENT,
    STALE,
    UNKNOWN,
    PENDING,
    UNAVAILABLE
  }

  public enum Source {
    BACKGROUND,
    EMPTY_SNAPSHOT,
    NONE
  }

  public enum Reason {
    NOT_MANAGED,
    COLLECTION_DISABLED,
    WAITING_FOR_STATISTICS,
    NO_VALID_STATISTICS,
    SNAPSHOT_OBSERVATION_MISMATCH
  }

  private final Status status;
  private final Source source;
  private final String snapshotId;
  private final String schemaId;
  private final String observedSnapshotId;
  private final String observedSchemaId;
  private final String fileCount;
  private final String totalSizeBytes;
  private final String averageFileSizeBytes;
  private final Long evaluatedAtMillis;
  private final Boolean collectionEnabled;
  private final List<Reason> reasonCodes;

  public TableFileStatisticsView(
      Status status,
      Source source,
      TableFileStatisticsSnapshot statistics,
      TableDetailObservation observation,
      Boolean collectionEnabled,
      List<Reason> reasonCodes) {
    this.status = status;
    this.source = source;
    this.collectionEnabled = collectionEnabled;
    this.reasonCodes = Collections.unmodifiableList(new ArrayList<>(reasonCodes));
    this.observedSnapshotId = observation == null ? null : decimal(observation.getSnapshotId());
    this.observedSchemaId = observation == null ? null : Long.toString(observation.getSchemaId());
    boolean empty = source == Source.EMPTY_SNAPSHOT;
    this.snapshotId = statistics == null ? null : decimal(statistics.getSnapshotId());
    this.schemaId =
        empty
            ? observedSchemaId
            : statistics == null ? null : Long.toString(statistics.getSchemaId());
    this.fileCount =
        empty ? "0" : statistics == null ? null : Long.toString(statistics.getFileCount());
    this.totalSizeBytes =
        empty ? "0" : statistics == null ? null : Long.toString(statistics.getTotalSizeBytes());
    this.averageFileSizeBytes =
        empty
            ? "0"
            : statistics == null ? null : Long.toString(statistics.getAverageFileSizeBytes());
    this.evaluatedAtMillis = statistics == null || empty ? null : statistics.getEvaluatedAtMillis();
  }

  private static String decimal(Long value) {
    return value == null ? null : value.toString();
  }

  public Status getStatus() {
    return status;
  }

  public Source getSource() {
    return source;
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  public String getSchemaId() {
    return schemaId;
  }

  public String getObservedSnapshotId() {
    return observedSnapshotId;
  }

  public String getObservedSchemaId() {
    return observedSchemaId;
  }

  public String getFileCount() {
    return fileCount;
  }

  public String getTotalSizeBytes() {
    return totalSizeBytes;
  }

  public String getAverageFileSizeBytes() {
    return averageFileSizeBytes;
  }

  public Long getEvaluatedAtMillis() {
    return evaluatedAtMillis;
  }

  public Boolean getCollectionEnabled() {
    return collectionEnabled;
  }

  public List<Reason> getReasonCodes() {
    return reasonCodes;
  }
}
