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

package org.apache.amoro.server.dashboard.controller;

import org.apache.amoro.server.dashboard.utils.AmsUtil;
import org.apache.amoro.server.table.RuntimeHealthSnapshot;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TableDetailObservation;
import org.apache.amoro.table.descriptor.TableFileStatisticsView;
import org.apache.amoro.table.descriptor.TableFileStatisticsView.Reason;
import org.apache.amoro.table.descriptor.TableFileStatisticsView.Source;
import org.apache.amoro.table.descriptor.TableFileStatisticsView.Status;
import org.apache.amoro.table.health.TableFileStatisticsSnapshot;
import org.apache.amoro.table.health.TableHealthEvaluation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/** Projects existing facts only: no table loading, persistence, scanning, or runtime refresh. */
final class TableFileStatisticsProjection {
  private TableFileStatisticsProjection() {}

  static TableFileStatisticsView project(
      TableDetailObservation observation,
      RuntimeHealthSnapshot health,
      boolean managed,
      Boolean enabled) {
    TableFileStatisticsSnapshot files = health == null ? null : health.getFileStatistics();
    List<Reason> reasons = new ArrayList<>();
    if (!managed) {
      reasons.add(Reason.NOT_MANAGED);
    }
    if (Boolean.FALSE.equals(enabled)) {
      reasons.add(Reason.COLLECTION_DISABLED);
    }
    Source source = files == null ? Source.NONE : Source.BACKGROUND;
    Status status;
    if (observation == null || (observation.isEmptySnapshot() && hasSnapshotEvidence(health))) {
      status = Status.UNKNOWN;
      reasons.add(Reason.SNAPSHOT_OBSERVATION_MISMATCH);
    } else if (observation.isEmptySnapshot()) {
      status = Status.CURRENT;
      source = Source.EMPTY_SNAPSHOT;
      files = null;
    } else if (files != null) {
      if (files.getSchemaId() != observation.getSchemaId()
          || (files.getSnapshotId() != null
              && files.getSnapshotId() > observation.getSnapshotId())) {
        status = Status.UNKNOWN;
        reasons.add(Reason.SNAPSHOT_OBSERVATION_MISMATCH);
      } else {
        status =
            Objects.equals(files.getSnapshotId(), observation.getSnapshotId())
                ? Status.CURRENT
                : Status.STALE;
      }
    } else if (!managed || !Boolean.TRUE.equals(enabled) || health != null) {
      status = Status.UNAVAILABLE;
      if (health != null) {
        reasons.add(Reason.NO_VALID_STATISTICS);
      }
    } else {
      status = Status.PENDING;
      reasons.add(Reason.WAITING_FOR_STATISTICS);
    }
    return new TableFileStatisticsView(status, source, files, observation, enabled, reasons);
  }

  static void apply(
      ServerTableMeta meta, RuntimeHealthSnapshot health, boolean managed, Boolean enabled) {
    TableFileStatisticsView view = project(meta.getDetailObservation(), health, managed, enabled);
    meta.setFileStatistics(view);
    Long count = view.getFileCount() == null ? null : Long.valueOf(view.getFileCount());
    String size = displaySize(view.getTotalSizeBytes());
    String average = displaySize(view.getAverageFileSizeBytes());
    if (meta.getBaseMetrics() == null) {
      meta.setBaseMetrics(new HashMap<>());
    }
    meta.getBaseMetrics().put("fileCount", count);
    meta.getBaseMetrics().put("totalSize", size);
    meta.getBaseMetrics().put("averageFileSize", average);
    meta.getTableSummary().setFile(count);
    meta.getTableSummary().setSize(size);
    meta.getTableSummary().setAverageFile(average);
  }

  private static String displaySize(String bytes) {
    return bytes == null
        ? null
        : "0".equals(bytes) ? "0 B" : AmsUtil.byteToXB(Long.parseLong(bytes));
  }

  private static boolean hasSnapshotEvidence(RuntimeHealthSnapshot health) {
    if (health == null) {
      return false;
    }
    TableHealthEvaluation evaluation = health.getEvaluation();
    return (health.getFileStatistics() != null
            && health.getFileStatistics().getSnapshotId() != null)
        || (health.getHealthDetails().getSnapshotId() != null
            && health.getHealthDetails().getSnapshotId() > 0)
        || (evaluation != null
            && (positive(evaluation.getCurrentSnapshotId())
                || positive(evaluation.getAttemptedSnapshotId())));
  }

  private static boolean positive(String snapshotId) {
    if (snapshotId == null) {
      return false;
    }
    try {
      return Long.parseLong(snapshotId) > 0;
    } catch (NumberFormatException invalidIdentity) {
      return true;
    }
  }
}
