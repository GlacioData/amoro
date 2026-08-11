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

package org.apache.amoro.formats.paimon.optimizing.health;

import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthComponent;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DeletionFile;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/** Immutable result of one complete scan of a fixed APPEND snapshot. */
public final class PaimonAppendSnapshotAnalysis implements FormatTableAnalysis {

  private final TableAnalysisKey key;
  private final PaimonPendingInput pendingInput;
  private final TableHealthDetails healthDetails;
  private final List<PlannerFile> plannerFiles;
  private final boolean plannerFactsAvailable;
  @Nullable private final Snapshot snapshot;
  private final long fullSnapshotFileCount;
  private final int fullSnapshotScanCount;

  private PaimonAppendSnapshotAnalysis(
      TableAnalysisKey key,
      PaimonPendingInput pendingInput,
      TableHealthDetails healthDetails,
      List<PlannerFile> plannerFiles,
      boolean plannerFactsAvailable,
      @Nullable Snapshot snapshot,
      long fullSnapshotFileCount,
      int fullSnapshotScanCount) {
    this.key = Objects.requireNonNull(key, "Analysis key must not be null");
    this.pendingInput = Objects.requireNonNull(pendingInput, "Pending input must not be null");
    this.healthDetails = Objects.requireNonNull(healthDetails, "Health details must not be null");
    this.plannerFiles = Collections.unmodifiableList(new ArrayList<>(plannerFiles));
    this.plannerFactsAvailable = plannerFactsAvailable;
    this.snapshot = snapshot;
    this.fullSnapshotFileCount = fullSnapshotFileCount;
    this.fullSnapshotScanCount = fullSnapshotScanCount;
  }

  /** Create an analysis after the scanner has exhausted the fixed-snapshot iterator. */
  public static PaimonAppendSnapshotAnalysis create(
      PaimonHealthEvaluationContext context,
      PaimonAppendHealthEvaluator.Result result,
      PaimonAppendHealthEvaluator.Result structuralResult,
      ScanTotals totals,
      List<PlannerFile> plannerFiles,
      boolean plannerFactsAvailable,
      @Nullable Snapshot snapshot,
      int fullSnapshotScanCount) {
    Objects.requireNonNull(context, "Health context must not be null");
    Objects.requireNonNull(result, "Health result must not be null");
    Objects.requireNonNull(totals, "Scan totals must not be null");

    PaimonPendingInput pendingInput =
        new PaimonPendingInput(
            safeInt(totals.totalFileCount),
            totals.totalFileSize,
            totals.totalRecordCount,
            safeInt(totals.smallFileCount),
            totals.smallFileSize,
            safeInt(totals.partitionCount),
            safeInt(totals.fileWithDeleteCount),
            totals.deleteRecordCount,
            result.healthScore());
    return new PaimonAppendSnapshotAnalysis(
        context.key(),
        pendingInput,
        details(context, result, structuralResult, totals),
        plannerFiles,
        plannerFactsAvailable,
        snapshot,
        totals.totalFileCount,
        fullSnapshotScanCount);
  }

  /** Create a bounded invalid result when no safe snapshot scan can be performed. */
  public static PaimonAppendSnapshotAnalysis invalid(
      PaimonHealthEvaluationContext context, String reasonCode) {
    PaimonPendingInput pendingInput = new PaimonPendingInput();
    return new PaimonAppendSnapshotAnalysis(
        context.key(),
        pendingInput,
        details(context, null, null, new ScanTotals(), Collections.singletonList(reasonCode)),
        Collections.emptyList(),
        false,
        context.snapshot().orElse(null),
        -1L,
        0);
  }

  private static TableHealthDetails details(
      PaimonHealthEvaluationContext context,
      @Nullable PaimonAppendHealthEvaluator.Result result,
      @Nullable PaimonAppendHealthEvaluator.Result structuralResult,
      ScanTotals totals) {
    return details(
        context,
        result,
        structuralResult,
        totals,
        result == null ? Collections.emptyList() : result.reasonCodes());
  }

  private static TableHealthDetails details(
      PaimonHealthEvaluationContext context,
      @Nullable PaimonAppendHealthEvaluator.Result result,
      @Nullable PaimonAppendHealthEvaluator.Result structuralResult,
      ScanTotals totals,
      List<String> reasonCodes) {
    int fileScore = structuralResult == null ? -1 : structuralResult.fileScore();
    int deleteScore = result == null ? -1 : result.deleteScore();
    int activityScore = result == null ? -1 : result.healthScore();
    List<TableHealthComponent> components =
        Arrays.asList(
            component("FILE_ORGANIZATION", fileScore, 80, "SUM"),
            component("MATERIALIZED_DELETE", deleteScore, 20, "SUM"),
            component("SNAPSHOT_ACTIVITY", activityScore, null, "DEBT_AMPLIFIER"));

    boolean structuralFactsAvailable = structuralResult != null;
    LinkedHashMap<String, String> metrics = new LinkedHashMap<>();
    metrics.put("bucketMode", context.bucketMode().name());
    metrics.put(
        "activePartitionCount", structuralFactsAvailable ? number(totals.partitionCount) : "N/A");
    metrics.put("effectiveUnitCount", structuralFactsAvailable ? number(totals.unitCount) : "N/A");
    metrics.put("totalFileCount", structuralFactsAvailable ? number(totals.totalFileCount) : "N/A");
    metrics.put("totalFileSize", structuralFactsAvailable ? number(totals.totalFileSize) : "N/A");
    metrics.put(
        "averageFileSize",
        structuralFactsAvailable
            ? number(totals.totalFileCount == 0 ? 0 : totals.totalFileSize / totals.totalFileCount)
            : "N/A");
    metrics.put("targetFileSize", positive(context.targetFileSize()));
    metrics.put("smallFileBoundary", nonNegative(context.smallFileBoundary()));
    metrics.put("smallFileCount", structuralFactsAvailable ? number(totals.smallFileCount) : "N/A");
    metrics.put("smallFileSize", structuralFactsAvailable ? number(totals.smallFileSize) : "N/A");
    metrics.put(
        "undersizedFileCount",
        structuralFactsAvailable ? number(totals.undersizedFileCount) : "N/A");
    metrics.put(
        "undersizedFileSize", structuralFactsAvailable ? number(totals.undersizedFileSize) : "N/A");
    metrics.put(
        "reducibleFileCount",
        structuralResult == null ? "N/A" : number(structuralResult.reducibleFileCount()));
    metrics.put(
        "expectedOutputFileCount",
        structuralResult == null ? "N/A" : number(structuralResult.expectedOutputFileCount()));
    metrics.put(
        "totalRecordCount", structuralFactsAvailable ? number(totals.totalRecordCount) : "N/A");
    metrics.put(
        "deleteRecordCount",
        structuralFactsAvailable && totals.deleteMetadataComplete
            ? number(totals.deleteRecordCount)
            : "N/A");
    metrics.put(
        "latestSnapshotTimeMillis",
        context.snapshotTimeMillis() > 0 ? number(context.snapshotTimeMillis()) : "N/A");
    metrics.put(
        "baselineSnapshotId", positive(context.key().getSuccessfulOptimizationBaselineId()));
    metrics.put(
        "baselineSnapshotTimeMillis",
        positive(context.key().getSuccessfulOptimizationBaselineTimeMillis()));
    metrics.put("timeThresholdMillis", "N/A");
    if (result != null && result.activity() != null) {
      metrics.put("newSnapshotCount", available(result.activity().newSnapshotCount()));
      metrics.put("snapshotTimeDistanceMillis", "N/A");
      metrics.put(
          "snapshotPressure",
          result.activity().baselineAvailable()
              ? Double.toString(result.activity().snapshotPressure())
              : "N/A");
      metrics.put("timePressure", "N/A");
      metrics.put(
          "activityPressure",
          result.activity().baselineAvailable()
              ? Double.toString(result.activity().activityPressure())
              : "N/A");
    } else {
      metrics.put("newSnapshotCount", "N/A");
      metrics.put("snapshotTimeDistanceMillis", "N/A");
      metrics.put("snapshotPressure", "N/A");
      metrics.put("timePressure", "N/A");
      metrics.put("activityPressure", "N/A");
    }

    TableAnalysisKey key = context.key();
    return new TableHealthDetails(
        context.formulaVersion(),
        nullableId(key.getSnapshotId()),
        null,
        nullableId(key.getSchemaId()),
        context.scoringConfigFingerprint(),
        key.encoded(),
        components,
        metrics,
        new ArrayList<>(new TreeSet<>(reasonCodes)));
  }

  private static TableHealthComponent component(
      String code, int score, @Nullable Integer weight, String combination) {
    return new TableHealthComponent(code, score, weight, combination, Collections.emptyMap());
  }

  private static Long nullableId(long id) {
    return id < 0 ? null : id;
  }

  private static String number(long value) {
    return Long.toString(value);
  }

  private static String available(long value) {
    return value < 0 ? "N/A" : number(value);
  }

  private static String positive(long value) {
    return value <= 0 ? "N/A" : number(value);
  }

  private static String nonNegative(long value) {
    return value < 0 ? "N/A" : number(value);
  }

  static int safeInt(long value) {
    if (value < 0) {
      throw new IllegalArgumentException("Paimon pending count must not be negative");
    }
    return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
  }

  @Override
  public TableAnalysisKey key() {
    return key;
  }

  @Override
  public PaimonPendingInput pendingInput() {
    return pendingInput;
  }

  @Override
  public TableHealthDetails healthDetails() {
    return healthDetails;
  }

  public List<PlannerFile> plannerFiles() {
    return plannerFiles;
  }

  public boolean plannerFactsAvailable() {
    return plannerFactsAvailable;
  }

  @Nullable
  public Snapshot snapshot() {
    return snapshot;
  }

  public long fullSnapshotFileCount() {
    return fullSnapshotFileCount;
  }

  public int fullSnapshotScanCount() {
    return fullSnapshotScanCount;
  }

  /** Primitive full-snapshot aggregates retained independently from formula validity. */
  public static final class ScanTotals {
    private long totalFileCount;
    private long totalFileSize;
    private long totalRecordCount;
    private long smallFileCount;
    private long smallFileSize;
    private long undersizedFileCount;
    private long undersizedFileSize;
    private long partitionCount;
    private long unitCount;
    private long fileWithDeleteCount;
    private long deleteRecordCount;
    private boolean deleteMetadataComplete = true;

    public void addFile(
        DataFileMeta file,
        long smallFileBoundary,
        long targetFileSize,
        @Nullable Long deleteCount) {
      if (file.fileSize() < 0 || file.rowCount() < 0) {
        throw new IllegalArgumentException("Paimon data file contains negative structural facts");
      }
      totalFileCount = Math.addExact(totalFileCount, 1L);
      totalFileSize = Math.addExact(totalFileSize, file.fileSize());
      totalRecordCount = Math.addExact(totalRecordCount, file.rowCount());
      if (file.fileSize() < smallFileBoundary) {
        smallFileCount = Math.addExact(smallFileCount, 1L);
        smallFileSize = Math.addExact(smallFileSize, file.fileSize());
      } else if (file.fileSize() < targetFileSize) {
        undersizedFileCount = Math.addExact(undersizedFileCount, 1L);
        undersizedFileSize = Math.addExact(undersizedFileSize, file.fileSize());
      }
      if (deleteCount == null || deleteCount < 0 || deleteCount > file.rowCount()) {
        deleteMetadataComplete = false;
      } else if (deleteCount > 0) {
        fileWithDeleteCount = Math.addExact(fileWithDeleteCount, 1L);
        deleteRecordCount = Math.addExact(deleteRecordCount, deleteCount);
      }
    }

    boolean deleteMetadataComplete() {
      return deleteMetadataComplete;
    }

    long deleteRecordCount() {
      return deleteRecordCount;
    }

    public void setPartitionCount(long partitionCount) {
      requireNonNegative(partitionCount, "Partition count");
      this.partitionCount = partitionCount;
    }

    public void setUnitCount(long unitCount) {
      requireNonNegative(unitCount, "Unit count");
      this.unitCount = unitCount;
    }

    private static void requireNonNegative(long value, String name) {
      if (value < 0) {
        throw new IllegalArgumentException(name + " must not be negative");
      }
    }
  }

  /** Bounded immutable file fact used to reconstruct the existing planner candidate. */
  public static final class PlannerFile {
    private final BinaryRow partition;
    private final DataFileMeta file;
    @Nullable private final DeletionFile deletionFile;
    @Nullable private final String dvGroupKey;

    public PlannerFile(
        BinaryRow partition,
        DataFileMeta file,
        @Nullable DeletionFile deletionFile,
        @Nullable String dvGroupKey) {
      this.partition = partition.copy();
      this.file = Objects.requireNonNull(file, "Data file must not be null");
      this.deletionFile = deletionFile;
      this.dvGroupKey = dvGroupKey;
    }

    public BinaryRow partition() {
      return partition.copy();
    }

    public DataFileMeta file() {
      return file;
    }

    public DeletionFile deletionFile() {
      return deletionFile;
    }

    public String dvGroupKey() {
      return dvGroupKey;
    }
  }
}
