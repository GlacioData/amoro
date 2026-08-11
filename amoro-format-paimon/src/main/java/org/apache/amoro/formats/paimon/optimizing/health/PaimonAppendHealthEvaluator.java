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

import org.apache.amoro.utils.ScoreUtil;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Pure APPEND-table health formula over bounded per-unit aggregates. */
public final class PaimonAppendHealthEvaluator {

  public static final String FORMULA_VERSION = "paimon-append-health-v2";
  public static final String EMPTY_TABLE = "EMPTY_TABLE";
  public static final String INVALID_SCORING_CONFIG = "INVALID_SCORING_CONFIG";
  public static final String DELETE_METADATA_INCOMPLETE = "DELETE_METADATA_INCOMPLETE";
  public static final String SNAPSHOT_SCAN_FAILED = "SNAPSHOT_SCAN_FAILED";

  private static final double DELETE_EXPONENT = 2.3D;

  private final long targetFileSize;
  private final long smallFileBoundary;

  public PaimonAppendHealthEvaluator(long targetFileSize, long smallFileBoundary) {
    this.targetFileSize = targetFileSize;
    this.smallFileBoundary = smallFileBoundary;
  }

  public UnitAccumulator newUnitAccumulator() {
    return new UnitAccumulator();
  }

  public Result evaluate(Iterable<UnitStatistics> units, PaimonActivityHealth.Input activityInput) {
    Objects.requireNonNull(activityInput, "activityInput");
    if (targetFileSize <= 0 || smallFileBoundary < 0 || smallFileBoundary > targetFileSize) {
      return Result.invalid(INVALID_SCORING_CONFIG);
    }
    if (units == null) {
      return Result.invalid(SNAPSHOT_SCAN_FAILED);
    }

    Aggregate aggregate = new Aggregate();
    for (UnitStatistics unit : units) {
      if (unit == null || !aggregate.add(unit, targetFileSize, smallFileBoundary)) {
        return Result.invalid(SNAPSHOT_SCAN_FAILED);
      }
    }
    aggregate.expectedOutputFileCount = aggregate.totalFileCount - aggregate.reducibleFileCount;

    if (aggregate.deleteMetadataIncomplete) {
      return Result.invalid(DELETE_METADATA_INCOMPLETE);
    }
    if (aggregate.totalFileCount == 0) {
      PaimonActivityHealth.Result activity =
          PaimonActivityHealth.evaluate(activityInput.withBaseScore(100));
      return Result.valid(100, 100, aggregate, activity, Collections.singletonList(EMPTY_TABLE));
    }

    int fileScore = fileScore(aggregate.reducibleFileCount, aggregate.totalFileCount);
    int deleteScore = deleteScore(aggregate);
    if (deleteScore < 0) {
      return Result.invalid(DELETE_METADATA_INCOMPLETE);
    }
    int baseScore = ScoreUtil.clampScore(Math.round(0.80D * fileScore + 0.20D * deleteScore));
    PaimonActivityHealth.Result activity =
        PaimonActivityHealth.evaluate(activityInput.withBaseScore(baseScore));
    List<String> reasonCodes = new ArrayList<>(activity.reasonCodes());
    return Result.valid(fileScore, deleteScore, aggregate, activity, reasonCodes);
  }

  private static int fileScore(long reducibleFileCount, long totalFileCount) {
    if (reducibleFileCount == 0) {
      return 100;
    }

    double x = StrictMath.log10(reducibleFileCount);
    double scale;
    if (x <= 1.0D) {
      scale = 0.10D + 0.40D * x;
    } else if (x <= 2.0D) {
      scale = 0.50D + 0.50D * (x - 1.0D);
    } else {
      scale = 1.0D;
    }

    double absolutePenalty;
    if (x <= 1.0D) {
      absolutePenalty = 0.05D * x;
    } else if (x <= 2.0D) {
      absolutePenalty = 0.05D + 0.10D * (x - 1.0D);
    } else if (x <= 3.0D) {
      absolutePenalty = 0.15D + 0.15D * (x - 2.0D);
    } else {
      absolutePenalty = 0.30D;
    }

    double relativeReduction = (double) reducibleFileCount / totalFileCount;
    double penalty = Math.min(1.0D, relativeReduction * scale + absolutePenalty);
    return ScoreUtil.clampScore(Math.round(100.0D * (1.0D - penalty)));
  }

  private static int deleteScore(Aggregate aggregate) {
    if (aggregate.totalRecordCount == 0 || aggregate.deleteRecordCount == 0) {
      return 100;
    }
    double score =
        aggregate
            .deleteHealthyRowWeight
            .multiply(BigDecimal.valueOf(100L))
            .divide(BigDecimal.valueOf(aggregate.totalRecordCount), MathContext.DECIMAL128)
            .doubleValue();
    if (!Double.isFinite(score)) {
      return -1;
    }
    return ScoreUtil.clampScore(Math.round(score));
  }

  private static long ceilDiv(long value, long divisor) {
    return value == 0 ? 0 : 1 + (value - 1) / divisor;
  }

  /** Mutable scan-side collector which retains only primitive aggregates for one unit. */
  public final class UnitAccumulator {
    private long totalFileCount;
    private long totalFileSize;
    private long smallFileCount;
    private long smallFileSize;
    private long undersizedFileCount;
    private long undersizedFileSize;
    private long totalRecordCount;
    private long deleteRecordCount;
    private BigDecimal deleteHealthyRowWeight = BigDecimal.ZERO;
    private boolean deleteMetadataIncomplete;
    private boolean scanMetadataInvalid;

    public void addFile(long fileSize, long rowCount, Long materializedDeleteCount) {
      if (fileSize < 0) {
        scanMetadataInvalid = true;
        return;
      }
      if (rowCount < 0
          || materializedDeleteCount == null
          || materializedDeleteCount < 0
          || materializedDeleteCount > rowCount
          || (rowCount == 0 && materializedDeleteCount > 0)) {
        deleteMetadataIncomplete = true;
        return;
      }

      try {
        totalFileCount = Math.addExact(totalFileCount, 1L);
        totalFileSize = Math.addExact(totalFileSize, fileSize);
        totalRecordCount = Math.addExact(totalRecordCount, rowCount);
        deleteRecordCount = Math.addExact(deleteRecordCount, materializedDeleteCount);
        if (fileSize < smallFileBoundary) {
          smallFileCount = Math.addExact(smallFileCount, 1L);
          smallFileSize = Math.addExact(smallFileSize, fileSize);
        } else if (fileSize < targetFileSize) {
          undersizedFileCount = Math.addExact(undersizedFileCount, 1L);
          undersizedFileSize = Math.addExact(undersizedFileSize, fileSize);
        }
      } catch (ArithmeticException e) {
        scanMetadataInvalid = true;
        return;
      }

      double healthyRatio =
          rowCount == 0 ? 1.0D : 1.0D - (double) materializedDeleteCount / rowCount;
      double perFileContribution = rowCount * StrictMath.pow(healthyRatio, DELETE_EXPONENT);
      if (!Double.isFinite(perFileContribution)) {
        scanMetadataInvalid = true;
        return;
      }
      deleteHealthyRowWeight = deleteHealthyRowWeight.add(BigDecimal.valueOf(perFileContribution));
    }

    public UnitStatistics snapshot() {
      return new UnitStatistics(
          targetFileSize,
          smallFileBoundary,
          totalFileCount,
          totalFileSize,
          smallFileCount,
          smallFileSize,
          undersizedFileCount,
          undersizedFileSize,
          totalRecordCount,
          deleteRecordCount,
          deleteHealthyRowWeight,
          deleteMetadataIncomplete,
          scanMetadataInvalid);
    }
  }

  /** Immutable per-(partition,bucket) aggregate consumed by the formula. */
  public static final class UnitStatistics {
    private final long targetFileSize;
    private final long smallFileBoundary;
    private final long totalFileCount;
    private final long totalFileSize;
    private final long smallFileCount;
    private final long smallFileSize;
    private final long undersizedFileCount;
    private final long undersizedFileSize;
    private final long totalRecordCount;
    private final long deleteRecordCount;
    private final BigDecimal deleteHealthyRowWeight;
    private final boolean deleteMetadataIncomplete;
    private final boolean scanMetadataInvalid;

    private UnitStatistics(
        long targetFileSize,
        long smallFileBoundary,
        long totalFileCount,
        long totalFileSize,
        long smallFileCount,
        long smallFileSize,
        long undersizedFileCount,
        long undersizedFileSize,
        long totalRecordCount,
        long deleteRecordCount,
        BigDecimal deleteHealthyRowWeight,
        boolean deleteMetadataIncomplete,
        boolean scanMetadataInvalid) {
      this.targetFileSize = targetFileSize;
      this.smallFileBoundary = smallFileBoundary;
      this.totalFileCount = totalFileCount;
      this.totalFileSize = totalFileSize;
      this.smallFileCount = smallFileCount;
      this.smallFileSize = smallFileSize;
      this.undersizedFileCount = undersizedFileCount;
      this.undersizedFileSize = undersizedFileSize;
      this.totalRecordCount = totalRecordCount;
      this.deleteRecordCount = deleteRecordCount;
      this.deleteHealthyRowWeight = deleteHealthyRowWeight;
      this.deleteMetadataIncomplete = deleteMetadataIncomplete;
      this.scanMetadataInvalid = scanMetadataInvalid;
    }
  }

  private static final class Aggregate {
    private long totalFileCount;
    private long totalFileSize;
    private long smallFileCount;
    private long smallFileSize;
    private long undersizedFileCount;
    private long undersizedFileSize;
    private long reducibleFileCount;
    private long expectedOutputFileCount;
    private long totalRecordCount;
    private long deleteRecordCount;
    private BigDecimal deleteHealthyRowWeight = BigDecimal.ZERO;
    private boolean deleteMetadataIncomplete;

    private boolean add(
        UnitStatistics unit, long expectedTargetFileSize, long expectedSmallFileBoundary) {
      if (unit.scanMetadataInvalid
          || unit.targetFileSize != expectedTargetFileSize
          || unit.smallFileBoundary != expectedSmallFileBoundary) {
        return false;
      }
      try {
        totalFileCount = Math.addExact(totalFileCount, unit.totalFileCount);
        totalFileSize = Math.addExact(totalFileSize, unit.totalFileSize);
        smallFileCount = Math.addExact(smallFileCount, unit.smallFileCount);
        smallFileSize = Math.addExact(smallFileSize, unit.smallFileSize);
        undersizedFileCount = Math.addExact(undersizedFileCount, unit.undersizedFileCount);
        undersizedFileSize = Math.addExact(undersizedFileSize, unit.undersizedFileSize);
        totalRecordCount = Math.addExact(totalRecordCount, unit.totalRecordCount);
        deleteRecordCount = Math.addExact(deleteRecordCount, unit.deleteRecordCount);
        long belowTargetCount = Math.addExact(unit.smallFileCount, unit.undersizedFileCount);
        long belowTargetSize = Math.addExact(unit.smallFileSize, unit.undersizedFileSize);
        long expectedSelectedOutput = ceilDiv(belowTargetSize, expectedTargetFileSize);
        long reducible = Math.max(0L, belowTargetCount - expectedSelectedOutput);
        reducibleFileCount = Math.addExact(reducibleFileCount, reducible);
        deleteHealthyRowWeight = deleteHealthyRowWeight.add(unit.deleteHealthyRowWeight);
      } catch (ArithmeticException e) {
        return false;
      }
      deleteMetadataIncomplete |= unit.deleteMetadataIncomplete;
      return true;
    }
  }

  /** Immutable formula output with the complete bounded APPEND aggregates. */
  public static final class Result {
    private final int healthScore;
    private final int baseScore;
    private final int fileScore;
    private final int deleteScore;
    private final long totalFileCount;
    private final long totalFileSize;
    private final long smallFileCount;
    private final long smallFileSize;
    private final long undersizedFileCount;
    private final long undersizedFileSize;
    private final long reducibleFileCount;
    private final long expectedOutputFileCount;
    private final long totalRecordCount;
    private final long deleteRecordCount;
    private final PaimonActivityHealth.Result activity;
    private final List<String> reasonCodes;

    private Result(
        int healthScore,
        int baseScore,
        int fileScore,
        int deleteScore,
        long totalFileCount,
        long totalFileSize,
        long smallFileCount,
        long smallFileSize,
        long undersizedFileCount,
        long undersizedFileSize,
        long reducibleFileCount,
        long expectedOutputFileCount,
        long totalRecordCount,
        long deleteRecordCount,
        PaimonActivityHealth.Result activity,
        List<String> reasonCodes) {
      this.healthScore = healthScore;
      this.baseScore = baseScore;
      this.fileScore = fileScore;
      this.deleteScore = deleteScore;
      this.totalFileCount = totalFileCount;
      this.totalFileSize = totalFileSize;
      this.smallFileCount = smallFileCount;
      this.smallFileSize = smallFileSize;
      this.undersizedFileCount = undersizedFileCount;
      this.undersizedFileSize = undersizedFileSize;
      this.reducibleFileCount = reducibleFileCount;
      this.expectedOutputFileCount = expectedOutputFileCount;
      this.totalRecordCount = totalRecordCount;
      this.deleteRecordCount = deleteRecordCount;
      this.activity = activity;
      this.reasonCodes = Collections.unmodifiableList(new ArrayList<>(reasonCodes));
    }

    private static Result invalid(String reasonCode) {
      return new Result(
          -1,
          -1,
          -1,
          -1,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          null,
          Collections.singletonList(reasonCode));
    }

    private static Result valid(
        int fileScore,
        int deleteScore,
        Aggregate aggregate,
        PaimonActivityHealth.Result activity,
        List<String> leadingReasonCodes) {
      List<String> reasonCodes = new ArrayList<>(leadingReasonCodes);
      for (String activityReason : activity.reasonCodes()) {
        if (!reasonCodes.contains(activityReason)) {
          reasonCodes.add(activityReason);
        }
      }
      return new Result(
          activity.healthScore(),
          activity.baseScore(),
          fileScore,
          deleteScore,
          aggregate.totalFileCount,
          aggregate.totalFileSize,
          aggregate.smallFileCount,
          aggregate.smallFileSize,
          aggregate.undersizedFileCount,
          aggregate.undersizedFileSize,
          aggregate.reducibleFileCount,
          aggregate.expectedOutputFileCount,
          aggregate.totalRecordCount,
          aggregate.deleteRecordCount,
          activity,
          reasonCodes);
    }

    public int healthScore() {
      return healthScore;
    }

    public int baseScore() {
      return baseScore;
    }

    public int fileScore() {
      return fileScore;
    }

    public int deleteScore() {
      return deleteScore;
    }

    public long totalFileCount() {
      return totalFileCount;
    }

    public long totalFileSize() {
      return totalFileSize;
    }

    public long smallFileCount() {
      return smallFileCount;
    }

    public long smallFileSize() {
      return smallFileSize;
    }

    public long undersizedFileCount() {
      return undersizedFileCount;
    }

    public long undersizedFileSize() {
      return undersizedFileSize;
    }

    public long reducibleFileCount() {
      return reducibleFileCount;
    }

    public long expectedOutputFileCount() {
      return expectedOutputFileCount;
    }

    public long totalRecordCount() {
      return totalRecordCount;
    }

    public long deleteRecordCount() {
      return deleteRecordCount;
    }

    public PaimonActivityHealth.Result activity() {
      return activity;
    }

    public List<String> reasonCodes() {
      return reasonCodes;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Result)) {
        return false;
      }
      Result that = (Result) other;
      return healthScore == that.healthScore
          && baseScore == that.baseScore
          && fileScore == that.fileScore
          && deleteScore == that.deleteScore
          && totalFileCount == that.totalFileCount
          && totalFileSize == that.totalFileSize
          && smallFileCount == that.smallFileCount
          && smallFileSize == that.smallFileSize
          && undersizedFileCount == that.undersizedFileCount
          && undersizedFileSize == that.undersizedFileSize
          && reducibleFileCount == that.reducibleFileCount
          && expectedOutputFileCount == that.expectedOutputFileCount
          && totalRecordCount == that.totalRecordCount
          && deleteRecordCount == that.deleteRecordCount
          && Objects.equals(activity, that.activity)
          && reasonCodes.equals(that.reasonCodes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          healthScore,
          baseScore,
          fileScore,
          deleteScore,
          totalFileCount,
          totalFileSize,
          smallFileCount,
          smallFileSize,
          undersizedFileCount,
          undersizedFileSize,
          reducibleFileCount,
          expectedOutputFileCount,
          totalRecordCount,
          deleteRecordCount,
          activity,
          reasonCodes);
    }
  }
}
