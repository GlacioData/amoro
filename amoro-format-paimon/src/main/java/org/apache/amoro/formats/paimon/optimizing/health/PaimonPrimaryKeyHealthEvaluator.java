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

import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyPendingInput;
import org.apache.amoro.utils.ScoreUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/** Pure primary-key health formula over fixed-snapshot per-bucket aggregates. */
public final class PaimonPrimaryKeyHealthEvaluator {

  public static final String FORMULA_VERSION = "paimon-primary-key-health-v2";
  public static final String EMPTY_TABLE = "EMPTY_TABLE";
  public static final String INVALID_SCORING_CONFIG = "INVALID_SCORING_CONFIG";
  public static final String DELETE_METADATA_INCOMPLETE = "DELETE_METADATA_INCOMPLETE";
  public static final String SNAPSHOT_SCAN_FAILED = "SNAPSHOT_SCAN_FAILED";
  public static final String KEY_DYNAMIC_OPTIMIZING_UNSUPPORTED =
      "KEY_DYNAMIC_OPTIMIZING_UNSUPPORTED";
  public static final String KEY_DYNAMIC_LOCAL_INDEX_NOT_EVALUATED =
      "KEY_DYNAMIC_LOCAL_INDEX_NOT_EVALUATED";

  private final int compactionTrigger;
  private final int stopTrigger;
  private final int numLevels;
  private final long targetFileSize;
  private final long compactionFileSize;

  public PaimonPrimaryKeyHealthEvaluator(
      int compactionTrigger,
      int stopTrigger,
      int numLevels,
      long targetFileSize,
      long compactionFileSize) {
    this.compactionTrigger = compactionTrigger;
    this.stopTrigger = stopTrigger;
    this.numLevels = numLevels;
    this.targetFileSize = targetFileSize;
    this.compactionFileSize = compactionFileSize;
  }

  public UnitAccumulator newUnitAccumulator() {
    return new UnitAccumulator();
  }

  public Result evaluate(
      Iterable<UnitStatistics> units,
      PaimonActivityHealth.Input activityInput,
      EvaluationMode mode) {
    Objects.requireNonNull(activityInput, "activityInput");
    Objects.requireNonNull(mode, "mode");
    if (!validConfiguration()) {
      return Result.invalid(INVALID_SCORING_CONFIG);
    }
    if (units == null) {
      return Result.invalid(SNAPSHOT_SCAN_FAILED);
    }

    Aggregate aggregate = new Aggregate();
    for (UnitStatistics unit : units) {
      if (unit == null || !aggregate.add(unit)) {
        return Result.invalid(SNAPSHOT_SCAN_FAILED);
      }
    }
    if (aggregate.deleteMetadataIncomplete) {
      return Result.invalid(DELETE_METADATA_INCOMPLETE);
    }

    boolean empty = aggregate.totalFileCount == 0;
    int runScore = empty ? 100 : runScore(aggregate);
    int materializedDeleteScore = empty ? 100 : materializedDeleteScore(aggregate);
    if (materializedDeleteScore < 0) {
      return Result.invalid(SNAPSHOT_SCAN_FAILED);
    }
    int baseScore =
        ScoreUtil.clampScore(Math.round((double) runScore * materializedDeleteScore / 100.0D));
    PaimonActivityHealth.Result activity =
        PaimonActivityHealth.evaluate(activityInput.withBaseScore(baseScore));

    List<String> reasonCodes = new ArrayList<>();
    if (empty) {
      reasonCodes.add(EMPTY_TABLE);
    }
    reasonCodes.addAll(activity.reasonCodes());
    if (activity.healthScore() >= 0 && mode == EvaluationMode.KEY_DYNAMIC) {
      reasonCodes.add(KEY_DYNAMIC_OPTIMIZING_UNSUPPORTED);
      reasonCodes.add(KEY_DYNAMIC_LOCAL_INDEX_NOT_EVALUATED);
    }

    return Result.valid(
        runScore,
        materializedDeleteScore,
        baseScore,
        aggregate,
        activity,
        sortedReasonCodes(reasonCodes));
  }

  private boolean validConfiguration() {
    return compactionTrigger >= 1
        && stopTrigger >= compactionTrigger
        && numLevels >= 2
        && targetFileSize > 0
        && compactionFileSize >= 0
        && compactionFileSize <= targetFileSize;
  }

  private int runScore(Aggregate aggregate) {
    double equalBaseExposure = aggregate.basePressureSum.value() / aggregate.effectiveUnitCount;
    double equalSevereExposure = aggregate.severePressureSum.value() / aggregate.effectiveUnitCount;
    double byteBaseExposure = equalBaseExposure;
    double byteSevereExposure = equalSevereExposure;
    if (aggregate.totalFileSize > 0) {
      byteBaseExposure = aggregate.byteWeightedBasePressure.value() / aggregate.totalFileSize;
      byteSevereExposure = aggregate.byteWeightedSeverePressure.value() / aggregate.totalFileSize;
    }

    double baseExposure = Math.max(equalBaseExposure, byteBaseExposure);
    double severeExposure = Math.max(equalSevereExposure, byteSevereExposure);
    double score;
    if (aggregate.hasSevereUnit) {
      score = Math.min(59.0D, 99.0D - 40.0D * baseExposure - 59.0D * severeExposure);
      score = clamp(score, 0.0D, 59.0D);
    } else {
      score = clamp(100.0D - 40.0D * baseExposure, 60.0D, 100.0D);
    }
    return ScoreUtil.clampScore(Math.round(score));
  }

  private static int materializedDeleteScore(Aggregate aggregate) {
    if (aggregate.totalRecordCount == 0) {
      return 100;
    }
    double score =
        100.0D * aggregate.materializedHealthyRowWeight.value() / aggregate.totalRecordCount;
    if (!Double.isFinite(score)) {
      return -1;
    }
    return ScoreUtil.clampScore(Math.round(score));
  }

  private Pressure pressure(int sortedRunCount) {
    if (sortedRunCount < compactionTrigger) {
      return Pressure.ZERO;
    }
    if (sortedRunCount < stopTrigger) {
      double progress =
          clamp01(
              (double) (sortedRunCount - compactionTrigger + 1)
                  / Math.max(stopTrigger - compactionTrigger, 1));
      double normalDebt = 20.0D + 20.0D * progress;
      return new Pressure(normalDebt / 40.0D, 0.0D);
    }
    double severePressure =
        clamp01(
            (double) (sortedRunCount - stopTrigger + 1)
                / Math.max(stopTrigger - compactionTrigger + 1, 1));
    return new Pressure(1.0D, severePressure);
  }

  private static double materializedHealth(long rowCount, long deleteCount) {
    if (rowCount == 0 || deleteCount >= rowCount - deleteCount) {
      return rowCount == 0 ? 1.0D : 0.0D;
    }
    double ratio = (double) deleteCount / rowCount;
    return 1.0D - ratio / (1.0D - ratio);
  }

  private static List<String> sortedReasonCodes(List<String> reasonCodes) {
    return Collections.unmodifiableList(new ArrayList<>(new TreeSet<>(reasonCodes)));
  }

  private static double clamp01(double value) {
    return clamp(value, 0.0D, 1.0D);
  }

  private static double clamp(double value, double minimum, double maximum) {
    return Math.max(minimum, Math.min(maximum, value));
  }

  /** Capability mode; routing from Paimon's {@code BucketMode} stays outside the pure formula. */
  public enum EvaluationMode {
    HASH,
    KEY_DYNAMIC
  }

  /** Mutable, bounded collector for one fixed-snapshot {@code (partition,bucket)} unit. */
  public final class UnitAccumulator {
    private long totalFileCount;
    private long totalFileSize;
    private long smallFileCount;
    private long smallFileSize;
    private long totalRecordCount;
    private long tombstoneRecordCount;
    private long deletionVectorRecordCount;
    private final CompensatedSum materializedHealthyRowWeight = new CompensatedSum();
    private boolean deleteMetadataIncomplete;
    private boolean scanMetadataInvalid;

    public void addFile(
        long fileSize, long rowCount, Long tombstoneCount, Long deletionVectorCardinality) {
      if (fileSize < 0 || rowCount < 0) {
        scanMetadataInvalid = true;
        return;
      }
      if (tombstoneCount == null
          || deletionVectorCardinality == null
          || tombstoneCount < 0
          || deletionVectorCardinality < 0
          || tombstoneCount > rowCount
          || deletionVectorCardinality > rowCount
          || (rowCount == 0 && (tombstoneCount > 0 || deletionVectorCardinality > 0))) {
        deleteMetadataIncomplete = true;
        return;
      }

      try {
        totalFileCount = Math.addExact(totalFileCount, 1L);
        totalFileSize = Math.addExact(totalFileSize, fileSize);
        totalRecordCount = Math.addExact(totalRecordCount, rowCount);
        tombstoneRecordCount = Math.addExact(tombstoneRecordCount, tombstoneCount);
        deletionVectorRecordCount =
            Math.addExact(deletionVectorRecordCount, deletionVectorCardinality);
        if (fileSize < compactionFileSize) {
          smallFileCount = Math.addExact(smallFileCount, 1L);
          smallFileSize = Math.addExact(smallFileSize, fileSize);
        }
      } catch (ArithmeticException e) {
        scanMetadataInvalid = true;
        return;
      }

      double contribution =
          rowCount
              * materializedHealth(rowCount, tombstoneCount)
              * materializedHealth(rowCount, deletionVectorCardinality);
      if (!Double.isFinite(contribution)) {
        scanMetadataInvalid = true;
        return;
      }
      materializedHealthyRowWeight.add(contribution);
    }

    public UnitStatistics snapshot(int sortedRunCount) {
      return new UnitStatistics(
          sortedRunCount,
          totalFileCount,
          totalFileSize,
          smallFileCount,
          smallFileSize,
          totalRecordCount,
          tombstoneRecordCount,
          deletionVectorRecordCount,
          materializedHealthyRowWeight.value(),
          deleteMetadataIncomplete,
          scanMetadataInvalid);
    }
  }

  /** Immutable aggregate for one fixed-snapshot {@code (partition,bucket)} unit. */
  public static final class UnitStatistics {
    private final int sortedRunCount;
    private final long totalFileCount;
    private final long totalFileSize;
    private final long smallFileCount;
    private final long smallFileSize;
    private final long totalRecordCount;
    private final long tombstoneRecordCount;
    private final long deletionVectorRecordCount;
    private final double materializedHealthyRowWeight;
    private final boolean deleteMetadataIncomplete;
    private final boolean scanMetadataInvalid;

    private UnitStatistics(
        int sortedRunCount,
        long totalFileCount,
        long totalFileSize,
        long smallFileCount,
        long smallFileSize,
        long totalRecordCount,
        long tombstoneRecordCount,
        long deletionVectorRecordCount,
        double materializedHealthyRowWeight,
        boolean deleteMetadataIncomplete,
        boolean scanMetadataInvalid) {
      this.sortedRunCount = sortedRunCount;
      this.totalFileCount = totalFileCount;
      this.totalFileSize = totalFileSize;
      this.smallFileCount = smallFileCount;
      this.smallFileSize = smallFileSize;
      this.totalRecordCount = totalRecordCount;
      this.tombstoneRecordCount = tombstoneRecordCount;
      this.deletionVectorRecordCount = deletionVectorRecordCount;
      this.materializedHealthyRowWeight = materializedHealthyRowWeight;
      this.deleteMetadataIncomplete = deleteMetadataIncomplete;
      this.scanMetadataInvalid = scanMetadataInvalid;
    }
  }

  private final class Aggregate {
    private long totalFileCount;
    private long totalFileSize;
    private long smallFileCount;
    private long smallFileSize;
    private long totalRecordCount;
    private long tombstoneRecordCount;
    private long deletionVectorRecordCount;
    private int effectiveUnitCount;
    private int maxSortedRunCount;
    private boolean hasSevereUnit;
    private boolean deleteMetadataIncomplete;
    private final CompensatedSum materializedHealthyRowWeight = new CompensatedSum();
    private final CompensatedSum basePressureSum = new CompensatedSum();
    private final CompensatedSum severePressureSum = new CompensatedSum();
    private final CompensatedSum byteWeightedBasePressure = new CompensatedSum();
    private final CompensatedSum byteWeightedSeverePressure = new CompensatedSum();

    private boolean add(UnitStatistics unit) {
      if (unit.deleteMetadataIncomplete) {
        deleteMetadataIncomplete = true;
        return true;
      }
      if (!valid(unit)) {
        return false;
      }
      if (unit.totalFileCount == 0) {
        return true;
      }

      Pressure pressure = pressure(unit.sortedRunCount);
      try {
        totalFileCount = Math.addExact(totalFileCount, unit.totalFileCount);
        totalFileSize = Math.addExact(totalFileSize, unit.totalFileSize);
        smallFileCount = Math.addExact(smallFileCount, unit.smallFileCount);
        smallFileSize = Math.addExact(smallFileSize, unit.smallFileSize);
        totalRecordCount = Math.addExact(totalRecordCount, unit.totalRecordCount);
        tombstoneRecordCount = Math.addExact(tombstoneRecordCount, unit.tombstoneRecordCount);
        deletionVectorRecordCount =
            Math.addExact(deletionVectorRecordCount, unit.deletionVectorRecordCount);
        effectiveUnitCount = Math.addExact(effectiveUnitCount, 1);
      } catch (ArithmeticException e) {
        return false;
      }
      if (totalFileCount > Integer.MAX_VALUE || smallFileCount > Integer.MAX_VALUE) {
        return false;
      }

      maxSortedRunCount = Math.max(maxSortedRunCount, unit.sortedRunCount);
      hasSevereUnit |= unit.sortedRunCount >= stopTrigger;
      materializedHealthyRowWeight.add(unit.materializedHealthyRowWeight);
      basePressureSum.add(pressure.base);
      severePressureSum.add(pressure.severe);
      byteWeightedBasePressure.add(unit.totalFileSize * pressure.base);
      byteWeightedSeverePressure.add(unit.totalFileSize * pressure.severe);
      return materializedHealthyRowWeight.finite()
          && basePressureSum.finite()
          && severePressureSum.finite()
          && byteWeightedBasePressure.finite()
          && byteWeightedSeverePressure.finite();
    }

    private boolean valid(UnitStatistics unit) {
      if (unit.scanMetadataInvalid
          || unit.sortedRunCount < 0
          || unit.totalFileCount < 0
          || unit.totalFileSize < 0
          || unit.smallFileCount < 0
          || unit.smallFileSize < 0
          || unit.totalRecordCount < 0
          || unit.tombstoneRecordCount < 0
          || unit.deletionVectorRecordCount < 0
          || unit.smallFileCount > unit.totalFileCount
          || unit.smallFileSize > unit.totalFileSize
          || !Double.isFinite(unit.materializedHealthyRowWeight)
          || unit.materializedHealthyRowWeight < 0.0D) {
        return false;
      }
      if (unit.totalFileCount == 0) {
        return unit.sortedRunCount == 0
            && unit.totalFileSize == 0
            && unit.smallFileCount == 0
            && unit.smallFileSize == 0
            && unit.totalRecordCount == 0
            && unit.tombstoneRecordCount == 0
            && unit.deletionVectorRecordCount == 0
            && unit.materializedHealthyRowWeight == 0.0D;
      }
      return unit.sortedRunCount > 0
          && unit.tombstoneRecordCount <= unit.totalRecordCount
          && unit.deletionVectorRecordCount <= unit.totalRecordCount
          && unit.materializedHealthyRowWeight <= unit.totalRecordCount;
    }
  }

  private static final class Pressure {
    private static final Pressure ZERO = new Pressure(0.0D, 0.0D);

    private final double base;
    private final double severe;

    private Pressure(double base, double severe) {
      this.base = base;
      this.severe = severe;
    }
  }

  private static final class CompensatedSum {
    private double sum;
    private double compensation;

    private void add(double value) {
      double corrected = value - compensation;
      double next = sum + corrected;
      compensation = (next - sum) - corrected;
      sum = next;
    }

    private double value() {
      return sum;
    }

    private boolean finite() {
      return Double.isFinite(sum) && Double.isFinite(compensation);
    }
  }

  /** Immutable formula output with bounded aggregate metrics. */
  public static final class Result {
    private final int healthScore;
    private final int baseScore;
    private final int runScore;
    private final int materializedDeleteScore;
    private final long totalFileCount;
    private final long totalFileSize;
    private final long smallFileCount;
    private final long smallFileSize;
    private final long totalRecordCount;
    private final long tombstoneRecordCount;
    private final long deletionVectorRecordCount;
    private final int effectiveUnitCount;
    private final int maxSortedRunCount;
    private final PaimonActivityHealth.Result activity;
    private final List<String> reasonCodes;

    private Result(
        int healthScore,
        int baseScore,
        int runScore,
        int materializedDeleteScore,
        long totalFileCount,
        long totalFileSize,
        long smallFileCount,
        long smallFileSize,
        long totalRecordCount,
        long tombstoneRecordCount,
        long deletionVectorRecordCount,
        int effectiveUnitCount,
        int maxSortedRunCount,
        PaimonActivityHealth.Result activity,
        List<String> reasonCodes) {
      this.healthScore = healthScore;
      this.baseScore = baseScore;
      this.runScore = runScore;
      this.materializedDeleteScore = materializedDeleteScore;
      this.totalFileCount = totalFileCount;
      this.totalFileSize = totalFileSize;
      this.smallFileCount = smallFileCount;
      this.smallFileSize = smallFileSize;
      this.totalRecordCount = totalRecordCount;
      this.tombstoneRecordCount = tombstoneRecordCount;
      this.deletionVectorRecordCount = deletionVectorRecordCount;
      this.effectiveUnitCount = effectiveUnitCount;
      this.maxSortedRunCount = maxSortedRunCount;
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
          0,
          0,
          null,
          Collections.singletonList(reasonCode));
    }

    private static Result valid(
        int runScore,
        int materializedDeleteScore,
        int baseScore,
        Aggregate aggregate,
        PaimonActivityHealth.Result activity,
        List<String> reasonCodes) {
      return new Result(
          activity.healthScore(),
          baseScore,
          runScore,
          materializedDeleteScore,
          aggregate.totalFileCount,
          aggregate.totalFileSize,
          aggregate.smallFileCount,
          aggregate.smallFileSize,
          aggregate.totalRecordCount,
          aggregate.tombstoneRecordCount,
          aggregate.deletionVectorRecordCount,
          aggregate.effectiveUnitCount,
          aggregate.maxSortedRunCount,
          activity,
          reasonCodes);
    }

    public PaimonPrimaryKeyPendingInput toPendingInput() {
      return new PaimonPrimaryKeyPendingInput(
          (int) totalFileCount,
          totalFileSize,
          totalRecordCount,
          (int) smallFileCount,
          smallFileSize,
          tombstoneRecordCount,
          deletionVectorRecordCount,
          effectiveUnitCount,
          maxSortedRunCount,
          runScore,
          materializedDeleteScore,
          baseScore,
          healthScore);
    }

    public int healthScore() {
      return healthScore;
    }

    public int baseScore() {
      return baseScore;
    }

    public int runScore() {
      return runScore;
    }

    public int materializedDeleteScore() {
      return materializedDeleteScore;
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

    public long totalRecordCount() {
      return totalRecordCount;
    }

    public long tombstoneRecordCount() {
      return tombstoneRecordCount;
    }

    public long deletionVectorRecordCount() {
      return deletionVectorRecordCount;
    }

    public int effectiveUnitCount() {
      return effectiveUnitCount;
    }

    public int maxSortedRunCount() {
      return maxSortedRunCount;
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
          && runScore == that.runScore
          && materializedDeleteScore == that.materializedDeleteScore
          && totalFileCount == that.totalFileCount
          && totalFileSize == that.totalFileSize
          && smallFileCount == that.smallFileCount
          && smallFileSize == that.smallFileSize
          && totalRecordCount == that.totalRecordCount
          && tombstoneRecordCount == that.tombstoneRecordCount
          && deletionVectorRecordCount == that.deletionVectorRecordCount
          && effectiveUnitCount == that.effectiveUnitCount
          && maxSortedRunCount == that.maxSortedRunCount
          && Objects.equals(activity, that.activity)
          && reasonCodes.equals(that.reasonCodes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          healthScore,
          baseScore,
          runScore,
          materializedDeleteScore,
          totalFileCount,
          totalFileSize,
          smallFileCount,
          smallFileSize,
          totalRecordCount,
          tombstoneRecordCount,
          deletionVectorRecordCount,
          effectiveUnitCount,
          maxSortedRunCount,
          activity,
          reasonCodes);
    }
  }
}
