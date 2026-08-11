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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Pure activity-debt formula shared by Paimon health evaluators. */
public final class PaimonActivityHealth {

  public static final String FORMULA_VERSION = "paimon-activity-v2";
  public static final String SUCCESS_BASELINE_UNAVAILABLE = "SUCCESS_BASELINE_UNAVAILABLE";
  public static final String SUCCESS_BASELINE_INVALID = "SUCCESS_BASELINE_INVALID";
  public static final String INVALID_SCORING_CONFIG = "INVALID_SCORING_CONFIG";

  private static final int SNAPSHOT_SATURATION_COUNT = 10;
  private static final double MAX_DEBT_AMPLIFICATION = 0.25D;

  private PaimonActivityHealth() {}

  public static Result evaluate(Input input) {
    Objects.requireNonNull(input, "input");
    if (input.baseScore < 0 || input.baseScore > 100) {
      return Result.invalid(input.baseScore, INVALID_SCORING_CONFIG);
    }

    if (!input.hasAnyBaselineValue()) {
      return Result.neutral(input.baseScore, SUCCESS_BASELINE_UNAVAILABLE);
    }

    if (!input.hasCompleteBaseline()
        || input.latestSnapshotId < 1
        || input.baselineSnapshotId < 1) {
      return Result.invalid(input.baseScore, SUCCESS_BASELINE_INVALID);
    }

    long newSnapshotCount = nonNegativeDifference(input.latestSnapshotId, input.baselineSnapshotId);
    double snapshotPressure = Math.min(1.0D, (double) newSnapshotCount / SNAPSHOT_SATURATION_COUNT);
    double activityPressure = snapshotPressure;
    double baseDebt = 1.0D - input.baseScore / 100.0D;
    double finalDebt =
        Math.min(1.0D, baseDebt * (1.0D + MAX_DEBT_AMPLIFICATION * activityPressure));
    int healthScore = ScoreUtil.clampScore(Math.round(100.0D * (1.0D - finalDebt)));

    return new Result(
        healthScore,
        input.baseScore,
        newSnapshotCount,
        snapshotPressure,
        activityPressure,
        true,
        Collections.emptyList());
  }

  private static long nonNegativeDifference(long latest, long baseline) {
    return latest <= baseline ? 0L : latest - baseline;
  }

  /** Immutable inputs fixed from Snapshot metadata and the existing optimizing checkpoint. */
  public static final class Input {
    private final int baseScore;
    private final Long latestSnapshotId;
    private final Long baselineSnapshotId;

    private Input(int baseScore, Long latestSnapshotId, Long baselineSnapshotId) {
      this.baseScore = baseScore;
      this.latestSnapshotId = latestSnapshotId;
      this.baselineSnapshotId = baselineSnapshotId;
    }

    public static Input withoutBaseline(int baseScore) {
      return new Input(baseScore, null, null);
    }

    public static Input withBaseline(
        int baseScore, long latestSnapshotId, long baselineSnapshotId) {
      return new Input(baseScore, latestSnapshotId, baselineSnapshotId);
    }

    public Input withBaseScore(int replacementBaseScore) {
      return new Input(replacementBaseScore, latestSnapshotId, baselineSnapshotId);
    }

    private boolean hasAnyBaselineValue() {
      return latestSnapshotId != null || baselineSnapshotId != null;
    }

    private boolean hasCompleteBaseline() {
      return latestSnapshotId != null && baselineSnapshotId != null;
    }
  }

  /** Immutable activity result suitable for later projection into common health details. */
  public static final class Result {
    private final int healthScore;
    private final int baseScore;
    private final long newSnapshotCount;
    private final double snapshotPressure;
    private final double activityPressure;
    private final boolean baselineAvailable;
    private final List<String> reasonCodes;

    private Result(
        int healthScore,
        int baseScore,
        long newSnapshotCount,
        double snapshotPressure,
        double activityPressure,
        boolean baselineAvailable,
        List<String> reasonCodes) {
      this.healthScore = healthScore;
      this.baseScore = baseScore;
      this.newSnapshotCount = newSnapshotCount;
      this.snapshotPressure = snapshotPressure;
      this.activityPressure = activityPressure;
      this.baselineAvailable = baselineAvailable;
      this.reasonCodes = Collections.unmodifiableList(new java.util.ArrayList<>(reasonCodes));
    }

    private static Result neutral(int baseScore, String reasonCode) {
      return new Result(
          baseScore, baseScore, -1L, 0.0D, 0.0D, false, Collections.singletonList(reasonCode));
    }

    private static Result invalid(int baseScore, String reasonCode) {
      return new Result(
          -1, baseScore, -1L, 0.0D, 0.0D, false, Collections.singletonList(reasonCode));
    }

    public int healthScore() {
      return healthScore;
    }

    public int baseScore() {
      return baseScore;
    }

    public long newSnapshotCount() {
      return newSnapshotCount;
    }

    public double snapshotPressure() {
      return snapshotPressure;
    }

    public double activityPressure() {
      return activityPressure;
    }

    public boolean baselineAvailable() {
      return baselineAvailable;
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
          && newSnapshotCount == that.newSnapshotCount
          && Double.compare(snapshotPressure, that.snapshotPressure) == 0
          && Double.compare(activityPressure, that.activityPressure) == 0
          && baselineAvailable == that.baselineAvailable
          && reasonCodes.equals(that.reasonCodes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          healthScore,
          baseScore,
          newSnapshotCount,
          snapshotPressure,
          activityPressure,
          baselineAvailable,
          reasonCodes);
    }
  }
}
