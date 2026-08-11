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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator.EvaluationMode;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator.Result;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator.UnitAccumulator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator.UnitStatistics;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyPendingInput;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestPaimonPrimaryKeyHealthEvaluator {

  private static final long TARGET_FILE_SIZE = 1_000L;
  private static final long COMPACTION_FILE_SIZE = 700L;

  @Test
  public void defaultSortedRunAnchorsMatchSpecification() {
    int[] sortedRuns = {1, 4, 5, 6, 7, 8, 9, 10, 11};
    int[] expectedScores = {100, 100, 73, 67, 60, 44, 30, 15, 0};

    for (int index = 0; index < sortedRuns.length; index++) {
      PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
      Result result =
          evaluator.evaluate(
              Collections.singletonList(healthyUnit(evaluator, sortedRuns[index], 100L)),
              PaimonActivityHealth.Input.withoutBaseline(0),
              EvaluationMode.HASH);

      assertEquals(expectedScores[index], result.runScore(), "R=" + sortedRuns[index]);
      assertEquals(expectedScores[index], result.baseScore(), "R=" + sortedRuns[index]);
      assertEquals(expectedScores[index], result.healthScore(), "R=" + sortedRuns[index]);
    }
  }

  @Test
  public void byteWeightedExposureCanDominateUnitMean() {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    UnitStatistics healthySmall = healthyUnit(evaluator, 1, 100L);
    UnitStatistics severeLarge = healthyUnit(evaluator, 8, 900L);

    Result result =
        evaluator.evaluate(
            Arrays.asList(healthySmall, severeLarge),
            PaimonActivityHealth.Input.withoutBaseline(0),
            EvaluationMode.HASH);

    assertEquals(50, result.runScore());
    assertEquals(50, result.healthScore());
  }

  @Test
  public void tombstoneAndDeletionVectorHealthAreMultipliedPerFile() {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    UnitAccumulator accumulator = evaluator.newUnitAccumulator();
    accumulator.addFile(900L, 100L, 10L, 20L);

    Result result =
        evaluator.evaluate(
            Collections.singletonList(accumulator.snapshot(1)),
            PaimonActivityHealth.Input.withoutBaseline(0),
            EvaluationMode.HASH);

    assertEquals(67, result.materializedDeleteScore());
    assertEquals(67, result.baseScore());
    assertEquals(67, result.healthScore());
    assertEquals(10L, result.tombstoneRecordCount());
    assertEquals(20L, result.deletionVectorRecordCount());
  }

  @Test
  public void deletionHealthIsWeightedByPhysicalRowsAcrossFiles() {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    UnitAccumulator accumulator = evaluator.newUnitAccumulator();
    accumulator.addFile(900L, 100L, 0L, 0L);
    accumulator.addFile(900L, 300L, 150L, 0L);

    Result result =
        evaluator.evaluate(
            Collections.singletonList(accumulator.snapshot(1)),
            PaimonActivityHealth.Input.withoutBaseline(0),
            EvaluationMode.HASH);

    assertEquals(25, result.materializedDeleteScore());
    assertEquals(25, result.healthScore());
  }

  @Test
  public void missingOrIllegalDeleteMetadataNeverBecomesZero() {
    List<UnitStatistics> incompleteUnits =
        Arrays.asList(
            unitWithDeleteMetadata(null, 0L, 100L),
            unitWithDeleteMetadata(0L, null, 100L),
            unitWithDeleteMetadata(-1L, 0L, 100L),
            unitWithDeleteMetadata(0L, -1L, 100L),
            unitWithDeleteMetadata(101L, 0L, 100L),
            unitWithDeleteMetadata(0L, 101L, 100L),
            unitWithDeleteMetadata(1L, 0L, 0L),
            unitWithDeleteMetadata(0L, 1L, 0L));

    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    for (UnitStatistics unit : incompleteUnits) {
      Result result =
          evaluator.evaluate(
              Collections.singletonList(unit),
              PaimonActivityHealth.Input.withoutBaseline(0),
              EvaluationMode.HASH);

      assertEquals(-1, result.healthScore());
      assertEquals(
          Collections.singletonList(PaimonPrimaryKeyHealthEvaluator.DELETE_METADATA_INCOMPLETE),
          result.reasonCodes());
    }
  }

  @Test
  public void invalidScoringConfigurationIsExplicit() {
    List<PaimonPrimaryKeyHealthEvaluator> invalidEvaluators =
        Arrays.asList(
            evaluator(0, 8, 6),
            evaluator(5, 4, 6),
            evaluator(5, 8, 1),
            new PaimonPrimaryKeyHealthEvaluator(5, 8, 6, 0L, 0L),
            new PaimonPrimaryKeyHealthEvaluator(5, 8, 6, 1_000L, 1_001L));

    for (PaimonPrimaryKeyHealthEvaluator evaluator : invalidEvaluators) {
      Result result =
          evaluator.evaluate(
              Collections.emptyList(),
              PaimonActivityHealth.Input.withoutBaseline(0),
              EvaluationMode.HASH);

      assertEquals(-1, result.healthScore());
      assertEquals(
          Collections.singletonList(PaimonPrimaryKeyHealthEvaluator.INVALID_SCORING_CONFIG),
          result.reasonCodes());
    }
  }

  @Test
  public void auxiliarySmallFileMetricsDoNotChangePrimaryKeyScore() {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    UnitAccumulator small = evaluator.newUnitAccumulator();
    small.addFile(100L, 100L, 0L, 0L);
    UnitAccumulator large = evaluator.newUnitAccumulator();
    large.addFile(900L, 100L, 0L, 0L);

    Result smallResult =
        evaluator.evaluate(
            Collections.singletonList(small.snapshot(5)),
            PaimonActivityHealth.Input.withoutBaseline(0),
            EvaluationMode.HASH);
    Result largeResult =
        evaluator.evaluate(
            Collections.singletonList(large.snapshot(5)),
            PaimonActivityHealth.Input.withoutBaseline(0),
            EvaluationMode.HASH);

    assertEquals(smallResult.healthScore(), largeResult.healthScore());
    assertEquals(1L, smallResult.smallFileCount());
    assertEquals(0L, largeResult.smallFileCount());
  }

  @Test
  public void activityOnlyAmplifiesExistingStructuralDebt() {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    UnitStatistics unit = healthyUnit(evaluator, 7, 900L);

    Result result =
        evaluator.evaluate(
            Collections.singletonList(unit),
            PaimonActivityHealth.Input.withBaseline(0, 110L, 100L),
            EvaluationMode.HASH);

    assertEquals(60, result.baseScore());
    assertEquals(50, result.healthScore());
  }

  @Test
  public void keyDynamicUsesSameStructureAndCheckpointAsHash() {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    UnitStatistics unit = healthyUnit(evaluator, 7, 900L);
    PaimonActivityHealth.Input staleHashBaseline =
        PaimonActivityHealth.Input.withBaseline(0, 110L, 100L);

    Result hash =
        evaluator.evaluate(Collections.singletonList(unit), staleHashBaseline, EvaluationMode.HASH);
    Result keyDynamic =
        evaluator.evaluate(
            Collections.singletonList(unit), staleHashBaseline, EvaluationMode.KEY_DYNAMIC);

    assertEquals(hash.runScore(), keyDynamic.runScore());
    assertEquals(hash.materializedDeleteScore(), keyDynamic.materializedDeleteScore());
    assertEquals(hash.baseScore(), keyDynamic.baseScore());
    assertEquals(50, hash.healthScore());
    assertEquals(50, keyDynamic.healthScore());
    assertEquals(
        Arrays.asList(
            PaimonPrimaryKeyHealthEvaluator.KEY_DYNAMIC_LOCAL_INDEX_NOT_EVALUATED,
            PaimonPrimaryKeyHealthEvaluator.KEY_DYNAMIC_OPTIMIZING_UNSUPPORTED),
        keyDynamic.reasonCodes());
  }

  @Test
  public void emptyTableIsHealthyAndRetainsCapabilityReasons() {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);

    Result result =
        evaluator.evaluate(
            Collections.emptyList(),
            PaimonActivityHealth.Input.withoutBaseline(0),
            EvaluationMode.KEY_DYNAMIC);

    assertEquals(100, result.healthScore());
    assertEquals(100, result.runScore());
    assertEquals(100, result.materializedDeleteScore());
    assertEquals(
        Arrays.asList(
            PaimonPrimaryKeyHealthEvaluator.EMPTY_TABLE,
            PaimonPrimaryKeyHealthEvaluator.KEY_DYNAMIC_LOCAL_INDEX_NOT_EVALUATED,
            PaimonPrimaryKeyHealthEvaluator.KEY_DYNAMIC_OPTIMIZING_UNSUPPORTED,
            PaimonActivityHealth.SUCCESS_BASELINE_UNAVAILABLE),
        result.reasonCodes());
  }

  @Test
  public void resultProjectsIndependentPrimaryKeyPendingInput() {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    UnitAccumulator first = evaluator.newUnitAccumulator();
    first.addFile(100L, 100L, 10L, 20L);
    UnitAccumulator second = evaluator.newUnitAccumulator();
    second.addFile(900L, 300L, 0L, 0L);

    Result result =
        evaluator.evaluate(
            Arrays.asList(first.snapshot(5), second.snapshot(8)),
            PaimonActivityHealth.Input.withoutBaseline(0),
            EvaluationMode.HASH);
    PaimonPrimaryKeyPendingInput pendingInput = result.toPendingInput();

    assertEquals(2, pendingInput.getDataFileCount());
    assertEquals(1_000L, pendingInput.getDataFileSize());
    assertEquals(400L, pendingInput.getDataRecordCount());
    assertEquals(1, pendingInput.getSmallFileCount());
    assertEquals(100L, pendingInput.getSmallFileSize());
    assertEquals(10L, pendingInput.getTombstoneRecordCount());
    assertEquals(20L, pendingInput.getDeletionVectorRecordCount());
    assertEquals(2, pendingInput.getEffectiveUnitCount());
    assertEquals(8, pendingInput.getMaxSortedRunCount());
    assertEquals(result.runScore(), pendingInput.getRunScore());
    assertEquals(result.materializedDeleteScore(), pendingInput.getMaterializedDeleteScore());
    assertEquals(result.baseScore(), pendingInput.getPrimaryKeyBaseScore());
    assertEquals(result.healthScore(), pendingInput.getHealthScore());
    assertTrue(pendingInput.getTotalFileCount() > 0);
  }

  @Test
  public void defaultPendingInputDoesNotPretendToHaveAValidScore() {
    PaimonPrimaryKeyPendingInput pendingInput = new PaimonPrimaryKeyPendingInput();

    assertEquals(0, pendingInput.getDataFileCount());
    assertEquals(0L, pendingInput.getDataFileSize());
    assertEquals(-1, pendingInput.getRunScore());
    assertEquals(-1, pendingInput.getMaterializedDeleteScore());
    assertEquals(-1, pendingInput.getPrimaryKeyBaseScore());
    assertEquals(-1, pendingInput.getHealthScore());
  }

  private static PaimonPrimaryKeyHealthEvaluator evaluator(
      int compactionTrigger, int stopTrigger, int numLevels) {
    return new PaimonPrimaryKeyHealthEvaluator(
        compactionTrigger, stopTrigger, numLevels, TARGET_FILE_SIZE, COMPACTION_FILE_SIZE);
  }

  private static UnitStatistics healthyUnit(
      PaimonPrimaryKeyHealthEvaluator evaluator, int sortedRunCount, long fileSize) {
    UnitAccumulator accumulator = evaluator.newUnitAccumulator();
    accumulator.addFile(fileSize, 100L, 0L, 0L);
    return accumulator.snapshot(sortedRunCount);
  }

  private static UnitStatistics unitWithDeleteMetadata(
      Long tombstoneCount, Long deletionVectorCount, long rowCount) {
    PaimonPrimaryKeyHealthEvaluator evaluator = evaluator(5, 8, 6);
    UnitAccumulator accumulator = evaluator.newUnitAccumulator();
    accumulator.addFile(900L, rowCount, tombstoneCount, deletionVectorCount);
    return accumulator.snapshot(1);
  }
}
