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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class TestPaimonAppendHealthEvaluator {

  private static final long TARGET_SIZE = 1_000L;
  private static final long SMALL_FILE_BOUNDARY = 500L;

  @Test
  public void emptyTableIsHealthy() {
    PaimonAppendHealthEvaluator.Result result =
        evaluator().evaluate(Collections.emptyList(), neutralActivity(100));

    assertEquals(100, result.healthScore());
    assertEquals(100, result.fileScore());
    assertEquals(100, result.deleteScore());
    assertEquals(0L, result.totalFileCount());
    assertEquals(PaimonAppendHealthEvaluator.EMPTY_TABLE, result.reasonCodes().get(0));
  }

  @Test
  public void fileOrganizationAnchorsMatchSpecification() {
    assertFileAnchor(unitsWithNaturalTails(100), 100, 0L, 100L);
    assertFileAnchor(Collections.singletonList(unit(2, 250L)), 95, 1L, 1L);
    assertFileAnchor(Collections.singletonList(unit(100, 900L)), 90, 10L, 90L);
    assertFileAnchor(Collections.singletonList(unit(10_000, 900L)), 60, 1_000L, 9_000L);
    assertFileAnchor(Collections.singletonList(unit(10_000, 500L)), 20, 5_000L, 5_000L);
  }

  @Test
  public void partitionImbalanceDoesNotCreateAnIndependentPenalty() {
    PaimonAppendHealthEvaluator.UnitStatistics largeUnit = unit(100, TARGET_SIZE);
    PaimonAppendHealthEvaluator.UnitStatistics smallUnit = unit(1, TARGET_SIZE);

    PaimonAppendHealthEvaluator.Result result =
        evaluator().evaluate(Arrays.asList(largeUnit, smallUnit), neutralActivity(100));

    assertEquals(100, result.fileScore());
    assertEquals(100, result.baseScore());
    assertEquals(100, result.healthScore());
  }

  @Test
  public void deleteScoreUsesRowWeightedNonLinearFormula() {
    PaimonAppendHealthEvaluator.UnitAccumulator unit = evaluator().newUnitAccumulator();
    unit.addFile(TARGET_SIZE, 100L, 0L);
    unit.addFile(TARGET_SIZE, 100L, 50L);

    PaimonAppendHealthEvaluator.Result result =
        evaluator().evaluate(Collections.singletonList(unit.snapshot()), neutralActivity(100));

    assertEquals(60, result.deleteScore());
    assertEquals(92, result.baseScore());
    assertEquals(92, result.healthScore());
    assertEquals(200L, result.totalRecordCount());
    assertEquals(50L, result.deleteRecordCount());
  }

  @Test
  public void exactDeleteMetadataValidationReturnsStableReason() {
    assertDeleteMetadataIncomplete(100L, null);
    assertDeleteMetadataIncomplete(100L, -1L);
    assertDeleteMetadataIncomplete(100L, 101L);
    assertDeleteMetadataIncomplete(0L, 1L);
  }

  @Test
  public void invalidScoringConfigurationReturnsStableReason() {
    PaimonAppendHealthEvaluator invalidTarget =
        new PaimonAppendHealthEvaluator(0L, SMALL_FILE_BOUNDARY);
    PaimonAppendHealthEvaluator invalidBoundary =
        new PaimonAppendHealthEvaluator(TARGET_SIZE, TARGET_SIZE + 1);

    assertEquals(
        PaimonAppendHealthEvaluator.INVALID_SCORING_CONFIG,
        invalidTarget.evaluate(Collections.emptyList(), neutralActivity(100)).reasonCodes().get(0));
    assertEquals(
        PaimonAppendHealthEvaluator.INVALID_SCORING_CONFIG,
        invalidBoundary
            .evaluate(Collections.emptyList(), neutralActivity(100))
            .reasonCodes()
            .get(0));
  }

  @Test
  public void invalidFileMetadataReturnsStableReason() {
    PaimonAppendHealthEvaluator.UnitAccumulator negative = evaluator().newUnitAccumulator();
    negative.addFile(-1L, 0L, 0L);

    assertEquals(
        PaimonAppendHealthEvaluator.SNAPSHOT_SCAN_FAILED,
        evaluator()
            .evaluate(Collections.singletonList(negative.snapshot()), neutralActivity(100))
            .reasonCodes()
            .get(0));
  }

  @Test
  public void unitAggregatesCannotBeEvaluatedUnderDifferentFileBoundaries() {
    PaimonAppendHealthEvaluator.UnitStatistics unit = unit(2L, 250L);
    PaimonAppendHealthEvaluator differentConfig =
        new PaimonAppendHealthEvaluator(TARGET_SIZE * 2, SMALL_FILE_BOUNDARY);

    PaimonAppendHealthEvaluator.Result result =
        differentConfig.evaluate(Collections.singletonList(unit), neutralActivity(100));
    assertEquals(-1, result.healthScore());
    assertEquals(PaimonAppendHealthEvaluator.SNAPSHOT_SCAN_FAILED, result.reasonCodes().get(0));
  }

  @Test
  public void activityAmplifiesOnlyExistingAppendDebt() {
    PaimonAppendHealthEvaluator.UnitStatistics fragmented = unit(10_000, 500L);
    PaimonActivityHealth.Input saturated = PaimonActivityHealth.Input.withBaseline(0, 110L, 100L);

    PaimonAppendHealthEvaluator.Result result =
        evaluator().evaluate(Collections.singletonList(fragmented), saturated);

    assertEquals(36, result.baseScore());
    assertEquals(20, result.fileScore());
    assertEquals(100, result.deleteScore());
    assertEquals(20, result.healthScore());

    PaimonAppendHealthEvaluator.Result healthy =
        evaluator().evaluate(unitsWithNaturalTails(100), saturated);
    assertEquals(100, healthy.healthScore());
  }

  @Test
  public void resultContainsDeterministicAggregatesAndIsRepeatable() {
    PaimonAppendHealthEvaluator.UnitAccumulator firstUnit = evaluator().newUnitAccumulator();
    firstUnit.addFile(100L, 10L, 0L);
    firstUnit.addFile(750L, 20L, 2L);
    PaimonAppendHealthEvaluator.UnitAccumulator secondUnit = evaluator().newUnitAccumulator();
    secondUnit.addFile(1_500L, 30L, 0L);

    PaimonActivityHealth.Input activity = PaimonActivityHealth.Input.withBaseline(0, 103L, 100L);
    PaimonAppendHealthEvaluator.Result first =
        evaluator().evaluate(Arrays.asList(firstUnit.snapshot(), secondUnit.snapshot()), activity);
    PaimonAppendHealthEvaluator.Result second =
        evaluator().evaluate(Arrays.asList(secondUnit.snapshot(), firstUnit.snapshot()), activity);

    assertEquals(first, second);
    assertEquals(3L, first.totalFileCount());
    assertEquals(2_350L, first.totalFileSize());
    assertEquals(1L, first.smallFileCount());
    assertEquals(100L, first.smallFileSize());
    assertEquals(1L, first.undersizedFileCount());
    assertEquals(750L, first.undersizedFileSize());
    assertEquals(1L, first.reducibleFileCount());
    assertEquals(2L, first.expectedOutputFileCount());
    assertTrue(first.healthScore() >= 0 && first.healthScore() <= 100);
  }

  @Test
  public void componentAndTotalScoresRemainBoundedAcrossDeleteRatios() {
    for (long deleteCount = 0; deleteCount <= 100; deleteCount++) {
      PaimonAppendHealthEvaluator.UnitAccumulator unit = evaluator().newUnitAccumulator();
      unit.addFile(250L, 100L, deleteCount);
      PaimonAppendHealthEvaluator.Result result =
          evaluator().evaluate(Collections.singletonList(unit.snapshot()), neutralActivity(100));

      assertTrue(result.fileScore() >= 0 && result.fileScore() <= 100);
      assertTrue(result.deleteScore() >= 0 && result.deleteScore() <= 100);
      assertTrue(result.baseScore() >= 0 && result.baseScore() <= 100);
      assertTrue(result.healthScore() >= 0 && result.healthScore() <= 100);
    }
  }

  private void assertFileAnchor(
      Iterable<PaimonAppendHealthEvaluator.UnitStatistics> units,
      int expectedFileScore,
      long expectedReducibleFiles,
      long expectedOutputFiles) {
    PaimonAppendHealthEvaluator.Result result = evaluator().evaluate(units, neutralActivity(100));
    assertEquals(expectedFileScore, result.fileScore());
    assertEquals(expectedReducibleFiles, result.reducibleFileCount());
    assertEquals(expectedOutputFiles, result.expectedOutputFileCount());
  }

  private void assertDeleteMetadataIncomplete(long rowCount, Long deleteCount) {
    PaimonAppendHealthEvaluator.UnitAccumulator unit = evaluator().newUnitAccumulator();
    unit.addFile(TARGET_SIZE, rowCount, deleteCount);
    PaimonAppendHealthEvaluator.Result result =
        evaluator().evaluate(Collections.singletonList(unit.snapshot()), neutralActivity(100));
    assertEquals(-1, result.healthScore());
    assertEquals(
        PaimonAppendHealthEvaluator.DELETE_METADATA_INCOMPLETE, result.reasonCodes().get(0));
  }

  private Iterable<PaimonAppendHealthEvaluator.UnitStatistics> unitsWithNaturalTails(int count) {
    PaimonAppendHealthEvaluator.UnitStatistics[] units =
        new PaimonAppendHealthEvaluator.UnitStatistics[count];
    Arrays.fill(units, unit(1, 900L));
    return Arrays.asList(units);
  }

  private PaimonAppendHealthEvaluator.UnitStatistics unit(long fileCount, long fileSize) {
    PaimonAppendHealthEvaluator.UnitAccumulator unit = evaluator().newUnitAccumulator();
    for (long i = 0; i < fileCount; i++) {
      unit.addFile(fileSize, 100L, 0L);
    }
    return unit.snapshot();
  }

  private PaimonAppendHealthEvaluator evaluator() {
    return new PaimonAppendHealthEvaluator(TARGET_SIZE, SMALL_FILE_BOUNDARY);
  }

  private PaimonActivityHealth.Input neutralActivity(int ignoredBaseScore) {
    return PaimonActivityHealth.Input.withoutBaseline(ignoredBaseScore);
  }
}
