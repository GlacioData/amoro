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

package org.apache.amoro.formats.paimon.optimizing.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.OptimizerProperties;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.stats.SimpleStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class TestPaimonFixedBucketTaskPacker {

  private static final BinaryRow PARTITION = BinaryRow.singleColumn(7);

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void omittedHealthyMiddleFileRemainsAnOrderedBarrier(boolean ordered) {
    PaimonPlanContext context = context(1000L, 10_000L, 10_000L, 3, -1, 0, false, 10L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta middle = file("healthy", 1000L, 10L);
    DataFileMeta last = file("last", 100L, 20L);
    PaimonFixedBucketFileWindow full = window(ordered, first, middle, last);
    PaimonFixedBucketFileWindow bounded =
        new PaimonFixedBucketFileWindow(
            full.unit(), full.totalBuckets(), 2, ordered, entry -> entry.file().fileSize() < 1000L);
    full.files().forEach(bounded::add);
    bounded.finish();
    if (bounded.needsScopeProof()) {
      full.files().forEach(bounded::verifyScopeEntry);
      bounded.finishScopeProof();
    }
    assertTrue(bounded.safe());
    assertEquals(2, bounded.files().size());
    // Paimon ac223f47, BaseAppendFileStoreWrite.java:187-222: omitted active files may not
    // disappear as ordered packing barriers even though their full metadata is not retained.
    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context)
            .pack(bounded, new PaimonPartitionEvaluator(context).evaluate(bounded));
    assertEquals(
        ordered ? Collections.emptyList() : Collections.singletonList(Arrays.asList(first, last)),
        tasks);
  }

  // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187-210 starts output at the
  // first input sequence and reads the supplied order. An unselected active file is a barrier.
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void fullRewriteChoiceControlsWhetherTheMiddleHealthyFileIsABarrier(boolean rewriteAll) {
    PaimonPlanContext context = context(1000L, 10_000L, 10_000L, 3, -1, 0, rewriteAll, 10L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta middle = file("healthy", 1000L, 10L);
    DataFileMeta last = file("last", 100L, 20L);
    PaimonFixedBucketFileWindow window = window(true, first, middle, last);
    PaimonPartitionEvaluation evaluation = new PaimonPartitionEvaluator(context).evaluate(window);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context).pack(window, evaluation);

    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    assertEquals(
        rewriteAll
            ? Collections.singletonList(Arrays.asList(first, middle, last))
            : Collections.emptyList(),
        tasks);
    assertExactUniqueScope(window, tasks);
  }

  @Test
  void orderedBarrierComesFromTheWindowEvenWhenEvaluationAllFilesOmitsIt() {
    PaimonPlanContext context = context(10_000L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta middle = file("healthy", 1000L, 10L);
    DataFileMeta last = file("last", 100L, 20L);
    PaimonFixedBucketFileWindow window = window(true, first, middle, last);
    PaimonPartitionEvaluation evaluation = evaluation(context, OptimizingType.FULL, first, last);

    assertTrue(new PaimonFixedBucketTaskPacker(context).pack(window, evaluation).isEmpty());
  }

  @Test
  void shuffledManifestAndSelectionOrderStillProduceSequenceOrderedInputs() {
    PaimonPlanContext context = context(10_000L);
    DataFileMeta first = file("first", 600L, 0L);
    DataFileMeta second = file("second", 100L, 10L);
    DataFileMeta third = file("third", 300L, 20L);
    PaimonFixedBucketFileWindow window = window(true, third, first, second);
    PaimonPartitionEvaluation evaluation =
        evaluation(context, OptimizingType.FULL, second, third, first);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context).pack(window, evaluation);

    assertEquals(Collections.singletonList(Arrays.asList(first, second, third)), tasks);
    assertExactUniqueScope(window, tasks);
  }

  // Paimon ac223f47, append/BucketedAppendCompactManager.java:204-229 has its own native
  // count rule. Amoro keeps its existing count/time/content rules within each safe run.
  @ParameterizedTest
  @CsvSource({"-1, 0", "0, 2"})
  void minorDoesNotPoolFileCountsAcrossAnUnselectedBarrier(int minorInterval, int taskCount) {
    PaimonPlanContext context = context(1000L, 10_000L, 10_000L, 3, minorInterval, -1, false, 10L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta second = file("second", 100L, 10L);
    DataFileMeta barrier = file("barrier", 1000L, 20L);
    DataFileMeta fourth = file("fourth", 100L, 30L);
    DataFileMeta fifth = file("fifth", 100L, 40L);
    PaimonFixedBucketFileWindow window = window(true, fifth, first, barrier, fourth, second);
    PaimonPartitionEvaluation evaluation = new PaimonPartitionEvaluator(context).evaluate(window);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context).pack(window, evaluation);

    assertEquals(OptimizingType.MINOR, evaluation.optimizingType());
    assertEquals(
        taskCount == 0
            ? Collections.emptyList()
            : Arrays.asList(Arrays.asList(first, second), Arrays.asList(fourth, fifth)),
        tasks);
    assertExactUniqueScope(window, tasks);
  }

  @Test
  void minorKeepsOnlyTheRunThatIndependentlyMeetsTheCountThreshold() {
    PaimonPlanContext context = context(10_000L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta second = file("second", 100L, 10L);
    DataFileMeta third = file("third", 100L, 20L);
    DataFileMeta barrier = file("barrier", 1000L, 30L);
    DataFileMeta fifth = file("fifth", 100L, 40L);
    DataFileMeta sixth = file("sixth", 100L, 50L);
    PaimonFixedBucketFileWindow window = window(true, first, second, third, barrier, fifth, sixth);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, new PaimonPartitionEvaluator(context).evaluate(window));

    assertEquals(Collections.singletonList(Arrays.asList(first, second, third)), tasks);
    assertExactUniqueScope(window, tasks);
  }

  @ParameterizedTest
  @CsvSource({"899, false", "900, true"})
  void minorContentThresholdIncludesOpenCostWithinEachRun(long openCost, boolean reachesThreshold) {
    PaimonPlanContext context = context(1000L, 1L, 1L, 5, -1, -1, false, openCost);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta second = file("second", 100L, 10L);
    PaimonFixedBucketFileWindow window = window(true, first, second);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, evaluation(context, OptimizingType.MINOR, first, second));

    assertEquals(
        reachesThreshold
            ? Collections.singletonList(Arrays.asList(first, second))
            : Collections.emptyList(),
        tasks);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void minorDoesNotAcquireMajorFullByteLimitsOrOversizedPartnerRules(boolean ordered) {
    PaimonPlanContext context = context(1L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta second = file("second", 100L, 10L);
    DataFileMeta third = file("third", 100L, 20L);
    PaimonFixedBucketFileWindow window = window(ordered, first, second, third);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, new PaimonPartitionEvaluator(context).evaluate(window));

    assertEquals(Collections.singletonList(Arrays.asList(first, second, third)), tasks);
    assertTrue(
        tasks.get(0).stream().mapToLong(DataFileMeta::fileSize).sum()
            > context.effectiveTaskInputLimit());
  }

  // Paimon ac223f47, utils/BinPacking.java:33-56 can emit singleton bins. Amoro's existing
  // non-DV legality and MAJOR benefit rules must remain stronger than that primitive.
  @ParameterizedTest
  @MethodSource("singletonCases")
  void ordinarySingletonIsNeverForcedIntoATask(boolean ordered, OptimizingType type) {
    PaimonPlanContext context = context(1000L);
    DataFileMeta single = file("single", 100L, 0L);
    PaimonFixedBucketFileWindow window = window(ordered, single);

    assertTrue(
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, evaluation(context, type, single))
            .isEmpty());
  }

  private static Stream<Arguments> singletonCases() {
    return Stream.of(false, true)
        .flatMap(
            ordered ->
                Stream.of(OptimizingType.MINOR, OptimizingType.MAJOR, OptimizingType.FULL)
                    .map(type -> Arguments.of(ordered, type)));
  }

  @ParameterizedTest
  @MethodSource("majorBenefitCases")
  void majorRequiresAnEstimatedReductionInOutputFileCount(
      boolean ordered, long secondSize, boolean beneficial) {
    PaimonPlanContext context = context(10_000L);
    DataFileMeta first = file("undersized", 700L, 0L);
    DataFileMeta second = file("small", secondSize, 10L);
    PaimonFixedBucketFileWindow window = window(ordered, first, second);
    PaimonPartitionEvaluation evaluation = new PaimonPartitionEvaluator(context).evaluate(window);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context).pack(window, evaluation);

    assertEquals(OptimizingType.MAJOR, evaluation.optimizingType());
    assertEquals(beneficial ? 1 : 0, tasks.size());
    assertExactUniqueScope(window, tasks);
  }

  private static Stream<Arguments> majorBenefitCases() {
    return Stream.of(false, true)
        .flatMap(
            ordered ->
                Stream.of(
                    Arguments.of(ordered, 299L, true),
                    Arguments.of(ordered, 300L, true),
                    Arguments.of(ordered, 301L, false)));
  }

  @Test
  void fullDoesNotAcquireTheMajorOnlyEstimatedReductionRule() {
    PaimonPlanContext context = context(1000L, 10_000L, 10_000L, 3, -1, 0, true, 10L);
    DataFileMeta first = file("undersized", 700L, 0L);
    DataFileMeta second = file("healthy", 1000L, 10L);
    PaimonFixedBucketFileWindow window = window(true, first, second);

    assertEquals(
        Collections.singletonList(Arrays.asList(first, second)),
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, new PaimonPartitionEvaluator(context).evaluate(window)));
  }

  @ParameterizedTest
  @MethodSource("softTargetCases")
  void orderedSoftTargetAvoidsSingletonTailWithoutForcingEverythingIntoOneTask(
      OptimizingType type, int fileCount) {
    PaimonPlanContext context = context(2000L, 1000L, 1000L, 3, -1, -1, false, 10L);
    DataFileMeta[] files = sequenceFiles(fileCount, 600L);
    PaimonFixedBucketFileWindow window = window(true, files);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context).pack(window, evaluation(context, type, files));

    assertEquals(
        fileCount == 3
            ? Collections.singletonList(Arrays.asList(files))
            : Arrays.asList(Arrays.asList(files[0], files[1]), Arrays.asList(files[2], files[3])),
        tasks);
    assertTrue(tasks.stream().allMatch(task -> size(task) > context.effectiveTaskInputLimit()));
    assertExactUniqueScope(window, tasks);
  }

  private static Stream<Arguments> softTargetCases() {
    return Stream.of(OptimizingType.MAJOR, OptimizingType.FULL)
        .flatMap(type -> Stream.of(Arguments.of(type, 3), Arguments.of(type, 4)));
  }

  @ParameterizedTest
  @MethodSource("targetBoundaryCases")
  void minimumLegalPairCanBeBelowEqualToOrAboveTheSoftInputTarget(
      OptimizingType type, long secondSize) {
    PaimonPlanContext context = context(2000L, 1000L, 1000L, 3, -1, -1, false, 10L);
    DataFileMeta first = file("first", 400L, 0L);
    DataFileMeta second = file("second", secondSize, 10L);
    PaimonFixedBucketFileWindow window = window(true, first, second);

    assertEquals(
        Collections.singletonList(Arrays.asList(first, second)),
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, evaluation(context, type, first, second)));
  }

  private static Stream<Arguments> targetBoundaryCases() {
    return Stream.of(OptimizingType.MAJOR, OptimizingType.FULL)
        .flatMap(type -> Stream.of(599L, 600L, 601L).map(size -> Arguments.of(type, size)));
  }

  @ParameterizedTest
  @MethodSource("orderedOversizedCases")
  void orderedOversizedFileNeedsItsOwnNormalPartnerWithoutCrossingSkippedFiles(
      String layout, int[][] expectedIndexes) {
    PaimonPlanContext context = context(2000L, 1000L, 1000L, 3, -1, -1, false, 10L);
    DataFileMeta[] files =
        IntStream.range(0, layout.length())
            .mapToObj(
                index ->
                    file(
                        "f" + index,
                        layout.charAt(index) == 'O'
                            ? 1100L
                            : layout.charAt(index) == 'E' ? 1000L : 100L,
                        index * 10L))
            .toArray(DataFileMeta[]::new);
    PaimonFixedBucketFileWindow window = window(true, files);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, evaluation(context, OptimizingType.FULL, files));

    List<List<DataFileMeta>> expected =
        Arrays.stream(expectedIndexes)
            .map(
                indexes ->
                    Arrays.stream(indexes)
                        .mapToObj(index -> files[index])
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());
    assertEquals(expected, tasks);
    assertTrue(
        tasks.stream()
            .allMatch(task -> task.stream().filter(file -> file.fileSize() > 1000L).count() <= 1));
    assertExactUniqueScope(window, tasks);
  }

  private static Stream<Arguments> orderedOversizedCases() {
    return Stream.of(
        Arguments.of("SO", new int[][] {{0, 1}}),
        Arguments.of("OS", new int[][] {{0, 1}}),
        Arguments.of("OOS", new int[][] {{1, 2}}),
        Arguments.of("SOO", new int[][] {{0, 1}}),
        Arguments.of("OO", new int[][] {}),
        Arguments.of("OSOS", new int[][] {{0, 1}, {2, 3}}),
        Arguments.of("SOSO", new int[][] {{0, 1}, {2, 3}}),
        Arguments.of("SSOS", new int[][] {{0, 1}, {2, 3}}),
        Arguments.of("OE", new int[][] {{0, 1}}),
        Arguments.of("EO", new int[][] {{0, 1}}),
        Arguments.of("EE", new int[][] {{0, 1}}));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  void orderedOversizedPairLeavesAnyLegalNormalPrefixAsASeparateTask(int normalCount) {
    PaimonPlanContext context = context(20_000L, 900L, 900L, 3, -1, -1, false, 10L);
    DataFileMeta[] files = sequenceFiles(normalCount + 1, 100L);
    files[normalCount] = file("oversized", 10_000L, normalCount * 10L);
    PaimonFixedBucketFileWindow window = window(true, files);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, evaluation(context, OptimizingType.FULL, files));

    assertEquals(
        normalCount == 2
            ? Collections.singletonList(Arrays.asList(files))
            : Arrays.asList(Arrays.asList(files[0], files[1]), Arrays.asList(files[2], files[3])),
        tasks);
    assertExactUniqueScope(window, tasks);
  }

  @ParameterizedTest
  @MethodSource("effectiveLimitCases")
  void bothModesUseTheExistingPositiveLimitAndFallbackRules(
      boolean ordered, OptimizingType type, long tableLimit, long threadLimit, long expectedLimit) {
    PaimonPlanContext context =
        context(expectedLimit * 2, tableLimit, threadLimit, 3, -1, -1, false, 10L);
    DataFileMeta[] files = sequenceFiles(4, expectedLimit * 2 / 5);
    PaimonFixedBucketFileWindow window = window(ordered, files);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context).pack(window, evaluation(context, type, files));

    assertEquals(expectedLimit, context.effectiveTaskInputLimit());
    assertEquals(
        Arrays.asList(Arrays.asList(files[0], files[1]), Arrays.asList(files[2], files[3])), tasks);
    assertExactUniqueScope(window, tasks);
  }

  private static Stream<Arguments> effectiveLimitCases() {
    long fallback = OptimizerProperties.MAX_INPUT_FILE_SIZE_PER_THREAD_DEFAULT;
    long[][] limits = {
      {1000L, 2000L, 1000L},
      {2000L, 1000L, 1000L},
      {1000L, 1000L, 1000L},
      {1000L, 0L, 1000L},
      {1000L, -1L, 1000L},
      {0L, 1000L, 1000L},
      {-1L, 1000L, 1000L},
      {0L, 0L, fallback},
      {-1L, 0L, fallback},
      {0L, -1L, fallback},
      {-1L, -1L, fallback}
    };
    return Stream.of(false, true)
        .flatMap(
            ordered ->
                Stream.of(OptimizingType.MAJOR, OptimizingType.FULL)
                    .flatMap(
                        type ->
                            Arrays.stream(limits)
                                .map(
                                    limit ->
                                        Arguments.of(
                                            ordered, type, limit[0], limit[1], limit[2]))));
  }

  // Paimon ac223f47, operation/BucketedAppendFileStoreWrite.java:75-89 permits write-only
  // unordered writers to ignore previous sequences. Overlap is not an ordered-mode invariant.
  @ParameterizedTest
  @EnumSource(
      value = OptimizingType.class,
      names = {"MINOR", "MAJOR", "FULL"})
  void unorderedOverlappingInputsKeepTheExistingSizeBasedPackingResults(OptimizingType type) {
    PaimonPlanContext context = context(1000L, 1000L, 1000L, 3, 0, -1, false, 10L);
    long[] sizes =
        type == OptimizingType.MINOR
            ? new long[] {300L, 100L, 200L, 100L}
            : type == OptimizingType.MAJOR
                ? new long[] {800L, 100L, 200L, 700L}
                : new long[] {1100L, 100L, 100L, 1100L};
    DataFileMeta[] files =
        IntStream.range(0, sizes.length)
            .mapToObj(index -> file("f" + index, sizes[index], 0L))
            .toArray(DataFileMeta[]::new);
    PaimonFixedBucketFileWindow window = window(false, files);
    PaimonPartitionEvaluation evaluation = evaluation(context, type, files);
    List<List<DataFileMeta>> expected =
        new PaimonAppendTaskPacker(context)
            .pack(evaluation).stream()
                .map(AppendCompactTask::compactBefore)
                .collect(Collectors.toList());

    List<List<DataFileMeta>> actual =
        new PaimonFixedBucketTaskPacker(context).pack(window, evaluation);

    assertFalse(expected.isEmpty());
    assertEquals(expected, actual);
    assertExactUniqueScope(window, actual);
  }

  @Test
  void unorderedFullCanCombineSelectedFilesAcrossAnUnselectedHealthyFile() {
    PaimonPlanContext context = context(1000L, 10_000L, 10_000L, 3, -1, 0, false, 10L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta middle = file("healthy", 1000L, 0L);
    DataFileMeta last = file("last", 100L, 0L);
    PaimonFixedBucketFileWindow window = window(false, first, middle, last);

    assertEquals(
        Collections.singletonList(Arrays.asList(first, last)),
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, new PaimonPartitionEvaluator(context).evaluate(window)));
  }

  @Test
  void provenTruncatedPrefixCanPackOnlyItsRetainedExactFiles() {
    PaimonPlanContext context = context(1000L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta second = file("second", 100L, 10L);
    DataFileMeta omitted = file("omitted", 100L, 20L);
    PaimonFixedBucketFileWindow window = boundedWindow(true, 2, omitted, second, first);

    List<List<DataFileMeta>> tasks =
        new PaimonFixedBucketTaskPacker(context)
            .pack(window, evaluation(context, OptimizingType.FULL, first, second));

    assertTrue(window.provenPrefix());
    assertEquals(Collections.singletonList(Arrays.asList(first, second)), tasks);
    assertExactUniqueScope(window, tasks);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unsafeOverlapOrUnprovenTruncatedBoundaryIsRejected(boolean truncated) {
    PaimonPlanContext context = context(1000L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta second = file("second", 100L, truncated ? 10L : 9L);
    DataFileMeta third = file("third", 100L, 19L);
    PaimonFixedBucketFileWindow window =
        boundedWindow(true, truncated ? 2 : 3, first, second, third);

    assertFalse(window.safe());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PaimonFixedBucketTaskPacker(context)
                .pack(window, evaluation(context, OptimizingType.FULL, first, second)));
  }

  @ParameterizedTest
  @MethodSource("invalidSelectionCases")
  void rejectsSelectedFilesOutsideTheExactUniqueWindowScope(boolean ordered, String defect) {
    PaimonPlanContext context = context(1000L);
    DataFileMeta first = file("first", 100L, 0L);
    DataFileMeta second = file("second", 100L, 10L);
    DataFileMeta omitted = file("omitted", 100L, 20L);
    PaimonFixedBucketFileWindow window = boundedWindow(ordered, 2, first, second, omitted);
    BinaryRow evaluationPartition = PARTITION;
    List<PaimonFileCandidate> selected = new ArrayList<>(candidates(context, first, second));
    switch (defect) {
      case "duplicate":
        selected.add(selected.get(0));
        break;
      case "unknown":
        selected.set(1, candidate(context, PARTITION, file("unknown", 100L, 30L)));
        break;
      case "changed-metadata":
        selected.set(1, candidate(context, PARTITION, file("second", 101L, 10L)));
        break;
      case "omitted":
        selected.set(1, candidate(context, PARTITION, omitted));
        break;
      case "evaluation-partition":
        evaluationPartition = BinaryRow.singleColumn(8);
        break;
      case "candidate-partition":
        selected.set(1, candidate(context, BinaryRow.singleColumn(8), second));
        break;
      default:
        throw new IllegalArgumentException("Unknown test defect: " + defect);
    }
    PaimonPartitionEvaluation evaluation =
        new PaimonPartitionEvaluation(
            evaluationPartition, OptimizingType.FULL, selected, selected, selected.size(), 0, 0);

    assertTrue(window.safe());
    assertThrows(
        IllegalArgumentException.class,
        () -> new PaimonFixedBucketTaskPacker(context).pack(window, evaluation));
  }

  private static Stream<Arguments> invalidSelectionCases() {
    return Stream.of(false, true)
        .flatMap(
            ordered ->
                Stream.of(
                        "duplicate",
                        "unknown",
                        "changed-metadata",
                        "omitted",
                        "evaluation-partition",
                        "candidate-partition")
                    .map(defect -> Arguments.of(ordered, defect)));
  }

  private static PaimonPlanContext context(long limit) {
    return context(1000L, limit, limit, 3, -1, -1, false, 10L);
  }

  private static PaimonPlanContext context(
      long targetSize,
      long tableLimit,
      long threadLimit,
      int minimumFiles,
      int minorInterval,
      int fullInterval,
      boolean rewriteAll,
      long openCost) {
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    options.put("bucket-append-ordered", "true");
    options.put("target-file-size", targetSize + " b");
    options.put("compaction.small-file-ratio", "0.7");
    options.put("compaction.min.file-num", Integer.toString(minimumFiles));
    options.put("source.split.open-file-cost", openCost + " b");
    OptimizingConfig optimizing =
        new OptimizingConfig()
            .setMaxTaskSize(tableLimit)
            .setMinorLeastInterval(minorInterval)
            .setFullTriggerInterval(fullInterval)
            .setFullRewriteAllFiles(rewriteAll);
    return PaimonPlanContext.forOptions(
        CoreOptions.fromMap(options), optimizing, 0L, 0L, 0L, 1.0D, threadLimit, 10_000L);
  }

  private static PaimonFixedBucketFileWindow window(boolean ordered, DataFileMeta... files) {
    return boundedWindow(ordered, files.length, files);
  }

  private static PaimonFixedBucketFileWindow boundedWindow(
      boolean ordered, int capacity, DataFileMeta... files) {
    PaimonFixedBucketFileWindow window =
        new PaimonFixedBucketFileWindow(
            new PaimonAppendUnitKey(PARTITION, 1), 2, capacity, ordered);
    for (DataFileMeta file : files) {
      window.add(ManifestEntry.create(FileKind.ADD, PARTITION, 1, 2, file));
    }
    window.finish();
    return window;
  }

  private static PaimonPartitionEvaluation evaluation(
      PaimonPlanContext context, OptimizingType type, DataFileMeta... files) {
    List<PaimonFileCandidate> candidates = candidates(context, files);
    return new PaimonPartitionEvaluation(
        PARTITION, type, candidates, candidates, candidates.size(), 0, 0);
  }

  private static List<PaimonFileCandidate> candidates(
      PaimonPlanContext context, DataFileMeta... files) {
    return Arrays.stream(files)
        .map(file -> candidate(context, PARTITION, file))
        .collect(Collectors.toList());
  }

  private static PaimonFileCandidate candidate(
      PaimonPlanContext context, BinaryRow partition, DataFileMeta file) {
    return PaimonFileCandidate.from(partition, file, context, null, null);
  }

  private static DataFileMeta[] sequenceFiles(int count, long bytes) {
    return IntStream.range(0, count)
        .mapToObj(index -> file("f" + index, bytes, index * 10L))
        .toArray(DataFileMeta[]::new);
  }

  // Paimon ac223f47, io/DataFileMeta.java:96-132 provides native append metadata, including
  // the sequence interval needed by BaseAppendFileStoreWrite.compactRewrite.
  private static DataFileMeta file(String name, long bytes, long sequence) {
    return DataFileMeta.forAppend(
        name,
        bytes,
        10L,
        SimpleStats.EMPTY_STATS,
        sequence,
        sequence + 9,
        0L,
        Collections.emptyList(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static long size(List<DataFileMeta> files) {
    return files.stream().mapToLong(DataFileMeta::fileSize).sum();
  }

  private static void assertExactUniqueScope(
      PaimonFixedBucketFileWindow window, List<List<DataFileMeta>> tasks) {
    List<DataFileMeta> retained =
        window.files().stream().map(ManifestEntry::file).collect(Collectors.toList());
    List<DataFileMeta> flattened =
        tasks.stream().flatMap(List::stream).collect(Collectors.toList());
    assertTrue(tasks.stream().allMatch(task -> task.size() >= 2));
    assertTrue(retained.containsAll(flattened));
    assertEquals(
        flattened.size(),
        new HashSet<>(flattened.stream().map(DataFileMeta::fileName).collect(Collectors.toList()))
            .size());
  }
}
