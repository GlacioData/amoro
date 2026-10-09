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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendRewriteOptions;
import org.apache.amoro.formats.paimon.optimizing.PaimonAppendRewriteOptions.Source;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.stats.SimpleStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TestPaimonFixedBucketEvaluation {
  @ParameterizedTest
  @CsvSource({"2,false", "3,true", "4,true"})
  void minorFileCountThresholdIsAppliedWithinOneBucket(int count, boolean necessary) {
    PaimonPlanContext context = context(0, -1, -1, false, 3, 10);
    List<DataFileMeta> files = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      files.add(file("small-" + i, 100, i * 10));
    }
    PaimonFixedBucketFileWindow window = window(7, 1, files.toArray(new DataFileMeta[0]));
    PaimonPartitionEvaluation evaluation = evaluate(context, window);

    // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
    // paimon-core/src/main/java/org/apache/paimon/append/BucketedAppendCompactManager.java:204-229
    // and paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:878-885.
    // Reuse Amoro's existing threshold with one native bucket as the complete evaluation scope.
    assertEquals(necessary, evaluation.necessary());
    assertEquals(necessary ? OptimizingType.MINOR : null, evaluation.optimizingType());
    assertEquals(necessary ? count : 0, evaluation.selectedFiles().size());
    assertEquals(count, evaluation.smallFileCount());
    assertEquals(count, evaluation.allFiles().size());
  }

  @ParameterizedTest
  @CsvSource({"999,false", "1000,false", "1001,true"})
  void minorTimeThresholdUsesTheExistingStrictComparison(long elapsed, boolean necessary) {
    PaimonPlanContext context = context(elapsed, 1000, -1, false, 3, 10);
    PaimonPartitionEvaluation evaluation =
        evaluate(context, window(7, 0, file("a", 100, 0), file("b", 100, 10)));

    // Paimon ac223f47, append/BucketedAppendCompactManager.java:204-229 has native selection
    // rules; AMS elapsed-time triggering is Amoro-owned and must retain its strict > boundary.
    assertEquals(necessary, evaluation.necessary());
    assertEquals(necessary ? OptimizingType.MINOR : null, evaluation.optimizingType());
  }

  @ParameterizedTest
  @CsvSource({"899,false", "900,true", "901,true"})
  void minorContentThresholdIncludesOpenCostAndAcceptsEquality(long openCost, boolean necessary) {
    PaimonPlanContext context = context(0, -1, -1, false, 3, openCost);
    PaimonPartitionEvaluation evaluation =
        evaluate(context, window(7, 0, file("a", 100, 0), file("b", 100, 10)));

    // Paimon ac223f47, paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:649-655
    // defines open-file cost. Reusing size + cost >= 2 * target is Amoro's existing MINOR rule,
    // not a new native compaction algorithm or a hard task-input byte limit.
    assertEquals(necessary, evaluation.necessary());
    assertEquals(necessary ? OptimizingType.MINOR : null, evaluation.optimizingType());
  }

  @Test
  void elapsedTimeAndLargeOpenCostDoNotTriggerOneOrdinarySmallFile() {
    PaimonPlanContext context = context(1, 0, -1, false, 3, 10000);
    PaimonPartitionEvaluation evaluation = evaluate(context, window(7, 0, file("only", 100, 0)));

    // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187-210 rewrites supplied files.
    // Amoro's ordinary MINOR rule still needs multiple files despite time or open-cost pressure.
    assertFalse(evaluation.necessary());
    assertNull(evaluation.optimizingType());
    assertTrue(evaluation.selectedFiles().isEmpty());
    assertEquals(1, evaluation.smallFileCount());
  }

  @ParameterizedTest
  @CsvSource({"699,false", "700,true", "999,true", "1000,false"})
  void majorUsesTheExistingSmallAndTargetFileSizeBoundaries(long size, boolean major) {
    PaimonPlanContext context = context(0, -1, -1, false, 3, 10);
    PaimonPartitionEvaluation evaluation =
        evaluate(context, window(7, 0, file("small", 100, 0), file("boundary", size, 10)));

    // Paimon ac223f47, paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:2895-2899
    // exposes the append target size. Amoro reuses its existing small/undersized classification,
    // including the lower boundary and excluding files already at target size.
    assertEquals(major, evaluation.necessary());
    assertEquals(major ? OptimizingType.MAJOR : null, evaluation.optimizingType());
    assertEquals(
        major ? Arrays.asList("small", "boundary") : Collections.emptyList(),
        names(evaluation.selectedFiles()));
    assertEquals(major ? 1 : 0, evaluation.undersizedFileCount());
    assertEquals(2, evaluation.allFiles().size());
  }

  @Test
  void singleUndersizedFileRemainsAnEvaluationCandidateWithoutPromisingAPackedTask() {
    PaimonPlanContext context = context(0, -1, -1, false, 3, 10);
    PaimonPartitionEvaluation evaluation =
        evaluate(context, window(7, 0, file("undersized", 800, 0)));

    // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187-210 accepts exact input;
    // Amoro's evaluation selects a problem file, while later packing enforces legal combinations.
    // Fixed-bucket adaptation must not silently introduce a second MAJOR trigger policy.
    assertTrue(evaluation.necessary());
    assertEquals(OptimizingType.MAJOR, evaluation.optimizingType());
    assertEquals(Collections.singletonList("undersized"), names(evaluation.selectedFiles()));
  }

  @ParameterizedTest
  @CsvSource({"999,false", "1000,false", "1001,true"})
  void fullUsesTheExistingStrictElapsedTimeBoundary(long elapsed, boolean full) {
    PaimonPlanContext context = context(elapsed, -1, 1000, true, 3, 10);
    PaimonPartitionEvaluation evaluation =
        evaluate(
            context,
            window(
                7, 0, file("small-a", 100, 0), file("large", 1200, 10), file("small-b", 100, 20)));

    // Paimon ac223f47, paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:236-241
    // controls rewrite selection, not AMS scheduling time. Reuse Amoro's strict FULL interval.
    assertEquals(full, evaluation.necessary());
    assertEquals(full ? OptimizingType.FULL : null, evaluation.optimizingType());
    assertEquals(full ? 3 : 0, evaluation.selectedFiles().size());
  }

  @ParameterizedTest
  @CsvSource({
    "AMORO_EXPLICIT,true",
    "AMORO_EXPLICIT,false",
    "PAIMON_EXPLICIT,true",
    "PAIMON_EXPLICIT,false",
    "AMORO_DEFAULT,true"
  })
  void fullUsesBoundRewritePolicyAndKeepsLargeFilesAsOrderingContext(
      Source source, boolean rewriteAll) {
    Map<String, String> properties = new HashMap<>();
    if (source == Source.AMORO_EXPLICIT) {
      properties.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, Boolean.toString(rewriteAll));
      properties.put("compaction.force-rewrite-all-files", Boolean.toString(!rewriteAll));
    } else if (source == Source.PAIMON_EXPLICIT) {
      properties.put("compaction.force-rewrite-all-files", Boolean.toString(rewriteAll));
    }
    PaimonAppendRewriteOptions policy = PaimonAppendRewriteOptions.from(properties);
    PaimonPlanContext context =
        context(1001, -1, 1000, !rewriteAll, 3, 10).withFixedRewriteOptions(policy);
    PaimonFixedBucketFileWindow window =
        window(7, 1, file("small-a", 100, 0), file("large", 1200, 10), file("small-b", 100, 20));
    PaimonPartitionEvaluation evaluation = evaluate(context, window);

    // Paimon ac223f47, append/BucketedAppendCompactManager.java:275-313 distinguishes full
    // rewrite selection, and operation/BaseAppendFileStoreWrite.java:187-210 keeps input order.
    // T09 selects problem files while retaining the middle large file for T10 range splitting.
    assertEquals(source, policy.source());
    assertEquals(rewriteAll, context.fullRewriteAllFiles());
    assertEquals(OptimizingType.FULL, evaluation.optimizingType());
    assertTrue(evaluation.necessary());
    assertEquals(
        rewriteAll
            ? Arrays.asList("small-a", "large", "small-b")
            : Arrays.asList("small-a", "small-b"),
        names(evaluation.selectedFiles()));
    assertEquals(Arrays.asList("small-a", "large", "small-b"), names(evaluation.allFiles()));
    assertEquals(2, evaluation.smallFileCount());
    assertEquals(0, evaluation.undersizedFileCount());
  }

  @Test
  void fullDueDoesNotTriggerAHealthyBucketOrAdjacentPartition() {
    PaimonPlanContext context = context(1001, -1, 1000, true, 3, 10);
    PaimonPartitionEvaluator evaluator = new PaimonPartitionEvaluator(context);
    PaimonFixedBucketFileWindow problem =
        window(7, 0, file("problem-small", 100, 0), file("problem-large", 1200, 10));
    PaimonFixedBucketFileWindow healthyBucket =
        window(7, 1, file("healthy-a", 1000, 0), file("healthy-b", 1200, 10));
    PaimonFixedBucketFileWindow healthyPartition =
        window(8, 0, file("other-a", 1000, 0), file("other-b", 1200, 10));

    // Paimon ac223f47, manifest/FileEntry.java:49-57 identifies the physical bucket scope.
    // FULL is an Amoro problem-unit trigger; one affected bucket cannot expand another unit.
    assertEquals(OptimizingType.FULL, evaluator.evaluate(problem).optimizingType());
    for (PaimonFixedBucketFileWindow healthy : Arrays.asList(healthyBucket, healthyPartition)) {
      PaimonPartitionEvaluation evaluation = evaluator.evaluate(healthy);
      assertFalse(evaluation.necessary());
      assertNull(evaluation.optimizingType());
      assertTrue(evaluation.selectedFiles().isEmpty());
      assertEquals(healthy.unit().partition(), evaluation.partition());
      assertEquals(2, evaluation.allFiles().size());
    }
  }

  @Test
  void differentBucketsCannotCombineTheirCountsToReachMinorThreshold() {
    PaimonPlanContext context = context(0, -1, -1, false, 3, 10);
    PaimonPartitionEvaluator evaluator = new PaimonPartitionEvaluator(context);
    PaimonFixedBucketFileWindow first =
        window(7, 0, file("bucket0-a", 100, 0), file("bucket0-b", 100, 10));
    PaimonFixedBucketFileWindow second =
        window(7, 1, file("bucket1-a", 100, 0), file("bucket1-b", 100, 10));

    // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187-210 rewrites one bucket.
    // Reusing one evaluator instance must not pool counts or file ownership across buckets.
    PaimonPartitionEvaluation firstEvaluation = evaluator.evaluate(first);
    PaimonPartitionEvaluation secondEvaluation = evaluator.evaluate(second);
    assertFalse(firstEvaluation.necessary());
    assertFalse(secondEvaluation.necessary());
    assertEquals(Arrays.asList("bucket0-a", "bucket0-b"), names(firstEvaluation.allFiles()));
    assertEquals(Arrays.asList("bucket1-a", "bucket1-b"), names(secondEvaluation.allFiles()));

    PaimonPartitionEvaluation qualifying =
        evaluator.evaluate(
            window(
                7,
                0,
                file("bucket0-a", 100, 0),
                file("bucket0-b", 100, 10),
                file("bucket0-c", 100, 20)));
    assertEquals(OptimizingType.MINOR, qualifying.optimizingType());
    assertEquals(
        Arrays.asList("bucket0-a", "bucket0-b", "bucket0-c"), names(qualifying.selectedFiles()));
    assertFalse(evaluator.evaluate(second).necessary());
  }

  @ParameterizedTest
  @ValueSource(strings = {"overlap", "layout", "unprovenBoundary"})
  void unsafeWindowCannotBecomeAnOptimizingEvaluation(String invalid) {
    PaimonPlanContext context = context(1001, 0, 1000, true, 2, 1000);
    PaimonAppendUnitKey unit = new PaimonAppendUnitKey(BinaryRow.singleColumn(7), 0);
    PaimonFixedBucketFileWindow window =
        new PaimonFixedBucketFileWindow(unit, 2, "unprovenBoundary".equals(invalid) ? 1 : 3, true);
    window.add(ManifestEntry.create(FileKind.ADD, unit.partition(), 0, 2, file("first", 100, 0)));
    window.add(
        ManifestEntry.create(
            FileKind.ADD,
            unit.partition(),
            "layout".equals(invalid) ? 1 : 0,
            2,
            file("second", 100, "layout".equals(invalid) ? 10 : 9)));
    window.finish();

    // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:197-210 starts at the first
    // input sequence; FULL/MAJOR/MINOR pressure must never bypass Amoro's scope proof.
    assertFalse(window.safe());
    assertThrows(
        IllegalArgumentException.class,
        () -> new PaimonPartitionEvaluator(context).evaluate(window));
  }

  @Test
  void unfinishedWindowCannotBecomeAnOptimizingEvaluation() {
    PaimonFixedBucketFileWindow window =
        new PaimonFixedBucketFileWindow(
            new PaimonAppendUnitKey(BinaryRow.singleColumn(7), 0), 2, 3, true);
    window.add(
        ManifestEntry.create(FileKind.ADD, BinaryRow.singleColumn(7), 0, 2, file("first", 100, 0)));

    // Paimon ac223f47, operation/AbstractFileStoreScan.java:395-407 provides a merged iterator.
    // Amoro cannot prove a prefix before exhausting the requested snapshot scope.
    assertThrows(
        IllegalStateException.class,
        () -> new PaimonPartitionEvaluator(context(1001, 0, 1000, true, 2, 1000)).evaluate(window));
  }

  @Test
  void safeTruncatedPrefixEvaluatesOnlyItsRetainedFiles() {
    PaimonFixedBucketFileWindow window =
        new PaimonFixedBucketFileWindow(
            new PaimonAppendUnitKey(BinaryRow.singleColumn(7), 0), 2, 3, true);
    for (int i = 0; i < 5; i++) {
      window.add(
          ManifestEntry.create(
              FileKind.ADD, BinaryRow.singleColumn(7), 0, 2, file("file-" + i, 100, i * 10)));
    }
    window.finish();
    PaimonPartitionEvaluation evaluation = evaluate(context(0, -1, -1, false, 3, 10), window);

    // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187-210 accepts exact files.
    // A proven prefix may be evaluated, but allFiles here means this retained scope, not all
    // five active bucket files and not a claim that one process finishes the whole bucket.
    assertFalse(window.complete());
    assertTrue(window.provenPrefix());
    assertEquals(OptimizingType.MINOR, evaluation.optimizingType());
    assertEquals(Arrays.asList("file-0", "file-1", "file-2"), names(evaluation.selectedFiles()));
    assertEquals(3, evaluation.allFiles().size());
    assertEquals(2, window.omittedFileCount());
  }

  private static PaimonPlanContext context(
      long elapsed,
      int minorInterval,
      int fullInterval,
      boolean rewriteAll,
      int minimumFiles,
      long openCost) {
    OptimizingConfig optimizing =
        new OptimizingConfig()
            .setMinorLeastInterval(minorInterval)
            .setFullTriggerInterval(fullInterval)
            .setFullRewriteAllFiles(rewriteAll)
            .setMaxTaskSize(4096L);
    Map<String, String> options = new HashMap<>();
    options.put("bucket", "2");
    options.put("bucket-append-ordered", "true");
    options.put("target-file-size", "1000 b");
    options.put("compaction.small-file-ratio", "0.7");
    options.put("compaction.min.file-num", Integer.toString(minimumFiles));
    options.put("source.split.open-file-cost", openCost + " b");
    return PaimonPlanContext.forOptions(
        CoreOptions.fromMap(options),
        optimizing,
        1000L,
        1000L,
        1000L,
        1.0D,
        4096L,
        1000L + elapsed);
  }

  private static PaimonFixedBucketFileWindow window(
      int partition, int bucket, DataFileMeta... files) {
    PaimonAppendUnitKey unit = new PaimonAppendUnitKey(BinaryRow.singleColumn(partition), bucket);
    PaimonFixedBucketFileWindow window = new PaimonFixedBucketFileWindow(unit, 2, 100, true);
    for (DataFileMeta file : files) {
      window.add(ManifestEntry.create(FileKind.ADD, unit.partition(), bucket, 2, file));
    }
    window.finish();
    assertTrue(window.safe());
    return window;
  }

  private static DataFileMeta file(String name, long bytes, long sequence) {
    return DataFileMeta.forAppend(
        name,
        bytes,
        10,
        SimpleStats.EMPTY_STATS,
        sequence,
        sequence + 9,
        0,
        Collections.emptyList(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static PaimonPartitionEvaluation evaluate(
      PaimonPlanContext context, PaimonFixedBucketFileWindow window) {
    PaimonPartitionEvaluator evaluator = new PaimonPartitionEvaluator(context);
    PaimonPartitionEvaluation actual = evaluator.evaluate(window);
    List<PaimonFileCandidate> originalCandidates =
        window.files().stream()
            .map(
                entry ->
                    PaimonFileCandidate.from(entry.partition(), entry.file(), context, null, null))
            .collect(Collectors.toList());
    PaimonPartitionEvaluation existing =
        evaluator.evaluate(window.unit().partition(), originalCandidates);
    assertEquals(existing.optimizingType(), actual.optimizingType());
    assertEquals(existing.necessary(), actual.necessary());
    assertEquals(existing.partition(), actual.partition());
    assertEquals(names(existing.selectedFiles()), names(actual.selectedFiles()));
    assertEquals(names(existing.allFiles()), names(actual.allFiles()));
    assertEquals(existing.smallFileCount(), actual.smallFileCount());
    assertEquals(existing.undersizedFileCount(), actual.undersizedFileCount());
    assertEquals(existing.highDeleteFileCount(), actual.highDeleteFileCount());
    return actual;
  }

  private static List<String> names(List<PaimonFileCandidate> files) {
    return files.stream().map(PaimonFileCandidate::fileName).collect(Collectors.toList());
  }
}
