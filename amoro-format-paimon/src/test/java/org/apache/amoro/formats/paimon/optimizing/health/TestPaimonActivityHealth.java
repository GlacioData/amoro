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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TestPaimonActivityHealth {

  @Test
  public void fixedContextCapturesMetadataWithoutOpeningAFileScan() {
    AppendOnlyFileStoreTable table = mock(AppendOnlyFileStoreTable.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    Snapshot snapshot = snapshot(17L, 2_000L);
    TableSchema schema = schema(3L);
    CoreOptions options = coreOptions("target-file-size", "1000 b");
    when(table.bucketMode()).thenReturn(BucketMode.BUCKET_UNAWARE);
    when(table.schema()).thenReturn(schema);
    when(table.coreOptions()).thenReturn(options);
    when(table.snapshotManager()).thenReturn(snapshotManager);
    when(snapshotManager.latestSnapshot()).thenReturn(snapshot);

    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.append_table", optimizationContext(11L, 10));

    assertEquals(PaimonHealthEvaluationContext.TableShape.APPEND, context.tableShape());
    assertEquals(BucketMode.BUCKET_UNAWARE, context.bucketMode());
    assertEquals(3L, context.schemaId());
    assertEquals(17L, context.snapshotId());
    assertEquals(2_000L, context.snapshotTimeMillis());
    assertEquals(1_000L, context.targetFileSize());
    assertEquals(700L, context.smallFileBoundary());
    assertEquals(
        PaimonAppendHealthEvaluator.FORMULA_VERSION + "+" + PaimonActivityHealth.FORMULA_VERSION,
        context.formulaVersion());
    assertTrue(context.scoringConfigFingerprint().matches("[0-9a-f]{64}"));
    assertFalse(context.configurationError().isPresent());
    assertEquals(TableAnalysisKey.NO_CHANGE_SNAPSHOT, context.key().getChangeSnapshotId());
    assertEquals(context.key(), new TableAnalysisKey(context.key()));

    verify(snapshotManager).latestSnapshot();
    verify(snapshot).id();
    verify(snapshot).timeMillis();
    verify(table, never()).newSnapshotReader();
    verify(table, never()).newScan();
  }

  @Test
  public void analysisKeyTracksSnapshotSchemaConfigAndBaseline() {
    AppendOnlyFileStoreTable table = mock(AppendOnlyFileStoreTable.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    TableSchema schema1 = schema(1L);
    TableSchema schema2 = schema(2L);
    Snapshot snapshot10 = snapshot(10L, 1_000L);
    Snapshot snapshot11 = snapshot(11L, 1_100L);
    when(table.bucketMode()).thenReturn(BucketMode.BUCKET_UNAWARE);
    when(table.schema()).thenReturn(schema1, schema1, schema2, schema2, schema2);
    when(table.coreOptions())
        .thenReturn(
            coreOptions("target-file-size", "1000 b"),
            coreOptions("target-file-size", "1000 b"),
            coreOptions("target-file-size", "1000 b"),
            coreOptions("target-file-size", "2000 b"),
            coreOptions("target-file-size", "2000 b"));
    when(table.snapshotManager()).thenReturn(snapshotManager);
    when(snapshotManager.latestSnapshot())
        .thenReturn(snapshot10, snapshot11, snapshot11, snapshot11, snapshot11);

    PaimonHealthEvaluationContext initial =
        PaimonHealthEvaluationContext.capture(table, "catalog.db.t", optimizationContext(5L, 10));
    PaimonHealthEvaluationContext snapshotChanged =
        PaimonHealthEvaluationContext.capture(table, "catalog.db.t", optimizationContext(5L, 10));
    PaimonHealthEvaluationContext schemaChanged =
        PaimonHealthEvaluationContext.capture(table, "catalog.db.t", optimizationContext(5L, 10));
    PaimonHealthEvaluationContext configChanged =
        PaimonHealthEvaluationContext.capture(table, "catalog.db.t", optimizationContext(5L, 10));
    PaimonHealthEvaluationContext baselineChanged =
        PaimonHealthEvaluationContext.capture(table, "catalog.db.t", optimizationContext(6L, 10));

    assertNotEquals(initial.key(), snapshotChanged.key());
    assertNotEquals(snapshotChanged.key(), schemaChanged.key());
    assertNotEquals(schemaChanged.key(), configChanged.key());
    assertNotEquals(configChanged.key(), baselineChanged.key());
  }

  @Test
  public void keyDynamicUsesCheckpointAndIgnoresPlannerOnlyConfiguration() {
    Map<String, String> firstOptions = primaryKeyOptions();
    firstOptions.put("paimon-optimizer.primary-key.major.max-bucket-ratio", "0.33");
    firstOptions.put(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(), "200");
    Map<String, String> secondOptions = primaryKeyOptions();
    secondOptions.put("paimon-optimizer.primary-key.major.max-bucket-ratio", "not-a-decimal");
    secondOptions.put(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(), "900");
    FileStoreTable firstTable =
        primaryKeyTable(BucketMode.KEY_DYNAMIC, 9L, snapshot(20L, 2_000L), firstOptions);
    FileStoreTable secondTable =
        primaryKeyTable(BucketMode.KEY_DYNAMIC, 9L, snapshot(20L, 2_000L), secondOptions);

    OptimizingConfig firstConfig =
        new OptimizingConfig()
            .setFilter("id > 1")
            .setMaxTaskSize(1024L)
            .setMinorLeastInterval(10)
            .setFullTriggerInterval(20);
    OptimizingConfig secondConfig =
        new OptimizingConfig()
            .setFilter("id < 100")
            .setMaxTaskSize(8192L)
            .setMinorLeastInterval(100)
            .setFullTriggerInterval(200);

    PaimonHealthEvaluationContext first =
        PaimonHealthEvaluationContext.capture(
            firstTable, "catalog.db.dynamic_pk", optimizationContext(7L, firstConfig));
    PaimonHealthEvaluationContext second =
        PaimonHealthEvaluationContext.capture(
            secondTable, "catalog.db.dynamic_pk", optimizationContext(19L, secondConfig));

    assertEquals(first.scoringConfigFingerprint(), second.scoringConfigFingerprint());
    assertNotEquals(first.key(), second.key());
    assertFalse(second.configurationError().isPresent());
    assertEquals(7L, first.key().getSuccessfulOptimizationBaselineId());
    assertEquals(
        TableAnalysisKey.NO_BASELINE_TIME,
        first.key().getSuccessfulOptimizationBaselineTimeMillis());
    PaimonActivityHealth.Result activity = PaimonActivityHealth.evaluate(first.activityInput(80));
    assertTrue(activity.baselineAvailable());
    assertEquals(13L, activity.newSnapshotCount());
  }

  @Test
  public void hashPrimaryKeyIncludesCandidateConfigurationAndSuccessfulBaseline() {
    FileStoreTable table = primaryKeyTable(BucketMode.HASH_DYNAMIC, 9L, snapshot(20L, 2_000L));
    OptimizingConfig firstConfig = new OptimizingConfig().setFilter("id > 1");
    OptimizingConfig secondConfig = new OptimizingConfig().setFilter("id > 2");

    PaimonHealthEvaluationContext first =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.hash_pk", optimizationContext(7L, firstConfig));
    PaimonHealthEvaluationContext configChanged =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.hash_pk", optimizationContext(7L, secondConfig));
    PaimonHealthEvaluationContext baselineChanged =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.hash_pk", optimizationContext(8L, secondConfig));

    assertNotEquals(first.scoringConfigFingerprint(), configChanged.scoringConfigFingerprint());
    assertNotEquals(first.key(), configChanged.key());
    assertNotEquals(configChanged.key(), baselineChanged.key());
    PaimonActivityHealth.Result activity = PaimonActivityHealth.evaluate(first.activityInput(80));
    assertTrue(activity.baselineAvailable());
    assertEquals(13L, activity.newSnapshotCount());
  }

  @Test
  public void emptyTableUsesNoSnapshotSentinel() {
    FileStoreTable table = primaryKeyTable(BucketMode.HASH_FIXED, 4L, null);

    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.empty_pk", optimizationContext(-1L, 10));

    assertEquals(TableAnalysisKey.NO_SNAPSHOT, context.snapshotId());
    assertEquals(TableAnalysisKey.NO_SNAPSHOT, context.key().getSnapshotId());
    assertFalse(PaimonActivityHealth.evaluate(context.activityInput(100)).baselineAvailable());
  }

  @Test
  public void missingCheckpointIsNeutralAndCheckpointDoesNotRequireTime() {
    FileStoreTable table = primaryKeyTable(BucketMode.HASH_FIXED, 4L, snapshot(2L, 200L));

    PaimonHealthEvaluationContext missingId =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.partial", optimizationContext(-1L, 10));
    PaimonHealthEvaluationContext checkpoint =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.partial", optimizationContext(1L, 10));

    PaimonActivityHealth.Result neutral =
        PaimonActivityHealth.evaluate(missingId.activityInput(80));
    PaimonActivityHealth.Result active =
        PaimonActivityHealth.evaluate(checkpoint.activityInput(80));
    assertEquals(80, neutral.healthScore());
    assertFalse(neutral.baselineAvailable());
    assertTrue(active.baselineAvailable());
    assertEquals(1L, active.newSnapshotCount());
  }

  @Test
  public void zeroRuntimeCheckpointIsNormalizedToUnavailable() {
    FileStoreTable table = primaryKeyTable(BucketMode.HASH_FIXED, 4L, snapshot(2L, 200L));

    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.zero_checkpoint", optimizationContext(0L, 10));
    PaimonActivityHealth.Result result = PaimonActivityHealth.evaluate(context.activityInput(80));

    assertEquals(TableAnalysisKey.NO_BASELINE, context.key().getSuccessfulOptimizationBaselineId());
    assertEquals(80, result.healthScore());
    assertFalse(result.baselineAvailable());
    assertEquals(PaimonActivityHealth.SUCCESS_BASELINE_UNAVAILABLE, result.reasonCodes().get(0));
  }

  @Test
  public void invalidPrimaryKeyOptionsAreCapturedAsDeterministicConfigurationError() {
    Map<String, String> options = primaryKeyOptions();
    options.put("paimon-optimizer.primary-key.major.max-bucket-ratio", "not-a-decimal");
    FileStoreTable table = primaryKeyTable(BucketMode.HASH_FIXED, 4L, snapshot(2L, 200L), options);

    PaimonHealthEvaluationContext first =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.invalid_pk", optimizationContext(-1L, 10));
    PaimonHealthEvaluationContext second =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.invalid_pk", optimizationContext(-1L, 10));

    assertTrue(first.configurationError().isPresent());
    assertEquals(first.configurationError(), second.configurationError());
    assertEquals(first.scoringConfigFingerprint(), second.scoringConfigFingerprint());
    verify(table, never()).newSnapshotReader();
    verify(table, never()).newScan();
  }

  @Test
  public void invalidConfigurationFingerprintDoesNotDependOnExceptionMessage() {
    PrimaryKeyFileStoreTable table = mock(PrimaryKeyFileStoreTable.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    TableSchema tableSchema = schema(4L);
    Snapshot latestSnapshot = snapshot(2L, 200L);
    when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    when(table.schema()).thenReturn(tableSchema);
    when(table.snapshotManager()).thenReturn(snapshotManager);
    when(snapshotManager.latestSnapshot()).thenReturn(latestSnapshot);
    when(table.coreOptions())
        .thenThrow(new IllegalArgumentException("first host message"))
        .thenThrow(new IllegalArgumentException("second host message"));

    PaimonHealthEvaluationContext first =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.invalid_message", optimizationContext(-1L, 10));
    PaimonHealthEvaluationContext second =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.invalid_message", optimizationContext(-1L, 10));

    assertEquals(
        IllegalArgumentException.class.getName(),
        first.configurationError().orElseThrow(AssertionError::new));
    assertEquals(first.configurationError(), second.configurationError());
    assertEquals(first.scoringConfigFingerprint(), second.scoringConfigFingerprint());
  }

  @Test
  public void invalidConfigurationFingerprintStillTracksRawOptions() {
    PrimaryKeyFileStoreTable firstTable = invalidPrimaryKeyTable("1000 b");
    PrimaryKeyFileStoreTable secondTable = invalidPrimaryKeyTable("2000 b");

    PaimonHealthEvaluationContext first =
        PaimonHealthEvaluationContext.capture(
            firstTable, "catalog.db.invalid_options", optimizationContext(-1L, 10));
    PaimonHealthEvaluationContext second =
        PaimonHealthEvaluationContext.capture(
            secondTable, "catalog.db.invalid_options", optimizationContext(-1L, 10));

    assertEquals(first.configurationError(), second.configurationError());
    assertNotEquals(first.scoringConfigFingerprint(), second.scoringConfigFingerprint());
  }

  @Test
  public void invalidConfigurationFingerprintStillTracksEligibility() {
    PrimaryKeyFileStoreTable eligibleTable = invalidPrimaryKeyTable("1000 b", "true", "true");
    PrimaryKeyFileStoreTable writeOnlyDisabledTable =
        invalidPrimaryKeyTable("1000 b", "false", "true");
    PrimaryKeyFileStoreTable selfOptimizingMissingTable =
        invalidPrimaryKeyTable("1000 b", "true", null);
    PrimaryKeyFileStoreTable selfOptimizingDisabledTable =
        invalidPrimaryKeyTable("1000 b", "true", "false");

    PaimonHealthEvaluationContext eligible =
        PaimonHealthEvaluationContext.capture(
            eligibleTable,
            "catalog.db.invalid_eligibility",
            optimizationContext(-1L, new OptimizingConfig().setEnabled(true)));
    PaimonHealthEvaluationContext writeOnlyDisabled =
        PaimonHealthEvaluationContext.capture(
            writeOnlyDisabledTable,
            "catalog.db.invalid_eligibility",
            optimizationContext(-1L, new OptimizingConfig().setEnabled(true)));
    PaimonHealthEvaluationContext selfOptimizingMissing =
        PaimonHealthEvaluationContext.capture(
            selfOptimizingMissingTable,
            "catalog.db.invalid_eligibility",
            optimizationContext(-1L, new OptimizingConfig().setEnabled(true)));
    PaimonHealthEvaluationContext selfOptimizingDisabled =
        PaimonHealthEvaluationContext.capture(
            selfOptimizingDisabledTable,
            "catalog.db.invalid_eligibility",
            optimizationContext(-1L, new OptimizingConfig().setEnabled(true)));

    assertTrue(eligible.configurationError().isPresent());
    assertNotEquals(
        eligible.scoringConfigFingerprint(), writeOnlyDisabled.scoringConfigFingerprint());
    assertNotEquals(
        eligible.scoringConfigFingerprint(), selfOptimizingMissing.scoringConfigFingerprint());
    assertNotEquals(
        eligible.scoringConfigFingerprint(), selfOptimizingDisabled.scoringConfigFingerprint());
    assertNotEquals(
        selfOptimizingMissing.scoringConfigFingerprint(),
        selfOptimizingDisabled.scoringConfigFingerprint());
  }

  @Test
  public void unknownFileStoreShapeIsStableAndNeverMisclassifiedAsPrimaryKey() {
    FileStoreTable table = mock(FileStoreTable.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    TableSchema tableSchema = schema(4L);
    Snapshot snapshot = snapshot(2L, 200L);
    when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    when(table.schema()).thenReturn(tableSchema);
    when(table.snapshotManager()).thenReturn(snapshotManager);
    when(snapshotManager.latestSnapshot()).thenReturn(snapshot);

    PaimonHealthEvaluationContext first =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.delegated", optimizationContext(1L, 10));
    PaimonHealthEvaluationContext second =
        PaimonHealthEvaluationContext.capture(
            table, "catalog.db.delegated", optimizationContext(9L, 100));

    assertEquals(PaimonHealthEvaluationContext.TableShape.UNSUPPORTED, first.tableShape());
    assertEquals(PaimonHealthEvaluationContext.UNSUPPORTED_FORMULA_VERSION, first.formulaVersion());
    assertEquals(first.key(), second.key());
    assertFalse(first.configurationError().isPresent());
    verify(table, never()).coreOptions();
    verify(table, never()).newSnapshotReader();
    verify(table, never()).newScan();
  }

  @Test
  public void activityAnchorsMatchSpecification() {
    assertEquals(100, evaluateWithSnapshotPressure(100, 0.0D).healthScore());
    assertEquals(100, evaluateWithSnapshotPressure(100, 0.5D).healthScore());
    assertEquals(100, evaluateWithSnapshotPressure(100, 1.0D).healthScore());
    assertEquals(90, evaluateWithSnapshotPressure(90, 0.0D).healthScore());
    assertEquals(89, evaluateWithSnapshotPressure(90, 0.5D).healthScore());
    assertEquals(88, evaluateWithSnapshotPressure(90, 1.0D).healthScore());
    assertEquals(75, evaluateWithSnapshotPressure(75, 0.0D).healthScore());
    assertEquals(72, evaluateWithSnapshotPressure(75, 0.5D).healthScore());
    assertEquals(69, evaluateWithSnapshotPressure(75, 1.0D).healthScore());
    assertEquals(50, evaluateWithSnapshotPressure(50, 0.0D).healthScore());
    assertEquals(44, evaluateWithSnapshotPressure(50, 0.5D).healthScore());
    assertEquals(38, evaluateWithSnapshotPressure(50, 1.0D).healthScore());
    assertEquals(0, evaluateWithSnapshotPressure(0, 0.0D).healthScore());
    assertEquals(0, evaluateWithSnapshotPressure(0, 0.5D).healthScore());
    assertEquals(0, evaluateWithSnapshotPressure(0, 1.0D).healthScore());
  }

  @Test
  public void snapshotPressureIsMonotonicAndSaturatesAtTen() {
    int previousScore = 75;
    for (long newSnapshotCount = 0; newSnapshotCount <= 10; newSnapshotCount++) {
      PaimonActivityHealth.Result result =
          PaimonActivityHealth.evaluate(
              PaimonActivityHealth.Input.withBaseline(75, 100 + newSnapshotCount, 100L));
      assertEquals(newSnapshotCount, result.newSnapshotCount());
      assertEquals(newSnapshotCount / 10.0D, result.snapshotPressure(), 0.000_001D);
      assertTrue(result.healthScore() <= previousScore);
      previousScore = result.healthScore();
    }

    PaimonActivityHealth.Result saturated =
        PaimonActivityHealth.evaluate(PaimonActivityHealth.Input.withBaseline(75, 1_000L, 100L));
    assertEquals(1.0D, saturated.snapshotPressure(), 0.0D);
    assertEquals(69, saturated.healthScore());
  }

  @Test
  public void unavailableBaselineIsNeutralAndExplicit() {
    PaimonActivityHealth.Result result =
        PaimonActivityHealth.evaluate(PaimonActivityHealth.Input.withoutBaseline(50));

    assertEquals(50, result.healthScore());
    assertEquals(0.0D, result.activityPressure(), 0.0D);
    assertFalse(result.baselineAvailable());
    assertEquals(PaimonActivityHealth.SUCCESS_BASELINE_UNAVAILABLE, result.reasonCodes().get(0));
  }

  @Test
  public void equalCheckpointIsNeutral() {
    PaimonActivityHealth.Result result =
        PaimonActivityHealth.evaluate(PaimonActivityHealth.Input.withBaseline(50, 2L, 2L));

    assertEquals(50, result.healthScore());
  }

  @Test
  public void snapshotIdRegressionClampsToZero() {
    PaimonActivityHealth.Result idRegression =
        PaimonActivityHealth.evaluate(PaimonActivityHealth.Input.withBaseline(75, 9L, 10L));
    PaimonActivityHealth.Result extremeRegression =
        PaimonActivityHealth.evaluate(
            PaimonActivityHealth.Input.withBaseline(75, 1L, Long.MAX_VALUE));

    assertEquals(0L, idRegression.newSnapshotCount());
    assertEquals(0.0D, idRegression.snapshotPressure(), 0.0D);
    assertEquals(75, idRegression.healthScore());
    assertEquals(0L, extremeRegression.newSnapshotCount());
    assertEquals(75, extremeRegression.healthScore());
  }

  @Test
  public void invalidBaselineReturnsStableReason() {
    PaimonActivityHealth.Result result =
        PaimonActivityHealth.evaluate(PaimonActivityHealth.Input.withBaseline(75, 9L, 0L));

    assertEquals(-1, result.healthScore());
    assertEquals(PaimonActivityHealth.SUCCESS_BASELINE_INVALID, result.reasonCodes().get(0));
  }

  @Test
  public void invalidBaseScoreReturnsStableReason() {
    PaimonActivityHealth.Result result =
        PaimonActivityHealth.evaluate(PaimonActivityHealth.Input.withoutBaseline(101));

    assertEquals(-1, result.healthScore());
    assertEquals(PaimonActivityHealth.INVALID_SCORING_CONFIG, result.reasonCodes().get(0));
  }

  private PaimonActivityHealth.Result evaluateWithSnapshotPressure(
      int baseScore, double snapshotPressure) {
    long newSnapshots = Math.round(snapshotPressure * 10);
    return PaimonActivityHealth.evaluate(
        PaimonActivityHealth.Input.withBaseline(baseScore, 100 + newSnapshots, 100L));
  }

  private static FileStoreTable primaryKeyTable(
      BucketMode bucketMode, long schemaId, Snapshot snapshot) {
    return primaryKeyTable(bucketMode, schemaId, snapshot, primaryKeyOptions());
  }

  private static FileStoreTable primaryKeyTable(
      BucketMode bucketMode, long schemaId, Snapshot snapshot, Map<String, String> optionMap) {
    FileStoreTable table = mock(PrimaryKeyFileStoreTable.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    TableSchema tableSchema = schema(schemaId);
    when(table.bucketMode()).thenReturn(bucketMode);
    when(table.schema()).thenReturn(tableSchema);
    when(table.coreOptions()).thenReturn(CoreOptions.fromMap(optionMap));
    when(table.snapshotManager()).thenReturn(snapshotManager);
    when(snapshotManager.latestSnapshot()).thenReturn(snapshot);
    return table;
  }

  private static OptimizationContext optimizationContext(long baselineId, int minorInterval) {
    return optimizationContext(
        baselineId,
        new OptimizingConfig().setMinorLeastInterval(minorInterval).setFullTriggerInterval(-1));
  }

  private static OptimizationContext optimizationContext(long baselineId, OptimizingConfig config) {
    OptimizationContext context = mock(OptimizationContext.class);
    when(context.getOptimizingConfig()).thenReturn(config);
    when(context.getLastOptimizedSnapshotId()).thenReturn(baselineId);
    return context;
  }

  private static Snapshot snapshot(long id, long timeMillis) {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(id);
    when(snapshot.timeMillis()).thenReturn(timeMillis);
    return snapshot;
  }

  private static TableSchema schema(long id) {
    TableSchema schema = mock(TableSchema.class);
    when(schema.id()).thenReturn(id);
    return schema;
  }

  private static CoreOptions coreOptions(String... keyValues) {
    Map<String, String> options = new HashMap<>();
    for (int index = 0; index < keyValues.length; index += 2) {
      options.put(keyValues[index], keyValues[index + 1]);
    }
    return CoreOptions.fromMap(options);
  }

  private static PrimaryKeyFileStoreTable invalidPrimaryKeyTable(String targetFileSize) {
    return invalidPrimaryKeyTable(targetFileSize, null, null);
  }

  private static PrimaryKeyFileStoreTable invalidPrimaryKeyTable(
      String targetFileSize, String writeOnly, String selfOptimizingEnabled) {
    PrimaryKeyFileStoreTable table = mock(PrimaryKeyFileStoreTable.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    Snapshot snapshot = snapshot(2L, 200L);
    TableSchema tableSchema = schema(4L);
    Map<String, String> options = new HashMap<>();
    options.put(CoreOptions.TARGET_FILE_SIZE.key(), targetFileSize);
    if (writeOnly != null) {
      options.put(CoreOptions.WRITE_ONLY.key(), writeOnly);
    }
    if (selfOptimizingEnabled != null) {
      options.put(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED, selfOptimizingEnabled);
    }
    when(tableSchema.options()).thenReturn(options);
    when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
    when(table.schema()).thenReturn(tableSchema);
    when(table.snapshotManager()).thenReturn(snapshotManager);
    when(snapshotManager.latestSnapshot()).thenReturn(snapshot);
    when(table.coreOptions()).thenThrow(new IllegalArgumentException("host-specific message"));
    return table;
  }

  private static Map<String, String> primaryKeyOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(CoreOptions.WRITE_ONLY.key(), "true");
    return options;
  }
}
