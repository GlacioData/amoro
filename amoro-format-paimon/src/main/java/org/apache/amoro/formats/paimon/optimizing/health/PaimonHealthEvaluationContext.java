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

import org.apache.amoro.TableFormat;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptions;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.utils.DigestUtil;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable metadata-only inputs fixed before a Paimon health snapshot scan starts. */
public final class PaimonHealthEvaluationContext {

  public static final String UNSUPPORTED_FORMULA_VERSION = "paimon-unsupported-health-v1";

  private static final String COMPACTION_FILE_NUM_LIMIT = "compaction.file-num-limit";
  private static final String COMPACTION_SMALL_FILE_RATIO = "compaction.small-file-ratio";
  private static final int DEFAULT_COMPACTION_FILE_NUM_LIMIT = 100_000;

  private final TableShape tableShape;
  private final BucketMode bucketMode;
  private final long schemaId;
  @Nullable private final Snapshot snapshot;
  private final long snapshotId;
  private final long snapshotTimeMillis;
  @Nullable private final CoreOptions coreOptions;
  private final EffectiveOptions effectiveOptions;
  private final long optimizingCheckpointSnapshotId;
  private final String formulaVersion;
  private final String scoringConfigFingerprint;
  @Nullable private final String configurationError;
  private final TableAnalysisKey key;

  private PaimonHealthEvaluationContext(
      String tableId,
      TableShape tableShape,
      BucketMode bucketMode,
      long schemaId,
      @Nullable Snapshot snapshot,
      long snapshotId,
      long snapshotTimeMillis,
      @Nullable CoreOptions coreOptions,
      EffectiveOptions effectiveOptions,
      long optimizingCheckpointSnapshotId,
      String formulaVersion,
      String scoringConfigFingerprint,
      @Nullable String configurationError) {
    this.tableShape = tableShape;
    this.bucketMode = bucketMode;
    this.schemaId = schemaId;
    this.snapshot = snapshot;
    this.snapshotId = snapshotId;
    this.snapshotTimeMillis = snapshotTimeMillis;
    this.coreOptions = coreOptions;
    this.effectiveOptions = effectiveOptions;
    this.optimizingCheckpointSnapshotId = optimizingCheckpointSnapshotId;
    this.formulaVersion = formulaVersion;
    this.scoringConfigFingerprint = scoringConfigFingerprint;
    this.configurationError = configurationError;
    this.key =
        new TableAnalysisKey(
            tableId,
            TableFormat.PAIMON,
            snapshotId,
            TableAnalysisKey.NO_CHANGE_SNAPSHOT,
            schemaId,
            scoringConfigFingerprint,
            formulaVersion,
            optimizingCheckpointSnapshotId,
            TableAnalysisKey.NO_BASELINE_TIME);
  }

  /**
   * Capture all metadata which can affect a health result or reusable planning facts. This method
   * intentionally does not open a Paimon scan, SnapshotReader, manifest, data file, or DV index.
   */
  public static PaimonHealthEvaluationContext capture(
      FileStoreTable table, String tableId, @Nullable OptimizationContext context) {
    return capture(table, tableId, context, null);
  }

  public static PaimonHealthEvaluationContext capture(
      FileStoreTable table,
      String tableId,
      @Nullable OptimizationContext context,
      @Nullable Map<String, String> eligibilityTableOptions) {
    Objects.requireNonNull(table, "Paimon table must not be null");
    Objects.requireNonNull(tableId, "Table id must not be null");

    TableShape shape;
    if (table instanceof PrimaryKeyFileStoreTable) {
      shape = TableShape.PRIMARY_KEY;
    } else if (table instanceof AppendOnlyFileStoreTable) {
      shape = TableShape.APPEND;
    } else {
      shape = TableShape.UNSUPPORTED;
    }
    BucketMode bucketMode =
        Objects.requireNonNull(table.bucketMode(), "Bucket mode must not be null");
    TableSchema schema = Objects.requireNonNull(table.schema(), "Table schema must not be null");
    long schemaId = schema.id();
    OptimizingConfig optimizingConfig = context == null ? null : context.getOptimizingConfig();

    CoreOptions coreOptions = null;
    EffectiveOptions effectiveOptions;
    String configurationError = null;
    Map<String, String> rawOptions = safeOptions(schema.options());
    if (shape == TableShape.UNSUPPORTED) {
      effectiveOptions = EffectiveOptions.invalid();
    } else {
      try {
        coreOptions = Objects.requireNonNull(table.coreOptions(), "Core options must not be null");
        rawOptions = safeOptions(coreOptions.toMap());
        effectiveOptions = EffectiveOptions.capture(shape, bucketMode, coreOptions, rawOptions);
      } catch (RuntimeException e) {
        configurationError = stableConfigurationError(e);
        effectiveOptions = EffectiveOptions.invalid();
      }
    }
    Map<String, String> eligibilityOptions =
        eligibilityTableOptions == null ? rawOptions : safeOptions(eligibilityTableOptions);

    Snapshot snapshot = table.snapshotManager().latestSnapshot();
    long snapshotId = snapshot == null ? TableAnalysisKey.NO_SNAPSHOT : snapshot.id();
    long snapshotTimeMillis = snapshot == null ? 0L : snapshot.timeMillis();
    long baselineId =
        context == null ? TableAnalysisKey.NO_BASELINE : context.getLastOptimizedSnapshotId();
    // Paimon snapshot IDs start at Snapshot.FIRST_SNAPSHOT_ID. Runtime defaults or legacy state
    // below that boundary represent an unavailable checkpoint, not corrupt table activity.
    if (baselineId < Snapshot.FIRST_SNAPSHOT_ID) {
      baselineId = TableAnalysisKey.NO_BASELINE;
    }
    if (shape == TableShape.UNSUPPORTED) {
      baselineId = TableAnalysisKey.NO_BASELINE;
    }

    String formulaVersion;
    if (shape == TableShape.APPEND) {
      formulaVersion = combinedFormulaVersion(PaimonAppendHealthEvaluator.FORMULA_VERSION);
    } else if (shape == TableShape.PRIMARY_KEY) {
      formulaVersion = combinedFormulaVersion(PaimonPrimaryKeyHealthEvaluator.FORMULA_VERSION);
    } else {
      formulaVersion = UNSUPPORTED_FORMULA_VERSION;
    }
    String fingerprint =
        fingerprint(
            shape,
            bucketMode,
            schemaId,
            formulaVersion,
            effectiveOptions,
            rawOptions,
            eligibilityOptions,
            optimizingConfig,
            configurationError);
    return new PaimonHealthEvaluationContext(
        tableId,
        shape,
        bucketMode,
        schemaId,
        snapshot,
        snapshotId,
        snapshotTimeMillis,
        coreOptions,
        effectiveOptions,
        baselineId,
        formulaVersion,
        fingerprint,
        configurationError);
  }

  private static Map<String, String> safeOptions(@Nullable Map<String, String> options) {
    return options == null ? Collections.emptyMap() : options;
  }

  private static String combinedFormulaVersion(String structuralFormulaVersion) {
    return structuralFormulaVersion + "+" + PaimonActivityHealth.FORMULA_VERSION;
  }

  private static String fingerprint(
      TableShape shape,
      BucketMode bucketMode,
      long schemaId,
      String formulaVersion,
      EffectiveOptions options,
      Map<String, String> rawOptions,
      Map<String, String> eligibilityOptions,
      @Nullable OptimizingConfig optimizingConfig,
      @Nullable String configurationError) {
    FingerprintBuilder builder =
        new FingerprintBuilder()
            .add("tableFormat", TableFormat.PAIMON.name())
            .add("tableShape", shape.name())
            .add("bucketMode", bucketMode.name())
            .add("schemaId", schemaId)
            .add("formulaVersion", formulaVersion)
            .add("activityFormulaVersion", PaimonActivityHealth.FORMULA_VERSION)
            .add("writeOnly", eligibilityOptions.get(CoreOptions.WRITE_ONLY.key()))
            .add(
                "selfOptimizingEnabled",
                eligibilityOptions.get(PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED));
    if (configurationError != null) {
      addInvalidOptions(builder, shape, bucketMode, rawOptions);
      return builder
          .add("configurationState", "INVALID")
          .add("configurationError", configurationError)
          .digest();
    }

    builder.add("configurationState", "VALID");
    if (shape == TableShape.APPEND) {
      addAppendOptions(builder, options, optimizingConfig);
    } else if (shape == TableShape.PRIMARY_KEY) {
      addPrimaryKeyOptions(builder, bucketMode, options, optimizingConfig);
    }
    return builder.digest();
  }

  private static void addAppendOptions(
      FingerprintBuilder builder,
      EffectiveOptions options,
      @Nullable OptimizingConfig optimizingConfig) {
    builder
        .add("targetFileSize", options.targetFileSize)
        .add("compactionFileSize", options.compactionFileSize)
        .add("smallFileBoundary", options.smallFileBoundary)
        .add("compactionMinFileNum", options.compactionMinFileNum)
        .add("compactionFileNumLimit", options.compactionFileNumLimit)
        .add(
            "compactionDeleteRatioThreshold",
            canonicalDecimal(options.compactionDeleteRatioThreshold))
        .add("deletionVectorsEnabled", options.deletionVectorsEnabled)
        .add("splitOpenFileCost", options.splitOpenFileCost)
        .add("manifestDeleteFileDropStats", options.manifestDeleteFileDropStats);
    addAppendPlanningOptions(builder, optimizingConfig);
  }

  private static void addPrimaryKeyOptions(
      FingerprintBuilder builder,
      BucketMode bucketMode,
      EffectiveOptions options,
      @Nullable OptimizingConfig optimizingConfig) {
    builder
        .add("numSortedRunCompactionTrigger", options.compactionTrigger)
        .add("numSortedRunStopTrigger", options.stopTrigger)
        .add("numLevels", options.numLevels)
        .add("targetFileSize", options.targetFileSize)
        .add("compactionFileSize", options.compactionFileSize)
        .add("deletionVectorsEnabled", options.deletionVectorsEnabled)
        .add("pkClusteringOverride", options.pkClusteringOverride);
    if (bucketMode == BucketMode.HASH_FIXED || bucketMode == BucketMode.HASH_DYNAMIC) {
      builder
          .add("maxSizeAmplificationPercent", options.maxSizeAmplificationPercent)
          .add("compactionForceUpLevel0", options.compactionForceUpLevel0)
          .add("sortedRunSizeRatio", options.sortedRunSizeRatio)
          .add("compactOffPeakStartHour", options.compactOffPeakStartHour)
          .add("compactOffPeakEndHour", options.compactOffPeakEndHour)
          .add("compactOffPeakRatio", options.compactOffPeakRatio)
          .add("needLookup", options.needLookup)
          .add("lookupCompact", options.lookupCompact)
          .add("lookupCompactMaxInterval", options.lookupCompactMaxInterval)
          .add("primaryKeyPartitionIdleTimeMillis", options.primaryKeyPartitionIdleTimeMillis)
          .add("primaryKeyMajorMaxBucketRatio", options.primaryKeyMajorMaxBucketRatio);
      addHashPlanningOptions(builder, optimizingConfig);
    }
  }

  private static void addAppendPlanningOptions(
      FingerprintBuilder builder, @Nullable OptimizingConfig config) {
    if (config == null) {
      builder.add("amoroPlanningConfig", "UNAVAILABLE");
      return;
    }
    builder
        .add("minorLeastInterval", config.getMinorLeastInterval())
        .add("fullTriggerInterval", config.getFullTriggerInterval())
        .add("fullRewriteAllFiles", config.isFullRewriteAllFiles())
        .add("filter", normalizeNullable(config.getFilter()));
  }

  private static void addHashPlanningOptions(
      FingerprintBuilder builder, @Nullable OptimizingConfig config) {
    if (config == null) {
      builder.add("amoroPlanningConfig", "UNAVAILABLE");
      return;
    }
    builder
        .add("minorLeastInterval", config.getMinorLeastInterval())
        .add("fullTriggerInterval", config.getFullTriggerInterval())
        .add("filter", normalizeNullable(config.getFilter()));
  }

  private static void addInvalidOptions(
      FingerprintBuilder builder,
      TableShape shape,
      BucketMode bucketMode,
      Map<String, String> rawOptions) {
    if (shape == TableShape.APPEND) {
      addRaw(builder, rawOptions, CoreOptions.TARGET_FILE_SIZE.key());
      addRaw(builder, rawOptions, COMPACTION_SMALL_FILE_RATIO);
      addRaw(builder, rawOptions, CoreOptions.COMPACTION_MIN_FILE_NUM.key());
      addRaw(builder, rawOptions, COMPACTION_FILE_NUM_LIMIT);
      addRaw(builder, rawOptions, CoreOptions.COMPACTION_DELETE_RATIO_THRESHOLD.key());
      addRaw(builder, rawOptions, CoreOptions.DELETION_VECTORS_ENABLED.key());
      return;
    }
    if (shape == TableShape.UNSUPPORTED) {
      return;
    }
    addRaw(builder, rawOptions, CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key());
    addRaw(builder, rawOptions, CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER.key());
    addRaw(builder, rawOptions, CoreOptions.NUM_LEVELS.key());
    addRaw(builder, rawOptions, CoreOptions.TARGET_FILE_SIZE.key());
    addRaw(builder, rawOptions, CoreOptions.DELETION_VECTORS_ENABLED.key());
    addRaw(builder, rawOptions, CoreOptions.PK_CLUSTERING_OVERRIDE.key());
    if (bucketMode == BucketMode.HASH_FIXED || bucketMode == BucketMode.HASH_DYNAMIC) {
      addRaw(builder, rawOptions, CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key());
      addRaw(builder, rawOptions, CoreOptions.COMPACTION_FORCE_UP_LEVEL_0.key());
      addRaw(builder, rawOptions, CoreOptions.COMPACTION_SIZE_RATIO.key());
      addRaw(builder, rawOptions, CoreOptions.LOOKUP_COMPACT.key());
      addRaw(builder, rawOptions, CoreOptions.LOOKUP_COMPACT_MAX_INTERVAL.key());
      addRaw(builder, rawOptions, PaimonPrimaryKeyOptions.PARTITION_IDLE_TIME);
      addRaw(builder, rawOptions, PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO);
    }
  }

  private static void addRaw(
      FingerprintBuilder builder, Map<String, String> options, String optionKey) {
    builder.add("raw." + optionKey, normalizeNullable(options.get(optionKey)));
  }

  private static String stableConfigurationError(RuntimeException exception) {
    return exception.getClass().getName();
  }

  private static String normalizeNullable(@Nullable String value) {
    return value == null ? "<absent>" : value;
  }

  private static String canonicalDecimal(double value) {
    return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
  }

  public TableShape tableShape() {
    return tableShape;
  }

  public BucketMode bucketMode() {
    return bucketMode;
  }

  public long schemaId() {
    return schemaId;
  }

  public Optional<Snapshot> snapshot() {
    return Optional.ofNullable(snapshot);
  }

  public long snapshotId() {
    return snapshotId;
  }

  public long snapshotTimeMillis() {
    return snapshotTimeMillis;
  }

  public Optional<CoreOptions> coreOptions() {
    return Optional.ofNullable(coreOptions);
  }

  public long targetFileSize() {
    return effectiveOptions.targetFileSize;
  }

  public long compactionFileSize() {
    return effectiveOptions.compactionFileSize;
  }

  public long smallFileBoundary() {
    return effectiveOptions.smallFileBoundary;
  }

  public int compactionTrigger() {
    return effectiveOptions.compactionTrigger;
  }

  public int stopTrigger() {
    return effectiveOptions.stopTrigger;
  }

  public int numLevels() {
    return effectiveOptions.numLevels;
  }

  public boolean deletionVectorsEnabled() {
    return effectiveOptions.deletionVectorsEnabled;
  }

  public boolean pkClusteringOverride() {
    return effectiveOptions.pkClusteringOverride;
  }

  public String formulaVersion() {
    return formulaVersion;
  }

  public String scoringConfigFingerprint() {
    return scoringConfigFingerprint;
  }

  public Optional<String> configurationError() {
    return Optional.ofNullable(configurationError);
  }

  public TableAnalysisKey key() {
    return key;
  }

  /** Construct activity input without reading historical Snapshot files. */
  public PaimonActivityHealth.Input activityInput(int baseScore) {
    if (snapshot == null || optimizingCheckpointSnapshotId == TableAnalysisKey.NO_BASELINE) {
      return PaimonActivityHealth.Input.withoutBaseline(baseScore);
    }
    return PaimonActivityHealth.Input.withBaseline(
        baseScore, snapshotId, optimizingCheckpointSnapshotId);
  }

  public enum TableShape {
    APPEND,
    PRIMARY_KEY,
    UNSUPPORTED
  }

  private static final class EffectiveOptions {
    private final long targetFileSize;
    private final long compactionFileSize;
    private final long smallFileBoundary;
    private final int compactionMinFileNum;
    private final int compactionFileNumLimit;
    private final double compactionDeleteRatioThreshold;
    private final long splitOpenFileCost;
    private final boolean manifestDeleteFileDropStats;
    private final int compactionTrigger;
    private final int stopTrigger;
    private final int numLevels;
    private final int maxSizeAmplificationPercent;
    private final boolean compactionForceUpLevel0;
    private final int sortedRunSizeRatio;
    private final int compactOffPeakStartHour;
    private final int compactOffPeakEndHour;
    private final int compactOffPeakRatio;
    private final boolean needLookup;
    private final String lookupCompact;
    private final int lookupCompactMaxInterval;
    private final boolean deletionVectorsEnabled;
    private final boolean pkClusteringOverride;
    private final long primaryKeyPartitionIdleTimeMillis;
    private final String primaryKeyMajorMaxBucketRatio;

    private EffectiveOptions(
        long targetFileSize,
        long compactionFileSize,
        long smallFileBoundary,
        int compactionMinFileNum,
        int compactionFileNumLimit,
        double compactionDeleteRatioThreshold,
        long splitOpenFileCost,
        boolean manifestDeleteFileDropStats,
        int compactionTrigger,
        int stopTrigger,
        int numLevels,
        int maxSizeAmplificationPercent,
        boolean compactionForceUpLevel0,
        int sortedRunSizeRatio,
        int compactOffPeakStartHour,
        int compactOffPeakEndHour,
        int compactOffPeakRatio,
        boolean needLookup,
        String lookupCompact,
        int lookupCompactMaxInterval,
        boolean deletionVectorsEnabled,
        boolean pkClusteringOverride,
        long primaryKeyPartitionIdleTimeMillis,
        String primaryKeyMajorMaxBucketRatio) {
      this.targetFileSize = targetFileSize;
      this.compactionFileSize = compactionFileSize;
      this.smallFileBoundary = smallFileBoundary;
      this.compactionMinFileNum = compactionMinFileNum;
      this.compactionFileNumLimit = compactionFileNumLimit;
      this.compactionDeleteRatioThreshold = compactionDeleteRatioThreshold;
      this.splitOpenFileCost = splitOpenFileCost;
      this.manifestDeleteFileDropStats = manifestDeleteFileDropStats;
      this.compactionTrigger = compactionTrigger;
      this.stopTrigger = stopTrigger;
      this.numLevels = numLevels;
      this.maxSizeAmplificationPercent = maxSizeAmplificationPercent;
      this.compactionForceUpLevel0 = compactionForceUpLevel0;
      this.sortedRunSizeRatio = sortedRunSizeRatio;
      this.compactOffPeakStartHour = compactOffPeakStartHour;
      this.compactOffPeakEndHour = compactOffPeakEndHour;
      this.compactOffPeakRatio = compactOffPeakRatio;
      this.needLookup = needLookup;
      this.lookupCompact = lookupCompact;
      this.lookupCompactMaxInterval = lookupCompactMaxInterval;
      this.deletionVectorsEnabled = deletionVectorsEnabled;
      this.pkClusteringOverride = pkClusteringOverride;
      this.primaryKeyPartitionIdleTimeMillis = primaryKeyPartitionIdleTimeMillis;
      this.primaryKeyMajorMaxBucketRatio = primaryKeyMajorMaxBucketRatio;
    }

    private static EffectiveOptions capture(
        TableShape shape,
        BucketMode bucketMode,
        CoreOptions coreOptions,
        Map<String, String> rawOptions) {
      if (shape == TableShape.UNSUPPORTED) {
        return invalid();
      }
      if (shape == TableShape.APPEND) {
        long targetFileSize = coreOptions.targetFileSize(false);
        long compactionFileSize = coreOptions.compactionFileSize(false);
        Options options = Options.fromMap(rawOptions);
        long smallFileBoundary =
            options.containsKey(COMPACTION_SMALL_FILE_RATIO)
                ? (long) (targetFileSize * options.getDouble(COMPACTION_SMALL_FILE_RATIO, 0.7D))
                : compactionFileSize;
        return new EffectiveOptions(
            targetFileSize,
            compactionFileSize,
            smallFileBoundary,
            coreOptions.compactionMinFileNum(),
            options.getInteger(COMPACTION_FILE_NUM_LIMIT, DEFAULT_COMPACTION_FILE_NUM_LIMIT),
            coreOptions.compactionDeleteRatioThreshold(),
            coreOptions.splitOpenFileCost(),
            coreOptions.manifestDeleteFileDropStats(),
            0,
            0,
            0,
            0,
            false,
            0,
            0,
            0,
            0,
            false,
            "<not-applicable>",
            0,
            coreOptions.deletionVectorsEnabled(),
            false,
            0L,
            "<not-applicable>");
      }

      boolean hashMode =
          bucketMode == BucketMode.HASH_FIXED || bucketMode == BucketMode.HASH_DYNAMIC;
      PaimonPrimaryKeyOptions primaryKeyOptions =
          hashMode ? PaimonPrimaryKeyOptions.from(rawOptions) : null;
      boolean needLookup = hashMode && coreOptions.needLookup();
      String lookupCompact = needLookup ? coreOptions.lookupCompact().name() : "<not-applicable>";
      int lookupCompactMaxInterval =
          needLookup && coreOptions.lookupCompact() == CoreOptions.LookupCompactMode.GENTLE
              ? coreOptions.lookupCompactMaxInterval()
              : 0;
      long partitionIdleTimeMillis =
          primaryKeyOptions == null
              ? TableAnalysisKey.NO_BASELINE_TIME
              : primaryKeyOptions
                  .partitionIdleTime()
                  .map(Duration::toMillis)
                  .orElse(TableAnalysisKey.NO_BASELINE_TIME);
      return new EffectiveOptions(
          coreOptions.targetFileSize(true),
          coreOptions.compactionFileSize(true),
          0L,
          0,
          0,
          0.0D,
          0L,
          false,
          coreOptions.numSortedRunCompactionTrigger(),
          coreOptions.numSortedRunStopTrigger(),
          coreOptions.numLevels(),
          hashMode ? coreOptions.maxSizeAmplificationPercent() : 0,
          hashMode && coreOptions.compactionForceUpLevel0(),
          hashMode ? coreOptions.sortedRunSizeRatio() : 0,
          hashMode ? coreOptions.compactOffPeakStartHour() : 0,
          hashMode ? coreOptions.compactOffPeakEndHour() : 0,
          hashMode ? coreOptions.compactOffPeakRatio() : 0,
          needLookup,
          lookupCompact,
          lookupCompactMaxInterval,
          coreOptions.deletionVectorsEnabled(),
          coreOptions.pkClusteringOverride(),
          partitionIdleTimeMillis,
          primaryKeyOptions == null
              ? "<not-applicable>"
              : primaryKeyOptions.majorMaxBucketRatio().stripTrailingZeros().toPlainString());
    }

    private static EffectiveOptions invalid() {
      return new EffectiveOptions(
          -1L,
          -1L,
          -1L,
          -1,
          -1,
          Double.NaN,
          -1L,
          false,
          -1,
          -1,
          -1,
          -1,
          false,
          -1,
          -1,
          -1,
          -1,
          false,
          "<invalid>",
          -1,
          false,
          false,
          -1L,
          "<invalid>");
    }
  }

  private static final class FingerprintBuilder {
    private final StringBuilder encoded = new StringBuilder();

    private FingerprintBuilder add(String name, Object value) {
      append(name);
      append(String.valueOf(value));
      return this;
    }

    private void append(String value) {
      encoded.append(value.length()).append(':').append(value);
    }

    private String digest() {
      return DigestUtil.sha256Hex(encoded.toString());
    }
  }
}
