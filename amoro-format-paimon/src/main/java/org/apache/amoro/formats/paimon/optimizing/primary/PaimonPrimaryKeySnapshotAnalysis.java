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

package org.apache.amoro.formats.paimon.optimizing.primary;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

import org.apache.amoro.formats.paimon.optimizing.health.PaimonActivityHealth;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator.EvaluationMode;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator.Result;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator.UnitAccumulator;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonPrimaryKeyHealthEvaluator.UnitStatistics;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthComponent;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** One fixed-snapshot primary-key analysis shared by health evaluation and HASH planning. */
public final class PaimonPrimaryKeySnapshotAnalysis implements FormatTableAnalysis {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonPrimaryKeySnapshotAnalysis.class);

  private static final int MAX_WORST_BUCKETS = 20;

  private static final Comparator<BucketFacts> WORST_BUCKET_ORDER =
      Comparator.comparingLong(BucketFacts::sortedRunCount)
          .reversed()
          .thenComparing(Comparator.comparingLong(BucketFacts::fileCount).reversed())
          .thenComparing(Comparator.comparingLong(BucketFacts::fileSize).reversed())
          .thenComparing(
              (left, right) -> compareUnsigned(left.partitionBytes(), right.partitionBytes()))
          .thenComparingInt(BucketFacts::bucket);

  private static final Comparator<DataFileMeta> STABLE_FILE_ORDER =
      Comparator.comparingInt(DataFileMeta::level)
          .thenComparingLong(DataFileMeta::minSequenceNumber)
          .thenComparingLong(DataFileMeta::maxSequenceNumber)
          .thenComparing(DataFileMeta::fileName);

  private final TableAnalysisKey key;
  private final PaimonPrimaryKeyPendingInput pendingInput;
  private final TableHealthDetails healthDetails;
  private final List<BucketFacts> bucketFacts;
  private final int activePartitionCount;
  private final boolean validForPlanning;
  private final Map<ByteBuffer, Long> partitionWatermarks;

  private PaimonPrimaryKeySnapshotAnalysis(
      TableAnalysisKey key,
      PaimonPrimaryKeyPendingInput pendingInput,
      TableHealthDetails healthDetails,
      List<BucketFacts> bucketFacts,
      int activePartitionCount,
      boolean validForPlanning,
      Map<ByteBuffer, Long> partitionWatermarks) {
    this.key = key;
    this.pendingInput = pendingInput;
    this.healthDetails = healthDetails;
    this.bucketFacts = Collections.unmodifiableList(new ArrayList<>(bucketFacts));
    this.activePartitionCount = activePartitionCount;
    this.validForPlanning = validForPlanning;
    this.partitionWatermarks = immutableWatermarks(partitionWatermarks);
  }

  /** Analyze the exact {@link Snapshot} already captured by the metadata-only context. */
  public static PaimonPrimaryKeySnapshotAnalysis analyze(
      FileStoreTable table, PaimonHealthEvaluationContext context) {
    Objects.requireNonNull(table, "Paimon table must not be null");
    Objects.requireNonNull(context, "Paimon health context must not be null");

    if (context.configurationError().isPresent() || !context.coreOptions().isPresent()) {
      return invalid(context, PaimonPrimaryKeyHealthEvaluator.INVALID_SCORING_CONFIG);
    }
    Object store = table.store();
    if (!(store instanceof KeyValueFileStore)) {
      return invalid(context, PaimonPrimaryKeyHealthEvaluator.SNAPSHOT_SCAN_FAILED);
    }
    BucketMode bucketMode = context.bucketMode();
    if (bucketMode != BucketMode.HASH_FIXED
        && bucketMode != BucketMode.HASH_DYNAMIC
        && bucketMode != BucketMode.KEY_DYNAMIC) {
      return invalid(context, PaimonPrimaryKeyHealthEvaluator.SNAPSHOT_SCAN_FAILED);
    }

    PaimonPrimaryKeyHealthEvaluator healthEvaluator =
        new PaimonPrimaryKeyHealthEvaluator(
            context.compactionTrigger(),
            context.stopTrigger(),
            context.numLevels(),
            context.targetFileSize(),
            context.compactionFileSize());
    Optional<Snapshot> snapshot = context.snapshot();
    if (!snapshot.isPresent()) {
      Result result =
          healthEvaluator.evaluate(
              Collections.emptyList(), context.activityInput(0), evaluationMode(bucketMode));
      return create(context, result, Collections.emptyList(), 0, true, Collections.emptyMap());
    }

    try {
      return scan(
          table,
          context,
          snapshot.get(),
          (KeyValueFileStore) store,
          healthEvaluator,
          evaluationMode(bucketMode));
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to scan Paimon primary-key table [{}] at snapshot [{}] for health evaluation.",
          context.key().getTableId(),
          snapshot.get().id(),
          e);
      return invalid(context, PaimonPrimaryKeyHealthEvaluator.SNAPSHOT_SCAN_FAILED);
    }
  }

  private static PaimonPrimaryKeySnapshotAnalysis scan(
      FileStoreTable table,
      PaimonHealthEvaluationContext context,
      Snapshot snapshot,
      KeyValueFileStore store,
      PaimonPrimaryKeyHealthEvaluator healthEvaluator,
      EvaluationMode mode) {
    SnapshotReader reader = table.newSnapshotReader().withSnapshot(snapshot);
    Map<BucketKey, MutableBucket> buckets = new LinkedHashMap<>();
    Set<BinaryRow> partitions = new HashSet<>();
    Iterator<ManifestEntry> entries = reader.readFileIterator();
    while (entries.hasNext()) {
      ManifestEntry entry = entries.next();
      BinaryRow partition = entry.partition().copy();
      partitions.add(partition);
      BucketKey key = new BucketKey(partition, entry.bucket());
      buckets
          .computeIfAbsent(key, ignored -> new MutableBucket(partition, entry.bucket()))
          .add(entry.file());
    }

    Map<ByteBuffer, Long> partitionWatermarks = new HashMap<>();
    if (!table.partitionKeys().isEmpty()) {
      try {
        for (PartitionEntry entry : reader.partitionEntries()) {
          byte[] partitionBytes = SerializationUtils.serializeBinaryRow(entry.partition().copy());
          partitionWatermarks.put(ByteBuffer.wrap(partitionBytes), entry.lastFileCreationTime());
        }
      } catch (RuntimeException e) {
        partitionWatermarks.clear();
      }
    }

    List<MutableBucket> orderedBuckets = new ArrayList<>(buckets.values());
    orderedBuckets.sort(
        (left, right) -> {
          int partitionOrder = compareUnsigned(left.partitionBytes, right.partitionBytes);
          return partitionOrder != 0 ? partitionOrder : Integer.compare(left.bucket, right.bucket);
        });

    List<BucketFacts> facts = new ArrayList<>(orderedBuckets.size());
    for (MutableBucket bucket : orderedBuckets) {
      bucket.files.sort(STABLE_FILE_ORDER);
      Levels levels = new Levels(store.newKeyComparator(), bucket.files, context.numLevels());
      int sortedRunCount = Math.toIntExact(levels.numberOfSortedRuns());
      facts.add(bucket.toFacts(levels, sortedRunCount));
    }

    try {
      Result result =
          evaluateHealth(
              context, snapshot, store, healthEvaluator, mode, orderedBuckets, partitions, facts);
      return create(context, result, facts, partitions.size(), true, partitionWatermarks);
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to evaluate Paimon primary-key table [{}] at snapshot [{}] after collecting [{}] bucket facts.",
          context.key().getTableId(),
          snapshot.id(),
          facts.size(),
          e);
      return invalidHealth(
          context,
          PaimonPrimaryKeyHealthEvaluator.SNAPSHOT_SCAN_FAILED,
          facts,
          partitions.size(),
          partitionWatermarks,
          true);
    }
  }

  private static Result evaluateHealth(
      PaimonHealthEvaluationContext context,
      Snapshot snapshot,
      KeyValueFileStore store,
      PaimonPrimaryKeyHealthEvaluator healthEvaluator,
      EvaluationMode mode,
      List<MutableBucket> orderedBuckets,
      Set<BinaryRow> partitions,
      List<BucketFacts> facts) {
    Map<BucketKey, List<IndexFileMeta>> deletionVectorFiles = Collections.emptyMap();
    IndexFileHandler indexFileHandler = null;
    if (context.deletionVectorsEnabled()) {
      indexFileHandler = store.newIndexFileHandler();
      Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> scanned =
          indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partitions);
      deletionVectorFiles = new HashMap<>();
      for (Map.Entry<Pair<BinaryRow, Integer>, List<IndexFileMeta>> entry : scanned.entrySet()) {
        BinaryRow partition = entry.getKey().getLeft().copy();
        deletionVectorFiles.put(
            new BucketKey(partition, entry.getKey().getRight()), entry.getValue());
      }
    }

    List<UnitStatistics> statistics = new ArrayList<>(orderedBuckets.size());
    int index = 0;
    for (MutableBucket bucket : orderedBuckets) {
      UnitAccumulator accumulator = healthEvaluator.newUnitAccumulator();
      BucketedDvMaintainer dvMaintainer =
          context.deletionVectorsEnabled()
              ? BucketedDvMaintainer.factory(indexFileHandler)
                  .create(
                      bucket.partition,
                      bucket.bucket,
                      deletionVectorFiles.getOrDefault(
                          new BucketKey(bucket.partition, bucket.bucket), Collections.emptyList()))
              : null;
      for (DataFileMeta file : bucket.files) {
        Long tombstoneCount = file.deleteRowCount().orElse(null);
        Long deletionVectorCount = deletionVectorCardinality(dvMaintainer, file.fileName());
        accumulator.addFile(file.fileSize(), file.rowCount(), tombstoneCount, deletionVectorCount);
      }
      statistics.add(accumulator.snapshot(Math.toIntExact(facts.get(index).sortedRunCount())));
      index++;
    }
    return healthEvaluator.evaluate(statistics, context.activityInput(0), mode);
  }

  private static long deletionVectorCardinality(
      @Nullable BucketedDvMaintainer maintainer, String fileName) {
    if (maintainer == null) {
      return 0L;
    }
    Optional<DeletionVector> deletionVector = maintainer.deletionVectorOf(fileName);
    return deletionVector.isPresent() ? deletionVector.get().getCardinality() : 0L;
  }

  private static EvaluationMode evaluationMode(BucketMode bucketMode) {
    return bucketMode == BucketMode.KEY_DYNAMIC ? EvaluationMode.KEY_DYNAMIC : EvaluationMode.HASH;
  }

  private static PaimonPrimaryKeySnapshotAnalysis create(
      PaimonHealthEvaluationContext context,
      Result result,
      List<BucketFacts> bucketFacts,
      int activePartitionCount,
      boolean validForPlanning,
      Map<ByteBuffer, Long> partitionWatermarks) {
    StructuralSummary structural = StructuralSummary.from(context, bucketFacts);
    PaimonPrimaryKeyPendingInput pendingInput =
        result.healthScore() < 0 && !bucketFacts.isEmpty()
            ? structuralPendingInput(bucketFacts, structural)
            : result.toPendingInput();
    TableHealthDetails details =
        details(
            context,
            result.healthScore() < 0 ? null : result,
            bucketFacts,
            activePartitionCount,
            result.reasonCodes(),
            structural);
    return new PaimonPrimaryKeySnapshotAnalysis(
        context.key(),
        pendingInput,
        details,
        bucketFacts,
        activePartitionCount,
        validForPlanning,
        partitionWatermarks);
  }

  private static PaimonPrimaryKeySnapshotAnalysis invalid(
      PaimonHealthEvaluationContext context, String reasonCode) {
    return invalidHealth(
        context, reasonCode, Collections.emptyList(), 0, Collections.emptyMap(), false);
  }

  private static PaimonPrimaryKeySnapshotAnalysis invalidHealth(
      PaimonHealthEvaluationContext context,
      String reasonCode,
      List<BucketFacts> bucketFacts,
      int activePartitionCount,
      Map<ByteBuffer, Long> partitionWatermarks,
      boolean validForPlanning) {
    StructuralSummary structural = StructuralSummary.from(context, bucketFacts);
    PaimonPrimaryKeyPendingInput pendingInput =
        bucketFacts.isEmpty()
            ? new PaimonPrimaryKeyPendingInput()
            : structuralPendingInput(bucketFacts, structural);
    TableHealthDetails details =
        details(
            context,
            null,
            bucketFacts,
            activePartitionCount,
            Collections.singletonList(reasonCode),
            structural);
    return new PaimonPrimaryKeySnapshotAnalysis(
        context.key(),
        pendingInput,
        details,
        bucketFacts,
        activePartitionCount,
        validForPlanning,
        partitionWatermarks);
  }

  private static PaimonPrimaryKeyPendingInput structuralPendingInput(
      List<BucketFacts> bucketFacts, StructuralSummary summary) {
    return new PaimonPrimaryKeyPendingInput(
        summary.pendingFileCount(),
        summary.totalFileSize,
        summary.totalRecordCount,
        summary.pendingSmallFileCount(),
        summary.smallFileSize,
        summary.tombstoneRecordCount,
        -1L,
        bucketFacts.size(),
        summary.maxSortedRunCount,
        -1,
        -1,
        -1,
        -1);
  }

  private static TableHealthDetails details(
      PaimonHealthEvaluationContext context,
      @Nullable Result result,
      List<BucketFacts> bucketFacts,
      int activePartitionCount,
      List<String> reasonCodes,
      StructuralSummary structural) {
    int runScore = result == null ? -1 : result.runScore();
    int deleteScore = result == null ? -1 : result.materializedDeleteScore();
    int activityScore = result == null ? -1 : result.healthScore();
    List<TableHealthComponent> components =
        Arrays.asList(
            component("SORTED_RUN", runScore, "MULTIPLY"),
            component("MATERIALIZED_DELETE", deleteScore, "MULTIPLY"),
            component("FILE_SIZE_AUXILIARY", -1, "AUXILIARY"),
            component("SNAPSHOT_ACTIVITY", activityScore, "DEBT_AMPLIFIER"));
    Map<String, String> metrics =
        metrics(context, result, bucketFacts, activePartitionCount, structural);
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
        reasonCodes);
  }

  private static TableHealthComponent component(String code, int score, String combination) {
    return new TableHealthComponent(code, score, null, combination, Collections.emptyMap());
  }

  private static Map<String, String> metrics(
      PaimonHealthEvaluationContext context,
      @Nullable Result result,
      List<BucketFacts> bucketFacts,
      int activePartitionCount,
      StructuralSummary structural) {
    boolean effectiveOptionsAvailable =
        !context.configurationError().isPresent() && context.coreOptions().isPresent();
    LinkedHashMap<String, String> metrics = new LinkedHashMap<>();
    metrics.put("bucketMode", context.bucketMode().name());
    metrics.put("activePartitionCount", Integer.toString(activePartitionCount));
    metrics.put("effectiveUnitCount", integer(bucketFacts.size()));
    metrics.put("totalFileCount", number(structural.totalFileCount));
    metrics.put("totalFileSize", number(structural.totalFileSize));
    metrics.put(
        "averageFileSize",
        number(
            structural.totalFileCount == 0
                ? 0L
                : structural.totalFileSize / structural.totalFileCount));
    metrics.put(
        "targetFileSize", effectiveOptionsAvailable ? number(context.targetFileSize()) : "N/A");
    metrics.put(
        "compactionFileSize",
        effectiveOptionsAvailable ? number(context.compactionFileSize()) : "N/A");
    metrics.put(
        "compactionTrigger",
        effectiveOptionsAvailable ? integer(context.compactionTrigger()) : "N/A");
    metrics.put("stopTrigger", effectiveOptionsAvailable ? integer(context.stopTrigger()) : "N/A");
    metrics.put("numLevels", effectiveOptionsAvailable ? integer(context.numLevels()) : "N/A");
    metrics.put("smallFileCount", number(structural.smallFileCount));
    metrics.put("smallFileSize", number(structural.smallFileSize));
    metrics.put("totalRecordCount", number(structural.totalRecordCount));
    boolean structuralFactsAvailable = result != null || !bucketFacts.isEmpty();
    long tombstoneRecordCount =
        result == null ? structural.tombstoneRecordCount : result.tombstoneRecordCount();
    metrics.put(
        "tombstoneRecordCount",
        structuralFactsAvailable && structural.tombstoneComplete && tombstoneRecordCount >= 0
            ? number(tombstoneRecordCount)
            : "N/A");
    metrics.put(
        "deletionVectorRecordCount",
        result == null ? "N/A" : number(result.deletionVectorRecordCount()));
    metrics.put("maxSortedRunCount", integer(structural.maxSortedRunCount));
    metrics.put("sortedRunDistribution", sortedRunDistribution(bucketFacts));
    metrics.put("worstBuckets", worstBuckets(bucketFacts));
    metrics.put("latestSnapshotTimeMillis", positiveOrNa(context.snapshotTimeMillis()));
    TableAnalysisKey key = context.key();
    metrics.put("baselineSnapshotId", nonNegativeOrNa(key.getSuccessfulOptimizationBaselineId()));
    metrics.put(
        "baselineSnapshotTimeMillis",
        positiveOrNa(key.getSuccessfulOptimizationBaselineTimeMillis()));
    metrics.put("timeThresholdMillis", "N/A");
    if (result != null && result.activity() != null) {
      PaimonActivityHealth.Result activity = result.activity();
      metrics.put("newSnapshotCount", nonNegativeOrNa(activity.newSnapshotCount()));
      metrics.put("snapshotTimeDistanceMillis", "N/A");
      metrics.put(
          "snapshotPressure",
          activity.baselineAvailable() ? Double.toString(activity.snapshotPressure()) : "N/A");
      metrics.put("timePressure", "N/A");
      metrics.put("activityPressure", Double.toString(activity.activityPressure()));
    } else {
      metrics.put("newSnapshotCount", "N/A");
      metrics.put("snapshotTimeDistanceMillis", "N/A");
      metrics.put("snapshotPressure", "N/A");
      metrics.put("timePressure", "N/A");
      metrics.put("activityPressure", "N/A");
    }
    return metrics;
  }

  private static String sortedRunDistribution(List<BucketFacts> bucketFacts) {
    int[] counts = new int[5];
    for (BucketFacts facts : bucketFacts) {
      long runs = facts.sortedRunCount();
      int index = runs <= 1 ? 0 : runs <= 4 ? 1 : runs <= 7 ? 2 : runs <= 10 ? 3 : 4;
      counts[index]++;
    }
    return "0-1=" + counts[0] + ",2-4=" + counts[1] + ",5-7=" + counts[2] + ",8-10=" + counts[3]
        + ",11+=" + counts[4];
  }

  private static String worstBuckets(List<BucketFacts> bucketFacts) {
    List<BucketFacts> sorted = new ArrayList<>(bucketFacts);
    sorted.sort(WORST_BUCKET_ORDER);
    StringBuilder builder = new StringBuilder();
    int maximum = Math.min(MAX_WORST_BUCKETS, sorted.size());
    for (int index = 0; index < maximum; index++) {
      if (index > 0) {
        builder.append(';');
      }
      BucketFacts facts = sorted.get(index);
      builder
          .append("p=")
          .append(Integer.toUnsignedString(Arrays.hashCode(facts.partitionBytes()), 16))
          .append(",b=")
          .append(facts.bucket())
          .append(",r=")
          .append(facts.sortedRunCount())
          .append(",f=")
          .append(facts.fileCount())
          .append(",s=")
          .append(facts.fileSize());
    }
    return builder.toString();
  }

  private static Long nullableId(long id) {
    return id < 0 ? null : id;
  }

  private static String number(long value) {
    return Long.toString(value);
  }

  private static String integer(int value) {
    return Integer.toString(value);
  }

  private static String nonNegativeOrNa(long value) {
    return value < 0 ? "N/A" : number(value);
  }

  private static String positiveOrNa(long value) {
    return value <= 0 ? "N/A" : number(value);
  }

  static int compareUnsigned(byte[] left, byte[] right) {
    int sharedLength = Math.min(left.length, right.length);
    for (int index = 0; index < sharedLength; index++) {
      int difference = (left[index] & 0xff) - (right[index] & 0xff);
      if (difference != 0) {
        return difference;
      }
    }
    return Integer.compare(left.length, right.length);
  }

  private static Map<ByteBuffer, Long> immutableWatermarks(Map<ByteBuffer, Long> source) {
    if (source.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<ByteBuffer, Long> copy = new HashMap<>();
    for (Map.Entry<ByteBuffer, Long> entry : source.entrySet()) {
      ByteBuffer buffer = entry.getKey().duplicate();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      copy.put(ByteBuffer.wrap(bytes).asReadOnlyBuffer(), entry.getValue());
    }
    return Collections.unmodifiableMap(copy);
  }

  @Override
  public TableAnalysisKey key() {
    return key;
  }

  @Override
  public PaimonPrimaryKeyPendingInput pendingInput() {
    return pendingInput;
  }

  @Override
  public TableHealthDetails healthDetails() {
    return healthDetails;
  }

  List<BucketFacts> bucketFacts() {
    return bucketFacts;
  }

  int activePartitionCount() {
    return activePartitionCount;
  }

  public boolean validForPlanning() {
    return validForPlanning;
  }

  Long partitionWatermark(byte[] partitionBytes) {
    return partitionWatermarks.get(
        ByteBuffer.wrap(Arrays.copyOf(partitionBytes, partitionBytes.length)));
  }

  static final class BucketFacts {
    private final BinaryRow partition;
    private final byte[] partitionBytes;
    private final int bucket;
    private final List<DataFileMeta> files;
    private final Levels levels;
    private final PaimonBucketCompactionUnit unit;

    private BucketFacts(
        BinaryRow partition,
        byte[] partitionBytes,
        int bucket,
        List<DataFileMeta> files,
        Levels levels,
        PaimonBucketCompactionUnit unit) {
      this.partition = partition.copy();
      this.partitionBytes = Arrays.copyOf(partitionBytes, partitionBytes.length);
      this.bucket = bucket;
      this.files = Collections.unmodifiableList(new ArrayList<>(files));
      this.levels = levels;
      this.unit = unit;
    }

    BinaryRow partition() {
      return partition.copy();
    }

    byte[] partitionBytes() {
      return Arrays.copyOf(partitionBytes, partitionBytes.length);
    }

    int bucket() {
      return bucket;
    }

    List<DataFileMeta> files() {
      return files;
    }

    Levels levels() {
      return levels;
    }

    PaimonBucketCompactionUnit unit() {
      return unit;
    }

    long sortedRunCount() {
      return unit.getSortedRunCount();
    }

    long fileCount() {
      return unit.getFileCount();
    }

    long fileSize() {
      return unit.getFileSizeInBytes();
    }
  }

  private static final class StructuralSummary {
    private long totalFileCount;
    private long totalFileSize;
    private long totalRecordCount;
    private long smallFileCount;
    private long smallFileSize;
    private long tombstoneRecordCount;
    private int maxSortedRunCount;
    private boolean tombstoneComplete = true;

    private static StructuralSummary from(
        PaimonHealthEvaluationContext context, List<BucketFacts> bucketFacts) {
      StructuralSummary summary = new StructuralSummary();
      for (BucketFacts facts : bucketFacts) {
        summary.maxSortedRunCount =
            Math.max(summary.maxSortedRunCount, Math.toIntExact(facts.sortedRunCount()));
        for (DataFileMeta file : facts.files()) {
          summary.totalFileCount = Math.addExact(summary.totalFileCount, 1L);
          summary.totalFileSize = Math.addExact(summary.totalFileSize, file.fileSize());
          summary.totalRecordCount = Math.addExact(summary.totalRecordCount, file.rowCount());
          if (file.fileSize() < context.compactionFileSize()) {
            summary.smallFileCount = Math.addExact(summary.smallFileCount, 1L);
            summary.smallFileSize = Math.addExact(summary.smallFileSize, file.fileSize());
          }
          Optional<Long> tombstoneCount = file.deleteRowCount();
          if (tombstoneCount.isPresent()
              && tombstoneCount.get() >= 0
              && tombstoneCount.get() <= file.rowCount()
              && summary.tombstoneComplete) {
            summary.tombstoneRecordCount =
                Math.addExact(summary.tombstoneRecordCount, tombstoneCount.get());
          } else {
            summary.tombstoneComplete = false;
            summary.tombstoneRecordCount = -1L;
          }
        }
      }
      return summary;
    }

    private int pendingFileCount() {
      return (int) Math.min(totalFileCount, Integer.MAX_VALUE);
    }

    private int pendingSmallFileCount() {
      return (int) Math.min(smallFileCount, Integer.MAX_VALUE);
    }
  }

  private static final class MutableBucket {
    private final BinaryRow partition;
    private final byte[] partitionBytes;
    private final int bucket;
    private final List<DataFileMeta> files = new ArrayList<>();
    private long fileSize;
    private long recordCount;
    private long lastFileCreationTime;

    private MutableBucket(BinaryRow partition, int bucket) {
      this.partition = partition.copy();
      this.partitionBytes = SerializationUtils.serializeBinaryRow(this.partition);
      this.bucket = bucket;
    }

    private void add(DataFileMeta file) {
      files.add(file);
      fileSize = Math.addExact(fileSize, file.fileSize());
      recordCount = Math.addExact(recordCount, file.rowCount());
      lastFileCreationTime = Math.max(lastFileCreationTime, file.creationTimeEpochMillis());
    }

    private BucketFacts toFacts(Levels levels, int sortedRunCount) {
      PaimonBucketCompactionUnit unit =
          new PaimonBucketCompactionUnit(
              partitionBytes,
              bucket,
              files.size(),
              sortedRunCount,
              fileSize,
              recordCount,
              lastFileCreationTime);
      return new BucketFacts(partition, partitionBytes, bucket, files, levels, unit);
    }
  }

  private static final class BucketKey {
    private final BinaryRow partition;
    private final int bucket;

    private BucketKey(BinaryRow partition, int bucket) {
      this.partition = partition.copy();
      this.bucket = bucket;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof BucketKey)) {
        return false;
      }
      BucketKey that = (BucketKey) other;
      return bucket == that.bucket && partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partition, bucket);
    }
  }
}
