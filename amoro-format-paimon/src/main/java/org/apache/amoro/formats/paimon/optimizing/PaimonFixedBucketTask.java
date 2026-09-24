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

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.AppendOnlyFileStoreTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Exact single-bucket rewrite scope. Commit identity remains solely on PaimonCompactionInput. */
public final class PaimonFixedBucketTask {
  private final String tableId;
  private final String tablePath;
  private final BinaryRow partition;
  private final int bucket;
  private final int totalBuckets;
  private final long snapshotId;
  private final long schemaId;
  private final boolean ordered;
  private final boolean rewriteAll;
  private final OptimizingType optimizingType;
  private final PaimonAppendRewriteOptions.Source rewriteSource;
  private final String rewriteFingerprint;
  private final List<DataFileMeta> files;

  PaimonFixedBucketTask(
      String tableId,
      String tablePath,
      BinaryRow partition,
      int bucket,
      int totalBuckets,
      long snapshotId,
      long schemaId,
      boolean ordered,
      boolean rewriteAll,
      OptimizingType optimizingType,
      PaimonAppendRewriteOptions.Source rewriteSource,
      String rewriteFingerprint,
      List<DataFileMeta> files) {
    this.tableId = requireText(tableId, "tableId");
    this.tablePath = requireText(tablePath, "tablePath");
    this.partition = Objects.requireNonNull(partition, "partition").copy();
    this.bucket = bucket;
    this.totalBuckets = totalBuckets;
    this.snapshotId = snapshotId;
    this.schemaId = schemaId;
    this.ordered = ordered;
    this.rewriteAll = rewriteAll;
    this.optimizingType = Objects.requireNonNull(optimizingType, "optimizingType");
    this.rewriteSource = Objects.requireNonNull(rewriteSource, "rewriteSource");
    this.rewriteFingerprint = requireText(rewriteFingerprint, "rewriteFingerprint");
    if (bucket < 0
        || totalBuckets <= bucket
        || snapshotId <= 0
        || schemaId < 0
        || files == null
        || files.isEmpty()
        || files.size() > PaimonFixedBucketTaskSerializer.MAX_FILES) {
      throw new IllegalArgumentException("Invalid fixed-bucket task scope");
    }
    Set<String> names = new HashSet<>();
    long previousMax = -1;
    for (DataFileMeta file : files) {
      if (file == null
          || file.fileName() == null
          || file.fileName().isEmpty()
          || file.fileName().contains("/")
          || file.fileName().contains("\\")
          || file.fileName().equals("..")
          || !names.add(file.fileName())
          || file.fileSize() < 0
          || file.rowCount() <= 0
          || file.schemaId() < 0
          || file.minSequenceNumber() < 0
          || file.maxSequenceNumber() < file.minSequenceNumber()
          || file.deleteRowCount().orElse(0L) != 0
          || file.firstRowId() != null
          || file.writeCols() != null) {
        throw new IllegalArgumentException("Invalid or duplicate fixed-bucket input file");
      }
      // Source:
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/operation/BucketedAppendFileStoreWrite.java#L75-L85
      // Unordered write-only files may legally overlap sequences; ordered inputs must not.
      if (ordered && file.minSequenceNumber() <= previousMax) {
        throw new IllegalArgumentException("Ordered fixed-bucket inputs overlap or are unsorted");
      }
      previousMax = file.maxSequenceNumber();
    }
    this.files = Collections.unmodifiableList(new ArrayList<>(files));
  }

  public static PaimonFixedBucketTask from(
      PaimonTable wrapper,
      List<ManifestEntry> entries,
      long snapshotId,
      OptimizingType optimizingType,
      PaimonAppendRewriteOptions rewriteOptions) {
    if (!(wrapper.originalTable() instanceof AppendOnlyFileStoreTable)) {
      throw new IllegalArgumentException("Fixed task requires AppendOnlyFileStoreTable");
    }
    AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) wrapper.originalTable();
    PaimonAppendCapabilities.fixedBucketRejection(table)
        .ifPresent(
            reason -> {
              throw new IllegalArgumentException(reason);
            });
    if (entries == null
        || entries.isEmpty()
        || entries.size() > PaimonFixedBucketTaskSerializer.MAX_FILES) {
      throw new IllegalArgumentException("Fixed task requires manifest inputs");
    }
    ManifestEntry first = entries.get(0);
    // Same SHA, paimon-core/src/main/java/org/apache/paimon/manifest/FileEntry.java:49.
    // Keep the manifest bucket identity, never infer a synthetic bucket from a partition path.
    for (ManifestEntry entry : entries) {
      if (entry.kind() != FileKind.ADD
          || !entry.partition().equals(first.partition())
          || entry.bucket() != first.bucket()
          || entry.totalBuckets() != first.totalBuckets()
          || entry.totalBuckets() != table.coreOptions().bucket()) {
        throw new IllegalArgumentException("Mixed or incompatible manifest bucket layout");
      }
    }
    PaimonFixedBucketTask task =
        new PaimonFixedBucketTask(
            wrapper.id().toString(),
            table.location().toString(),
            first.partition(),
            first.bucket(),
            first.totalBuckets(),
            snapshotId,
            table.schema().id(),
            table.coreOptions().bucketAppendOrdered(),
            rewriteOptions.fullRewriteAllFiles(),
            optimizingType,
            rewriteOptions.source(),
            PaimonAppendCapabilities.rewriteFingerprint(table),
            entries.stream().map(ManifestEntry::file).collect(Collectors.toList()));
    task.validateBinding(wrapper);
    return task;
  }

  /** Check both freshly-built inputs and deserialized inputs before any writer is created. */
  public void validateBinding(PaimonTable wrapper) {
    if (wrapper == null || !(wrapper.originalTable() instanceof AppendOnlyFileStoreTable)) {
      throw new IllegalArgumentException("Fixed task requires a bound Append table");
    }
    AppendOnlyFileStoreTable table = (AppendOnlyFileStoreTable) wrapper.originalTable();
    if (PaimonAppendCapabilities.fixedBucketRejection(table).isPresent()
        || !PaimonOptimizingEligibility.isEligible(wrapper.properties())) {
      throw new IllegalArgumentException("Bound fixed task options are not eligible");
    }
    PaimonAppendRewriteOptions rewriteOptions =
        PaimonAppendRewriteOptions.from(wrapper.properties());
    if (!tableId.equals(wrapper.id().toString())
        || !tablePath.equals(table.location().toString())
        || schemaId != table.schema().id()
        || totalBuckets != table.coreOptions().bucket()
        || ordered != table.coreOptions().bucketAppendOrdered()
        || partition.getFieldCount() != table.schema().partitionKeys().size()
        || rewriteAll != rewriteOptions.fullRewriteAllFiles()
        || rewriteSource != rewriteOptions.source()
        || !rewriteFingerprint.equals(PaimonAppendCapabilities.rewriteFingerprint(table))) {
      throw new IllegalArgumentException("Fixed task table identity or execution binding mismatch");
    }
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Missing " + name);
    }
    return value;
  }

  public String tableId() {
    return tableId;
  }

  public String tablePath() {
    return tablePath;
  }

  public BinaryRow partition() {
    return partition.copy();
  }

  public int bucket() {
    return bucket;
  }

  public int totalBuckets() {
    return totalBuckets;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public long schemaId() {
    return schemaId;
  }

  public boolean ordered() {
    return ordered;
  }

  public boolean rewriteAll() {
    return rewriteAll;
  }

  public OptimizingType optimizingType() {
    return optimizingType;
  }

  public PaimonAppendRewriteOptions.Source rewriteSource() {
    return rewriteSource;
  }

  public String rewriteFingerprint() {
    return rewriteFingerprint;
  }

  public List<DataFileMeta> files() {
    return files;
  }

  PaimonFixedBucketTask withFiles(List<DataFileMeta> subset) {
    return new PaimonFixedBucketTask(
        tableId,
        tablePath,
        partition,
        bucket,
        totalBuckets,
        snapshotId,
        schemaId,
        ordered,
        rewriteAll,
        optimizingType,
        rewriteSource,
        rewriteFingerprint,
        subset);
  }
}
