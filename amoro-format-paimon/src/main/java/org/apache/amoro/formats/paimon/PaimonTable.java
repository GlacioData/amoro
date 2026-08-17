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

package org.apache.amoro.formats.paimon;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableSnapshot;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonAppendFileScanner;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluation;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyOptimizingEvaluator;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyPendingInput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeySnapshotAnalysis;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.shade.guava32.com.google.common.collect.ImmutableMap;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableMetaStore;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.utils.CatalogUtil;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

public class PaimonTable implements AmoroTable<Table>, Serializable {

  private static final long serialVersionUID = 1L;
  private static final String UNSUPPORTED_TABLE_SHAPE = "UNSUPPORTED_TABLE_SHAPE";
  private static final String UNSUPPORTED_BUCKET_MODE = "UNSUPPORTED_BUCKET_MODE";
  private static final String PK_CLUSTERING_OVERRIDE_UNSUPPORTED =
      "PK_CLUSTERING_OVERRIDE_UNSUPPORTED";
  private static final int NOT_EQUIVALENT = -1;

  private final TableIdentifier tableIdentifier;

  private final Table table;

  private final Map<String, String> catalogProperties;

  private transient TableMetaStore tableMetaStore;

  public PaimonTable(TableIdentifier tableIdentifier, Table table) {
    this(tableIdentifier, table, ImmutableMap.of());
  }

  public PaimonTable(
      TableIdentifier tableIdentifier, Table table, Map<String, String> catalogProperties) {
    this(tableIdentifier, table, catalogProperties, null);
  }

  public PaimonTable(
      TableIdentifier tableIdentifier,
      Table table,
      Map<String, String> catalogProperties,
      TableMetaStore tableMetaStore) {
    this.tableIdentifier = tableIdentifier;
    this.table = table;
    this.catalogProperties =
        catalogProperties == null ? ImmutableMap.of() : ImmutableMap.copyOf(catalogProperties);
    this.tableMetaStore = tableMetaStore;
  }

  @Override
  public TableIdentifier id() {
    return tableIdentifier;
  }

  @Override
  public TableFormat format() {
    return TableFormat.PAIMON;
  }

  @Override
  public Map<String, String> properties() {
    Map<String, String> tableOptions = table.options();
    Map<String, String> properties =
        CatalogUtil.mergeCatalogPropertiesToTable(tableOptions, catalogProperties);
    retainRawTableOption(properties, tableOptions, CoreOptions.WRITE_ONLY.key());
    retainRawTableOption(
        properties, tableOptions, PaimonOptimizingEligibility.SELF_OPTIMIZING_ENABLED);
    return properties;
  }

  @Override
  public Table originalTable() {
    return table;
  }

  @Override
  public TableSnapshot currentSnapshot() {
    return doAs(
        () -> {
          if (!(table instanceof DataTable)) {
            return null;
          }

          Snapshot snapshot = ((DataTable) table).snapshotManager().latestSnapshot();
          return snapshot == null ? null : new PaimonSnapshot(snapshot);
        });
  }

  @Override
  public long snapshotCount() {
    return doAs(
        () ->
            table instanceof DataTable
                ? ((DataTable) table).snapshotManager().snapshotCount()
                : 0L);
  }

  @Override
  public Optional<PendingInputResult> evaluatePendingInput(
      OptimizationContext context, int maxPendingPartitions) {
    return doAs(
        () -> {
          if (!(table instanceof FileStoreTable)) {
            return Optional.empty();
          }
          FileStoreTable fileStoreTable = (FileStoreTable) table;
          Map<String, String> tableProperties = properties();
          if (!PaimonOptimizingEligibility.isEligible(tableProperties)) {
            return Optional.empty();
          }
          PaimonHealthEvaluationContext healthContext =
              PaimonHealthEvaluationContext.capture(
                  fileStoreTable, id().toString(), context, tableProperties);
          if (fileStoreTable instanceof PrimaryKeyFileStoreTable) {
            if (healthContext.pkClusteringOverride()) {
              return Optional.of(
                  unsupportedResult(healthContext, PK_CLUSTERING_OVERRIDE_UNSUPPORTED));
            }
            if (!isSupportedPrimaryKeyMode(healthContext.bucketMode())) {
              return Optional.of(unsupportedResult(healthContext, UNSUPPORTED_BUCKET_MODE));
            }
            return Optional.of(
                evaluatePrimaryKey(fileStoreTable, healthContext, context, tableProperties));
          }
          if (fileStoreTable instanceof AppendOnlyFileStoreTable) {
            return Optional.of(
                evaluateAppend(
                    (AppendOnlyFileStoreTable) fileStoreTable,
                    healthContext,
                    context,
                    tableProperties));
          }
          return Optional.of(unsupportedResult(healthContext, UNSUPPORTED_TABLE_SHAPE));
        });
  }

  @Override
  public Optional<TableAnalysisKey> currentAnalysisKey(OptimizationContext context) {
    return doAs(
        () -> {
          if (!(table instanceof FileStoreTable)) {
            return Optional.empty();
          }
          return Optional.of(
              PaimonHealthEvaluationContext.capture(
                      (FileStoreTable) table, id().toString(), context, properties())
                  .key());
        });
  }

  public <T> T doAs(Callable<T> callable) {
    if (tableMetaStore == null) {
      return call(callable);
    }
    return tableMetaStore.doAs(callable);
  }

  private <T> T call(Callable<T> callable) {
    try {
      return callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Run with Paimon table authentication context failed.", e);
    }
  }

  private boolean isOptimizingEligible(
      Map<String, String> tableProperties, OptimizationContext context) {
    OptimizingConfig config = context == null ? null : context.getOptimizingConfig();
    return PaimonOptimizingEligibility.isEligible(tableProperties)
        && (config == null || config.isEnabled());
  }

  private static void retainRawTableOption(
      Map<String, String> mergedOptions, Map<String, String> tableOptions, String key) {
    mergedOptions.remove(key);
    if (tableOptions.containsKey(key)) {
      mergedOptions.put(key, tableOptions.get(key));
    }
  }

  private PendingInputResult evaluateAppend(
      AppendOnlyFileStoreTable appendTable,
      PaimonHealthEvaluationContext healthContext,
      OptimizationContext context,
      Map<String, String> tableProperties) {
    PaimonAppendSnapshotAnalysis analysis =
        PaimonAppendFileScanner.analyze(appendTable, healthContext, null);
    boolean optimizingNecessary =
        isOptimizingEligible(tableProperties, context)
            && appendTable.bucketMode() == BucketMode.BUCKET_UNAWARE;
    return new PendingInputResult(
        analysis.pendingInput(), analysis.pendingInput(), optimizingNecessary, analysis);
  }

  private PendingInputResult evaluatePrimaryKey(
      FileStoreTable fileStoreTable,
      PaimonHealthEvaluationContext healthContext,
      OptimizationContext context,
      Map<String, String> tableProperties) {
    PaimonPrimaryKeyOptimizingEvaluation evaluation =
        PaimonPrimaryKeyOptimizingEvaluator.evaluate(
            fileStoreTable,
            id().toString(),
            healthContext,
            context == null ? null : context.getOptimizingConfig(),
            context == null ? 0L : context.getLastMinorOptimizingTime(),
            context == null ? 0L : context.getLastFullOptimizingTime(),
            null,
            System.currentTimeMillis());
    PaimonPrimaryKeySnapshotAnalysis analysis =
        evaluation
            .analysis()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Supported Paimon primary-key evaluation did not return analysis."));
    boolean hashMode =
        healthContext.bucketMode() == BucketMode.HASH_FIXED
            || healthContext.bucketMode() == BucketMode.HASH_DYNAMIC;
    boolean optimizingNecessary =
        isOptimizingEligible(tableProperties, context)
            && hashMode
            && analysis.validForPlanning()
            && evaluation.necessary();
    return new PendingInputResult(
        analysis.pendingInput(),
        legacyPendingInput(analysis.pendingInput()),
        optimizingNecessary,
        analysis);
  }

  private static PaimonPendingInput legacyPendingInput(PaimonPrimaryKeyPendingInput source) {
    return new PaimonPendingInput(
        source.getDataFileCount(),
        source.getDataFileSize(),
        source.getDataRecordCount(),
        source.getSmallFileCount(),
        source.getSmallFileSize(),
        NOT_EQUIVALENT,
        NOT_EQUIVALENT,
        NOT_EQUIVALENT,
        source.getHealthScore());
  }

  private static boolean isSupportedPrimaryKeyMode(BucketMode mode) {
    return mode == BucketMode.HASH_FIXED
        || mode == BucketMode.HASH_DYNAMIC
        || mode == BucketMode.KEY_DYNAMIC;
  }

  private static PendingInputResult unsupportedResult(
      PaimonHealthEvaluationContext healthContext, String reasonCode) {
    PaimonPendingInput pendingInput = new PaimonPendingInput();
    FormatTableAnalysis analysis =
        new UnsupportedPaimonAnalysis(healthContext, pendingInput, reasonCode);
    return new PendingInputResult(pendingInput, pendingInput, false, analysis);
  }

  private static final class UnsupportedPaimonAnalysis implements FormatTableAnalysis {

    private final TableAnalysisKey key;
    private final PaimonPendingInput pendingInput;
    private final TableHealthDetails healthDetails;

    private UnsupportedPaimonAnalysis(
        PaimonHealthEvaluationContext context, PaimonPendingInput pendingInput, String reasonCode) {
      this.key = context.key();
      this.pendingInput = pendingInput;
      Map<String, String> metrics = new LinkedHashMap<>();
      metrics.put("tableShape", context.tableShape().name());
      metrics.put("bucketMode", context.bucketMode().name());
      this.healthDetails =
          new TableHealthDetails(
              context.formulaVersion(),
              context.snapshotId() < 0 ? null : context.snapshotId(),
              null,
              context.schemaId() < 0 ? null : context.schemaId(),
              context.scoringConfigFingerprint(),
              context.key().encoded(),
              Collections.emptyList(),
              metrics,
              Collections.singletonList(reasonCode));
    }

    @Override
    public TableAnalysisKey key() {
      return key;
    }

    @Override
    public PaimonPendingInput pendingInput() {
      return pendingInput;
    }

    @Override
    public TableHealthDetails healthDetails() {
      return healthDetails;
    }
  }
}
