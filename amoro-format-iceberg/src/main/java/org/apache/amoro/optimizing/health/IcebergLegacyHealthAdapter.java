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

package org.apache.amoro.optimizing.health;

import org.apache.amoro.TableFormat;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.table.KeyedTableSnapshot;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableSnapshot;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthComponent;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.utils.DigestUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

/** Adapts the existing Iceberg 40/40/20 score to the common health result without rescanning. */
public final class IcebergLegacyHealthAdapter {

  public static final String FORMULA_VERSION = "iceberg-legacy-v1";

  private IcebergLegacyHealthAdapter() {}

  public static TableAnalysisKey createKey(
      TableIdentifier identifier,
      TableFormat tableFormat,
      TableSnapshot snapshot,
      long schemaId,
      OptimizingConfig config) {
    Objects.requireNonNull(identifier, "Table identifier must not be null");
    Objects.requireNonNull(tableFormat, "Table format must not be null");
    Objects.requireNonNull(snapshot, "Table snapshot must not be null");
    Objects.requireNonNull(config, "Optimizing config must not be null");

    long changeSnapshotId = TableAnalysisKey.NO_CHANGE_SNAPSHOT;
    if (snapshot instanceof KeyedTableSnapshot) {
      changeSnapshotId = ((KeyedTableSnapshot) snapshot).changeSnapshotId();
    }
    return new TableAnalysisKey(
        identifier.toString(),
        tableFormat,
        snapshot.snapshotId(),
        changeSnapshotId,
        schemaId,
        scoringConfigFingerprint(config),
        FORMULA_VERSION,
        TableAnalysisKey.NO_BASELINE,
        TableAnalysisKey.NO_BASELINE_TIME);
  }

  public static FormatTableAnalysis adapt(
      TableAnalysisKey key, AbstractOptimizingEvaluator.PendingInput pendingInput) {
    Objects.requireNonNull(key, "Table analysis key must not be null");
    Objects.requireNonNull(pendingInput, "Pending input must not be null");

    TableHealthDetails details =
        new TableHealthDetails(
            FORMULA_VERSION,
            nullableId(key.getSnapshotId()),
            nullableId(key.getChangeSnapshotId()),
            nullableId(key.getSchemaId()),
            key.getScoringConfigFingerprint(),
            key.encoded(),
            Arrays.asList(
                component("SMALL_FILE", pendingInput.getSmallFileScore(), 40),
                component("EQUALITY_DELETE", pendingInput.getEqualityDeleteScore(), 40),
                component("POSITIONAL_DELETE", pendingInput.getPositionalDeleteScore(), 20)),
            Collections.emptyMap(),
            Collections.emptyList());
    return new LegacyAnalysis(key, pendingInput, details);
  }

  private static TableHealthComponent component(String code, int score, int weight) {
    return new TableHealthComponent(code, score, weight, "SUM", Collections.emptyMap());
  }

  private static Long nullableId(long id) {
    return id < 0 ? null : id;
  }

  private static String scoringConfigFingerprint(OptimizingConfig config) {
    String canonical =
        "targetSize="
            + config.getTargetSize()
            + "\nfragmentRatio="
            + config.getFragmentRatio()
            + "\nminTargetSizeRatio="
            + Double.toString(config.getMinTargetSizeRatio())
            + "\nminorLeastFileCount="
            + config.getMinorLeastFileCount()
            + "\nmajorDuplicateRatio="
            + Double.toString(config.getMajorDuplicateRatio())
            + "\nformulaVersion="
            + FORMULA_VERSION;
    return DigestUtil.sha256Hex(canonical);
  }

  private static final class LegacyAnalysis implements FormatTableAnalysis {

    private final TableAnalysisKey key;
    private final AbstractOptimizingEvaluator.PendingInput pendingInput;
    private final TableHealthDetails healthDetails;

    private LegacyAnalysis(
        TableAnalysisKey key,
        AbstractOptimizingEvaluator.PendingInput pendingInput,
        TableHealthDetails healthDetails) {
      this.key = key;
      this.pendingInput = pendingInput;
      this.healthDetails = healthDetails;
    }

    @Override
    public TableAnalysisKey key() {
      return key;
    }

    @Override
    public AbstractOptimizingEvaluator.PendingInput pendingInput() {
      return pendingInput;
    }

    @Override
    public TableHealthDetails healthDetails() {
      return healthDetails;
    }
  }
}
