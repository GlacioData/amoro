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

package org.apache.amoro.table.health;

import org.apache.amoro.TableFormat;

import java.util.Objects;

/** Immutable format-agnostic identity of all inputs that affect a table health evaluation. */
public final class TableAnalysisKey {

  public static final long NO_SNAPSHOT = -1L;
  public static final long NO_CHANGE_SNAPSHOT = -1L;
  public static final long NO_SCHEMA = -1L;
  public static final long NO_BASELINE = -1L;
  public static final long NO_BASELINE_TIME = 0L;

  private static final String ENCODING_VERSION = "1";

  private final String tableId;
  private final TableFormat tableFormat;
  private final long snapshotId;
  private final long changeSnapshotId;
  private final long schemaId;
  private final String scoringConfigFingerprint;
  private final String formulaVersion;
  private final long successfulOptimizationBaselineId;
  private final long successfulOptimizationBaselineTimeMillis;

  public TableAnalysisKey(
      String tableId,
      TableFormat tableFormat,
      long snapshotId,
      long changeSnapshotId,
      long schemaId,
      String scoringConfigFingerprint,
      String formulaVersion,
      long successfulOptimizationBaselineId,
      long successfulOptimizationBaselineTimeMillis) {
    this.tableId = Objects.requireNonNull(tableId, "Table id must not be null");
    this.tableFormat = Objects.requireNonNull(tableFormat, "Table format must not be null");
    this.snapshotId = snapshotId;
    this.changeSnapshotId = changeSnapshotId;
    this.schemaId = schemaId;
    this.scoringConfigFingerprint =
        Objects.requireNonNull(
            scoringConfigFingerprint, "Scoring config fingerprint must not be null");
    this.formulaVersion =
        Objects.requireNonNull(formulaVersion, "Formula version must not be null");
    this.successfulOptimizationBaselineId = successfulOptimizationBaselineId;
    this.successfulOptimizationBaselineTimeMillis = successfulOptimizationBaselineTimeMillis;
  }

  public TableAnalysisKey(TableAnalysisKey key) {
    this(
        key.tableId,
        key.tableFormat,
        key.snapshotId,
        key.changeSnapshotId,
        key.schemaId,
        key.scoringConfigFingerprint,
        key.formulaVersion,
        key.successfulOptimizationBaselineId,
        key.successfulOptimizationBaselineTimeMillis);
  }

  public String getTableId() {
    return tableId;
  }

  public TableFormat getTableFormat() {
    return tableFormat;
  }

  public long getSnapshotId() {
    return snapshotId;
  }

  public long getChangeSnapshotId() {
    return changeSnapshotId;
  }

  public long getSchemaId() {
    return schemaId;
  }

  public String getScoringConfigFingerprint() {
    return scoringConfigFingerprint;
  }

  public String getFormulaVersion() {
    return formulaVersion;
  }

  public long getSuccessfulOptimizationBaselineId() {
    return successfulOptimizationBaselineId;
  }

  public long getSuccessfulOptimizationBaselineTimeMillis() {
    return successfulOptimizationBaselineTimeMillis;
  }

  /** Returns a deterministic length-prefixed encoding with no field-concatenation ambiguity. */
  public String encoded() {
    StringBuilder builder = new StringBuilder();
    append(builder, ENCODING_VERSION);
    append(builder, tableId);
    append(builder, tableFormat.name());
    append(builder, Long.toString(snapshotId));
    append(builder, Long.toString(changeSnapshotId));
    append(builder, Long.toString(schemaId));
    append(builder, scoringConfigFingerprint);
    append(builder, formulaVersion);
    append(builder, Long.toString(successfulOptimizationBaselineId));
    append(builder, Long.toString(successfulOptimizationBaselineTimeMillis));
    return builder.toString();
  }

  private static void append(StringBuilder builder, String value) {
    builder.append(value.length()).append(':').append(value);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TableAnalysisKey)) {
      return false;
    }
    TableAnalysisKey that = (TableAnalysisKey) other;
    return snapshotId == that.snapshotId
        && changeSnapshotId == that.changeSnapshotId
        && schemaId == that.schemaId
        && successfulOptimizationBaselineId == that.successfulOptimizationBaselineId
        && successfulOptimizationBaselineTimeMillis == that.successfulOptimizationBaselineTimeMillis
        && tableId.equals(that.tableId)
        && tableFormat.equals(that.tableFormat)
        && scoringConfigFingerprint.equals(that.scoringConfigFingerprint)
        && formulaVersion.equals(that.formulaVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        tableId,
        tableFormat,
        snapshotId,
        changeSnapshotId,
        schemaId,
        scoringConfigFingerprint,
        formulaVersion,
        successfulOptimizationBaselineId,
        successfulOptimizationBaselineTimeMillis);
  }

  @Override
  public String toString() {
    return encoded();
  }
}
