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

import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable format-agnostic explanation of a health score for one analysis key. */
public final class TableHealthDetails {

  private final String formulaVersion;
  private final Long snapshotId;
  private final Long changeSnapshotId;
  private final Long schemaId;
  private final String scoringConfigFingerprint;
  private final String evaluationKey;
  private final List<TableHealthComponent> components;
  private final Map<String, String> metrics;
  private final List<String> reasonCodes;

  @JsonCreator
  public TableHealthDetails(
      @JsonProperty("formulaVersion") String formulaVersion,
      @JsonProperty("snapshotId") Long snapshotId,
      @JsonProperty("changeSnapshotId") Long changeSnapshotId,
      @JsonProperty("schemaId") Long schemaId,
      @JsonProperty("scoringConfigFingerprint") String scoringConfigFingerprint,
      @JsonProperty("evaluationKey") String evaluationKey,
      @JsonProperty("components") List<TableHealthComponent> components,
      @JsonProperty("metrics") Map<String, String> metrics,
      @JsonProperty("reasonCodes") List<String> reasonCodes) {
    this.formulaVersion = requireNonEmpty(formulaVersion, "Formula version must not be empty");
    this.snapshotId = snapshotId;
    this.changeSnapshotId = changeSnapshotId;
    this.schemaId = schemaId;
    this.scoringConfigFingerprint =
        Objects.requireNonNull(
            scoringConfigFingerprint, "Scoring config fingerprint must not be null");
    this.evaluationKey = requireNonEmpty(evaluationKey, "Evaluation key must not be empty");
    this.components = immutableList(components, "Health component must not be null");
    this.metrics = immutableMap(metrics);
    this.reasonCodes = immutableList(reasonCodes, "Reason code must not be null");
  }

  public String getFormulaVersion() {
    return formulaVersion;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  public Long getChangeSnapshotId() {
    return changeSnapshotId;
  }

  public Long getSchemaId() {
    return schemaId;
  }

  public String getScoringConfigFingerprint() {
    return scoringConfigFingerprint;
  }

  public String getEvaluationKey() {
    return evaluationKey;
  }

  public List<TableHealthComponent> getComponents() {
    return components;
  }

  public Map<String, String> getMetrics() {
    return metrics;
  }

  public List<String> getReasonCodes() {
    return reasonCodes;
  }

  private static <T> List<T> immutableList(List<T> source, String nullElementMessage) {
    if (source == null || source.isEmpty()) {
      return Collections.emptyList();
    }
    ArrayList<T> copy = new ArrayList<>(source.size());
    for (T value : source) {
      copy.add(Objects.requireNonNull(value, nullElementMessage));
    }
    return Collections.unmodifiableList(copy);
  }

  private static Map<String, String> immutableMap(Map<String, String> source) {
    if (source == null || source.isEmpty()) {
      return Collections.emptyMap();
    }
    LinkedHashMap<String, String> copy = new LinkedHashMap<>();
    source.forEach(
        (key, value) ->
            copy.put(
                Objects.requireNonNull(key, "Metric code must not be null"),
                Objects.requireNonNull(value, "Metric value must not be null")));
    return Collections.unmodifiableMap(copy);
  }

  private static String requireNonEmpty(String value, String message) {
    Objects.requireNonNull(value, message);
    if (value.isEmpty()) {
      throw new IllegalArgumentException(message);
    }
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof TableHealthDetails)) {
      return false;
    }
    TableHealthDetails that = (TableHealthDetails) other;
    return formulaVersion.equals(that.formulaVersion)
        && Objects.equals(snapshotId, that.snapshotId)
        && Objects.equals(changeSnapshotId, that.changeSnapshotId)
        && Objects.equals(schemaId, that.schemaId)
        && scoringConfigFingerprint.equals(that.scoringConfigFingerprint)
        && evaluationKey.equals(that.evaluationKey)
        && components.equals(that.components)
        && metrics.equals(that.metrics)
        && reasonCodes.equals(that.reasonCodes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        formulaVersion,
        snapshotId,
        changeSnapshotId,
        schemaId,
        scoringConfigFingerprint,
        evaluationKey,
        components,
        metrics,
        reasonCodes);
  }
}
