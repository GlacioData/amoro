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

package org.apache.amoro.table.descriptor;

import org.apache.amoro.table.health.TableHealthComponent;
import org.apache.amoro.table.health.TableHealthDetails;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** REST-safe projection of table health details. Long identifiers are rendered as strings. */
public final class TableHealthDetailsView {

  private final String formulaVersion;
  private final String snapshotId;
  private final String changeSnapshotId;
  private final String schemaId;
  private final String scoringConfigFingerprint;
  private final String evaluationKey;
  private final List<Component> components;
  private final Map<String, String> metrics;
  private final List<String> reasonCodes;

  private TableHealthDetailsView(TableHealthDetails details) {
    this.formulaVersion = details.getFormulaVersion();
    this.snapshotId = decimalString(details.getSnapshotId());
    this.changeSnapshotId = decimalString(details.getChangeSnapshotId());
    this.schemaId = decimalString(details.getSchemaId());
    this.scoringConfigFingerprint = details.getScoringConfigFingerprint();
    this.evaluationKey = details.getEvaluationKey();
    ArrayList<Component> componentViews = new ArrayList<>(details.getComponents().size());
    for (TableHealthComponent component : details.getComponents()) {
      componentViews.add(new Component(component));
    }
    this.components = Collections.unmodifiableList(componentViews);
    this.metrics = immutableMap(details.getMetrics());
    this.reasonCodes = Collections.unmodifiableList(new ArrayList<>(details.getReasonCodes()));
  }

  public static TableHealthDetailsView from(TableHealthDetails details) {
    return details == null ? null : new TableHealthDetailsView(details);
  }

  public String getFormulaVersion() {
    return formulaVersion;
  }

  public String getSnapshotId() {
    return snapshotId;
  }

  public String getChangeSnapshotId() {
    return changeSnapshotId;
  }

  public String getSchemaId() {
    return schemaId;
  }

  public String getScoringConfigFingerprint() {
    return scoringConfigFingerprint;
  }

  public String getEvaluationKey() {
    return evaluationKey;
  }

  public List<Component> getComponents() {
    return components;
  }

  public Map<String, String> getMetrics() {
    return metrics;
  }

  public List<String> getReasonCodes() {
    return reasonCodes;
  }

  private static String decimalString(Long value) {
    return value == null ? null : Long.toString(value);
  }

  private static Map<String, String> immutableMap(Map<String, String> source) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(source));
  }

  /** REST-safe projection of one health component. */
  public static final class Component {
    private final String code;
    private final int score;
    private final Integer weight;
    private final String combination;
    private final Map<String, String> metrics;

    private Component(TableHealthComponent component) {
      this.code = component.getCode();
      this.score = component.getScore();
      this.weight = component.getWeight();
      this.combination = component.getCombination();
      this.metrics = immutableMap(component.getMetrics());
    }

    public String getCode() {
      return code;
    }

    public int getScore() {
      return score;
    }

    public Integer getWeight() {
      return weight;
    }

    public String getCombination() {
      return combination;
    }

    public Map<String, String> getMetrics() {
      return metrics;
    }
  }
}
