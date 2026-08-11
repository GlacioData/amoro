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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable score component in a format-agnostic table health explanation. */
public final class TableHealthComponent {

  private final String code;
  private final int score;
  private final Integer weight;
  private final String combination;
  private final Map<String, String> metrics;

  @JsonCreator
  public TableHealthComponent(
      @JsonProperty("code") String code,
      @JsonProperty("score") int score,
      @JsonProperty("weight") Integer weight,
      @JsonProperty("combination") String combination,
      @JsonProperty("metrics") Map<String, String> metrics) {
    if (score < -1 || score > 100) {
      throw new IllegalArgumentException("Health component score must be -1 or between 0 and 100");
    }
    if (weight != null && (weight < 0 || weight > 100)) {
      throw new IllegalArgumentException("Health component weight must be between 0 and 100");
    }
    this.code = requireNonEmpty(code, "Health component code must not be empty");
    this.score = score;
    this.weight = weight;
    this.combination = combination;
    this.metrics = immutableMap(metrics);
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
    if (!(other instanceof TableHealthComponent)) {
      return false;
    }
    TableHealthComponent that = (TableHealthComponent) other;
    return score == that.score
        && code.equals(that.code)
        && Objects.equals(weight, that.weight)
        && Objects.equals(combination, that.combination)
        && metrics.equals(that.metrics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(code, score, weight, combination, metrics);
  }
}
