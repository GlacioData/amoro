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

package org.apache.amoro.server.table;

import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Query parameters for table optimizing information. */
public final class TableOptimizingQuery {

  private static final long MILLIS_PER_MINUTE = 60_000L;

  public enum SortField {
    DURATION,
    QUOTA
  }

  public enum SortOrder {
    ASC,
    DESC
  }

  private final @Nullable String optimizerGroup;
  private final @Nullable String fuzzyDbName;
  private final @Nullable String fuzzyTableName;
  private final @Nullable List<Integer> statusCodeFilters;
  private final @Nullable Integer healthScoreMin;
  private final @Nullable Integer healthScoreMax;
  private final @Nullable Long durationMinMinutes;
  private final @Nullable SortField sortField;
  private final @Nullable SortOrder sortOrder;
  private final int limit;
  private final int offset;

  private TableOptimizingQuery(Builder builder) {
    validate(builder);
    this.optimizerGroup = builder.optimizerGroup;
    this.fuzzyDbName = builder.fuzzyDbName;
    this.fuzzyTableName = builder.fuzzyTableName;
    this.statusCodeFilters = immutableCopy(builder.statusCodeFilters);
    this.healthScoreMin = builder.healthScoreMin;
    this.healthScoreMax = builder.healthScoreMax;
    this.durationMinMinutes = builder.durationMinMinutes;
    this.sortField = builder.sortField;
    this.sortOrder = builder.sortField == null ? null : defaultSortOrder(builder.sortOrder);
    this.limit = builder.limit;
    this.offset = builder.offset;
  }

  public static Builder builder(int limit, int offset) {
    return new Builder(limit, offset);
  }

  @Nullable
  public String getOptimizerGroup() {
    return optimizerGroup;
  }

  @Nullable
  public String getFuzzyDbName() {
    return fuzzyDbName;
  }

  @Nullable
  public String getFuzzyTableName() {
    return fuzzyTableName;
  }

  @Nullable
  public List<Integer> getStatusCodeFilters() {
    return statusCodeFilters;
  }

  @Nullable
  public Integer getHealthScoreMin() {
    return healthScoreMin;
  }

  @Nullable
  public Integer getHealthScoreMax() {
    return healthScoreMax;
  }

  @Nullable
  public Long getDurationMinMinutes() {
    return durationMinMinutes;
  }

  public long getDurationMinMillis() {
    Preconditions.checkState(durationMinMinutes != null, "Duration filter is not configured");
    return durationMinMinutes * MILLIS_PER_MINUTE;
  }

  @Nullable
  public SortField getSortField() {
    return sortField;
  }

  @Nullable
  public SortOrder getSortOrder() {
    return sortOrder;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public boolean requiresDerivedProcessing() {
    return healthScoreMin != null
        || healthScoreMax != null
        || durationMinMinutes != null
        || sortField != null;
  }

  private static void validate(Builder builder) {
    Preconditions.checkArgument(builder.limit > 0, "Page size must be greater than zero");
    Preconditions.checkArgument(builder.offset >= 0, "Page offset must not be negative");
    validateHealthScore(builder.healthScoreMin, "Minimum health score");
    validateHealthScore(builder.healthScoreMax, "Maximum health score");
    Preconditions.checkArgument(
        builder.healthScoreMin == null
            || builder.healthScoreMax == null
            || builder.healthScoreMin <= builder.healthScoreMax,
        "Minimum health score must not exceed maximum health score");
    Preconditions.checkArgument(
        builder.durationMinMinutes == null || builder.durationMinMinutes >= 0,
        "Minimum duration must not be negative");
    Preconditions.checkArgument(
        builder.durationMinMinutes == null
            || builder.durationMinMinutes <= Long.MAX_VALUE / MILLIS_PER_MINUTE,
        "Minimum duration is too large");
    Preconditions.checkArgument(
        builder.sortField != null || builder.sortOrder == null, "Sort order requires a sort field");
  }

  private static void validateHealthScore(@Nullable Integer score, String name) {
    Preconditions.checkArgument(
        score == null || (score >= 0 && score <= 100), "%s must be between 0 and 100", name);
  }

  private static SortOrder defaultSortOrder(@Nullable SortOrder order) {
    return order == null ? SortOrder.DESC : order;
  }

  @Nullable
  private static List<Integer> immutableCopy(@Nullable List<Integer> values) {
    return values == null ? null : Collections.unmodifiableList(new ArrayList<>(values));
  }

  public static final class Builder {
    private final int limit;
    private final int offset;
    private @Nullable String optimizerGroup;
    private @Nullable String fuzzyDbName;
    private @Nullable String fuzzyTableName;
    private @Nullable List<Integer> statusCodeFilters;
    private @Nullable Integer healthScoreMin;
    private @Nullable Integer healthScoreMax;
    private @Nullable Long durationMinMinutes;
    private @Nullable SortField sortField;
    private @Nullable SortOrder sortOrder;

    private Builder(int limit, int offset) {
      this.limit = limit;
      this.offset = offset;
    }

    public Builder optimizerGroup(@Nullable String optimizerGroup) {
      this.optimizerGroup = optimizerGroup;
      return this;
    }

    public Builder fuzzyDbName(@Nullable String fuzzyDbName) {
      this.fuzzyDbName = fuzzyDbName;
      return this;
    }

    public Builder fuzzyTableName(@Nullable String fuzzyTableName) {
      this.fuzzyTableName = fuzzyTableName;
      return this;
    }

    public Builder statusCodeFilters(@Nullable List<Integer> statusCodeFilters) {
      this.statusCodeFilters = statusCodeFilters;
      return this;
    }

    public Builder healthScoreRange(
        @Nullable Integer healthScoreMin, @Nullable Integer healthScoreMax) {
      this.healthScoreMin = healthScoreMin;
      this.healthScoreMax = healthScoreMax;
      return this;
    }

    public Builder durationMinMinutes(@Nullable Long durationMinMinutes) {
      this.durationMinMinutes = durationMinMinutes;
      return this;
    }

    public Builder sort(@Nullable SortField sortField, @Nullable SortOrder sortOrder) {
      this.sortField = sortField;
      this.sortOrder = sortOrder;
      return this;
    }

    public TableOptimizingQuery build() {
      return new TableOptimizingQuery(this);
    }
  }
}
