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

package org.apache.amoro.server.persistence.converter;

import org.apache.amoro.optimizing.MetricsSummary;
import org.apache.amoro.optimizing.TaskMetricsSummary;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

/** Reads format-specific history summaries without applying execution recovery admission rules. */
public class OptimizingTaskSummaryConverter extends JsonObjectConverter<MetricsSummary> {
  public OptimizingTaskSummaryConverter() {
    super(MetricsSummary.class);
  }

  @Override
  public MetricsSummary getNullableResult(ResultSet rs, String column) throws SQLException {
    return adapt(summaryConverter(rs).getNullableResult(rs, column));
  }

  @Override
  public MetricsSummary getNullableResult(ResultSet rs, int column) throws SQLException {
    return adapt(summaryConverter(rs).getNullableResult(rs, column));
  }

  @Override
  public MetricsSummary getNullableResult(CallableStatement cs, int column) {
    throw new UnsupportedOperationException("Task history requires ResultSet properties");
  }

  private JsonObjectConverter<? extends TaskMetricsSummary> summaryConverter(ResultSet rs)
      throws SQLException {
    return new JsonObjectConverter<>(
        TaskDescriptorRecoveryTypes.historySummaryClass(
            TaskDescriptorRecoveryTypes.readFactoryImpl(rs)));
  }

  static MetricsSummary adapt(TaskMetricsSummary summary) {
    if (summary == null || summary instanceof MetricsSummary) {
      return (MetricsSummary) summary;
    }
    return MetricsSummary.aggregate(Collections.singletonList(summary));
  }
}
