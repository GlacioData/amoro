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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableRuntimeStore;
import org.apache.amoro.table.descriptor.TableHealthComparisonView;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Optional format-owned health semantics; the default preserves legacy summaries. */
public interface TableHealthSupport {
  TableHealthSupport NONE = new TableHealthSupport() {};

  /**
   * Extract only complete, already collected aggregates. Never perform IO or traverse file facts.
   */
  default Optional<TableFileStatisticsSnapshot> extractFileStatistics(TableHealthDetails details) {
    return Optional.empty();
  }

  default boolean enabled() {
    return false;
  }

  default List<StateKey<?>> requiredStates() {
    return Collections.emptyList();
  }

  default boolean sameInput(TableAnalysisKey key, TableHealthDetails details) {
    return key.encoded().equals(details.getEvaluationKey());
  }

  default boolean retryable(TableHealthDetails details) {
    return false;
  }

  default String readFailureReason() {
    return "HEALTH_READ_FAILED";
  }

  /** Capture only existing plan facts; this hook must not scan table files. */
  default void capture(
      TableRuntimeStore store,
      AmoroTable<?> table,
      OptimizingPlanResult<?> plan,
      FormatTableAnalysis analysis) {}

  /** Observe a successful process without changing its lifecycle or commit result. */
  default void observeSuccess(
      TableRuntimeStore store, long processId, String processType, long completedAtMillis) {}

  /** Perform bounded historical work under the server's shared scan budget. */
  default void refreshComparison(
      TableRuntimeStore store,
      AmoroTable<?> table,
      TableHealthDetails currentDetails,
      int currentScore) {}

  /** Project persisted facts only; dashboard reads must not perform native table IO. */
  default TableHealthComparisonView comparisonView(TableRuntimeStore store) {
    return null;
  }
}
