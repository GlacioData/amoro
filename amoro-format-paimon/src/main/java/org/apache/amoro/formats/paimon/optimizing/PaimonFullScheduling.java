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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonPrimaryKeyOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;

/**
 * Refresh wakeups and scheduling admission for Paimon primary-key FULL compaction; the format
 * planner still selects files.
 */
public final class PaimonFullScheduling {
  private PaimonFullScheduling() {}

  public static boolean isPrimaryKeyTable(AmoroTable<?> table) {
    return table instanceof PaimonTable
        && table.originalTable() instanceof PrimaryKeyFileStoreTable;
  }

  public static boolean shouldEvaluate(
      OptimizationContext runtime, AmoroTable<?> table, boolean pending, long now) {
    if (!isPrimaryKeyTable(table)) {
      return false;
    }
    // Existing demand must remain revocable after optimization is disabled.
    if (pending) {
      return true;
    }
    return runtime.isIdle()
        && PaimonPrimaryKeyOptimizingPlanner.supports((PaimonTable) table)
        && isDue(runtime, now);
  }

  static boolean isDue(OptimizationContext runtime, long now) {
    OptimizingConfig config = runtime.getOptimizingConfig();
    long interval = config.getFullTriggerInterval();
    long lastFull = runtime.getLastFullOptimizingTime();
    return config.isEnabled()
        && PaimonOptimizingEligibility.isEligible(runtime.getTableConfig())
        && interval > 0L
        && now >= lastFull
        && now - lastFull >= interval
        && now >= runtime.getFullRetryNotBefore();
  }
}
