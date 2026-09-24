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

package org.apache.amoro.formats.paimon.optimizing.primary;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;

import java.util.Map;

/** Scope and timing of task-based GENTLE lookup compaction managed by Amoro. */
public final class PaimonManagedGentle {
  private PaimonManagedGentle() {}

  public static boolean isAffected(FileStoreTable table) {
    return table instanceof PrimaryKeyFileStoreTable
        && !table.primaryKeys().isEmpty()
        && (table.bucketMode() == BucketMode.HASH_FIXED
            || table.bucketMode() == BucketMode.HASH_DYNAMIC)
        && isAffected(table.options());
  }

  // Callers selecting files must also verify the table shape. Persisted MINOR demand is only
  // created after that check; this options check revalidates its current scheduling eligibility.
  static boolean isAffected(Map<String, String> rawOptions) {
    if (!PaimonOptimizingEligibility.isEligible(rawOptions)) {
      return false;
    }
    CoreOptions options = CoreOptions.fromMap(rawOptions);
    return !options.isChainTable()
        && !options.pkClusteringOverride()
        && (options.bucket() == -1 || options.bucket() > 0)
        && options.needLookup()
        && options.lookupCompact() == CoreOptions.LookupCompactMode.GENTLE;
  }

  public static boolean canSchedule(OptimizationContext context, long now) {
    OptimizingConfig config = context.getOptimizingConfig();
    if (config == null || !config.isEnabled()) {
      return false;
    }
    try {
      return isAffected(context.getTableConfig())
          && config.getMinorLeastInterval() >= 0
          && now >= context.getLastMinorOptimizingTime()
          && now - context.getLastMinorOptimizingTime() > config.getMinorLeastInterval();
    } catch (RuntimeException invalidOptions) {
      // Invalid or changed options must never revive previously confirmed demand.
      return false;
    }
  }

  static long nextEvaluationTime(long lastMinorTime, long interval) {
    if (interval < 0) {
      return 0L;
    }
    // Preserve the evaluator's strict elapsed > interval boundary without wrapping on overflow.
    if (lastMinorTime > Long.MAX_VALUE - interval - 1L) {
      return Long.MAX_VALUE;
    }
    return Math.max(1L, lastMinorTime + interval + 1L);
  }
}
