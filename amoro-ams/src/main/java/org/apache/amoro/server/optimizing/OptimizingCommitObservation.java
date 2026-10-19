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

package org.apache.amoro.server.optimizing;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/** Best-effort timings for existing process boundaries, independent of persistence decisions. */
final class OptimizingCommitObservation {
  private OptimizingCommitObservation() {}

  static void record(
      Logger logger,
      Object table,
      long processId,
      Object type,
      Object mode,
      String stage,
      long startNanos,
      String outcome,
      long committingSince,
      boolean completed,
      long entryMillis) {
    try {
      if (!logger.isInfoEnabled()) {
        return;
      }
      long now = System.currentTimeMillis();
      Object age =
          committingSince > 0 && now >= committingSince ? now - committingSince : "unavailable";
      logger.info(
          "OptimizingCommitTiming table={} processId={} optimizingType={} commitMode={} "
              + "stage={} outcome={} elapsedMs={} committing_status_age_ms={} "
              + "committing_end_to_end_ms={}",
          table,
          processId,
          type,
          mode,
          stage,
          outcome,
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
          committingSince > 0 && entryMillis >= committingSince
              ? entryMillis - committingSince
              : "unavailable",
          completed ? age : "unavailable");
    } catch (RuntimeException ignored) {
      // Do not change the process state, SQL exception, or retry decision for a logging failure.
    }
  }
}
