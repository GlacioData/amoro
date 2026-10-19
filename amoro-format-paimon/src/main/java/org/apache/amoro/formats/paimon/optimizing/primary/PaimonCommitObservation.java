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

import org.apache.paimon.table.FileStoreTable;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Per-invocation observations. Never read storage or deserialize output for logging. */
final class PaimonCommitObservation {
  private final Logger logger;
  private final String mode;
  private String table = "unavailable";
  private String bucketMode = "unavailable";
  private final int tasks;
  private final Map<ByteBuffer, Set<Integer>> units = new HashMap<>();
  private int unitCount;
  private long messageBytes;
  String user = "unavailable";
  Object identifier = "unavailable";
  Object target = "unavailable";
  Object type = "unavailable";
  boolean inputsComplete;
  Object messageCount = "unavailable";
  String outcome = "failed";

  PaimonCommitObservation(Logger logger, FileStoreTable table, String mode, int tasks) {
    this.logger = logger;
    this.mode = mode;
    this.tasks = tasks;
    try {
      if (table != null) {
        this.table = table.name();
        this.bucketMode = String.valueOf(table.bucketMode());
      }
    } catch (RuntimeException ignored) {
      // Observability must not make a valid commit fail.
    }
  }

  void input(PaimonPrimaryKeyCompactionInput input) {
    try {
      if (!logger.isInfoEnabled() || input.getUnits() == null) {
        return;
      }
      for (PaimonBucketCompactionUnit unit : input.getUnits()) {
        if (unit != null && unit.getPartitionBytes() != null) {
          if (units
              .computeIfAbsent(ByteBuffer.wrap(unit.getPartitionBytes()), key -> new HashSet<>())
              .add(unit.getBucket())) {
            unitCount++;
          }
        }
      }
    } catch (RuntimeException ignored) {
      // Invalid input is handled by the existing business validation, not this observation.
    }
  }

  void output(PaimonPrimaryKeyCompactionOutput output) {
    try {
      if (logger.isInfoEnabled()) {
        messageBytes += output.commitMessageBytesSize();
      }
    } catch (RuntimeException ignored) {
      // Diagnostic counts must not change message validation or publication.
    }
  }

  void directFailure(Exception failure) {
    try {
      // Retain the cause even when a later publication check or fallback succeeds.
      // SLF4J accepts a trailing Throwable: https://www.slf4j.org/faq.html#paramException
      logger.warn(
          "Paimon commit failed table={} mode={} commitUser={} identifier={} stage=direct "
              + "api=commit maintenancePolicy=external; checking publication before fallback",
          table,
          mode,
          user,
          identifier,
          failure);
    } catch (RuntimeException ignored) {
      // A warning failure must not replace the original failure or prevent recovery.
    }
  }

  void record(String stage, String phase, long startNanos, String result) {
    try {
      if (!logger.isInfoEnabled()) {
        return;
      }
      logger.info(
          "PaimonCommitTiming table={} bucketMode={} mode={} commitUser={} identifier={} "
              + "targetSnapshotId={} optimizingType={} stage={} phase={} outcome={} elapsedMs={} "
              + "successTaskCount={} messageCount={} messageBytes={} partitionCount={} unitCount={} "
              + "api={} maintenancePolicy=external nativeInternalTimings=unavailable",
          table,
          bucketMode,
          mode,
          user,
          identifier,
          target,
          type,
          stage,
          phase,
          result,
          startNanos == -1
              ? "unavailable"
              : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
          tasks,
          messageCount,
          "unavailable".equals(messageCount) ? "unavailable" : messageBytes,
          inputsComplete ? units.size() : "unavailable",
          inputsComplete ? unitCount : "unavailable",
          phase.contains(":") ? phase.substring(phase.indexOf(':') + 1) : "unavailable");
    } catch (RuntimeException ignored) {
      // Logging cannot replace the business exception or turn a published commit into a retry.
    }
  }
}
