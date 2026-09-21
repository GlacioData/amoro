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

/** REST projection of the most recent successful Amoro compaction comparison. */
public final class TableHealthComparisonView {
  private final String processId;
  private final String optimizingType;
  private final String status;
  private final String reason;
  private final boolean concurrentChanges;
  private final int beforeScore;
  private final int afterScore;
  private final TableHealthDetailsView before;
  private final TableHealthDetailsView after;
  private final long completedAtMillis;

  public TableHealthComparisonView(
      String processId,
      String optimizingType,
      String status,
      String reason,
      boolean concurrentChanges,
      int beforeScore,
      int afterScore,
      TableHealthDetailsView before,
      TableHealthDetailsView after,
      long completedAtMillis) {
    this.processId = processId;
    this.optimizingType = optimizingType;
    this.status = status;
    this.reason = reason;
    this.concurrentChanges = concurrentChanges;
    this.beforeScore = beforeScore;
    this.afterScore = afterScore;
    this.before = before;
    this.after = after;
    this.completedAtMillis = completedAtMillis;
  }

  public String getProcessId() {
    return processId;
  }

  public String getOptimizingType() {
    return optimizingType;
  }

  public String getStatus() {
    return status;
  }

  public String getReason() {
    return reason;
  }

  public boolean getConcurrentChanges() {
    return concurrentChanges;
  }

  public int getBeforeScore() {
    return beforeScore;
  }

  public int getAfterScore() {
    return afterScore;
  }

  public TableHealthDetailsView getBefore() {
    return before;
  }

  public TableHealthDetailsView getAfter() {
    return after;
  }

  public long getCompletedAtMillis() {
    return completedAtMillis;
  }
}
