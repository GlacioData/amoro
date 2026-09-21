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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Read-only runtime freshness, separate from the immutable evaluated snapshot. */
public final class TableHealthEvaluation {
  private final String status;
  private final boolean stale;
  private final String currentSnapshotId;
  private final String attemptedSnapshotId;
  private final long attemptedAtMillis;
  private final boolean retryable;
  private final List<String> reasonCodes;

  public TableHealthEvaluation(
      String status,
      boolean stale,
      String currentSnapshotId,
      String attemptedSnapshotId,
      long attemptedAtMillis,
      boolean retryable,
      List<String> reasonCodes) {
    this.status = status;
    this.stale = stale;
    this.currentSnapshotId = currentSnapshotId;
    this.attemptedSnapshotId = attemptedSnapshotId;
    this.attemptedAtMillis = attemptedAtMillis;
    this.retryable = retryable;
    this.reasonCodes = Collections.unmodifiableList(new ArrayList<>(reasonCodes));
  }

  public String getStatus() {
    return status;
  }

  public boolean isStale() {
    return stale;
  }

  public String getCurrentSnapshotId() {
    return currentSnapshotId;
  }

  public String getAttemptedSnapshotId() {
    return attemptedSnapshotId;
  }

  public long getAttemptedAtMillis() {
    return attemptedAtMillis;
  }

  public boolean isRetryable() {
    return retryable;
  }

  public List<String> getReasonCodes() {
    return reasonCodes;
  }
}
