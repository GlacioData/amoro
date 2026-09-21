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

package org.apache.amoro.formats.paimon.optimizing.health;

import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.health.TableHealthDetails;

/**
 * Versioned bounded state; pending input and the last observed successful process survive restart.
 */
public final class PaimonHealthComparisonState {
  public static final StateKey<PaimonHealthComparisonState> KEY =
      StateKey.stateKey("paimon_health_comparison_v1")
          .jsonType(PaimonHealthComparisonState.class)
          .defaultValue(new PaimonHealthComparisonState())
          .withOptimisticConcurrency();
  private int version = 1;
  private Entry pending = null;
  private Entry latest = null;

  public int getVersion() {
    return version;
  }

  public void setVersion(int value) {
    version = value;
  }

  public Entry getPending() {
    return pending;
  }

  public void setPending(Entry value) {
    pending = value;
  }

  public Entry getLatest() {
    return latest;
  }

  public void setLatest(Entry value) {
    latest = value;
  }

  /** Detached JSON value. Store transactions, not callbacks, own mutations of this record. */
  public static final class Entry {
    private long processId = 0;
    private long inputSnapshotId = -1;
    private long schemaId = -1;
    private String commitUser = null;
    private long commitIdentifier = -1;
    private String optimizingType = null;
    private String status = "PREPARING";
    private String reason = null;
    private boolean concurrentChanges = true;
    private int beforeScore = -1;
    private int afterScore = -1;
    private TableHealthDetails before = null;
    private TableHealthDetails after = null;
    private long completedAtMillis = 0;
    private int attempts = 0;
    private long retryAtMillis = 0;

    public long getProcessId() {
      return processId;
    }

    public void setProcessId(long value) {
      processId = value;
    }

    public long getInputSnapshotId() {
      return inputSnapshotId;
    }

    public void setInputSnapshotId(long value) {
      inputSnapshotId = value;
    }

    public long getSchemaId() {
      return schemaId;
    }

    public void setSchemaId(long value) {
      schemaId = value;
    }

    public String getCommitUser() {
      return commitUser;
    }

    public void setCommitUser(String value) {
      commitUser = value;
    }

    public long getCommitIdentifier() {
      return commitIdentifier;
    }

    public void setCommitIdentifier(long value) {
      commitIdentifier = value;
    }

    public String getOptimizingType() {
      return optimizingType;
    }

    public void setOptimizingType(String value) {
      optimizingType = value;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String value) {
      status = value;
    }

    public String getReason() {
      return reason;
    }

    public void setReason(String value) {
      reason = value;
    }

    public boolean getConcurrentChanges() {
      return concurrentChanges;
    }

    public void setConcurrentChanges(boolean value) {
      concurrentChanges = value;
    }

    public int getBeforeScore() {
      return beforeScore;
    }

    public void setBeforeScore(int value) {
      beforeScore = value;
    }

    public int getAfterScore() {
      return afterScore;
    }

    public void setAfterScore(int value) {
      afterScore = value;
    }

    public TableHealthDetails getBefore() {
      return before;
    }

    public void setBefore(TableHealthDetails value) {
      before = value;
    }

    public TableHealthDetails getAfter() {
      return after;
    }

    public void setAfter(TableHealthDetails value) {
      after = value;
    }

    public long getCompletedAtMillis() {
      return completedAtMillis;
    }

    public void setCompletedAtMillis(long value) {
      completedAtMillis = value;
    }

    public int getAttempts() {
      return attempts;
    }

    public void setAttempts(int value) {
      attempts = value;
    }

    public long getRetryAtMillis() {
      return retryAtMillis;
    }

    public void setRetryAtMillis(long value) {
      retryAtMillis = value;
    }
  }
}
