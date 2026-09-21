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

import org.apache.amoro.formats.paimon.optimizing.plan.PaimonAppendFileScanner;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeySnapshotAnalysis;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

/** Metadata-only historical evaluation. Callers must retain the table's authorization context. */
public final class PaimonHistoricalHealth {
  public static final int MAX_COMMIT_LOOKUP = 128;

  private PaimonHistoricalHealth() {}

  public static FormatTableAnalysis evaluate(
      FileStoreTable current, String tableId, long snapshotId) {
    Snapshot snapshot = current.snapshotManager().snapshot(snapshotId);
    // FileStoreTable.copy(TableSchema) preserves the FileIO and binds historical schema/options.
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java
    FileStoreTable historical = current.copy(current.schemaManager().schema(snapshot.schemaId()));
    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.forSnapshot(historical, tableId, snapshot);
    if (historical instanceof PrimaryKeyFileStoreTable) {
      return PaimonPrimaryKeySnapshotAnalysis.analyzeHealth(historical, context);
    }
    if (historical instanceof AppendOnlyFileStoreTable) {
      return PaimonAppendFileScanner.analyzeHealth((AppendOnlyFileStoreTable) historical, context);
    }
    throw new IllegalArgumentException("Unsupported Paimon historical table shape");
  }

  /** A missing match is not proof of an empty commit or of no rewrite. */
  public static CommitMatch findCommit(
      FileStoreTable table, long inputSnapshot, String user, long identifier) {
    SnapshotManager manager = table.snapshotManager();
    Long latest = manager.latestSnapshotId();
    if (latest == null || latest <= inputSnapshot) {
      return new CommitMatch(null, "NO_COMMIT_EVIDENCE");
    }
    long lower = Math.max(inputSnapshot + 1, latest - MAX_COMMIT_LOOKUP + 1);
    Snapshot match = null;
    boolean missing = false;
    for (long id = latest; id >= lower; id--) {
      if (!manager.snapshotExists(id)) {
        missing = true;
        continue;
      }
      Snapshot snapshot = manager.snapshot(id);
      if (user.equals(snapshot.commitUser()) && snapshot.commitIdentifier() == identifier) {
        if (match != null) {
          return new CommitMatch(null, "AMBIGUOUS_COMMIT");
        }
        match = snapshot;
      }
    }
    // A truncated search cannot establish uniqueness across the whole process interval.
    if (lower > inputSnapshot + 1) {
      return new CommitMatch(null, "LOOKUP_LIMIT");
    }
    if (missing) {
      return new CommitMatch(null, "SNAPSHOT_EXPIRED");
    }
    return new CommitMatch(match, match == null ? "NO_COMMIT_EVIDENCE" : "MATCHED");
  }

  public static final class CommitMatch {
    private final Snapshot snapshot;
    private final String status;

    private CommitMatch(Snapshot snapshot, String status) {
      this.snapshot = snapshot;
      this.status = status;
    }

    public Snapshot snapshot() {
      return snapshot;
    }

    public String status() {
      return status;
    }
  }
}
