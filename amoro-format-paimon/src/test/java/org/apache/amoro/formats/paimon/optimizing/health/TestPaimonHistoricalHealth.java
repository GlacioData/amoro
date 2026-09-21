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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;

class TestPaimonHistoricalHealth {
  @Test
  void exactIdentityAndBoundedSearchDoNotGuessEmptyCommits() {
    FileStoreTable table = mock(FileStoreTable.class);
    SnapshotManager manager = mock(SnapshotManager.class);
    when(table.snapshotManager()).thenReturn(manager);
    when(manager.latestSnapshotId()).thenReturn(4L);
    when(manager.snapshotExists(anyLong())).thenReturn(true);
    Snapshot other = snapshot("writer", 7);
    Snapshot own = snapshot("ams", 7);
    when(manager.snapshot(4)).thenReturn(other);
    when(manager.snapshot(3)).thenReturn(own);
    assertSame(own, PaimonHistoricalHealth.findCommit(table, 2, "ams", 7).snapshot());
    assertEquals(
        "NO_COMMIT_EVIDENCE", PaimonHistoricalHealth.findCommit(table, 2, "ams", 8).status());
    when(manager.snapshot(4)).thenReturn(own);
    assertEquals(
        "AMBIGUOUS_COMMIT", PaimonHistoricalHealth.findCommit(table, 2, "ams", 7).status());
    when(manager.snapshotExists(4)).thenReturn(false);
    assertEquals(
        "SNAPSHOT_EXPIRED", PaimonHistoricalHealth.findCommit(table, 2, "ams", 7).status());
  }

  @Test
  void lookupNeverWalksUnboundedHistory() {
    FileStoreTable table = mock(FileStoreTable.class);
    SnapshotManager manager = mock(SnapshotManager.class);
    when(table.snapshotManager()).thenReturn(manager);
    when(manager.latestSnapshotId()).thenReturn(10000L);
    when(manager.snapshotExists(anyLong())).thenReturn(true);
    Snapshot other = snapshot("other", 7);
    when(manager.snapshot(anyLong())).thenReturn(other);
    assertEquals("LOOKUP_LIMIT", PaimonHistoricalHealth.findCommit(table, 1, "ams", 7).status());
    verify(manager, times(128)).snapshot(anyLong());
  }

  private static Snapshot snapshot(String user, long identifier) {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.commitUser()).thenReturn(user);
    when(snapshot.commitIdentifier()).thenReturn(identifier);
    return snapshot;
  }
}
