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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

class TestPaimonPrimaryKeyDynamicCommit {
  @Test
  void realIndexesSurviveCompactionReplayAndSubsequentUpdates(@TempDir Path warehouse)
      throws Exception {
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, true, Collections.emptyMap())) {
      fixture.writeGeneration(0);
      fixture.writeGeneration(1);
      List<PaimonPrimaryKeyCompactionTask> tasks = fixture.prepareTasks();
      BinaryRow partition =
          SerializationUtils.deserializeBinaryRow(
              tasks.get(0).getInput().getUnits().get(0).getPartitionBytes());
      Map<Integer, List<Integer>> before = indexes(fixture.table(), partition);
      assertEquals(2, before.size());
      assertEquals(4, before.values().stream().mapToInt(List::size).sum());
      List<String> rows = PaimonPrimaryKeyCommitTestSupport.rows(fixture.table());

      new PaimonPrimaryKeyTableCommit(fixture.table(), tasks).commit();
      FileStoreTable committed = fixture.table();
      Snapshot snapshot = committed.snapshotManager().latestSnapshot();
      assertEquals(Snapshot.CommitKind.COMPACT, snapshot.commitKind());
      assertNotNull(snapshot.indexManifest());
      assertEquals(before, indexes(committed, partition));
      assertEquals(rows, PaimonPrimaryKeyCommitTestSupport.rows(committed));
      assertRestoredAssignments(committed, partition, before);

      new PaimonPrimaryKeyTableCommit(fixture.table(), tasks).commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(snapshot.id(), fixture.table().snapshotManager().latestSnapshotId());
      assertEquals(before, indexes(fixture.table(), partition));
      assertRestoredAssignments(fixture.table(), partition, before);

      fixture.writeGeneration(2);
      assertEquals(before, indexes(fixture.table(), partition));
      List<String> expected = new ArrayList<>();
      for (int key = 0; key < 4; key++) {
        expected.add(key + "|value-2-" + key + "|" + PaimonPrimaryKeyCommitTestSupport.PARTITION);
      }
      assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(fixture.table()));
    }
  }

  static Map<Integer, List<Integer>> indexes(FileStoreTable table, BinaryRow partition)
      throws Exception {
    IndexFileHandler handler = table.store().newIndexFileHandler();
    Snapshot snapshot = table.snapshotManager().latestSnapshot();
    assertNotNull(snapshot.indexManifest());
    Map<Integer, List<Integer>> result = new TreeMap<>();
    for (int bucket = 0; bucket < 2; bucket++) {
      IndexFileMeta meta = handler.scanHashIndex(snapshot, partition, bucket).orElse(null);
      assertNotNull(meta, "Real hash-index file must exist for bucket " + bucket);
      List<Integer> hashes = new ArrayList<>(handler.hashIndex(partition, bucket).readList(meta));
      Collections.sort(hashes);
      result.put(bucket, hashes);
    }
    return result;
  }

  private static void assertRestoredAssignments(
      FileStoreTable table, BinaryRow partition, Map<Integer, List<Integer>> expected) {
    // Restore the native assigner from committed index metadata, without mocking allocation.
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/test/java/org/apache/paimon/index/HashBucketAssignerTest.java#L60-L70
    HashBucketAssigner assigner =
        new HashBucketAssigner(
            table.snapshotManager(),
            "restored-writer",
            table.store().newIndexFileHandler(),
            1,
            1,
            0,
            1000,
            -1);
    expected.forEach(
        (bucket, hashes) ->
            hashes.forEach(
                hash -> assertEquals(bucket.intValue(), assigner.assign(partition, hash))));
  }
}
