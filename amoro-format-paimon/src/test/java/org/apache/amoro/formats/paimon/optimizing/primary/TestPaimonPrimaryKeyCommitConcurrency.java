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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class TestPaimonPrimaryKeyCommitConcurrency {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nativeSnapshotRetryPreservesConcurrentUpdatesAndIndependentExpiry(
      boolean dynamic, @TempDir Path warehouse) throws Exception {
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, dynamic, Collections.emptyMap())) {
      fixture.writeGeneration(0);
      fixture.writeGeneration(1);
      List<PaimonPrimaryKeyCompactionTask> tasks = fixture.prepareTasks();
      BinaryRow partition =
          SerializationUtils.deserializeBinaryRow(
              tasks.get(0).getInput().getUnits().get(0).getPartitionBytes());
      Map<Integer, List<Integer>> indexes =
          dynamic
              ? TestPaimonPrimaryKeyDynamicCommit.indexes(fixture.table(), partition)
              : Collections.emptyMap();
      FileStoreTable original = fixture.table();
      FileIO io = spy(original.fileIO());
      CountDownLatch publishing = new CountDownLatch(1);
      CountDownLatch resume = new CountDownLatch(1);
      AtomicBoolean first = new AtomicBoolean(true);
      // Paimon 1.4.2 RenamingSnapshotCommit publishes through this public atomic FileIO API.
      // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-core/src/main/java/org/apache/paimon/catalog/RenamingSnapshotCommit.java
      doAnswer(
              call -> {
                if (call.getArgument(0, org.apache.paimon.fs.Path.class)
                        .toString()
                        .contains("/snapshot/snapshot-3")
                    && first.compareAndSet(true, false)) {
                  publishing.countDown();
                  assertTrue(resume.await(20, TimeUnit.SECONDS));
                }
                return call.callRealMethod();
              })
          .when(io)
          .tryToWriteAtomic(any(org.apache.paimon.fs.Path.class), anyString());
      FileStoreTable observed =
          FileStoreTableFactory.create(io, original.location(), original.schema());
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future<?> commit =
          executor.submit(
              () -> {
                new PaimonPrimaryKeyTableCommit(observed, tasks).commit();
                return null;
              });
      try {
        assertTrue(publishing.await(20, TimeUnit.SECONDS));
        fixture.writeGeneration(2);
        List<String> expected = PaimonPrimaryKeyCommitTestSupport.rows(fixture.table());
        fixture
            .table()
            .newExpireSnapshots()
            .config(ExpireConfig.builder().snapshotRetainMax(1).build())
            .expire();
        assertFalse(fixture.table().snapshotManager().snapshotExists(1));
        resume.countDown();
        commit.get(20, TimeUnit.SECONDS);
        assertEquals(4L, fixture.table().snapshotManager().latestSnapshotId());
        assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(fixture.table()));
        new PaimonPrimaryKeyTableCommit(fixture.table(), tasks).commit(CommitMode.RECOVERY_REPLAY);
        assertEquals(4L, fixture.table().snapshotManager().latestSnapshotId());
        if (dynamic) {
          assertEquals(
              indexes, TestPaimonPrimaryKeyDynamicCommit.indexes(fixture.table(), partition));
        }
      } finally {
        resume.countDown();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(20, TimeUnit.SECONDS));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void competingCompactionKeepsNativeMissingInputConflict(boolean dynamic, @TempDir Path warehouse)
      throws Exception {
    try (PaimonPrimaryKeyCommitTestSupport fixture =
        new PaimonPrimaryKeyCommitTestSupport(warehouse, dynamic, Collections.emptyMap())) {
      fixture.writeGeneration(0);
      fixture.writeGeneration(1);
      List<PaimonPrimaryKeyCompactionTask> losing = fixture.prepareTasks(100L, 7L);
      List<PaimonPrimaryKeyCompactionTask> winning = fixture.prepareTasks(100L, 8L);
      List<String> expected = PaimonPrimaryKeyCommitTestSupport.rows(fixture.table());
      new PaimonPrimaryKeyTableCommit(fixture.table(), winning).commit();
      long snapshot = fixture.table().snapshotManager().latestSnapshotId();
      OptimizingCommitException failure =
          assertThrows(
              OptimizingCommitException.class,
              () -> new PaimonPrimaryKeyTableCommit(fixture.table(), losing).commit());
      assertEquals(1, failure.getSuppressed().length);
      assertEquals(snapshot, fixture.table().snapshotManager().latestSnapshotId());
      assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(fixture.table()));
    }
  }
}
