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

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.append;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.compact;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.wrap;
import static org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyReadDependencies.independentLookupCompact;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.SerializationUtils;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class TestPaimonPrimaryKeyProtectedSemantics {
  @ParameterizedTest
  @CsvSource({
    "deduplicate,none",
    "deduplicate,lookup",
    "sequence,none",
    "sequence,lookup",
    "partial-update,none",
    "partial-update,lookup",
    "aggregation,none",
    "aggregation,lookup"
  })
  void protectedCompactionMatchesNativeMergeRowsAndChangelog(
      String engine, String producer, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(false);
      opts.put("deletion-vectors.enabled", "true");
      opts.put("changelog-producer", producer);
      opts.put("merge-engine", engine.equals("sequence") ? "deduplicate" : engine);
      if (engine.equals("sequence")) {
        opts.put("sequence.field", "seq");
      }
      if (engine.equals("aggregation")) {
        opts.put("fields.a.aggregate-function", "sum");
        opts.put("fields.b.aggregate-function", "sum");
        opts.put("fields.seq.aggregate-function", "max");
      }
      FileStoreTable managed = table(catalog, "managed", opts);
      FileStoreTable nativeTable = table(catalog, "native", opts);
      StreamTableScan managedScan = managed.newReadBuilder().newStreamScan();
      StreamTableScan nativeScan = nativeTable.newReadBuilder().newStreamScan();
      managedScan.restore(1L);
      nativeScan.restore(1L);
      for (FileStoreTable t : Arrays.asList(managed, nativeTable)) {
        append(
            t,
            GenericRow.of(1, 10, engine.equals("partial-update") ? null : 10, 2L),
            GenericRow.of(2, 20, 20, 1L));
        append(t, GenericRow.of(1, engine.equals("partial-update") ? null : 11, 11, 1L));
      }
      assertNotNull(compact(managed, OptimizingType.FULL, 101).getReadDependency());
      nativeFull(nativeTable);
      String first =
          engine.equals("aggregation")
              ? "1|21|21|2"
              : engine.equals("partial-update")
                  ? "1|10|11|1"
                  : engine.equals("sequence") ? "1|10|10|2" : "1|11|11|1";
      assertEquals(Arrays.asList(first, "2|20|20|1"), rows(managed));
      assertEquals(rows(nativeTable), rows(managed));
      if (producer.equals("lookup")) {
        List<String> initial = drain(nativeTable, nativeScan);
        assertEquals(2, initial.size());
        assertEquals(initial, drain(managed, managedScan));
      }
      for (FileStoreTable t : Arrays.asList(managed, nativeTable)) {
        append(t, GenericRow.of(1, 12, 12, 3L));
      }
      compact(managed, OptimizingType.FULL, 102);
      nativeFull(nativeTable);
      assertEquals(rows(nativeTable), rows(managed));
      if (producer.equals("lookup")) {
        List<String> changes = drain(nativeTable, nativeScan);
        assertEquals(2, changes.size());
        assertEquals(changes, drain(managed, managedScan));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void participatingBucketsRejectAtomicallyWhileOtherBucketDvCanAdvance(
      boolean includeChangedBucket, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> opts = options(false);
      opts.put("bucket", "2");
      opts.put("deletion-vectors.enabled", "true");
      opts.put("changelog-producer", "lookup");
      opts.put("num-levels", "4");
      opts.put("num-sorted-run.compaction-trigger", "2");
      opts.put("compaction.max-size-amplification-percent", "1000000");
      opts.put("compaction.size-ratio", "1000000");
      FileStoreTable t = table(catalog, "buckets", opts);
      GenericRow[] initial = new GenericRow[32];
      for (int i = 0; i < initial.length; i++) {
        initial[i] = GenericRow.of(i, i, i, 1L);
      }
      append(t, initial);
      upgradeAll(t, 3);
      GenericRow[] bulk = new GenericRow[1000];
      for (int i = 0; i < bulk.length; i++) {
        bulk[i] = GenericRow.of(100 + i, i, i, 1L);
      }
      append(t, bulk);
      upgradeAll(t, 2);
      GenericRow update = GenericRow.of(1, 11, 11, 2L);
      int changedBucket;
      try (BatchTableWrite write = t.newBatchWriteBuilder().newWrite()) {
        changedBucket = write.getBucket(update);
      }
      List<PaimonBucketCompactionUnit> units = new ArrayList<>();
      for (int bucket = 0; bucket < 2; bucket++) {
        if (includeChangedBucket || bucket != changedBucket) {
          units.add(
              new PaimonBucketCompactionUnit(
                  SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW), bucket, 2, 0, 0, 1));
        }
      }
      PaimonPrimaryKeyCompactionInput input =
          new PaimonPrimaryKeyCompactionInput(
              wrap(t), units, OptimizingType.MINOR, false, 4L, "f1-multibucket", 800L);
      PaimonPrimaryKeyCompactionTask task =
          new PaimonPrimaryKeyCompactionTask(1, "buckets", input, Collections.emptyMap());
      task.setOutput(
          SerializationUtil.simpleSerialize(
              new PaimonPrimaryKeyCompactionExecutor(input).execute()));
      assertEquals(units.size(), task.getOutput().getCompactedBucketCount());
      List<String> expected = rows(t);
      assertEquals(1032, expected.size());
      assertTrue(expected.remove("1|1|1|1"));
      expected.add("1|11|11|2");
      Collections.sort(expected);
      append(t, update);
      independentLookupCompact(t, changedBucket);
      assertEquals(expected, rows(t));
      PaimonPrimaryKeyTableCommit commit =
          new PaimonPrimaryKeyTableCommit(t, Collections.singletonList(task));
      if (includeChangedBucket) {
        assertThrows(OptimizingCommitException.class, commit::commit);
        assertEquals(6L, t.snapshotManager().latestSnapshotId());
        TestPaimonPrimaryKeyProtectedCommit.assertOutputExists(t, task);
      } else {
        commit.commit();
        assertEquals(7L, t.snapshotManager().latestSnapshotId());
      }
      assertEquals(expected, rows(t));
    }
  }

  private static void upgradeAll(FileStoreTable table, int level) throws Exception {
    List<CommitMessage> messages = new ArrayList<>();
    for (ManifestEntry entry : table.store().newScan().plan().files()) {
      DataFileMeta file = entry.file();
      assertEquals(
          PaimonPrimaryKeyReadDependency.fileIdentity(file),
          PaimonPrimaryKeyReadDependency.fileIdentity(file.copyWithoutStats()));
      if (file.level() == 0) {
        messages.add(
            new CommitMessageImpl(
                entry.partition(),
                entry.bucket(),
                entry.totalBuckets(),
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                    Collections.singletonList(file),
                    Collections.singletonList(file.upgrade(level)),
                    Collections.emptyList())));
      }
    }
    assertEquals(2, messages.size());
    try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
      commit.commit(messages);
    }
  }

  static void nativeFull(FileStoreTable table) throws Exception {
    BatchWriteBuilder builder =
        table.copy(Collections.singletonMap("write-only", "false")).newBatchWriteBuilder();
    try (IOManager io = IOManager.create(System.getProperty("java.io.tmpdir"));
        BatchTableWrite write = builder.newWrite().withIOManager(io);
        BatchTableCommit commit = builder.newCommit()) {
      write.compact(BinaryRow.EMPTY_ROW, 0, true);
      commit.commit(write.prepareCommit());
    }
  }

  static List<String> drain(FileStoreTable table, StreamTableScan scan) throws Exception {
    List<String> changes = new ArrayList<>();
    for (int attempt = 0; attempt < 64; attempt++) {
      TableScan.Plan plan = scan.plan();
      if (!plan.splits().isEmpty()) {
        try (RecordReader<InternalRow> reader =
            table.newReadBuilder().newRead().createReader(plan)) {
          reader.forEachRemaining(
              row ->
                  changes.add(
                      row.getRowKind().shortString()
                          + ":"
                          + row.getInt(0)
                          + ":"
                          + row.getInt(1)
                          + ":"
                          + row.getInt(2)
                          + ":"
                          + row.getLong(3)));
        }
      }
      if (scan.checkpoint() != null && scan.checkpoint() > table.latestSnapshot().get().id()) {
        Collections.sort(changes);
        return changes;
      }
    }
    throw new AssertionError("Changelog scan did not reach latest snapshot");
  }
}
