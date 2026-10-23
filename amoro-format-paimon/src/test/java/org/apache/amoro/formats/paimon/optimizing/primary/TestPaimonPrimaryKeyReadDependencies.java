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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.files;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Conditional external-compactor race: the table's normal writer remains write-only, while B is an
 * independent native compactor. The preconditions deliberately exclude the earlier overlapping
 * L0-delete conflict case. Normal write-only ingestion does not run B's independent compaction.
 */
class TestPaimonPrimaryKeyReadDependencies {
  @ParameterizedTest
  @ValueSource(strings = {"none", "lookup"})
  void independentLookupCompactionCannotInvalidateStaleManagedCompaction(
      String producer, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable t = fixture(catalog, producer);
      DataFileMeta older = files(t).stream().filter(f -> f.level() == 3).findFirst().get();
      DataFileMeta newer = files(t).stream().filter(f -> f.level() == 2).findFirst().get();

      PaimonPrimaryKeyCompactionInput planned = input(t, OptimizingType.MINOR, 9901L);
      PaimonPrimaryKeyCompactionOutput prepared =
          new PaimonPrimaryKeyCompactionExecutor(planned).execute();
      List<CommitMessageImpl> aMessages = decode(prepared.getCommitMessageBytesList());
      Set<String> aDeleted = deletedFiles(aMessages);
      assertTrue(aDeleted.contains(older.fileName()), "A must compact old L3 data");
      assertTrue(aDeleted.contains(newer.fileName()), "A must compact L2 data");

      org.apache.paimon.table.source.StreamTableScan changes = t.newReadBuilder().newStreamScan();
      changes.restore(planned.getTargetSnapshotId() + 1);
      // The managed table's ordinary append is write-only; only an explicit native compactor B
      // creates the L0->L1 lookup rewrite and DV pointing at A's old L3 file.
      append(t, GenericRow.of(1, 11, 11, 2L));
      assertTrue(levels(t).contains(0));
      List<CommitMessageImpl> bMessages = independentLookupCompact(t);
      Set<String> bDeleted = deletedFiles(bMessages);
      assertFalse(bDeleted.isEmpty(), "B must compact its new L0 file");
      assertTrue(
          Collections.disjoint(aDeleted, bDeleted), "A/B data-file deletes must be disjoint");
      assertTrue(
          bMessages.stream().anyMatch(m -> !m.compactIncrement().newIndexFiles().isEmpty()),
          "B must actually write a deletion-vector index");
      assertTrue(
          files(t).stream().anyMatch(f -> f.fileName().equals(older.fileName())),
          "B must leave A's old L3 data file live");
      assertTrue(
          files(t).stream().anyMatch(f -> f.fileName().equals(newer.fileName())),
          "B must leave A's L2 data file live");

      List<String> expected = new ArrayList<>();
      expected.add("1|11|11|2");
      for (int i = 0; i < 1000; i++) {
        expected.add((100 + i) + "|" + i + "|" + i + "|1");
      }
      Collections.sort(expected);
      assertEquals(expected, rows(t), "native B must preserve expected PK rows");
      if (producer.equals("lookup")) {
        assertEquals(
            Arrays.asList("+U:1:11:11:2", "-U:1:10:10:1"),
            TestPaimonPrimaryKeyProtectedSemantics.drain(t, changes));
      }

      boolean rejected = false;
      try {
        PaimonPrimaryKeyIntervalTestSupport.commit(t, planned, prepared);
      } catch (OptimizingCommitException | RuntimeException conflict) {
        rejected = true;
      }
      List<String> actual = rows(t);
      assertEquals(
          expected.size(), actual.size(), "stale A must preserve unique primary keys or reject");
      assertEquals(expected, actual, "stale A must preserve B's update or explicitly reject");
      assertTrue(rejected, "A must reject the stale read dependency");
      // A newly planned FULL consumes B's current DV and can make progress after rejection.
      PaimonPrimaryKeyIntervalTestSupport.compact(t, OptimizingType.FULL, 9902L);
      assertEquals(expected, rows(t));
      if (producer.equals("lookup")) {
        assertEquals(
            Collections.emptyList(), TestPaimonPrimaryKeyProtectedSemantics.drain(t, changes));
      }
    }
  }

  static FileStoreTable fixture(Catalog catalog, String producer) throws Exception {
    Map<String, String> opts = options(false);
    opts.put("deletion-vectors.enabled", "true");
    opts.put("changelog-producer", producer);
    opts.put("num-levels", "4");
    opts.put("num-sorted-run.compaction-trigger", "2");
    opts.put("compaction.max-size-amplification-percent", "1000000");
    opts.put("compaction.size-ratio", "1000000");
    FileStoreTable t = table(catalog, "dv_disjoint", opts);

    append(t, GenericRow.of(1, 10, 10, 1L));
    DataFileMeta older = onlyFile(t);
    upgrade(t, older, 3);

    GenericRow[] bulk = new GenericRow[1000];
    for (int i = 0; i < bulk.length; i++) {
      bulk[i] = GenericRow.of(100 + i, i, i, 1L);
    }
    append(t, bulk);
    DataFileMeta newer = files(t).stream().filter(f -> f.level() == 0).findFirst().get();
    upgrade(t, newer, 2);
    assertEquals(2, files(t).size());
    assertEquals(new HashSet<>(Arrays.asList(2, 3)), levels(t));

    return t;
  }

  private static DataFileMeta onlyFile(FileStoreTable table) {
    assertEquals(1, files(table).size());
    return files(table).get(0);
  }

  private static Set<Integer> levels(FileStoreTable table) {
    return files(table).stream().map(DataFileMeta::level).collect(Collectors.toSet());
  }

  private static void upgrade(FileStoreTable table, DataFileMeta before, int level)
      throws Exception {
    // Paimon represents a level upgrade as a real compact-before/compact-after manifest pair;
    // the data file stays identical, so the fixture does not forge a physical data file.
    try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
      commit.commit(
          Collections.singletonList(
              new CommitMessageImpl(
                  BinaryRow.EMPTY_ROW,
                  0,
                  1,
                  DataIncrement.emptyIncrement(),
                  new CompactIncrement(
                      Collections.singletonList(before),
                      Collections.singletonList(before.upgrade(level)),
                      Collections.emptyList()))));
    }
    assertTrue(
        files(table).stream()
            .anyMatch(f -> f.fileName().equals(before.fileName()) && f.level() == level));
  }

  static List<CommitMessageImpl> independentLookupCompact(FileStoreTable table) throws Exception {
    return independentLookupCompact(table, 0);
  }

  static List<CommitMessageImpl> independentLookupCompact(FileStoreTable table, int bucket)
      throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("write-only", "false");
    overrides.put("lookup-compact", "radical");
    overrides.put("num-sorted-run.compaction-trigger", "1000");
    overrides.put("compaction.size-ratio", "0");
    overrides.put("compaction.max-size-amplification-percent", "1000000");
    FileStoreTable writable = table.copy(overrides);
    BatchWriteBuilder builder = writable.newBatchWriteBuilder();
    try (IOManager io = IOManager.create(System.getProperty("java.io.tmpdir"));
        BatchTableWrite write = builder.newWrite().withIOManager(io);
        BatchTableCommit commit = builder.newCommit()) {
      write.compact(BinaryRow.EMPTY_ROW, bucket, false);
      List<CommitMessage> messages = write.prepareCommit();
      List<CommitMessageImpl> result =
          messages.stream().map(m -> (CommitMessageImpl) m).collect(Collectors.toList());
      commit.commit(messages);
      return result;
    }
  }

  static List<CommitMessageImpl> decode(List<byte[]> bytes) throws Exception {
    return CommitMessageSerializer.deserializeAll(bytes).stream()
        .map(m -> (CommitMessageImpl) m)
        .collect(Collectors.toList());
  }

  static Set<String> deletedFiles(List<CommitMessageImpl> messages) {
    return messages.stream()
        .flatMap(m -> m.compactIncrement().compactBefore().stream())
        .map(DataFileMeta::fileName)
        .collect(Collectors.toSet());
  }
}
