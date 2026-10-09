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
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Compares managed GENTLE tasks with native RADICAL for rows, changelog, and recovery. */
class TestPaimonPrimaryKeyGentleSemantics {
  @ParameterizedTest(name = "first-row bucket={0}")
  @ValueSource(strings = {"1", "-1"})
  void firstRowMinorMatchesNativeRadical(String bucket, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable managed = create(catalog, "managed", bucket, false);
      FileStoreTable nativeTable = create(catalog, "native", bucket, false);
      append(managed, row(RowKind.INSERT, 1, 10, 1));
      append(nativeTable, row(RowKind.INSERT, 1, 10, 1));
      assertEquals(Collections.emptyList(), rows(managed));
      nativeMinor(nativeTable);
      managedMinorAndReplay(managed, 1);
      assertEquals(Collections.singletonList("1|10|10|1"), rows(nativeTable));
      assertEquals(rows(nativeTable), rows(managed));

      append(managed, row(RowKind.INSERT, 1, 99, 2), row(RowKind.INSERT, 2, 20, 2));
      append(nativeTable, row(RowKind.INSERT, 1, 99, 2), row(RowKind.INSERT, 2, 20, 2));
      nativeMinor(nativeTable);
      managedMinorAndReplay(managed, 2);
      assertEquals(Arrays.asList("1|10|10|1", "2|20|20|2"), rows(nativeTable));
      assertEquals(rows(nativeTable), rows(managed));
      assertEquals("gentle", managed.options().get("lookup-compact"));
      assertEquals("1000", managed.options().get("lookup-compact.max-interval"));
    }
  }

  @ParameterizedTest(name = "lookup changelog bucket={0}")
  @ValueSource(strings = {"1", "-1"})
  void lookupMinorMatchesNativeRowKindsAndReplay(String bucket, @TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable managed = create(catalog, "managed", bucket, true);
      FileStoreTable nativeTable = create(catalog, "native", bucket, true);
      StreamTableScan managedScan = managed.newReadBuilder().newStreamScan();
      StreamTableScan nativeScan = nativeTable.newReadBuilder().newStreamScan();
      managedScan.restore(1L);
      nativeScan.restore(1L);
      append(managed, row(RowKind.INSERT, 1, 10, 1), row(RowKind.INSERT, 2, 20, 1));
      append(nativeTable, row(RowKind.INSERT, 1, 10, 1), row(RowKind.INSERT, 2, 20, 1));
      assertEquals(Collections.emptyList(), drain(managed, managedScan));
      assertEquals(Collections.emptyList(), drain(nativeTable, nativeScan));
      nativeMinor(nativeTable);
      managedMinorAndReplay(managed, 1);
      List<String> initial = drain(nativeTable, nativeScan);
      assertEquals(Arrays.asList("+I:1:10:1", "+I:2:20:1"), initial);
      assertEquals(initial, drain(managed, managedScan));
      assertEquals(rows(nativeTable), rows(managed));

      GenericRow[] changes = {
        row(RowKind.UPDATE_BEFORE, 1, 10, 1),
        row(RowKind.UPDATE_AFTER, 1, 11, 2),
        row(RowKind.DELETE, 2, 20, 1),
        row(RowKind.INSERT, 3, 30, 2)
      };
      append(managed, changes);
      append(nativeTable, changes);
      assertEquals(Collections.emptyList(), drain(managed, managedScan));
      assertEquals(Collections.emptyList(), drain(nativeTable, nativeScan));
      nativeMinor(nativeTable);
      managedMinorAndReplay(managed, 2);
      List<String> expectedChanges =
          Arrays.asList("+I:3:30:2", "+U:1:11:2", "-D:2:20:1", "-U:1:10:1");
      assertEquals(expectedChanges, drain(nativeTable, nativeScan));
      assertEquals(expectedChanges, drain(managed, managedScan));
      assertEquals(Arrays.asList("1|11|11|2", "3|30|30|2"), rows(nativeTable));
      assertEquals(rows(nativeTable), rows(managed));
      Long checkpoint = managedScan.checkpoint();
      StreamTableScan restoredScan = managed.newReadBuilder().newStreamScan();
      restoredScan.restore(checkpoint);
      assertEquals(Collections.emptyList(), drain(managed, restoredScan));
      assertEquals(Collections.emptyList(), drain(managed, managedScan));
      assertEquals("gentle", managed.options().get("lookup-compact"));
    }
  }

  @ParameterizedTest(name = "lookup schema and DV bucket={0}")
  @ValueSource(strings = {"1", "-1"})
  void schemaRefreshBeforeRadicalOverridesPreservesValuesAndIndexes(
      String bucket, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> properties = options(false);
      properties.put("bucket", bucket);
      properties.put("changelog-producer", "lookup");
      properties.put("deletion-vectors.enabled", "true");
      properties.put("lookup-compact", "gentle");
      properties.put("lookup-compact.max-interval", "1000");
      properties.put("num-sorted-run.compaction-trigger", "5");
      properties.put("self-optimizing.full.trigger.interval", "-1");
      FileStoreTable old = table(catalog, "schema", properties);
      append(old, row(RowKind.INSERT, 1, 10, 1), row(RowKind.INSERT, 2, 20, 1));
      nativeMinor(old);
      append(old, row(RowKind.INSERT, 1, 11, 2));
      nativeMinor(old);
      List<IndexFileMeta> oldIndexes =
          old.store()
              .newIndexFileHandler()
              .scan(
                  old.latestSnapshot().get(),
                  org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX,
                  BinaryRow.EMPTY_ROW,
                  0);
      // Require committed deletion vectors so empty indexes cannot hide a restore regression.
      long deleted =
          old.store().newIndexFileHandler()
              .readAllDeletionVectors(BinaryRow.EMPTY_ROW, 0, oldIndexes).values().stream()
              .mapToLong(DeletionVector::getCardinality)
              .sum();
      assertEquals(1, deleted);
      if (bucket.equals("-1")) {
        assertTrue(
            old.store()
                .newIndexFileHandler()
                .scanHashIndex(old.latestSnapshot().get(), BinaryRow.EMPTY_ROW, 0)
                .isPresent());
      }
      PaimonPrimaryKeyCompactionInput stale = roundTrip(input(old, OptimizingType.MINOR, 31));
      catalog.alterTable(
          Identifier.create("db", "schema"),
          Collections.singletonList(SchemaChange.addColumn("new_col", DataTypes.INT())),
          false);
      FileStoreTable current = (FileStoreTable) catalog.getTable(Identifier.create("db", "schema"));
      append(current, GenericRow.of(3, 30, 30, 3L, 12345));
      assertEquals(0, ((FileStoreTable) stale.getTable().originalTable()).schema().id());
      executeManagedInputAndReplay(current, stale);
      List<String> actual = new ArrayList<>();
      org.apache.paimon.table.source.ReadBuilder builder = current.newReadBuilder();
      try (RecordReader<InternalRow> reader =
          builder.newRead().createReader(builder.newScan().plan())) {
        reader.forEachRemaining(
            row ->
                actual.add(
                    row.getInt(0)
                        + "|"
                        + row.getInt(1)
                        + "|"
                        + (row.isNullAt(4) ? "null" : row.getInt(4))));
      }
      Collections.sort(actual);
      assertEquals(Arrays.asList("1|11|null", "2|20|null", "3|30|12345"), actual);
      assertEquals("gentle", current.options().get("lookup-compact"));
      assertEquals(0, ((FileStoreTable) stale.getTable().originalTable()).schema().id());
    }
  }

  private static FileStoreTable create(Catalog catalog, String name, String bucket, boolean lookup)
      throws Exception {
    Map<String, String> properties = options(false);
    properties.put("bucket", bucket);
    properties.put("lookup-compact", "gentle");
    properties.put("lookup-compact.max-interval", "1000");
    properties.put("num-sorted-run.compaction-trigger", "5");
    properties.put("self-optimizing.full.trigger.interval", "-1");
    if (lookup) {
      properties.put("changelog-producer", "lookup");
      properties.put("sequence.field", "seq");
    } else {
      properties.put("merge-engine", "first-row");
    }
    return table(catalog, name, properties);
  }

  private static GenericRow row(RowKind kind, int id, int value, long sequence) {
    GenericRow row = GenericRow.of(id, value, value, sequence);
    row.setRowKind(kind);
    return row;
  }

  private static void nativeMinor(FileStoreTable table) throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("write-only", "false");
    overrides.put("lookup-compact", "radical");
    // Use the native Paimon writer as the reference independently of the Amoro executor.
    BatchWriteBuilder builder = table.copy(overrides).newBatchWriteBuilder();
    try (IOManager io = IOManager.create(System.getProperty("java.io.tmpdir"));
        BatchTableWrite writer = builder.newWrite().withIOManager(io);
        BatchTableCommit commit = builder.newCommit()) {
      writer.compact(BinaryRow.EMPTY_ROW, 0, false);
      commit.commit(writer.prepareCommit());
    }
  }

  private static void managedMinorAndReplay(FileStoreTable table, long identifier)
      throws Exception {
    PaimonPrimaryKeyCompactionInput restored =
        roundTrip(input(table, OptimizingType.MINOR, identifier));
    executeManagedInputAndReplay(table, restored);
  }

  private static void executeManagedInputAndReplay(
      FileStoreTable table, PaimonPrimaryKeyCompactionInput restored) throws Exception {
    assertFalse(restored.isFullCompaction());
    assertEquals(
        "gentle",
        ((FileStoreTable) restored.getTable().originalTable()).options().get("lookup-compact"));
    PaimonPrimaryKeyCompactionOutput output =
        new PaimonPrimaryKeyCompactionExecutor(restored).execute();
    System.out.println(
        "F1_COUNTS table="
            + table.name()
            + " bucketMode="
            + table.bucketMode()
            + " engine="
            + table.options().get("merge-engine")
            + " changelog="
            + table.options().get("changelog-producer")
            + " schema="
            + table.schema().id()
            + " identifier="
            + restored.getCommitIdentifier()
            + " inputFiles="
            + output.getCompactedFileCount()
            + " inputBytes="
            + output.getCompactedFileSize()
            + " outputFiles="
            + output.getProducedFileCount()
            + " outputBytes="
            + output.getProducedFileSize());
    assertTrue(
        output.commitMessageCount() > 0,
        "MINOR must advance visible rows or lookup changelog for a single L0 file");
    PaimonPrimaryKeyCompactionTask task =
        new PaimonPrimaryKeyCompactionTask(1, "bucket=0", restored, Collections.emptyMap());
    task.setOutput(roundTrip(output));
    new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(task)).commit();
    long committedSnapshot = table.latestSnapshot().get().id();
    for (int attempt = 0; attempt < 2; attempt++) {
      PaimonPrimaryKeyCompactionTask replay =
          new PaimonPrimaryKeyCompactionTask(
              1, "bucket=0", roundTrip(restored), Collections.emptyMap());
      replay.setOutput(roundTrip(output));
      new PaimonPrimaryKeyTableCommit(table, Collections.singletonList(replay))
          .commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(committedSnapshot, table.latestSnapshot().get().id());
    }
  }

  private static List<String> drain(FileStoreTable table, StreamTableScan scan) throws Exception {
    List<String> changes = new ArrayList<>();
    for (int attempts = 0; attempts < 64; attempts++) {
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
                          + row.getLong(3)));
        }
      }
      if (scan.checkpoint() != null && scan.checkpoint() > table.latestSnapshot().get().id()) {
        Collections.sort(changes);
        return changes;
      }
    }
    throw new AssertionError("Changelog scan did not reach the latest snapshot in 64 attempts");
  }

  private static <T> T roundTrip(T value) {
    ByteBuffer buffer = SerializationUtil.simpleSerialize(value);
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return SerializationUtil.simpleDeserialize(bytes);
  }
}
