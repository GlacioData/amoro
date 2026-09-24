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

package org.apache.amoro.formats.paimon.optimizing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.RoaringBitmap32;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class TestPaimonFixedBucketRangeBitmap {
  private static final String COMMIT_USER = "fixed-range-bitmap-test";
  private static final String THRESHOLD = "file-index.in-manifest-threshold";
  private static final String INDEX_COLUMNS = "file-index.range-bitmap.columns";

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rebuildsRealIndexesAndAppliesChangedOptionsOnlyToTheNextRound(
      boolean embedded, @TempDir Path warehouse) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(THRESHOLD, threshold(embedded));
    options.put("target-file-size", "64 kb");
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true, options)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      fixture.append(2, 1);
      Map<String, List<ManifestEntry>> originalUnits = fixture.units();
      assertEquals(
          new HashSet<>(Arrays.asList("0:0", "0:1", "1:0", "1:1")), originalUnits.keySet());
      originalUnits.values().forEach(files -> assertEquals(3, files.size()));
      Map<String, List<String>> beforeByUnit = fixture.rowsByUnit();
      List<String> before = fixture.snapshotRows();
      assertEquals(before.size() / 2, before.stream().distinct().count());
      assertTrue(before.stream().anyMatch(row -> hm(row).equals("0007")));
      long sourceSnapshot = fixture.table.latestSnapshot().get().id();
      Map<String, PaimonCompactionTask> published = plan(fixture.table, originalUnits, 101L);

      // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
      // paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java:382-389.
      // Published tasks retain their table options; only a catalog reload binds the next round.
      fixture.catalog.alterTable(
          fixture.identifier,
          Arrays.asList(
              SchemaChange.setOption(THRESHOLD, threshold(!embedded)),
              SchemaChange.setOption(INDEX_COLUMNS, "hm,reserve7")),
          false);
      AppendOnlyFileStoreTable refreshed =
          (AppendOnlyFileStoreTable) fixture.catalog.getTable(fixture.identifier);
      assertEquals(sourceSnapshot, refreshed.latestSnapshot().get().id());
      assertEquals(threshold(!embedded), refreshed.options().get(THRESHOLD));
      assertEquals("hm,reserve7", refreshed.options().get(INDEX_COLUMNS));
      Map<String, List<DataFileMeta>> firstOutputs =
          executeAndVerify(originalUnits, published, embedded, Collections.singleton("hm"));
      new PaimonTableCommit(refreshed, new ArrayList<>(published.values()), COMMIT_USER, 101L)
          .commit();
      assertEquals(sourceSnapshot + 1, refreshed.latestSnapshot().get().id());
      assertEquals(firstOutputs, manifestFiles(fixture.units()));
      assertEquals(before, fixture.snapshotRows());
      assertEquals(beforeByUnit, fixture.rowsByUnit());
      assertEquals(before, snapshotRows(refreshed, sourceSnapshot));

      Map<String, List<ManifestEntry>> nextUnits = fixture.units();
      nextUnits.values().forEach(files -> assertEquals(1, files.size()));
      Map<String, PaimonCompactionTask> next = plan(refreshed, nextUnits, 102L);
      Map<String, List<DataFileMeta>> nextOutputs =
          executeAndVerify(
              nextUnits, next, !embedded, new HashSet<>(Arrays.asList("hm", "reserve7")));
      new PaimonTableCommit(refreshed, new ArrayList<>(next.values()), COMMIT_USER, 102L).commit();
      assertEquals(sourceSnapshot + 2, refreshed.latestSnapshot().get().id());
      assertEquals(nextOutputs, manifestFiles(fixture.units()));
      assertEquals(before, fixture.snapshotRows());
      assertEquals(beforeByUnit, fixture.rowsByUnit());
      assertEquals(before, snapshotRows(refreshed, sourceSnapshot));
      assertEquals(before, snapshotRows(refreshed, sourceSnapshot + 1));
    }
  }

  private static Map<String, PaimonCompactionTask> plan(
      AppendOnlyFileStoreTable table, Map<String, List<ManifestEntry>> units, long identifier) {
    PaimonTable wrapper = new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), table);
    Map<String, PaimonCompactionTask> tasks = new LinkedHashMap<>();
    units.forEach(
        (unit, files) -> {
          PaimonFixedBucketTask fixed =
              PaimonFixedBucketTask.from(
                  wrapper,
                  files,
                  table.latestSnapshot().get().id(),
                  OptimizingType.FULL,
                  PaimonAppendRewriteOptions.from(wrapper.properties()));
          tasks.put(
              unit,
              new PaimonCompactionTask(
                  1L,
                  "",
                  PaimonCompactionInput.forFixedBucket(wrapper, fixed, COMMIT_USER, "", identifier),
                  Collections.emptyMap()));
        });
    return tasks;
  }

  private static Map<String, List<DataFileMeta>> executeAndVerify(
      Map<String, List<ManifestEntry>> units,
      Map<String, PaimonCompactionTask> tasks,
      boolean embedded,
      Set<String> indexColumns)
      throws Exception {
    Map<String, List<DataFileMeta>> rewritten = new LinkedHashMap<>();
    for (Map.Entry<String, PaimonCompactionTask> entry : tasks.entrySet()) {
      PaimonCompactionTask task = entry.getValue();
      AppendOnlyFileStoreTable table =
          (AppendOnlyFileStoreTable) task.getInput().getTable().originalTable();
      assertEquals(threshold(embedded), table.options().get(THRESHOLD));
      PaimonCompactionOutput output = new PaimonCompactionExecutor(task.getInput()).execute();
      ByteBuffer serialized = SerializationUtil.simpleSerialize(output);
      byte[] bytes = new byte[serialized.remaining()];
      serialized.get(bytes);
      task.setOutputBytes(bytes);
      CommitMessageImpl message =
          (CommitMessageImpl)
              new CommitMessageSerializer()
                  .deserialize(output.getCommitMessageVersion(), output.getCommitMessageBytes());
      List<ManifestEntry> inputs = units.get(entry.getKey());
      ManifestEntry unit = inputs.get(0);
      assertEquals(unit.partition(), message.partition());
      assertEquals(unit.bucket(), message.bucket());
      assertEquals(2, message.totalBuckets());
      assertEquals(
          inputs.stream().map(ManifestEntry::file).collect(Collectors.toList()),
          message.compactIncrement().compactBefore());
      List<DataFileMeta> outputs = message.compactIncrement().compactAfter();
      rewritten.put(entry.getKey(), outputs);
      assertFalse(outputs.isEmpty());
      assertEquals(
          inputs.stream().mapToLong(file -> file.file().rowCount()).sum(),
          outputs.stream().mapToLong(DataFileMeta::rowCount).sum());
      for (DataFileMeta file : outputs) {
        assertFalse(inputs.stream().anyMatch(input -> input.fileName().equals(file.fileName())));
        assertTrue(
            inputs.stream()
                .allMatch(
                    input -> Collections.disjoint(input.file().extraFiles(), indexFiles(file))));
        assertEquals(table.schema().id(), file.schemaId());
        verifyIndex(table, unit, file, embedded, indexColumns);
      }
    }
    return rewritten;
  }

  private static Map<String, List<DataFileMeta>> manifestFiles(
      Map<String, List<ManifestEntry>> units) {
    Map<String, List<DataFileMeta>> files = new LinkedHashMap<>();
    units.forEach(
        (unit, entries) ->
            files.put(
                unit, entries.stream().map(ManifestEntry::file).collect(Collectors.toList())));
    return files;
  }

  private static void verifyIndex(
      AppendOnlyFileStoreTable table,
      ManifestEntry unit,
      DataFileMeta file,
      boolean embedded,
      Set<String> columns)
      throws Exception {
    // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
    // paimon-core/src/main/java/org/apache/paimon/io/RowDataFileWriter.java:82-120;
    // paimon-core/src/main/java/org/apache/paimon/io/DataFileIndexWriter.java:151-175.
    // Force each storage layout and resolve the NEW output metadata, never the input index.
    List<String> external = indexFiles(file);
    if (embedded) {
      assertNotNull(file.embeddedIndex());
      assertTrue(file.embeddedIndex().length > 0);
      assertTrue(external.isEmpty());
    } else {
      assertNull(file.embeddedIndex());
      assertEquals(1, external.size());
    }
    try (FileIndexFormat.Reader reader =
        FileIndexFormat.createReader(indexStream(table, unit, file), table.rowType())) {
      Map<String, Map<String, byte[]>> indexes = reader.readAll();
      assertEquals(columns, indexes.keySet());
      for (Map<String, byte[]> column : indexes.values()) {
        assertEquals(Collections.singleton("range-bitmap"), column.keySet());
        assertTrue(column.get("range-bitmap").length > 0);
      }
    }

    AppendOnlyFileStoreTable noIndex =
        (AppendOnlyFileStoreTable)
            table.copy(Collections.singletonMap("file-index.read.enabled", "false"));
    List<String> rows = fileRows(noIndex, unit, file, null);
    assertEquals(file.rowCount(), rows.size());
    List<Query> queries = queries(table);
    try (FileIndexPredicate reader =
        new FileIndexPredicate(indexStream(table, unit, file), table.rowType())) {
      for (Query query : queries) {
        // Same SHA, paimon-common/src/main/java/org/apache/paimon/fileindex/
        // rangebitmap/RangeBitmapFileIndex.java:113-163. These STRING operators return exact
        // row bitmaps; compare positions, not just counts, after duplicates moved across files.
        FileIndexResult result = reader.evaluate(query.predicate);
        assertTrue(result instanceof BitmapIndexResult, query.name + " must decode an index");
        RoaringBitmap32 actual = ((BitmapIndexResult) result).get();
        List<Integer> positions = new ArrayList<>();
        Iterator<Integer> iterator = actual.iterator();
        while (iterator.hasNext()) {
          int position = iterator.next();
          assertTrue(position >= 0 && position < rows.size(), query.name + " invalid row id");
          positions.add(position);
        }
        List<Integer> expectedPositions = new ArrayList<>();
        List<String> expectedRows = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
          if (query.matches.test(hm(rows.get(i)))) {
            expectedPositions.add(i);
            expectedRows.add(rows.get(i));
          }
        }
        assertEquals(expectedPositions, positions, file.fileName() + ": " + query.name);
        assertEquals(
            multiset(expectedRows),
            multiset(fileRows(table, unit, file, query.predicate)),
            query.name + " indexed reader versus unindexed full-row filtering");
      }
    }

    // Same SHA,
    // paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java:263-320;
    // io/FileIndexEvaluator.java:112-139. A broken index must fail only with index reading enabled.
    // Corrupt metadata copies only: this proves real reader usage without altering stored files.
    DataFileMeta brokenIndex =
        embedded
            ? file.copy(new byte[] {0})
            : file.copy(
                Collections.singletonList(
                    "missing-output" + DataFilePathFactory.INDEX_PATH_SUFFIX));
    Predicate filter =
        new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString(hm(rows.get(0))));
    assertThrows(Exception.class, () -> fileRows(table, unit, brokenIndex, filter));
    assertEquals(
        multiset(fileRows(noIndex, unit, file, filter)),
        multiset(fileRows(noIndex, unit, brokenIndex, filter)));
  }

  private static List<Query> queries(AppendOnlyFileStoreTable table) {
    PredicateBuilder builder = new PredicateBuilder(table.rowType());
    List<Query> queries = new ArrayList<>();
    for (int key = 0; key < 16; key++) {
      String value = String.format("%04d", key);
      queries.add(
          new Query(
              "equals " + value, builder.equal(1, BinaryString.fromString(value)), value::equals));
    }
    List<String> values = new ArrayList<>(Arrays.asList("0001", "0007", "0014"));
    for (int key = 1000; key < 1018; key++) {
      values.add(String.valueOf(key));
    }
    // Same SHA, paimon-common/src/main/java/org/apache/paimon/predicate/PredicateBuilder.java:198.
    // Twenty-one literals exercise the native IN visitor; smaller IN lists are lowered to OR.
    queries.add(
        new Query(
            "IN",
            builder.in(
                1, values.stream().map(BinaryString::fromString).collect(Collectors.toList())),
            values::contains));
    BinaryString lower = BinaryString.fromString("0003");
    BinaryString upper = BinaryString.fromString("0012");
    queries.add(
        new Query(
            "open range",
            PredicateBuilder.and(builder.greaterThan(1, lower), builder.lessThan(1, upper)),
            value -> value.compareTo("0003") > 0 && value.compareTo("0012") < 0));
    queries.add(
        new Query(
            "closed range",
            builder.between(1, lower, upper),
            value -> value.compareTo("0003") >= 0 && value.compareTo("0012") <= 0));
    queries.add(
        new Query(
            "left closed range",
            PredicateBuilder.and(builder.greaterOrEqual(1, lower), builder.lessThan(1, upper)),
            value -> value.compareTo("0003") >= 0 && value.compareTo("0012") < 0));
    queries.add(
        new Query(
            "right closed range",
            PredicateBuilder.and(builder.greaterThan(1, lower), builder.lessOrEqual(1, upper)),
            value -> value.compareTo("0003") > 0 && value.compareTo("0012") <= 0));
    queries.add(
        new Query(
            "leading zero is significant",
            builder.equal(1, BinaryString.fromString("7")),
            value -> false));
    queries.add(
        new Query("no match", builder.equal(1, BinaryString.fromString("9999")), value -> false));
    return queries;
  }

  private static SeekableInputStream indexStream(
      AppendOnlyFileStoreTable table, ManifestEntry unit, DataFileMeta file) throws Exception {
    if (file.embeddedIndex() != null) {
      return new ByteArraySeekableStream(file.embeddedIndex());
    }
    DataFilePathFactory paths =
        table.store().pathFactory().createDataFilePathFactory(unit.partition(), unit.bucket());
    return table.fileIO().newInputStream(paths.toAlignedPath(indexFiles(file).get(0), file));
  }

  private static List<String> indexFiles(DataFileMeta file) {
    return file.extraFiles().stream()
        .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
        .collect(Collectors.toList());
  }

  private static List<String> fileRows(
      AppendOnlyFileStoreTable table, ManifestEntry unit, DataFileMeta file, Predicate filter)
      throws Exception {
    DataSplit split =
        DataSplit.builder()
            .withPartition(unit.partition())
            .withBucket(unit.bucket())
            .withTotalBuckets(unit.totalBuckets())
            .withBucketPath(
                table.store().pathFactory().bucketPath(unit.partition(), unit.bucket()).toString())
            .withDataFiles(Collections.singletonList(file))
            .build();
    ReadBuilder builder = table.newReadBuilder();
    if (filter != null) {
      builder.withFilter(filter);
    }
    try (RecordReader<InternalRow> reader = builder.newRead().executeFilter().createReader(split)) {
      return collectRows(reader);
    }
  }

  private static List<String> snapshotRows(AppendOnlyFileStoreTable table, long snapshot)
      throws Exception {
    ReadBuilder builder =
        table
            .copy(Collections.singletonMap("scan.snapshot-id", String.valueOf(snapshot)))
            .newReadBuilder();
    try (RecordReader<InternalRow> reader =
        builder.newRead().createReader(builder.newScan().plan())) {
      return multiset(collectRows(reader));
    }
  }

  private static List<String> collectRows(RecordReader<InternalRow> reader) throws Exception {
    List<String> rows = new ArrayList<>();
    reader.forEachRemaining(
        row ->
            rows.add(
                row.getInt(0)
                    + "|"
                    + row.getString(1)
                    + "|"
                    + row.getInt(2)
                    + "|"
                    + row.getInt(3)
                    + "|"
                    + row.getString(4)));
    return rows;
  }

  private static List<String> multiset(List<String> rows) {
    List<String> sorted = new ArrayList<>(rows);
    Collections.sort(sorted);
    return sorted;
  }

  private static String hm(String row) {
    return row.split("\\|", 3)[1];
  }

  private static String threshold(boolean embedded) {
    return embedded ? "1 mb" : "0 b";
  }

  private static class Query {
    private final String name;
    private final Predicate predicate;
    private final java.util.function.Predicate<String> matches;

    private Query(String name, Predicate predicate, java.util.function.Predicate<String> matches) {
      this.name = name;
      this.predicate = predicate;
      this.matches = matches;
    }
  }
}
