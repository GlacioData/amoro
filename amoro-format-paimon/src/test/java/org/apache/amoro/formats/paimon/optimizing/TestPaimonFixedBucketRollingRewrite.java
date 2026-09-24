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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.Snapshot;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class TestPaimonFixedBucketRollingRewrite {
  @ParameterizedTest
  @CsvSource({"true,true", "false,false"})
  void commitsRollingOutputsWithModeSpecificSequences(
      boolean ordered, boolean embedded, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(
            warehouse,
            false,
            ordered,
            Collections.singletonMap(
                "file-index.in-manifest-threshold", embedded ? "1 mb" : "0 b"))) {
      // Seed two large files per bucket before lowering the output target. More than 1000
      // records cross RollingFileWriter's periodic size check; assert actual rolling below.
      fixture.append(0, 160);
      fixture.append(160, 160);
      List<ManifestEntry> selected = fixture.units().get("0:0");
      assertEquals(2, selected.size());
      List<DataFileMeta> before =
          selected.stream().map(ManifestEntry::file).collect(Collectors.toList());
      List<String> beforeRows = fixture.rows(selected.get(0), before);
      List<String> completeRows = fixture.snapshotRows();
      long sourceSnapshot = fixture.table.latestSnapshot().get().id();
      fixture.catalog.alterTable(
          fixture.identifier,
          Collections.singletonList(SchemaChange.setOption("target-file-size", "1 kb")),
          false);
      AppendOnlyFileStoreTable table =
          (AppendOnlyFileStoreTable) fixture.catalog.getTable(fixture.identifier);

      // Native reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8.
      // BaseAppendFileStoreWrite.java:187-205 supplies a NEW LongCounter(first input min)
      // for each RowDataFileWriter (RowDataRollingFileWriter.java:48-66 and
      // RowDataFileWriter.java:71). Raw rolling outputs therefore reset, not accumulate.
      List<DataFileMeta> nativeAfter;
      BaseAppendFileStoreWrite nativeWrite = table.store().newWrite("rolling-native-control");
      try {
        nativeAfter =
            nativeWrite.compactRewrite(
                selected.get(0).partition(), selected.get(0).bucket(), null, before);
      } finally {
        nativeWrite.close();
      }
      assertTrue(nativeAfter.size() > 1, "The real native writer must roll, not merely rewrite");
      assertSequences(nativeAfter, before.get(0).minSequenceNumber(), false);
      assertRows(beforeRows, fixture.rows(selected.get(0), nativeAfter), ordered);
      assertEquals(sourceSnapshot, table.latestSnapshot().get().id());

      PaimonTable wrapper =
          new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), table);
      PaimonFixedBucketTask fixed =
          PaimonFixedBucketTask.from(
              wrapper,
              selected,
              sourceSnapshot,
              OptimizingType.FULL,
              PaimonAppendRewriteOptions.from(wrapper.properties()));
      String commitUser = "rolling-amoro-test";
      long commitIdentifier = 101L;
      PaimonCompactionInput input =
          PaimonCompactionInput.forFixedBucket(wrapper, fixed, commitUser, "", commitIdentifier);
      // Regression: ordered output needs cumulative metadata; unordered native metadata
      // must remain acceptable to the real commit guard even when multiple files roll.
      PaimonCompactionOutput output = new PaimonCompactionExecutor(input).execute();
      CommitMessageImpl message =
          (CommitMessageImpl)
              new CommitMessageSerializer()
                  .deserialize(output.getCommitMessageVersion(), output.getCommitMessageBytes());
      assertEquals(before, message.compactIncrement().compactBefore());
      List<DataFileMeta> after = message.compactIncrement().compactAfter();
      assertTrue(after.size() > 1);
      assertEquals(nativeAfter.size(), after.size());
      assertEquals(
          nativeAfter.stream().map(DataFileMeta::rowCount).collect(Collectors.toList()),
          after.stream().map(DataFileMeta::rowCount).collect(Collectors.toList()));
      // Same native SHA: RawFileSplitRead.java:214-224 concatenates supplied inputs;
      // RollingFileWriterImpl.java:119-133 appends outputs in close order. Ordered adaptation
      // must retain that physical stream while making metadata safe for the min-sequence
      // sort in AppendOnlySplitGenerator.java:52-58. Unordered retains native reset metadata.
      assertSequences(after, before.get(0).minSequenceNumber(), ordered);
      List<DataFileMeta> corruptedAfter = new ArrayList<>(after);
      DataFileMeta second = after.get(1);
      corruptedAfter.set(
          1,
          second.assignSequenceNumber(
              second.minSequenceNumber() + 1, second.maxSequenceNumber() + 1));
      CommitMessageImpl corrupted =
          new CommitMessageImpl(
              message.partition(),
              message.bucket(),
              message.totalBuckets(),
              message.newFilesIncrement(),
              new CompactIncrement(before, corruptedAfter, Collections.emptyList()));
      assertThrows(
          IllegalArgumentException.class,
          () -> PaimonFixedBucketOutputValidator.validate(table, fixed, output, corrupted));
      assertRows(beforeRows, fixture.rows(selected.get(0), after), ordered);
      assertEquals(beforeRows.size(), output.getProducedRecordCount());
      for (DataFileMeta file : after) {
        long external =
            file.extraFiles().stream()
                .filter(name -> name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                .count();
        if (embedded) {
          assertNotNull(file.embeddedIndex());
          assertEquals(0, external);
        } else {
          assertNull(file.embeddedIndex());
          assertEquals(1, external);
        }
      }

      PaimonCompactionTask task = new PaimonCompactionTask(1L, "", input, Collections.emptyMap());
      ByteBuffer serialized = SerializationUtil.simpleSerialize(output);
      byte[] bytes = new byte[serialized.remaining()];
      serialized.get(bytes);
      task.setOutputBytes(bytes);
      new PaimonTableCommit(table, Collections.singletonList(task), commitUser, commitIdentifier)
          .commit();
      Snapshot committed = table.latestSnapshot().get();
      assertEquals(sourceSnapshot + 1, committed.id());
      assertEquals(Snapshot.CommitKind.COMPACT, committed.commitKind());
      assertEquals(commitUser, committed.commitUser());
      assertEquals(commitIdentifier, committed.commitIdentifier());
      assertEquals(completeRows, fixture.snapshotRows());
      List<DataFileMeta> published =
          fixture.units().get("0:0").stream().map(ManifestEntry::file).collect(Collectors.toList());
      assertEquals(after.size(), published.size());
      assertRows(beforeRows, fixture.rows(selected.get(0), published), ordered);
    }
  }

  private static void assertSequences(List<DataFileMeta> files, long start, boolean contiguous) {
    long next = start;
    for (DataFileMeta file : files) {
      assertEquals(next, file.minSequenceNumber());
      assertEquals(next + file.rowCount() - 1, file.maxSequenceNumber());
      if (contiguous) {
        next += file.rowCount();
      }
    }
  }

  private static void assertRows(List<String> expected, List<String> actual, boolean ordered) {
    if (ordered) {
      assertEquals(
          expected, actual, "Ordered rolling output must preserve the physical row stream");
    }
    List<String> expectedMultiset = new ArrayList<>(expected);
    List<String> actualMultiset = new ArrayList<>(actual);
    Collections.sort(expectedMultiset);
    Collections.sort(actualMultiset);
    assertEquals(
        expectedMultiset, actualMultiset, "Rolling must preserve duplicate multiplicities");
  }
}
