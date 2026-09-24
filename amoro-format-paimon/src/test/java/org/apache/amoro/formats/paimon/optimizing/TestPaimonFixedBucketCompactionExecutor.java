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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TestPaimonFixedBucketCompactionExecutor {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void closesWriterAndPreservesNativeFailure(boolean rewriteFails, @TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      PaimonCompactionInput original = input(fixture, fixture.units().values().iterator().next());
      BaseAppendFileStoreWrite write = mock(BaseAppendFileStoreWrite.class);
      IOException rewriteFailure = new IOException("rewrite-probe");
      IOException closeFailure = new IOException("close-probe");
      if (rewriteFails) {
        doThrow(rewriteFailure).when(write).compactRewrite(any(), anyInt(), isNull(), anyList());
      } else {
        when(write.compactRewrite(any(), anyInt(), isNull(), anyList()))
            .thenReturn(Collections.emptyList());
      }
      doThrow(closeFailure).when(write).close();
      AppendOnlyFileStore store = mock(AppendOnlyFileStore.class);
      when(store.options()).thenReturn(fixture.table.coreOptions());
      when(store.bucketMode()).thenReturn(fixture.table.bucketMode());
      when(store.newWrite(anyString())).thenReturn(write);
      AppendOnlyFileStoreTable table = spy(fixture.table);
      doReturn(store).when(table).store();
      PaimonTable wrapper = new PaimonTable(original.getTable().id(), table);
      PaimonCompactionInput input =
          new PaimonCompactionInput(
              wrapper,
              original.getTaskBytes(),
              original.getSerializerVersion(),
              original.getCommitUser(),
              original.getPartitionPath(),
              original.getTargetSnapshotId(),
              original.getCommitIdentifier());
      IllegalStateException error =
          assertThrows(
              IllegalStateException.class, () -> new PaimonCompactionExecutor(input).execute());
      assertEquals(rewriteFails ? rewriteFailure : closeFailure, error.getCause());
      verify(write).close();
      if (rewriteFails) {
        assertEquals(closeFailure, rewriteFailure.getSuppressed()[0]);
      }
      assertEquals(1, fixture.table.latestSnapshot().get().id());
    }
  }

  @ParameterizedTest
  @CsvSource({"true,true", "true,false", "false,true", "false,false"})
  void rewritesExactFilesWithoutChangingOtherUnits(
      boolean partitioned, boolean ordered, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, partitioned, ordered)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      fixture.append(2, 1);
      List<String> originalRows = fixture.snapshotRows();
      Map<String, List<ManifestEntry>> units = fixture.units();
      for (List<ManifestEntry> unit : units.values()) {
        List<ManifestEntry> selected = unit.subList(0, 2);
        List<DataFileMeta> before =
            selected.stream().map(ManifestEntry::file).collect(Collectors.toList());
        PaimonCompactionInput input = input(fixture, selected);
        long snapshot = fixture.table.latestSnapshot().get().id();
        PaimonCompactionOutput output = new PaimonCompactionExecutor(input).execute();
        CommitMessageImpl message = message(output);
        assertEquals(unit.get(0).bucket(), message.bucket());
        assertEquals(Integer.valueOf(2), message.totalBuckets());
        assertEquals(unit.get(0).partition(), message.partition());
        assertEquals(before, message.compactIncrement().compactBefore());
        assertTrue(message.newFilesIncrement().isEmpty());
        List<DataFileMeta> after = message.compactIncrement().compactAfter();
        assertEquals(2, output.getCompactedFileCount());
        assertEquals(1, output.getProducedFileCount());
        assertEquals(
            before.stream().mapToLong(DataFileMeta::fileSize).sum(), output.getCompactedFileSize());
        assertEquals(
            after.stream().mapToLong(DataFileMeta::fileSize).sum(), output.getProducedFileSize());
        assertEquals(
            before.stream().mapToLong(DataFileMeta::rowCount).sum(),
            output.getCompactedRecordCount());
        assertEquals(output.getCompactedRecordCount(), output.getProducedRecordCount());
        // Paimon ac223f47, operation/BaseAppendFileStoreWrite.java:187.
        // Ordered inputs retain record order; unordered inputs retain duplicate multiplicities.
        List<String> oldRows = fixture.rows(unit.get(0), before);
        List<String> newRows = fixture.rows(unit.get(0), after);
        if (!ordered) {
          Collections.sort(oldRows);
          Collections.sort(newRows);
        }
        assertEquals(oldRows, newRows);
        assertEquals(
            snapshot,
            fixture.table.latestSnapshot().get().id(),
            "Executor must not commit its own output");
        fixture.commit(message);
        assertEquals(originalRows, fixture.snapshotRows());
        assertTrue(
            fixture.units().values().stream()
                .flatMap(List::stream)
                .anyMatch(entry -> entry.fileName().equals(unit.get(2).fileName())));
      }
      assertEquals("true", fixture.table.options().get("write-only"));
      assertEquals(originalRows, fixture.snapshotRows());
    }
  }

  @Test
  void publishedTaskKeepsBoundOptionsAfterAllowedDdl(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      PaimonCompactionInput input = input(fixture, fixture.units().values().iterator().next());
      List<String> before = fixture.snapshotRows();
      List<SchemaChange> changes = new ArrayList<>();
      changes.add(SchemaChange.setOption("bucket-append-ordered", "false"));
      changes.add(SchemaChange.setOption("file-index.range-bitmap.columns", "payload"));
      fixture.catalog.alterTable(fixture.identifier, changes, false);
      // Same SHA, table/AbstractFileStoreTable.java:382.
      // An already-published task does not reload changed options midway through execution.
      CommitMessageImpl message = message(new PaimonCompactionExecutor(input).execute());
      assertEquals(
          "hm", input.getTable().originalTable().options().get("file-index.range-bitmap.columns"));
      for (DataFileMeta file : message.compactIncrement().compactAfter()) {
        assertEquals(fixture.table.schema().id(), file.schemaId());
      }
      fixture.commit(message);
      assertEquals(before, fixture.snapshotRows());
      assertEquals(
          "payload",
          fixture
              .catalog
              .getTable(fixture.identifier)
              .options()
              .get("file-index.range-bitmap.columns"));
    }
  }

  @Test
  void refusesChangedExecutionBindingBeforeWriting(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      PaimonCompactionInput input = input(fixture, fixture.units().values().iterator().next());
      PaimonTable changed =
          new PaimonTable(
              input.getTable().id(),
              fixture.table.copy(
                  Collections.singletonMap("file-index.range-bitmap.columns", "payload")));
      PaimonCompactionInput wrongOptions =
          new PaimonCompactionInput(
              changed,
              input.getTaskBytes(),
              input.getSerializerVersion(),
              input.getCommitUser(),
              input.getPartitionPath(),
              input.getTargetSnapshotId(),
              input.getCommitIdentifier());
      assertThrows(
          IllegalStateException.class, () -> new PaimonCompactionExecutor(wrongOptions).execute());
      PaimonCompactionInput wrongSnapshot =
          new PaimonCompactionInput(
              input.getTable(),
              input.getTaskBytes(),
              input.getSerializerVersion(),
              input.getCommitUser(),
              input.getPartitionPath(),
              input.getTargetSnapshotId() + 1,
              input.getCommitIdentifier());
      assertThrows(
          IllegalStateException.class, () -> new PaimonCompactionExecutor(wrongSnapshot).execute());
      assertEquals(1, fixture.table.latestSnapshot().get().id());
    }
  }

  @Test
  void nativeReadFailureIsNotConvertedToSuccess(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true)) {
      fixture.append(0, 1);
      PaimonFixedBucketTask valid =
          TestPaimonFixedBucketTaskSerialization.task(
              fixture, fixture.units().values().iterator().next());
      List<DataFileMeta> missing =
          Collections.singletonList(valid.files().get(0).rename("data-does-not-exist.orc"));
      PaimonFixedBucketTask task =
          new PaimonFixedBucketTask(
              valid.tableId(),
              valid.tablePath(),
              valid.partition(),
              valid.bucket(),
              valid.totalBuckets(),
              valid.snapshotId(),
              valid.schemaId(),
              valid.ordered(),
              valid.rewriteAll(),
              valid.optimizingType(),
              valid.rewriteSource(),
              valid.rewriteFingerprint(),
              missing);
      PaimonTable table =
          new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), fixture.table);
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  new PaimonCompactionExecutor(
                          PaimonCompactionInput.forFixedBucket(
                              table, task, "missing-input", "", 11))
                      .execute());
      assertNotNull(failure.getCause());
      assertEquals(1, fixture.table.latestSnapshot().get().id());
    }
  }

  private static PaimonCompactionInput input(
      PaimonFixedBucketTestSupport fixture, List<ManifestEntry> selected) {
    PaimonTable table =
        new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), fixture.table);
    PaimonFixedBucketTask task =
        PaimonFixedBucketTask.from(
            table,
            selected,
            fixture.table.latestSnapshot().get().id(),
            OptimizingType.MINOR,
            PaimonAppendRewriteOptions.from(table.properties()));
    return PaimonCompactionInput.forFixedBucket(table, task, "fixed-executor-test", "", 10);
  }

  private static CommitMessageImpl message(PaimonCompactionOutput output) throws Exception {
    return (CommitMessageImpl)
        new CommitMessageSerializer()
            .deserialize(output.getCommitMessageVersion(), output.getCommitMessageBytes());
  }
}
