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

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TriFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class TestPaimonPrimaryKeyCommitGuard {
  @ParameterizedTest
  @ValueSource(strings = {"added", "changed", "removed"})
  void everyDvTransitionRejectsAllBucketsAndCachesFailure(String transition) throws Exception {
    Harness h = new Harness(2, !transition.equals("added"));
    h.indexState.put(
        key(1),
        transition.equals("removed")
            ? Collections.emptyList()
            : Collections.singletonList(index("replacement", 8, 3L)));
    h.guard.prepare(h.messages);
    assertThrows(PaimonPrimaryKeyCommitGuard.ReadConflict.class, h.guard::preflight);
    SnapshotCommit catalog = mock(SnapshotCommit.class);
    SnapshotCommit wrapped = h.guard.wrap(catalog, h.manager);
    for (int i = 0; i < 4; i++) {
      assertThrows(
          PaimonPrimaryKeyCommitGuard.ReadConflict.class,
          () -> wrapped.commit(h.candidate(11), "main", Collections.emptyList()));
    }
    verifyNoInteractions(catalog);
    verify(h.scan).readFileIterator();
    verify(h.indexes).scan(eq(h.snapshot), eq(DELETION_VECTORS_INDEX), anySet());
  }

  @Test
  void lookupOnlyFileIsAlsoAReadDependency() {
    Harness h = new Harness(1, false);
    h.entries.removeIf(e -> e.file().fileName().startsWith("lookup"));
    h.guard.prepare(h.messages);
    assertThrows(PaimonPrimaryKeyCommitGuard.ReadConflict.class, h.guard::preflight);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 64})
  void parentIsScannedOnceForAllBucketsAndUnrelatedUpdatesAreAllowed(int bucketCount)
      throws Exception {
    Harness h = new Harness(bucketCount, true);
    h.entries.add(
        ManifestEntry.create(FileKind.ADD, BinaryRow.EMPTY_ROW, 999, 1024, file("other")));
    h.indexState.put(key(999), Collections.singletonList(index("other-dv", 99, 2L)));
    h.guard.prepare(h.messages);
    h.guard.preflight();
    h.guard.preflight();
    SnapshotCommit catalog = mock(SnapshotCommit.class);
    when(catalog.commit(any(), anyString(), anyList())).thenReturn(false);
    SnapshotCommit wrapped = h.guard.wrap(catalog, h.manager);
    assertFalse(wrapped.commit(h.candidate(11), "main", Collections.emptyList()));
    verify(h.scan).readFileIterator();
    verify(h.scan).dropStats();
    verify(h.indexes).scan(eq(h.snapshot), eq(DELETION_VECTORS_INDEX), anySet());
    ArgumentCaptor<TriFilter> filter = ArgumentCaptor.forClass(TriFilter.class);
    verify(h.scan).withTotalAwareBucketFilter(filter.capture());
    assertTrue(filter.getValue().test(BinaryRow.EMPTY_ROW, 0, 1024));
    assertFalse(filter.getValue().test(BinaryRow.EMPTY_ROW, 999, 1024));
    int evidenceBytes =
        SerializationUtil.simpleSerialize(h.task.getOutput().getReadDependency()).remaining();
    assertTrue(
        evidenceBytes < 2000 + 350 * bucketCount,
        "Evidence must remain bounded per bucket: " + evidenceBytes);
    // A new candidate parent requires one new batch scan, even after a successful preflight.
    Snapshot next = h.candidate(11);
    when(h.manager.snapshot(11L)).thenReturn(next);
    wrapped.commit(h.candidate(12), "main", Collections.emptyList());
    verify(h.scan, times(2)).readFileIterator();
    verify(h.indexes).scan(eq(next), eq(DELETION_VECTORS_INDEX), anySet());
  }

  @Test
  void readFailureIsRetryableAndIsNotCachedAsAConflict() {
    Harness h = new Harness(1, false);
    when(h.scan.readFileIterator())
        .thenThrow(new IllegalStateException(new IOException("read failed")))
        .thenAnswer(call -> h.entries.iterator());
    h.guard.prepare(h.messages);
    assertThrows(IllegalStateException.class, h.guard::preflight);
    assertFalse(h.guard.rejected());
    h.guard.preflight();
    verify(h.scan, times(2)).readFileIterator();
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "duplicate", "uncovered", "write", "foreign-file"})
  void invalidCoverageAndNonCompactMessagesAreRejected(String corruption) {
    Harness h = new Harness(1, false);
    if (corruption.equals("missing")) {
      h.task.getOutput().setReadDependency(null);
    }
    if (corruption.equals("duplicate")) {
      h.messages.add(h.messages.get(0));
    }
    if (corruption.equals("uncovered")) {
      h.messages.add(
          message(1, Collections.singletonList(file("uncovered")), DataIncrement.emptyIncrement()));
    }
    if (corruption.equals("write")) {
      h.messages.set(
          0,
          message(
              0,
              Collections.emptyList(),
              new DataIncrement(
                  Collections.singletonList(file("insert")),
                  Collections.emptyList(),
                  Collections.emptyList())));
    }
    if (corruption.equals("foreign-file")) {
      h.messages.set(
          0,
          message(0, Collections.singletonList(file("foreign")), DataIncrement.emptyIncrement()));
    }
    assertThrows(PaimonPrimaryKeyCommitGuard.ReadConflict.class, () -> h.guard.prepare(h.messages));
    verifyNoInteractions(h.scan, h.indexes);
  }

  @ParameterizedTest
  @ValueSource(strings = {"version", "uuid", "location", "branch", "snapshotId"})
  void invalidContextCannotBeMaskedByAnAlreadyPublishedIdentity(String field) throws Exception {
    Harness h = new Harness(1, false);
    PaimonPrimaryKeyReadDependency dependency = h.task.getOutput().getReadDependency();
    Field corrupted = PaimonPrimaryKeyReadDependency.class.getDeclaredField(field);
    corrupted.setAccessible(true);
    if (field.equals("version")) {
      corrupted.set(dependency, 2);
    } else if (field.equals("snapshotId")) {
      corrupted.set(dependency, 9L);
    } else {
      corrupted.set(dependency, "another");
    }
    doReturn(h.candidate(11)).when(h.manager).latestSnapshot();
    assertThrows(PaimonPrimaryKeyCommitGuard.ReadConflict.class, h.guard::inspectPublication);
    verifyNoInteractions(h.scan, h.indexes);
  }

  @Test
  void historyProbeBudgetAndExpiredIntervalNeverProveAbsence() throws Exception {
    Harness h = new Harness(1, false);
    Snapshot latest = h.candidate(400);
    when(latest.commitUser()).thenReturn("other");
    when(h.manager.latestSnapshot()).thenReturn(latest);
    when(h.manager.tryGetSnapshot(anyLong()))
        .thenAnswer(
            call -> {
              Snapshot s = h.candidate(call.getArgument(0));
              when(s.commitUser()).thenReturn("other");
              return s;
            });
    assertEquals(PaimonPrimaryKeyCommitGuard.Publication.UNKNOWN, h.guard.inspectPublication());
    verify(h.manager, times(255)).tryGetSnapshot(anyLong());
    doReturn(h.candidate(12)).when(h.manager).latestSnapshot();
    // An exact COMPACT is sufficient even if older history has expired.
    assertEquals(PaimonPrimaryKeyCommitGuard.Publication.PUBLISHED, h.guard.inspectPublication());
  }

  @Test
  void capturedModeCannotBeBypassedByDisablingDvAtCommit() {
    Harness h = new Harness(1, false);
    when(h.table.options()).thenReturn(Collections.emptyMap());
    assertTrue(PaimonPrimaryKeyCommitGuard.applies(h.table, Collections.singletonList(h.task)));
    assertThrows(PaimonPrimaryKeyCommitGuard.ReadConflict.class, () -> h.guard.prepare(h.messages));
    h.task.getOutput().setReadDependency(null);
    assertFalse(PaimonPrimaryKeyCommitGuard.applies(h.table, Collections.singletonList(h.task)));
    verifyNoInteractions(h.scan, h.indexes);
  }

  @Test
  void dvFingerprintIncludesRangesAndIgnoresIterationOrder() {
    IndexFileMeta first = index("dv", 7, 1L);
    IndexFileMeta second = index("dv2", 17, null);
    assertEquals(
        PaimonPrimaryKeyReadDependency.dvIdentities(Arrays.asList(first, second)),
        PaimonPrimaryKeyReadDependency.dvIdentities(Arrays.asList(second, first)));
    assertNotEquals(
        PaimonPrimaryKeyReadDependency.dvIdentities(Collections.singletonList(first)),
        PaimonPrimaryKeyReadDependency.dvIdentities(Collections.singletonList(index("dv", 8, 1L))));
    assertNotEquals(
        PaimonPrimaryKeyReadDependency.dvIdentities(Collections.singletonList(first)),
        PaimonPrimaryKeyReadDependency.dvIdentities(Collections.singletonList(index("dv", 7, 2L))));
    LinkedHashMap<String, DeletionVectorMeta> reversed = new LinkedHashMap<>();
    reversed.put("z", new DeletionVectorMeta("z", 20, 8, 1L));
    reversed.putAll(first.dvRanges());
    LinkedHashMap<String, DeletionVectorMeta> forward = new LinkedHashMap<>(first.dvRanges());
    forward.put("z", new DeletionVectorMeta("z", 20, 8, 1L));
    assertEquals(
        PaimonPrimaryKeyReadDependency.dvIdentities(
            Collections.singletonList(
                new IndexFileMeta("DELETION_VECTORS", "dv", 50, 2, forward, null))),
        PaimonPrimaryKeyReadDependency.dvIdentities(
            Collections.singletonList(
                new IndexFileMeta("DELETION_VECTORS", "dv", 50, 2, reversed, null))));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "fileName",
        "externalPath",
        "level",
        "schemaId",
        "fileSize",
        "rowCount",
        "minSequenceNumber",
        "maxSequenceNumber"
      })
  void everyReadIdentityFieldMatters(String field) {
    DataFileMeta before = file("same");
    DataFileMeta after = file("same");
    switch (field) {
      case "fileName":
        when(after.fileName()).thenReturn("different");
        break;
      case "externalPath":
        when(after.externalPath()).thenReturn(Optional.of("/other"));
        break;
      case "level":
        when(after.level()).thenReturn(3);
        break;
      case "schemaId":
        when(after.schemaId()).thenReturn(2L);
        break;
      case "fileSize":
        when(after.fileSize()).thenReturn(2L);
        break;
      case "rowCount":
        when(after.rowCount()).thenReturn(2L);
        break;
      case "minSequenceNumber":
        when(after.minSequenceNumber()).thenReturn(2L);
        break;
      case "maxSequenceNumber":
        when(after.maxSequenceNumber()).thenReturn(2L);
        break;
      default:
        throw new AssertionError(field);
    }
    assertNotEquals(
        PaimonPrimaryKeyReadDependency.fileIdentity(before),
        PaimonPrimaryKeyReadDependency.fileIdentity(after));
  }

  @Test
  void baselineOutputStillDeserializesWithoutFabricatedReadEvidence() {
    // Serialized with the unchanged dev@faf0524ae output class (serialVersionUID = 1).
    String legacy =
        "rO0ABXNyAFNvcmcuYXBhY2hlLmFtb3JvLmZvcm1hdHMucGFpbW9uLm9wdGltaXppbmcucHJpbWFyeS5QYWltb25QcmltYXJ5"
            + "S2V5Q29tcGFjdGlvbk91dHB1dAAAAAAAAAABAgAJSQAUY29tcGFjdGVkQnVja2V0Q291bnRKABJjb21wYWN0ZWRGaWxlQ291"
            + "bnRKABFjb21wYWN0ZWRGaWxlU2l6ZUoAFGNvbXBhY3RlZFJlY29yZENvdW50SgARcHJvZHVjZWRGaWxlQ291bnRKABBwcm9k"
            + "dWNlZEZpbGVTaXplSgATcHJvZHVjZWRSZWNvcmRDb3VudEwAFmNvbW1pdE1lc3NhZ2VCeXRlc0xpc3R0ABBMamF2YS91dGls"
            + "L0xpc3Q7TAAKc2tpcFJlYXNvbnQAEkxqYXZhL2xhbmcvU3RyaW5nO3hwAAAAAQAAAAAAAAACAAAAAAAAAAMAAAAAAAAABAAA"
            + "AAAAAAAFAAAAAAAAAAYAAAAAAAAAAHNyAB9qYXZhLnV0aWwuQ29sbGVjdGlvbnMkRW1wdHlMaXN0ergXtDynnt4CAAB4cHA=";
    PaimonPrimaryKeyCompactionOutput output =
        SerializationUtil.simpleDeserialize(java.util.Base64.getDecoder().decode(legacy));
    org.junit.jupiter.api.Assertions.assertNull(output.getReadDependency());
    assertEquals(1, output.getCompactedBucketCount());
    assertEquals(4, output.getCompactedRecordCount());
    assertEquals(5, output.getProducedFileCount());
  }

  static Pair<BinaryRow, Integer> key(int bucket) {
    return Pair.of(BinaryRow.EMPTY_ROW, bucket);
  }

  static IndexFileMeta index(String name, int offset, Long cardinality) {
    LinkedHashMap<String, DeletionVectorMeta> ranges = new LinkedHashMap<>();
    ranges.put("read", new DeletionVectorMeta("read", offset, 8, cardinality));
    return new IndexFileMeta(DELETION_VECTORS_INDEX, name, 40, 1, ranges, null);
  }

  static DataFileMeta file(String name) {
    DataFileMeta file = mock(DataFileMeta.class);
    when(file.fileName()).thenReturn(name);
    when(file.externalPath()).thenReturn(Optional.empty());
    return file;
  }

  static CommitMessage message(int bucket, List<DataFileMeta> before, DataIncrement data) {
    return new CommitMessageImpl(
        BinaryRow.EMPTY_ROW,
        bucket,
        1024,
        data,
        new CompactIncrement(
            before, Collections.singletonList(file("output-" + bucket)), Collections.emptyList()));
  }

  static final class Harness {
    final FileStoreTable table = mock(FileStoreTable.class);
    final SnapshotManager manager = mock(SnapshotManager.class);
    final Snapshot snapshot = candidate(10);
    final FileStoreScan scan = mock(FileStoreScan.class, RETURNS_SELF);
    final IndexFileHandler indexes = mock(IndexFileHandler.class);
    final List<ManifestEntry> entries = new ArrayList<>();
    final Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> indexState = new HashMap<>();
    final List<CommitMessage> messages = new ArrayList<>();
    final PaimonPrimaryKeyCompactionTask task;
    final PaimonPrimaryKeyCommitGuard guard;

    Harness(int count, boolean existingDv) {
      when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
      when(table.options())
          .thenReturn(Collections.singletonMap("deletion-vectors.enabled", "true"));
      when(table.location()).thenReturn(new Path("/table"));
      when(table.uuid()).thenReturn("uuid");
      when(table.name()).thenReturn("table");
      when(table.snapshotManager()).thenReturn(manager);
      when(manager.branch()).thenReturn("main");
      when(manager.latestSnapshot()).thenReturn(snapshot);
      when(manager.snapshot(10L)).thenReturn(snapshot);
      FileStore<?> store = mock(FileStore.class);
      doReturn(store).when(table).store();
      doReturn(scan).when(store).newScan();
      when(store.newIndexFileHandler()).thenReturn(indexes);
      when(scan.readFileIterator()).thenAnswer(call -> entries.iterator());
      when(indexes.scan(any(Snapshot.class), eq(DELETION_VECTORS_INDEX), anySet()))
          .thenAnswer(call -> indexState);
      Map<Integer, RestoreFiles> restored = new HashMap<>();
      List<PaimonBucketCompactionUnit> units = new ArrayList<>();
      for (int bucket = 0; bucket < count; bucket++) {
        List<DataFileMeta> files = Arrays.asList(file("read-" + bucket), file("lookup-" + bucket));
        for (DataFileMeta file : files) {
          entries.add(ManifestEntry.create(FileKind.ADD, BinaryRow.EMPTY_ROW, bucket, 1024, file));
        }
        List<IndexFileMeta> dv =
            existingDv
                ? Collections.singletonList(index("dv-" + bucket, 7, 1L))
                : Collections.emptyList();
        restored.put(bucket, new RestoreFiles(snapshot, 1024, files, null, dv));
        indexState.put(key(bucket), dv);
        units.add(
            new PaimonBucketCompactionUnit(
                SerializationUtils.serializeBinaryRow(BinaryRow.EMPTY_ROW), bucket, 2, 0, 0, 1));
        messages.add(
            message(
                bucket, Collections.singletonList(files.get(0)), DataIncrement.emptyIncrement()));
      }
      PaimonPrimaryKeyCompactionInput input = mock(PaimonPrimaryKeyCompactionInput.class);
      when(input.getTargetSnapshotId()).thenReturn(10L);
      when(input.getUnits()).thenReturn(units);
      task = new PaimonPrimaryKeyCompactionTask(1, "bucket", input, new HashMap<>());
      PaimonPrimaryKeyCompactionOutput output = new PaimonPrimaryKeyCompactionOutput();
      output.setReadDependency(
          new PaimonPrimaryKeyReadDependency(
              table, 10, Collections.singletonMap(BinaryRow.EMPTY_ROW, restored)));
      task.setOutput(output);
      guard =
          new PaimonPrimaryKeyCommitGuard(table, Collections.singletonList(task), "user", 99, 10);
    }

    Snapshot candidate(long id) {
      Snapshot snapshot = mock(Snapshot.class);
      when(snapshot.id()).thenReturn(id);
      when(snapshot.toJson()).thenReturn("snapshot-" + id);
      when(snapshot.commitUser()).thenReturn("user");
      when(snapshot.commitIdentifier()).thenReturn(99L);
      when(snapshot.commitKind()).thenReturn(Snapshot.CommitKind.COMPACT);
      return snapshot;
    }
  }
}
