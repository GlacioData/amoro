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

package org.apache.amoro.formats.paimon.tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.amoro.formats.paimon.PaimonCatalog;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.table.tag.CopyTagRequest;
import org.apache.amoro.table.tag.CreateTagRequest;
import org.apache.amoro.table.tag.DeleteTagRequest;
import org.apache.amoro.table.tag.TableTagInfo;
import org.apache.amoro.table.tag.TableTagOperations;
import org.apache.amoro.table.tag.TableVersionRef;
import org.apache.amoro.table.tag.TagOperationException;
import org.apache.amoro.table.tag.TagRetentionRequest;
import org.apache.amoro.table.tag.TagScope;
import org.apache.amoro.table.tag.UpdateTagRetentionRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Real SDK mutations must remain behind the public table API and business preconditions. */
class TestPaimonNativeTagApi {
  private static final Duration DEFAULT_RETENTION = Duration.ofHours(2);
  private static final Duration CUSTOM_RETENTION = Duration.ofHours(3);

  @TempDir java.nio.file.Path warehouse;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void nativeApiKeepsExpiredSnapshotAndDataForPrimaryKeyAndAppendTables(boolean primaryKey)
      throws Exception {
    try (Catalog catalog = catalog(primaryKey)) {
      PaimonTable owner = owner(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long originalId = commit(nativeTable, 1);
      FileStoreTable refreshed = spy(nativeTable);
      TableTagOperations operations = new PaimonTagOperations(owner, () -> refreshed, false);

      // There is no OS-specific update gate: the SDK owns the underlying storage operation.
      assertTrue(operations.capabilities().getActions().contains("UPDATE_RETENTION"));
      TableTagInfo source = operations.createTag(create(operations, "source", originalId));
      verify(refreshed, times(1)).createTag("source", originalId, DEFAULT_RETENTION);
      assertEquals(
          DEFAULT_RETENTION, nativeTable.tagManager().getOrThrow("source").getTagTimeRetained());
      String originalSnapshot =
          nativeTable.tagManager().getOrThrow("source").trimToSnapshot().toJson();
      long latestId = commit(nativeTable, 2);
      assertNotEquals(originalId, latestId);
      nativeTable.snapshotManager().deleteSnapshot(originalId);
      nativeTable.snapshotManager().commitEarliestHint(latestId);
      assertFalse(nativeTable.snapshotManager().snapshotExists(originalId));

      TableTagInfo copied = operations.copyTag(copy(source, "copied"));
      verify(refreshed, times(1)).createTag("copied", originalId, null);
      assertEquals(Long.toString(originalId), copied.getTarget().getId());
      assertEquals("UNSET", copied.getRetention().getAgeLimit());
      assertNull(nativeTable.tagManager().getOrThrow("copied").getTagCreateTime());

      TableTagInfo renewed = operations.updateTagRetention(update(source, CUSTOM_RETENTION));
      verify(refreshed, times(1)).replaceTag("source", Long.valueOf(originalId), CUSTOM_RETENTION);
      verify(refreshed, never())
          .replaceTag(anyString(), org.mockito.ArgumentMatchers.isNull(), any());
      Tag actual = nativeTable.tagManager().getOrThrow("source");
      assertEquals(originalSnapshot, actual.trimToSnapshot().toJson());
      assertEquals(CUSTOM_RETENTION, actual.getTagTimeRetained());
      assertEquals(CUSTOM_RETENTION.toMillis(), renewed.getRetention().getMaxAgeMillis());
      assertNotEquals(source.getObservationToken(), renewed.getObservationToken());
      assertEquals(1, rows(nativeTable.copy(Collections.singletonMap("scan.tag-name", "source"))));
      assertEquals(1, rows(nativeTable.copy(Collections.singletonMap("scan.tag-name", "copied"))));

      operations.deleteTag(delete(copied));
      verify(refreshed, times(1)).deleteTag("copied");
      assertFalse(nativeTable.tagManager().tagExists("copied"));
      assertEquals(
          originalSnapshot,
          nativeTable.tagManager().getOrThrow("source").trimToSnapshot().toJson());
      assertEquals(2, rows(nativeTable));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"CREATE", "COPY", "UPDATE", "DELETE"})
  void sdkCallFailureIsUnknownAndDoesNotReturnAFalseSuccess(String operation) throws Exception {
    try (Catalog catalog = catalog(true)) {
      PaimonTable owner = owner(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 1);
      nativeTable.createTag("source", id, DEFAULT_RETENTION);
      String before = nativeTable.tagManager().getOrThrow("source").toJson();
      FileStoreTable refreshed = spy(nativeTable);
      TableTagOperations operations = new PaimonTagOperations(owner, () -> refreshed, false);
      TableTagInfo source = row(owner, refreshed, "source");
      IllegalStateException storageFailure =
          new IllegalStateException("native acknowledgement lost");
      Runnable mutation;
      switch (operation) {
        case "CREATE":
          doThrow(storageFailure).when(refreshed).createTag("created", id, DEFAULT_RETENTION);
          mutation = () -> operations.createTag(create(operations, "created", id));
          break;
        case "COPY":
          doThrow(storageFailure).when(refreshed).createTag("copied", id, null);
          mutation = () -> operations.copyTag(copy(source, "copied"));
          break;
        case "UPDATE":
          doThrow(storageFailure)
              .when(refreshed)
              .replaceTag("source", Long.valueOf(id), CUSTOM_RETENTION);
          mutation = () -> operations.updateTagRetention(update(source, CUSTOM_RETENTION));
          break;
        case "DELETE":
          doThrow(storageFailure).when(refreshed).deleteTag("source");
          mutation = () -> operations.deleteTag(delete(source));
          break;
        default:
          throw new AssertionError(operation);
      }

      TagOperationException failure = assertThrows(TagOperationException.class, mutation::run);
      assertEquals(500, failure.getStatus());
      assertEquals("UNKNOWN", failure.getOutcome());
      assertEquals(before, nativeTable.tagManager().getOrThrow("source").toJson());
      assertFalse(nativeTable.tagManager().tagExists("created"));
      assertFalse(nativeTable.tagManager().tagExists("copied"));
      if ("CREATE".equals(operation)) {
        verify(refreshed, times(1)).createTag("created", id, DEFAULT_RETENTION);
      } else if ("COPY".equals(operation)) {
        verify(refreshed, times(1)).createTag("copied", id, null);
      } else if ("UPDATE".equals(operation)) {
        verify(refreshed, times(1)).replaceTag("source", Long.valueOf(id), CUSTOM_RETENTION);
      } else {
        verify(refreshed, times(1)).deleteTag("source");
      }
    }
  }

  @Test
  void successfulNativeUpdateFollowedByReadFailureIsAppliedWithErrors() throws Exception {
    try (Catalog catalog = catalog(true)) {
      PaimonTable owner = owner(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 1);
      nativeTable.createTag("source", id, DEFAULT_RETENTION);
      FileStoreTable refreshed = spy(nativeTable);
      FileIO verificationIO = spy(nativeTable.fileIO());
      AtomicBoolean nativeReturned = new AtomicBoolean();
      doReturn(verificationIO).when(refreshed).fileIO();
      doAnswer(
              call -> {
                if (nativeReturned.get()) {
                  throw new IOException("readback unavailable after successful native update");
                }
                return nativeTable.fileIO().newInputStream(call.getArgument(0));
              })
          .when(verificationIO)
          .newInputStream(nativeTable.tagManager().tagPath("source"));
      doAnswer(
              call -> {
                call.callRealMethod();
                nativeReturned.set(true);
                return null;
              })
          .when(refreshed)
          .replaceTag("source", Long.valueOf(id), CUSTOM_RETENTION);
      TableTagOperations operations = new PaimonTagOperations(owner, () -> refreshed, false);
      TableTagInfo source = row(owner, refreshed, "source");

      TagOperationException failure =
          assertThrows(
              TagOperationException.class,
              () -> operations.updateTagRetention(update(source, CUSTOM_RETENTION)));

      assertEquals(500, failure.getStatus());
      assertEquals("APPLIED_WITH_ERRORS", failure.getOutcome());
      verify(refreshed, times(1)).replaceTag("source", Long.valueOf(id), CUSTOM_RETENTION);
      Tag actual = nativeTable.tagManager().getOrThrow("source");
      assertEquals(id, actual.id());
      assertEquals(CUSTOM_RETENTION, actual.getTagTimeRetained());
      assertEquals(1, rows(nativeTable.copy(Collections.singletonMap("scan.tag-name", "source"))));
    }
  }

  @Test
  void missingStaleOrChangedClockObservationPreventsAllNativeWrites() throws Exception {
    try (Catalog catalog = catalog(true)) {
      PaimonTable owner = owner(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 1);
      nativeTable.createTag("source", id, DEFAULT_RETENTION);
      String before = nativeTable.tagManager().getOrThrow("source").toJson();
      FileStoreTable refreshed = spy(nativeTable);
      TableTagOperations operations = new PaimonTagOperations(owner, () -> refreshed, false);
      TableTagInfo source = row(owner, refreshed, "source");
      UpdateTagRetentionRequest request = update(source, CUSTOM_RETENTION);
      request.setExpectedToken(null);
      assertEquals(
          428,
          assertThrows(TagOperationException.class, () -> operations.updateTagRetention(request))
              .getStatus());
      request.setExpectedToken("stale");
      assertEquals(
          412,
          assertThrows(TagOperationException.class, () -> operations.updateTagRetention(request))
              .getStatus());
      request.setExpectedToken(source.getObservationToken());
      request.setExpectedUpdateClock("PRESERVE_ANCHOR");
      assertEquals(
          412,
          assertThrows(TagOperationException.class, () -> operations.updateTagRetention(request))
              .getStatus());

      verify(refreshed, never()).createTag(anyString(), anyLong(), any());
      verify(refreshed, never()).replaceTag(anyString(), any(), any());
      verify(refreshed, never()).deleteTag(anyString());
      assertEquals(before, nativeTable.tagManager().getOrThrow("source").toJson());
    }
  }

  @Test
  void successfulCreateWithChangedSnapshotCannotProduceASuccessReceipt() throws Exception {
    try (Catalog catalog = catalog(true)) {
      PaimonTable owner = owner(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long requestedId = commit(nativeTable, 1);
      long changedId = commit(nativeTable, 2);
      FileStoreTable refreshed = spy(nativeTable);
      doAnswer(
              call -> {
                call.callRealMethod();
                // Simulate an external native writer between publication and business readback.
                nativeTable.replaceTag("created", Long.valueOf(changedId), DEFAULT_RETENTION);
                return null;
              })
          .when(refreshed)
          .createTag("created", requestedId, DEFAULT_RETENTION);
      TableTagOperations operations = new PaimonTagOperations(owner, () -> refreshed, false);

      TagOperationException failure =
          assertThrows(
              TagOperationException.class,
              () -> operations.createTag(create(operations, "created", requestedId)));

      assertEquals(500, failure.getStatus());
      assertEquals("APPLIED_WITH_ERRORS", failure.getOutcome());
      verify(refreshed, times(1)).createTag("created", requestedId, DEFAULT_RETENTION);
      assertEquals(changedId, nativeTable.tagManager().getOrThrow("created").id());
      assertEquals(2, rows(nativeTable));
    }
  }

  @Test
  void participatingUpdateAndDeleteShareJvmLockAndRequireFreshObservation() throws Exception {
    try (Catalog catalog = catalog(true)) {
      PaimonTable owner = owner(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 1);
      nativeTable.createTag("source", id, DEFAULT_RETENTION);
      String before = nativeTable.tagManager().getOrThrow("source").toJson();
      FileStoreTable refreshed = spy(nativeTable);
      TableTagOperations updater = new PaimonTagOperations(owner, () -> refreshed, false);
      TableTagOperations deleter = new PaimonTagOperations(owner, () -> refreshed, false);
      TableTagInfo source = row(owner, refreshed, "source");
      CountDownLatch enteredNativeUpdate = new CountDownLatch(1);
      CountDownLatch finishNativeUpdate = new CountDownLatch(1);
      doAnswer(
              call -> {
                enteredNativeUpdate.countDown();
                if (!finishNativeUpdate.await(20, TimeUnit.SECONDS)) {
                  throw new AssertionError("Native update was not released by the test");
                }
                call.callRealMethod();
                return null;
              })
          .when(refreshed)
          .replaceTag("source", Long.valueOf(id), CUSTOM_RETENTION);
      ExecutorService writer = Executors.newSingleThreadExecutor();
      try {
        Future<TableTagInfo> pending =
            writer.submit(() -> updater.updateTagRetention(update(source, CUSTOM_RETENTION)));
        try {
          assertTrue(enteredNativeUpdate.await(10, TimeUnit.SECONDS));
          TagOperationException busyDelete =
              assertThrows(TagOperationException.class, () -> deleter.deleteTag(delete(source)));
          assertEquals(429, busyDelete.getStatus());
          assertEquals("BUSY", busyDelete.getReason());
          assertEquals("NOT_APPLIED", busyDelete.getOutcome());
          TagOperationException busyUpdate =
              assertThrows(
                  TagOperationException.class,
                  () -> deleter.updateTagRetention(update(source, CUSTOM_RETENTION)));
          assertEquals(429, busyUpdate.getStatus());
          assertEquals("NOT_APPLIED", busyUpdate.getOutcome());
          verify(refreshed, never()).deleteTag(anyString());
          assertEquals(before, nativeTable.tagManager().getOrThrow("source").toJson());
        } finally {
          finishNativeUpdate.countDown();
        }

        TableTagInfo renewed = pending.get(10, TimeUnit.SECONDS);
        TagOperationException staleDelete =
            assertThrows(TagOperationException.class, () -> deleter.deleteTag(delete(source)));
        assertEquals(412, staleDelete.getStatus());
        assertEquals("NOT_APPLIED", staleDelete.getOutcome());
        verify(refreshed, never()).deleteTag(anyString());
        deleter.deleteTag(delete(renewed));
        verify(refreshed, times(1)).replaceTag("source", Long.valueOf(id), CUSTOM_RETENTION);
        verify(refreshed, times(1)).deleteTag("source");
        assertFalse(nativeTable.tagManager().tagExists("source"));
        assertEquals(1, rows(nativeTable));
      } finally {
        finishNativeUpdate.countDown();
        writer.shutdownNow();
        assertTrue(writer.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  private Catalog catalog(boolean primaryKey) throws Exception {
    Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration());
    catalog.createDatabase("db", true);
    Schema.Builder schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .option("bucket", primaryKey ? "1" : "-1")
            .option("write-only", "true")
            .option("self-optimizing.enabled", "true")
            .option("tag.default-time-retained", "2 h");
    if (primaryKey) {
      schema.primaryKey("id");
    }
    catalog.createTable(Identifier.create("db", "test"), schema.build(), false);
    return catalog;
  }

  private PaimonTable owner(Catalog catalog) {
    return (PaimonTable)
        new PaimonCatalog(catalog, "local", Collections.emptyMap()).loadTable("db", "test");
  }

  private long commit(FileStoreTable table, int value) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      write.write(GenericRow.of(value));
      commit.commit(write.prepareCommit());
    }
    return table.snapshotManager().latestSnapshotId();
  }

  private TableTagInfo row(PaimonTable owner, FileStoreTable table, String name) {
    return PaimonTagReader.view(owner, table, name, table.tagManager().getOrThrow(name));
  }

  private CreateTagRequest create(TableTagOperations operations, String name, long id) {
    CreateTagRequest request = new CreateTagRequest();
    request.setName(name);
    request.setScope(TagScope.main());
    request.setTarget(TableVersionRef.of(Long.toString(id), "main"));
    request.setExpectedRetentionModel("TAG_CREATION_AGE");
    request.setExpectedRetentionConfigToken(
        operations.capabilities().getRetention().getConfigObservationToken());
    return request;
  }

  private CopyTagRequest copy(TableTagInfo source, String name) {
    CopyTagRequest request = new CopyTagRequest();
    request.setSourceName(source.getName());
    request.setNewName(name);
    request.setScope(source.getScope());
    request.setExpectedSourceToken(source.getObservationToken());
    request.setExpectedRetentionModel("TAG_CREATION_AGE");
    request.getRetention().setMode("NO_AGE_LIMIT");
    return request;
  }

  private UpdateTagRetentionRequest update(TableTagInfo source, Duration duration) {
    UpdateTagRetentionRequest request = new UpdateTagRetentionRequest();
    request.setName(source.getName());
    request.setScope(source.getScope());
    request.setExpectedToken(source.getObservationToken());
    request.setExpectedRetentionModel("TAG_CREATION_AGE");
    request.setExpectedUpdateClock("RESET_ON_UPDATE");
    request.setRetention(new TagRetentionRequest());
    request.getRetention().setMode("AGE_LIMIT");
    request.getRetention().setMaxAgeMillis(duration.toMillis());
    return request;
  }

  private DeleteTagRequest delete(TableTagInfo source) {
    DeleteTagRequest request = new DeleteTagRequest();
    request.setName(source.getName());
    request.setScope(source.getScope());
    request.setExpectedToken(source.getObservationToken());
    return request;
  }

  private int rows(FileStoreTable table) throws Exception {
    ReadBuilder read = table.newReadBuilder();
    int count = 0;
    try (RecordReaderIterator<InternalRow> rows =
        new RecordReaderIterator<>(read.newRead().createReader(read.newScan().plan()))) {
      while (rows.hasNext()) {
        rows.next();
        count++;
      }
    }
    return count;
  }
}
