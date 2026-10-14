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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.amoro.formats.paimon.PaimonCatalog;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.PaimonTableDescriptor;
import org.apache.amoro.formats.paimon.optimizing.PaimonOptimizingEligibility;
import org.apache.amoro.table.tag.CopyTagRequest;
import org.apache.amoro.table.tag.CreateTagRequest;
import org.apache.amoro.table.tag.DeleteTagRequest;
import org.apache.amoro.table.tag.TableTagInfo;
import org.apache.amoro.table.tag.TableTagOperations;
import org.apache.amoro.table.tag.TableVersionRef;
import org.apache.amoro.table.tag.TagOperationException;
import org.apache.amoro.table.tag.TagQuery;
import org.apache.amoro.table.tag.TagRetentionRequest;
import org.apache.amoro.table.tag.TagScope;
import org.apache.amoro.table.tag.UpdateTagRetentionRequest;
import org.apache.amoro.utils.CatalogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestPaimonTagOperations {
  @TempDir java.nio.file.Path warehouse;

  private Catalog catalog() throws Exception {
    Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration());
    catalog.createDatabase("db", true);
    catalog.createTable(
        Identifier.create("db", "test"),
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .primaryKey("id")
            .option("bucket", "1")
            .option("write-only", "true")
            .option("tag.default-time-retained", "2 h")
            .build(),
        false);
    return catalog;
  }

  private PaimonTable table(Catalog catalog) {
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

  private CreateTagRequest create(TableTagOperations ops, String name, long id) {
    CreateTagRequest request = new CreateTagRequest();
    request.setName(name);
    request.setTarget(TableVersionRef.of(Long.toString(id), "main"));
    request.setScope(TagScope.main());
    request.setExpectedRetentionModel("TAG_CREATION_AGE");
    request.setExpectedRetentionConfigToken(
        ops.capabilities().getRetention().getConfigObservationToken());
    return request;
  }

  @ParameterizedTest
  @CsvSource({
    "unset,TABLE,unset",
    "false,TABLE,true",
    "true,TABLE,false",
    "unset,CATALOG,unset",
    "false,CATALOG,true",
    "true,CATALOG,false"
  })
  void nativeTagMutationsPreserveOptimizingAndTimeExpiryConfiguration(
      String ownership, String optimizingSource, String timeExpiry) throws Exception {
    try (Catalog catalog = catalog()) {
      if (!"unset".equals(timeExpiry)) {
        catalog.alterTable(
            Identifier.create("db", "test"),
            SchemaChange.setOption("tag.time-expire-enabled", timeExpiry),
            false);
      }
      Map<String, String> options = new HashMap<>();
      if (!"unset".equals(ownership)) {
        options.put("tag-management.exclusive-maintenance", ownership);
      }
      if ("TABLE".equals(optimizingSource)) {
        catalog.alterTable(
            Identifier.create("db", "test"),
            SchemaChange.setOption("self-optimizing.enabled", "true"),
            false);
      } else {
        options.put("table.self-optimizing.enabled", "true");
      }
      PaimonTable owner =
          (PaimonTable) new PaimonCatalog(catalog, "local", options).loadTable("db", "test");
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      Map<String, String> originalOptions = new HashMap<>(nativeTable.options());
      Map<String, String> originalProperties = new HashMap<>(owner.properties());
      assertEquals(!"false".equals(timeExpiry), nativeTable.coreOptions().tagTimeExpireEnabled());
      assertTrue(
          PaimonOptimizingEligibility.isEligible(
              CatalogUtil.mergeCatalogPropertiesToTable(nativeTable.options(), options)));
      TableTagOperations ops = owner.tagOperations().get();
      long id = commit(nativeTable, 1);

      TableTagInfo source = ops.createTag(create(ops, "source", id));
      assertEquals(4, ops.capabilities().getActions().size());
      assertNull(ops.capabilities().getUnavailableReason());
      String snapshot = nativeTable.tagManager().getOrThrow("source").trimToSnapshot().toJson();
      TableTagInfo renewed = ops.updateTagRetention(update(ops, source, "AGE_LIMIT", 10800000L));
      assertEquals(10800000L, renewed.getRetention().getMaxAgeMillis());
      assertEquals(
          snapshot, nativeTable.tagManager().getOrThrow("source").trimToSnapshot().toJson());

      CopyTagRequest copy = new CopyTagRequest();
      copy.setSourceName(renewed.getName());
      copy.setNewName("copy");
      copy.setScope(renewed.getScope());
      copy.setExpectedSourceToken(renewed.getObservationToken());
      copy.setExpectedRetentionModel("TAG_CREATION_AGE");
      copy.getRetention().setMode("NO_AGE_LIMIT");
      TableTagInfo copied = ops.copyTag(copy);
      assertEquals(source.getTarget().getId(), copied.getTarget().getId());

      DeleteTagRequest delete = new DeleteTagRequest();
      delete.setName(copied.getName());
      delete.setScope(copied.getScope());
      delete.setExpectedToken(copied.getObservationToken());
      ops.deleteTag(delete);
      assertFalse(nativeTable.tagManager().tagExists("copy"));
      assertTrue(nativeTable.tagManager().tagExists("source"));
      assertEquals(1, readRows(nativeTable));
      assertEquals(id, nativeTable.snapshotManager().latestSnapshotId());
      assertEquals(
          originalOptions,
          ((FileStoreTable) catalog.getTable(Identifier.create("db", "test"))).options());
      assertEquals(options, owner.catalogProperties());
      assertEquals(originalProperties, owner.properties());
    }
  }

  @Test
  void delegatesCreationToNativeTableApiAndReadsBackRetention() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable table = spy((FileStoreTable) owner.originalTable());
      long id = commit(table, 1);
      TableTagOperations ops = new PaimonTagOperations(owner, () -> table, false);
      TableTagInfo created = ops.createTag(create(ops, "native-api", id));
      assertEquals(7200000L, created.getRetention().getMaxAgeMillis());
      verify(table, times(1)).createTag("native-api", id, java.time.Duration.ofHours(2));
      assertEquals(id, table.tagManager().getOrThrow("native-api").id());
    }
  }

  @Test
  void createsCopiesExpiresAndDeletesNativeTagsWithExactRetention() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable table = table(catalog);
      FileStoreTable nativeTable = (FileStoreTable) table.originalTable();
      long id = commit(nativeTable, 1);
      TableTagOperations ops = table.tagOperations().get();
      assertEquals(4, ops.capabilities().getActions().size());
      TableTagInfo source = ops.createTag(create(ops, "中文 release%", id));
      assertEquals(7200000L, source.getRetention().getMaxAgeMillis());
      assertNull(source.getCreatedAt());
      assertEquals(
          409,
          assertThrows(
                  TagOperationException.class,
                  () -> ops.createTag(create(ops, source.getName(), id)))
              .getStatus());
      // Remove only ordinary snapshot metadata to exercise Tag-backed copy, retaining native data.
      nativeTable.fileIO().delete(nativeTable.snapshotManager().snapshotPath(id), false);
      CopyTagRequest copy = new CopyTagRequest();
      copy.setSourceName(source.getName());
      copy.setNewName("copy");
      copy.setScope(TagScope.main());
      copy.setExpectedSourceToken(source.getObservationToken());
      copy.setExpectedRetentionModel("TAG_CREATION_AGE");
      copy.getRetention().setMode("NO_AGE_LIMIT");
      TableTagInfo copied = ops.copyTag(copy);
      assertEquals(source.getTarget().getId(), copied.getTarget().getId());
      assertEquals("UNSET", copied.getRetention().getAgeLimit());
      assertNull(nativeTable.tagManager().getOrThrow("copy").getTagCreateTime());
      DeleteTagRequest delete = new DeleteTagRequest();
      delete.setName("copy");
      delete.setScope(TagScope.main());
      delete.setExpectedToken("stale");
      assertEquals(
          412, assertThrows(TagOperationException.class, () -> ops.deleteTag(delete)).getStatus());
      delete.setExpectedToken(copied.getObservationToken());
      assertEquals("NOT_REQUIRED", ops.deleteTag(delete).getHistoryCleanup());
      assertFalse(nativeTable.tagManager().tagExists("copy"));
      assertTrue(nativeTable.tagManager().tagExists(source.getName()));
    }
  }

  @Test
  void refreshesDefaultsAndKeepsAutomaticTagCreationGate() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable table = table(catalog);
      long id = commit((FileStoreTable) table.originalTable(), 1);
      TableTagOperations ops = table.tagOperations().get();
      CreateTagRequest request = create(ops, "release", id);
      catalog.alterTable(
          Identifier.create("db", "test"),
          SchemaChange.setOption("tag.default-time-retained", "3 h"),
          false);
      assertEquals(
          412, assertThrows(TagOperationException.class, () -> ops.createTag(request)).getStatus());
      catalog.alterTable(
          Identifier.create("db", "test"),
          SchemaChange.setOption("self-optimizing.enabled", "true"),
          false);
      assertNull(ops.capabilities().getUnavailableReason());
      assertEquals(4, ops.capabilities().getActions().size());
      catalog.alterTable(
          Identifier.create("db", "test"),
          SchemaChange.setOption("tag.automatic-creation", "process-time"),
          false);
      assertEquals("AUTOMATIC_TAG_MAINTENANCE_ENABLED", ops.capabilities().getUnavailableReason());
      assertTrue(ops.capabilities().getActions().isEmpty());
      assertThrows(TagOperationException.class, () -> ops.createTag(request));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "write-only,false,WRITE_ONLY_REQUIRED",
    "tag.automatic-creation,process-time,AUTOMATIC_TAG_MAINTENANCE_ENABLED"
  })
  void keepsOtherWriteRequirementsBeforePublishing(String key, String value, String reason)
      throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 1);
      TableTagOperations ops = owner.tagOperations().get();
      CreateTagRequest request = create(ops, "refused", id);
      catalog.alterTable(
          Identifier.create("db", "test"), SchemaChange.setOption(key, value), false);

      assertEquals(reason, ops.capabilities().getUnavailableReason());
      assertTrue(ops.capabilities().getActions().isEmpty());
      TagOperationException failure =
          assertThrows(TagOperationException.class, () -> ops.createTag(request));
      assertEquals(422, failure.getStatus());
      assertEquals("NOT_APPLIED", failure.getOutcome());
      assertFalse(nativeTable.tagManager().tagExists("refused"));
      assertEquals(1, readRows(nativeTable));
    }
  }

  @Test
  void paginatesAllNamesAndRejectsTraversal() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable table = table(catalog);
      long id = commit((FileStoreTable) table.originalTable(), 1);
      TableTagOperations ops = table.tagOperations().get();
      for (int i = 0; i < 23; i++) {
        ops.createTag(create(ops, String.format("tag%02d", i), id));
      }
      TagQuery query = new TagQuery();
      query.setPage(2);
      org.apache.commons.lang3.tuple.Pair<java.util.List<TableTagInfo>, Long> page =
          new PaimonTableDescriptor().getTableTags(table, query);
      assertEquals(23L, page.getRight());
      assertEquals(3, page.getLeft().size());
      assertEquals("tag20", page.getLeft().get(0).getName());
      query.setPage(1);
      query.setKeyword("22");
      assertEquals(1L, new PaimonTableDescriptor().getTableTags(table, query).getRight());
      for (String name : new String[] {"../outside", ".", "..", "a/b", "a\\b", "a\u0000b"}) {
        assertThrows(TagOperationException.class, () -> ops.createTag(create(ops, name, id)));
      }
    }
  }

  private int readRows(FileStoreTable table) throws Exception {
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

  @Test
  void lastHistoricalReferenceUsesNativeCleanupAndPreservesLiveData() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable table = (FileStoreTable) owner.originalTable();
      TableTagOperations ops = owner.tagOperations().get();
      long old = commit(table, 1);
      TableTagInfo tag = ops.createTag(create(ops, "historical", old));
      long current = commit(table, 2);
      table.fileIO().delete(table.snapshotManager().snapshotPath(old), false);
      table.snapshotManager().commitEarliestHint(current);
      // Explicit-ID create must reuse the Tag's retained Snapshot, as the SDK does.
      TableTagInfo restored = ops.createTag(create(ops, "restored", old));
      assertEquals(tag.getTarget().getId(), restored.getTarget().getId());
      DeleteTagRequest delete = new DeleteTagRequest();
      delete.setScope(TagScope.main());
      delete.setName(restored.getName());
      delete.setExpectedToken(restored.getObservationToken());
      assertEquals("NOT_REQUIRED", ops.deleteTag(delete).getHistoryCleanup());
      delete.setName(tag.getName());
      delete.setExpectedToken(tag.getObservationToken());
      long start = System.nanoTime();
      assertEquals("BEST_EFFORT_UNVERIFIED", ops.deleteTag(delete).getHistoryCleanup());
      System.out.println(
          "Historical Tag cleanup fixture: 2 snapshots, 2 commits, elapsedMillis="
              + (System.nanoTime() - start) / 1000000);
      assertFalse(table.tagManager().tagExists(tag.getName()));
      assertEquals(2, readRows(table));
    }
  }

  @Test
  void appendTagsRetainReadableDataAfterOrdinarySnapshotMetadataExpires() throws Exception {
    try (Catalog catalog = catalog()) {
      catalog.createTable(
          Identifier.create("db", "append"),
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .option("bucket", "-1")
              .option("write-only", "true")
              .option("tag.time-expire-enabled", "false")
              .build(),
          false);
      PaimonTable owner =
          (PaimonTable)
              new PaimonCatalog(catalog, "local", Collections.emptyMap()).loadTable("db", "append");
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 7);
      TableTagOperations ops = owner.tagOperations().get();
      TableTagInfo tag = ops.createTag(create(ops, "append-tag", id));
      nativeTable.fileIO().delete(nativeTable.snapshotManager().snapshotPath(id), false);
      FileStoreTable tagged =
          nativeTable.copy(Collections.singletonMap("scan.tag-name", tag.getName()));
      assertEquals(1, readRows(tagged));
      assertEquals("UNSET", tag.getRetention().getAgeLimit());
    }
  }

  @Test
  void existingNativeRetentionIsReadableWhileInvalidDefaultsCloseWrites() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 1);
      nativeTable.createTag("zero", id, java.time.Duration.ZERO);
      nativeTable.createTag("negative", id, java.time.Duration.ofMillis(-1));
      nativeTable.createTag("huge", id, java.time.Duration.ofSeconds(Long.MAX_VALUE));
      java.util.List<TableTagInfo> tags =
          new PaimonTableDescriptor().getTableTags(owner, new TagQuery()).getLeft();
      assertEquals(3, tags.size());
      assertEquals(
          0L,
          tags.stream()
              .filter(t -> "zero".equals(t.getName()))
              .findFirst()
              .get()
              .getRetention()
              .getMaxAgeMillis());
      assertEquals(
          -1L,
          tags.stream()
              .filter(t -> "negative".equals(t.getName()))
              .findFirst()
              .get()
              .getRetention()
              .getMaxAgeMillis());
      org.apache.amoro.table.tag.TagRetentionInfo huge =
          tags.stream().filter(t -> "huge".equals(t.getName())).findFirst().get().getRetention();
      assertEquals("UNKNOWN", huge.getState());
      assertEquals("SET", huge.getAgeLimit());
      assertNull(huge.getMaxAgeMillis());
      catalog.alterTable(
          Identifier.create("db", "test"),
          SchemaChange.setOption("tag.default-time-retained", "0 h"),
          false);
      TableTagOperations operations = owner.tagOperations().get();
      assertEquals("INVALID_NATIVE_RETENTION", operations.capabilities().getUnavailableReason());
      assertTrue(operations.capabilities().getActions().isEmpty());
      assertThrows(
          TagOperationException.class,
          () -> operations.createTag(create(operations, "refused", id)));
      assertFalse(nativeTable.tagManager().tagExists("refused"));
    }
  }

  @Test
  void rejectsNonCanonicalSnapshotIdsBeforePublishingMetadata() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 1);
      TableTagOperations operations = owner.tagOperations().get();
      CreateTagRequest request = create(operations, "refused", id);
      for (String value :
          new String[] {"01", "+1", "0001", " 1", "0", "-1", "9223372036854775808"}) {
        request.setTarget(TableVersionRef.of(value, "main"));
        TagOperationException failure =
            assertThrows(TagOperationException.class, () -> operations.createTag(request), value);
        assertEquals(400, failure.getStatus(), value);
        assertEquals("NOT_APPLIED", failure.getOutcome(), value);
        assertFalse(nativeTable.tagManager().tagExists("refused"), value);
      }
    }
  }

  @Test
  void retentionProjectionDoesNotRoundSubMillisOrUnsafeJsonValues() {
    for (java.time.Duration value :
        new java.time.Duration[] {
          java.time.Duration.ofNanos(1),
          java.time.Duration.ofMillis(9007199254740992L),
          java.time.Duration.ofSeconds(Long.MAX_VALUE)
        }) {
      org.apache.amoro.table.tag.TagRetentionInfo info = PaimonTagReader.retention(value);
      assertEquals("UNKNOWN", info.getState());
      assertEquals("SET", info.getAgeLimit());
      assertNull(info.getMaxAgeMillis());
    }
  }

  @Test
  void createAndCopyPreserveNativeMetadataPermissionsAndGroup() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable nativeTable = (FileStoreTable) owner.originalTable();
      long id = commit(nativeTable, 1);
      nativeTable.createTag("native", id);
      java.nio.file.Path nativePath =
          java.nio.file.Paths.get(nativeTable.tagManager().tagPath("native").toUri());
      org.junit.jupiter.api.Assumptions.assumeTrue(
          java.nio.file.Files.getFileStore(nativePath).supportsFileAttributeView("posix"));
      java.nio.file.attribute.PosixFileAttributes expected =
          java.nio.file.Files.readAttributes(
              nativePath, java.nio.file.attribute.PosixFileAttributes.class);
      TableTagOperations operations = owner.tagOperations().get();
      TableTagInfo created = operations.createTag(create(operations, "created", id));
      CopyTagRequest copy = new CopyTagRequest();
      copy.setSourceName(created.getName());
      copy.setNewName("copied");
      copy.setScope(TagScope.main());
      copy.setExpectedSourceToken(created.getObservationToken());
      copy.setExpectedRetentionModel("TAG_CREATION_AGE");
      copy.getRetention().setMode("NO_AGE_LIMIT");
      operations.copyTag(copy);
      for (String name : new String[] {"created", "copied"}) {
        java.nio.file.attribute.PosixFileAttributes actual =
            java.nio.file.Files.readAttributes(
                java.nio.file.Paths.get(nativeTable.tagManager().tagPath(name).toUri()),
                java.nio.file.attribute.PosixFileAttributes.class);
        assertEquals(expected.permissions(), actual.permissions());
        assertEquals(expected.group(), actual.group());
        assertEquals(expected.owner(), actual.owner());
      }
    }
  }

  @Test
  void closesWritingForUnverifiedSdkProfiles() throws Exception {
    try (Catalog catalog = catalog()) {
      TableTagOperations operations = table(catalog).tagOperations().get();
      boolean verified = "1.4.2".equals(PaimonTagOperations.version());
      assertEquals(verified, !operations.capabilities().getActions().isEmpty());
      if (!verified) {
        assertEquals("UNVERIFIED_PAIMON_VERSION", operations.capabilities().getUnavailableReason());
      }
      System.out.println(
          "Tag SDK gate verified for Paimon "
              + PaimonTagOperations.version()
              + ": writable="
              + verified);
    }
  }

  @Test
  void nativeHistoricalCleanupHandlesPartitionedOverwriteWithoutDamagingCurrentData()
      throws Exception {
    try (Catalog catalog = catalog()) {
      catalog.createTable(
          Identifier.create("db", "partitioned"),
          Schema.newBuilder()
              .column("p", DataTypes.INT())
              .column("id", DataTypes.INT())
              .partitionKeys("p")
              .option("bucket", "-1")
              .option("write-only", "true")
              .option("tag.time-expire-enabled", "false")
              .build(),
          false);
      PaimonTable owner =
          (PaimonTable)
              new PaimonCatalog(catalog, "local", Collections.emptyMap())
                  .loadTable("db", "partitioned");
      FileStoreTable table = (FileStoreTable) owner.originalTable();
      for (int batch = 0; batch < 2; batch++) {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        if (batch > 0) {
          builder = builder.withOverwrite();
        }
        try (BatchTableWrite write = builder.newWrite();
            BatchTableCommit commit = builder.newCommit()) {
          for (int partition = 0; partition < 200; partition++) {
            write.write(GenericRow.of(partition, batch));
          }
          commit.commit(write.prepareCommit());
        }
        if (batch == 0) {
          table.createTag("historical", table.snapshotManager().latestSnapshotId());
        }
      }
      TableTagOperations ops = owner.tagOperations().get();
      TableTagInfo tag =
          new PaimonTableDescriptor().getTableTags(owner, new TagQuery()).getLeft().get(0);
      long id = Long.parseLong(tag.getTarget().getId());
      table.snapshotManager().deleteSnapshot(id);
      table.snapshotManager().commitEarliestHint(table.snapshotManager().latestSnapshotId());
      DeleteTagRequest request = new DeleteTagRequest();
      request.setName(tag.getName());
      request.setScope(TagScope.main());
      request.setExpectedToken(tag.getObservationToken());
      long started = System.nanoTime();
      assertEquals("BEST_EFFORT_UNVERIFIED", ops.deleteTag(request).getHistoryCleanup());
      System.out.println(
          "Partitioned historical cleanup fixture: 200 partitions, 2 commits, elapsedMillis="
              + (System.nanoTime() - started) / 1000000);
      assertEquals(200, readRows(table));
      assertFalse(table.tagManager().tagExists(tag.getName()));
    }
  }

  private UpdateTagRetentionRequest update(
      TableTagOperations ops, TableTagInfo tag, String mode, Long millis) {
    UpdateTagRetentionRequest request = new UpdateTagRetentionRequest();
    request.setName(tag.getName());
    request.setScope(tag.getScope());
    request.setExpectedToken(tag.getObservationToken());
    request.setExpectedRetentionModel("TAG_CREATION_AGE");
    request.setExpectedUpdateClock("RESET_ON_UPDATE");
    request.setExpectedRetentionConfigToken(
        ops.capabilities().getRetention().getConfigObservationToken());
    TagRetentionRequest retention = new TagRetentionRequest();
    retention.setMode(mode);
    retention.setMaxAgeMillis(millis);
    request.setRetention(retention);
    return request;
  }

  @Test
  void retentionUpdateRenewsFiniteTtlAndPreservesExpiredSnapshotAndOtherTags() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable table = (FileStoreTable) owner.originalTable();
      long id = commit(table, 1);
      table.createTag("kept", id, java.time.Duration.ofHours(2));
      table.createTag("other", id);
      String other = table.fileIO().readFileUtf8(table.tagManager().tagPath("other"));
      String snapshot = table.tagManager().getOrThrow("kept").trimToSnapshot().toJson();
      table.fileIO().delete(table.snapshotManager().snapshotPath(id), false);
      TableTagOperations ops = owner.tagOperations().get();
      assertTrue(ops.capabilities().getActions().contains("UPDATE_RETENTION"));
      assertEquals("RESET_ON_UPDATE", ops.capabilities().getRetention().getUpdateClock());
      TableTagInfo row =
          new PaimonTableDescriptor()
              .getTableTags(owner, new TagQuery()).getLeft().stream()
                  .filter(t -> "kept".equals(t.getName()))
                  .findFirst()
                  .get();
      for (long millis : new long[] {10800000L, 3600000L, 3600000L}) {
        java.time.LocalDateTime before = table.tagManager().getOrThrow("kept").getTagCreateTime();
        TableTagInfo result = ops.updateTagRetention(update(ops, row, "AGE_LIMIT", millis));
        assertEquals(row.getName(), result.getName());
        assertEquals(row.getTarget().getId(), result.getTarget().getId());
        assertEquals(millis, result.getRetention().getMaxAgeMillis());
        assertTrue(table.tagManager().getOrThrow("kept").getTagCreateTime().isAfter(before));
        assertEquals(snapshot, table.tagManager().getOrThrow("kept").trimToSnapshot().toJson());
        assertEquals(other, table.fileIO().readFileUtf8(table.tagManager().tagPath("other")));
        assertEquals(1, readRows(table.copy(Collections.singletonMap("scan.tag-name", "kept"))));
        row = result;
      }
      row = ops.updateTagRetention(update(ops, row, "NATIVE_DEFAULT", null));
      assertEquals(7200000L, row.getRetention().getMaxAgeMillis());
      row = ops.updateTagRetention(update(ops, row, "NO_AGE_LIMIT", null));
      assertEquals("UNSET", row.getRetention().getAgeLimit());
      assertNull(table.tagManager().getOrThrow("kept").getTagCreateTime());
      assertEquals(snapshot, table.tagManager().getOrThrow("kept").trimToSnapshot().toJson());
    }
  }

  @Test
  void updateRejectsStaleMissingChangedConfigurationAndClockBeforeWriting() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable table = (FileStoreTable) owner.originalTable();
      long id = commit(table, 1);
      TableTagOperations ops = owner.tagOperations().get();
      TableTagInfo row = ops.createTag(create(ops, "kept", id));
      String before = table.fileIO().readFileUtf8(table.tagManager().tagPath("kept"));
      UpdateTagRetentionRequest request = update(ops, row, "NATIVE_DEFAULT", null);
      request.setExpectedToken(null);
      assertEquals(
          428,
          assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request))
              .getStatus());
      request.setExpectedToken("stale");
      assertEquals(
          412,
          assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request))
              .getStatus());
      request.setExpectedToken(row.getObservationToken());
      request.setExpectedUpdateClock("PRESERVE_ANCHOR");
      assertEquals(
          412,
          assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request))
              .getStatus());
      request.setExpectedUpdateClock("RESET_ON_UPDATE");
      catalog.alterTable(
          Identifier.create("db", "test"),
          SchemaChange.setOption("tag.default-time-retained", "3 h"),
          false);
      assertEquals(
          412,
          assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request))
              .getStatus());
      assertEquals(before, table.fileIO().readFileUtf8(table.tagManager().tagPath("kept")));
      request.setName("missing");
      assertEquals(
          404,
          assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request))
              .getStatus());
      request.setName("kept");
      request.setExpectedRetentionModel("TARGET_COMMIT_AGE");
      assertEquals(
          412,
          assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request))
              .getStatus());
    }
  }

  @Test
  void updateAndDeleteInvalidateEachOthersObservations() throws Exception {
    try (Catalog catalog = catalog()) {
      PaimonTable owner = table(catalog);
      FileStoreTable table = (FileStoreTable) owner.originalTable();
      long id = commit(table, 1);
      TableTagOperations ops = owner.tagOperations().get();
      TableTagInfo row = ops.createTag(create(ops, "kept", id));
      UpdateTagRetentionRequest request = update(ops, row, "AGE_LIMIT", 10000L);
      TableTagInfo renewed = ops.updateTagRetention(request);
      assertEquals(
          412,
          assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request))
              .getStatus());
      DeleteTagRequest delete = new DeleteTagRequest();
      delete.setName(row.getName());
      delete.setScope(row.getScope());
      delete.setExpectedToken(row.getObservationToken());
      assertEquals(
          412, assertThrows(TagOperationException.class, () -> ops.deleteTag(delete)).getStatus());
      delete.setExpectedToken(renewed.getObservationToken());
      ops.deleteTag(delete);
      request.setExpectedToken(renewed.getObservationToken());
      assertEquals(
          404,
          assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request))
              .getStatus());
    }
  }

  @Test
  void appendRetentionUpdateWorksAfterSnapshotExpiryWithoutRunningCleanup() throws Exception {
    try (Catalog catalog = catalog()) {
      catalog.createTable(
          Identifier.create("db", "append-update"),
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .option("bucket", "-1")
              .option("write-only", "true")
              .option("tag.time-expire-enabled", "false")
              .build(),
          false);
      PaimonTable owner =
          (PaimonTable)
              new PaimonCatalog(catalog, "local", Collections.emptyMap())
                  .loadTable("db", "append-update");
      FileStoreTable table = (FileStoreTable) owner.originalTable();
      long id = commit(table, 7);
      TableTagOperations ops = owner.tagOperations().get();
      TableTagInfo row = ops.createTag(create(ops, "kept", id));
      table.snapshotManager().deleteSnapshot(id);
      row = ops.updateTagRetention(update(ops, row, "AGE_LIMIT", 5000L));
      assertEquals(5000L, row.getRetention().getMaxAgeMillis());
      assertEquals(1, readRows(table.copy(Collections.singletonMap("scan.tag-name", "kept"))));
      row = ops.updateTagRetention(update(ops, row, "NATIVE_DEFAULT", null));
      assertEquals("UNSET", row.getRetention().getAgeLimit());
    }
  }
}
