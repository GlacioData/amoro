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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
import org.apache.amoro.table.tag.TagWriteCapabilities;
import org.apache.amoro.table.tag.UpdateTagRetentionRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.iceberg.IcebergCommitCallback;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** Exercises native Paimon 1.4.2 Tag metadata and retained data on an actual HDFS service. */
class TestPaimonHdfsTags {
  @TempDir static java.nio.file.Path clusterDirectory;
  private static MiniDFSCluster cluster;
  private Catalog catalog;

  @BeforeAll
  static void startHdfs() throws Exception {
    Configuration configuration = new HdfsConfiguration();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterDirectory.toString());
    configuration.setInt("dfs.replication", 1);
    cluster = new MiniDFSCluster.Builder(configuration).numDataNodes(1).format(true).build();
    cluster.waitActive();
  }

  @AfterAll
  static void stopHdfs() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @BeforeEach
  void openCatalog() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", cluster.getURI() + "/warehouse");
    properties.put("metastore", "filesystem");
    catalog = PaimonCatalogFactory.paimonCatalog(properties, cluster.getConfiguration(0));
    catalog.createDatabase("db", true);
  }

  @AfterEach
  void closeCatalog() throws Exception {
    if (catalog != null) {
      catalog.close();
    }
  }

  private PaimonTable table(String name, boolean primaryKey) throws Exception {
    Schema.Builder schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .option("bucket", primaryKey ? "1" : "-1")
            .option("write-only", "true")
            .option("self-optimizing.enabled", "true")
            .option("tag.automatic-creation", "none")
            .option("tag.default-time-retained", "2 h");
    if (primaryKey) {
      schema.primaryKey("id");
    }
    catalog.createTable(Identifier.create("db", name), schema.build(), false);
    PaimonTable owner =
        (PaimonTable)
            new PaimonCatalog(catalog, "hdfs", Collections.emptyMap()).loadTable("db", name);
    assertEquals("hdfs", ((FileStoreTable) owner.originalTable()).location().toUri().getScheme());
    return owner;
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

  private List<Integer> rows(FileStoreTable table) throws Exception {
    ReadBuilder read = table.newReadBuilder();
    List<Integer> values = new ArrayList<>();
    try (RecordReaderIterator<InternalRow> rows =
        new RecordReaderIterator<>(read.newRead().createReader(read.newScan().plan()))) {
      while (rows.hasNext()) {
        values.add(rows.next().getInt(0));
      }
    }
    Collections.sort(values);
    return values;
  }

  private void retainedState(
      FileStoreTable table, String snapshot, String other, String expired, long current)
      throws Exception {
    assertEquals("true", table.options().get("self-optimizing.enabled"));
    assertTrue(table.coreOptions().tagTimeExpireEnabled());
    assertEquals(snapshot, table.tagManager().getOrThrow("kept").trimToSnapshot().toJson());
    assertEquals(other, table.fileIO().readFileUtf8(table.tagManager().tagPath("other")));
    assertEquals(expired, table.fileIO().readFileUtf8(table.tagManager().tagPath("expired")));
    assertFalse(table.snapshotManager().snapshotExists(1L));
    assertEquals(current, table.snapshotManager().latestSnapshotId());
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "kept"))));
    assertEquals(Arrays.asList(7, 8), rows(table));
  }

  @ParameterizedTest(name = "HDFS retention lifecycle, primaryKey={0}")
  @ValueSource(booleans = {true, false})
  void retentionLifecyclePreservesExpiredSnapshotOtherTagsAndData(boolean primaryKey)
      throws Exception {
    PaimonTable owner = table(primaryKey ? "pk-lifecycle" : "append-lifecycle", primaryKey);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    TableTagOperations ops = owner.tagOperations().get();
    TagWriteCapabilities capabilities = ops.capabilities();
    assertEquals(
        new HashSet<>(Arrays.asList("CREATE", "COPY", "DELETE", "UPDATE_RETENTION")),
        capabilities.getActions());
    assertNull(capabilities.getUnavailableReason());
    assertEquals("OBSERVATION_CHECK", capabilities.getPreconditionGuarantee());
    assertEquals("RESET_ON_UPDATE", capabilities.getRetention().getUpdateClock());
    assertEquals("1.4.2", PaimonTagOperations.version());

    long first = commit(table, 7);
    assertEquals(1L, first);
    TableTagInfo row = ops.createTag(create(ops, "kept", first));
    assertEquals(7200000L, row.getRetention().getMaxAgeMillis());
    Snapshot original = table.tagManager().getOrThrow("kept").trimToSnapshot();
    TableTagInfo otherTag = ops.createTag(create(ops, "other", first));
    String other = table.fileIO().readFileUtf8(table.tagManager().tagPath(otherTag.getName()));
    table.createTag("expired", first, Duration.ofMillis(1));
    // An already expired native Tag proves these operations do not run automatic Tag cleanup.
    table
        .fileIO()
        .overwriteFileUtf8(
            table.tagManager().tagPath("expired"),
            Tag.fromSnapshotAndTagTtl(
                    original, Duration.ofMillis(1), LocalDateTime.of(2000, 1, 1, 0, 0))
                .toJson());
    String expired = table.fileIO().readFileUtf8(table.tagManager().tagPath("expired"));
    long current = commit(table, 8);
    assertTrue(
        table
                .newExpireSnapshots()
                .config(
                    ExpireConfig.builder()
                        .snapshotRetainMin(1)
                        .snapshotRetainMax(1)
                        .snapshotTimeRetain(Duration.ZERO)
                        .build())
                .expire()
            > 0);
    retainedState(table, original.toJson(), other, expired, current);

    for (long millis : new long[] {10800000L, 3600000L, 3600000L}) {
      LocalDateTime before = table.tagManager().getOrThrow("kept").getTagCreateTime();
      String observed = row.getObservationToken();
      row = ops.updateTagRetention(update(ops, row, "AGE_LIMIT", millis));
      assertEquals("kept", row.getName());
      assertEquals(Long.toString(first), row.getTarget().getId());
      assertEquals("main", row.getTarget().getBranch());
      assertEquals(millis, row.getRetention().getMaxAgeMillis());
      assertNotEquals(observed, row.getObservationToken());
      assertTrue(table.tagManager().getOrThrow("kept").getTagCreateTime().isAfter(before));
      retainedState(table, original.toJson(), other, expired, current);
    }
    row = ops.updateTagRetention(update(ops, row, "NATIVE_DEFAULT", null));
    assertEquals(7200000L, row.getRetention().getMaxAgeMillis());
    retainedState(table, original.toJson(), other, expired, current);
    row = ops.updateTagRetention(update(ops, row, "NO_AGE_LIMIT", null));
    assertEquals("UNSET", row.getRetention().getAgeLimit());
    assertNull(row.getRetention().getMaxAgeMillis());
    assertNull(table.tagManager().getOrThrow("kept").getTagCreateTime());
    retainedState(table, original.toJson(), other, expired, current);
    row = ops.updateTagRetention(update(ops, row, "AGE_LIMIT", 3600000L));
    assertNotNull(table.tagManager().getOrThrow("kept").getTagCreateTime());
    retainedState(table, original.toJson(), other, expired, current);

    TableTagInfo copied = ops.copyTag(copy(row, "copy"));
    assertEquals(row.getTarget().getId(), copied.getTarget().getId());
    assertEquals(
        original.toJson(), table.tagManager().getOrThrow("copy").trimToSnapshot().toJson());
    assertEquals("UNSET", copied.getRetention().getAgeLimit());
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "copy"))));
    DeleteTagRequest delete = new DeleteTagRequest();
    delete.setName(copied.getName());
    delete.setScope(copied.getScope());
    delete.setExpectedToken(copied.getObservationToken());
    assertEquals("NOT_REQUIRED", ops.deleteTag(delete).getHistoryCleanup());
    assertFalse(table.tagManager().tagExists("copy"));
    retainedState(table, original.toJson(), other, expired, current);
  }

  @ParameterizedTest(name = "HDFS create collision, primaryKey={0}")
  @ValueSource(booleans = {true, false})
  void createCollisionPreservesOriginalTagAndData(boolean primaryKey) throws Exception {
    PaimonTable owner = table(primaryKey ? "pk-collision" : "append-collision", primaryKey);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    TableTagOperations ops = owner.tagOperations().get();
    long first = commit(table, 7);
    ops.createTag(create(ops, "kept", first));
    String original = table.fileIO().readFileUtf8(table.tagManager().tagPath("kept"));
    long second = commit(table, 8);
    TagOperationException failure =
        assertThrows(TagOperationException.class, () -> ops.createTag(create(ops, "kept", second)));
    assertEquals(409, failure.getStatus());
    assertEquals("NOT_APPLIED", failure.getOutcome());
    assertEquals(original, table.fileIO().readFileUtf8(table.tagManager().tagPath("kept")));
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "kept"))));
    assertEquals(Arrays.asList(7, 8), rows(table));
  }

  @ParameterizedTest(name = "HDFS stale observation and defaults, primaryKey={0}")
  @ValueSource(booleans = {true, false})
  void updateRequiresCurrentObservationAndRefreshedNativeDefault(boolean primaryKey)
      throws Exception {
    String name = primaryKey ? "pk-preconditions" : "append-preconditions";
    PaimonTable owner = table(name, primaryKey);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    TableTagOperations ops = owner.tagOperations().get();
    long first = commit(table, 7);
    TableTagInfo row = ops.createTag(create(ops, "kept", first));
    UpdateTagRetentionRequest request = update(ops, row, "AGE_LIMIT", 3600000L);
    row = ops.updateTagRetention(request);
    String renewed = table.fileIO().readFileUtf8(table.tagManager().tagPath("kept"));
    TagOperationException stale =
        assertThrows(TagOperationException.class, () -> ops.updateTagRetention(request));
    assertEquals(412, stale.getStatus());
    assertEquals("NOT_APPLIED", stale.getOutcome());
    assertEquals(renewed, table.fileIO().readFileUtf8(table.tagManager().tagPath("kept")));

    UpdateTagRetentionRequest defaultRequest = update(ops, row, "NATIVE_DEFAULT", null);
    catalog.alterTable(
        Identifier.create("db", name),
        SchemaChange.setOption("tag.default-time-retained", "3 h"),
        false);
    assertEquals(
        412,
        assertThrows(TagOperationException.class, () -> ops.updateTagRetention(defaultRequest))
            .getStatus());
    assertEquals(renewed, table.fileIO().readFileUtf8(table.tagManager().tagPath("kept")));
    TableTagInfo refreshed = ops.updateTagRetention(update(ops, row, "NATIVE_DEFAULT", null));
    assertEquals(10800000L, refreshed.getRetention().getMaxAgeMillis());
    assertEquals(
        Tag.fromJson(renewed).trimToSnapshot().toJson(),
        table.tagManager().getOrThrow("kept").trimToSnapshot().toJson());
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "kept"))));
  }

  @Test
  void lostNativeOverwriteAcknowledgementReportsUnknownAndPreservesOtherReferencesAndData()
      throws Exception {
    PaimonTable owner = table("append-lost-overwrite-ack", false);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    FileIO nativeIO = table.fileIO();
    long first = commit(table, 7);
    TableTagOperations normal = owner.tagOperations().get();
    TableTagInfo row = normal.createTag(create(normal, "kept", first));
    normal.createTag(create(normal, "other", first));
    Path kept = table.tagManager().tagPath("kept");
    Path other = table.tagManager().tagPath("other");
    Path marker = new Path(table.location(), ".amoro-tag-management.lock");
    String originalSnapshot = nativeIO.readFileUtf8(table.snapshotManager().snapshotPath(first));
    String originalTagSnapshot = table.tagManager().getOrThrow("kept").trimToSnapshot().toJson();
    String originalOther = nativeIO.readFileUtf8(other);
    FileIO failing = spy(nativeIO);
    FileStoreTable refreshed = spy(table);
    doReturn(failing).when(refreshed).fileIO();
    doReturn(
            new org.apache.paimon.utils.TagManager(
                failing, table.location(), "main", table.coreOptions()))
        .when(refreshed)
        .tagManager();
    FileStore<?> store = spy(table.store());
    doReturn(store).when(refreshed).store();
    IcebergCommitCallback callback = mock(IcebergCommitCallback.class);
    doReturn(Collections.singletonList(callback)).when(store).createTagCallbacks(refreshed);
    doAnswer(
            call -> {
              // Lose acknowledgement after the native write, whether HadoopFileIO selects its
              // atomic rename path or its fallback because a filesystem wrapper hides rename.
              // The caller still cannot claim success or retry the mutation.
              String replacement = call.getArgument(1);
              nativeIO.overwriteFileUtf8(kept, replacement);
              throw new IOException("native HDFS overwrite acknowledgement lost");
            })
        .when(failing)
        .overwriteFileUtf8(eq(kept), anyString());
    TableTagOperations ops = new PaimonTagOperations(owner, () -> refreshed, false);
    TagOperationException failure =
        assertThrows(
            TagOperationException.class,
            () -> ops.updateTagRetention(update(ops, row, "AGE_LIMIT", 3600000L)));
    assertEquals(500, failure.getStatus());
    assertEquals("UNKNOWN", failure.getOutcome());
    Tag actual = table.tagManager().getOrThrow("kept");
    assertEquals(Duration.ofHours(1), actual.getTagTimeRetained());
    assertEquals(originalTagSnapshot, actual.trimToSnapshot().toJson());
    verify(failing, times(1)).overwriteFileUtf8(eq(kept), anyString());
    verify(callback, never()).notifyCreation(anyString(), anyLong());
    verify(callback, never()).close();
    assertFalse(nativeIO.exists(marker));
    assertEquals(
        originalSnapshot, nativeIO.readFileUtf8(table.snapshotManager().snapshotPath(first)));
    assertEquals(originalOther, nativeIO.readFileUtf8(other));
    assertEquals(Collections.singletonList(7), rows(table));
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "kept"))));
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "other"))));
  }

  @Test
  void nativeDistributedFileSystemTemporaryFailurePreservesOriginalTagAndNeverRetries()
      throws Exception {
    PaimonTable owner = table("append-temporary-write-failure", false);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    FileIO nativeIO = table.fileIO();
    long first = commit(table, 7);
    TableTagOperations normal = owner.tagOperations().get();
    TableTagInfo row = normal.createTag(create(normal, "kept", first));
    Path kept = table.tagManager().tagPath("kept");
    Path marker = new Path(table.location(), ".amoro-tag-management.lock");
    String original = nativeIO.readFileUtf8(kept);
    HadoopFileIO configured = new HadoopFileIO(table.location());
    configured.configure(
        CatalogContext.create(Options.fromMap(table.options()), cluster.getConfiguration(0)));
    // The default secured wrapper exposes only two-argument rename and selects direct overwrite.
    // Use the actual DistributedFileSystem to exercise the SDK's native atomic temporary branch.
    configured.setFileSystem(cluster.getFileSystem());
    HadoopFileIO failing = spy(configured);
    // HadoopFileIO's native HDFS overwrite must finish its temporary file before replacement.
    // Match only a temporary file in the Tag directory.
    doThrow(new IOException("native HDFS temporary creation failed"))
        .when(failing)
        .newOutputStream(
            argThat(path -> kept.getParent().equals(path.getParent()) && !kept.equals(path)),
            eq(false));
    FileStoreTable refreshed = spy(table);
    doReturn(failing).when(refreshed).fileIO();
    doReturn(
            new org.apache.paimon.utils.TagManager(
                failing, table.location(), "main", table.coreOptions()))
        .when(refreshed)
        .tagManager();
    TableTagOperations ops = new PaimonTagOperations(owner, () -> refreshed, false);
    TagOperationException failure =
        assertThrows(
            TagOperationException.class,
            () -> ops.updateTagRetention(update(ops, row, "AGE_LIMIT", 3600000L)));
    assertEquals(500, failure.getStatus());
    assertEquals("UNKNOWN", failure.getOutcome());
    assertEquals(original, nativeIO.readFileUtf8(kept));
    assertEquals(Duration.ofHours(2), table.tagManager().getOrThrow("kept").getTagTimeRetained());
    verify(failing, times(1)).overwriteFileUtf8(eq(kept), anyString());
    verify(failing, times(1)).tryAtomicOverwriteViaRename(eq(kept), anyString());
    verify(failing, times(1))
        .newOutputStream(
            argThat(path -> kept.getParent().equals(path.getParent()) && !kept.equals(path)),
            eq(false));
    assertFalse(nativeIO.exists(marker));
    assertEquals(Collections.singletonList(7), rows(table));
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "kept"))));
  }

  @Test
  void defaultSecuredFilesystemFallbackFailureReportsUnknownAfterTruncation() throws Exception {
    PaimonTable owner = table("append-default-fallback-truncation", false);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    FileIO nativeIO = table.fileIO();
    long first = commit(table, 7);
    TableTagOperations normal = owner.tagOperations().get();
    TableTagInfo row = normal.createTag(create(normal, "kept", first));
    normal.createTag(create(normal, "other", first));
    Path kept = table.tagManager().tagPath("kept");
    Path other = table.tagManager().tagPath("other");
    Path marker = new Path(table.location(), ".amoro-tag-management.lock");
    String originalSnapshot = nativeIO.readFileUtf8(table.snapshotManager().snapshotPath(first));
    String originalOther = nativeIO.readFileUtf8(other);
    assertTrue(nativeIO.getFileStatus(kept).getLen() > 0);
    FileIO failing = spy(nativeIO);
    FileStoreTable refreshed = spy(table);
    doReturn(failing).when(refreshed).fileIO();
    doReturn(
            new org.apache.paimon.utils.TagManager(
                failing, table.location(), "main", table.coreOptions()))
        .when(refreshed)
        .tagManager();
    FileStore<?> store = spy(table.store());
    doReturn(store).when(refreshed).store();
    IcebergCommitCallback callback = mock(IcebergCommitCallback.class);
    doReturn(Collections.singletonList(callback)).when(store).createTagCallbacks(refreshed);
    doAnswer(
            call -> {
              // The default Paimon 1.4.2 secured filesystem hides public three-argument rename.
              // Native overwrite therefore opens the actual target with overwrite=true. Model
              // failure after HDFS has truncated it, before any replacement bytes are written.
              try (PositionOutputStream truncated = nativeIO.newOutputStream(kept, true)) {
                // Close the actual HDFS stream without writing replacement metadata.
              }
              throw new IOException("native HDFS target creation failed after truncation");
            })
        .when(failing)
        .newOutputStream(kept, true);
    TableTagOperations ops = new PaimonTagOperations(owner, () -> refreshed, false);
    TagOperationException failure =
        assertThrows(
            TagOperationException.class,
            () -> ops.updateTagRetention(update(ops, row, "AGE_LIMIT", 3600000L)));
    assertEquals(500, failure.getStatus());
    assertEquals("UNKNOWN", failure.getOutcome());
    assertEquals(0L, nativeIO.getFileStatus(kept).getLen());
    assertEquals("", nativeIO.readFileUtf8(kept));
    verify(failing, times(1)).overwriteFileUtf8(eq(kept), anyString());
    verify(failing, times(1)).newOutputStream(kept, true);
    verify(callback, never()).notifyCreation(anyString(), anyLong());
    verify(callback, never()).close();
    assertFalse(nativeIO.exists(marker));
    assertEquals(
        originalSnapshot, nativeIO.readFileUtf8(table.snapshotManager().snapshotPath(first)));
    assertEquals(originalOther, nativeIO.readFileUtf8(other));
    assertEquals(Collections.singletonList(7), rows(table));
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "other"))));
  }

  @Test
  void nativeQuietDeleteFailureIsRejectedByReferenceReadback() throws Exception {
    PaimonTable owner = table("append-quiet-delete-failure", false);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    long first = commit(table, 7);
    TableTagOperations normal = owner.tagOperations().get();
    TableTagInfo row = normal.createTag(create(normal, "kept", first));
    Path path = table.tagManager().tagPath("kept");
    String original = table.fileIO().readFileUtf8(path);
    FileIO failing = spy(table.fileIO());
    doReturn(false).when(failing).delete(path, false);
    FileStoreTable refreshed = spy(table);
    doReturn(failing).when(refreshed).fileIO();
    doReturn(
            new org.apache.paimon.utils.TagManager(
                failing, table.location(), "main", table.coreOptions()))
        .when(refreshed)
        .tagManager();
    TableTagOperations ops = new PaimonTagOperations(owner, () -> refreshed, false);
    DeleteTagRequest request = new DeleteTagRequest();
    request.setName(row.getName());
    request.setScope(row.getScope());
    request.setExpectedToken(row.getObservationToken());
    TagOperationException failure =
        assertThrows(TagOperationException.class, () -> ops.deleteTag(request));
    assertEquals("UNKNOWN", failure.getOutcome());
    verify(refreshed, times(1)).deleteTag("kept");
    verify(failing, times(1)).delete(path, false);
    assertEquals(original, table.fileIO().readFileUtf8(path));
    assertEquals(Collections.singletonList(7), rows(table));
  }

  @Test
  void nativeReplacementOwnsCallbackFilteringAndQuietClose() throws Exception {
    PaimonTable owner = table("append-native-callbacks", false);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    long first = commit(table, 7);
    TableTagOperations normal = owner.tagOperations().get();
    TableTagInfo row = normal.createTag(create(normal, "kept", first));
    FileStoreTable refreshed = spy(table);
    FileStore<?> store = spy(table.store());
    doReturn(store).when(refreshed).store();
    IcebergCommitCallback selected = mock(IcebergCommitCallback.class);
    TagCallback other = mock(TagCallback.class);
    doReturn(Arrays.asList(other, selected)).when(store).createTagCallbacks(refreshed);
    doThrow(new IOException("native callback close failed")).when(selected).close();
    TableTagOperations ops = new PaimonTagOperations(owner, () -> refreshed, false);
    TableTagInfo actual = ops.updateTagRetention(update(ops, row, "AGE_LIMIT", 3600000L));
    assertEquals(3600000L, actual.getRetention().getMaxAgeMillis());
    verify(refreshed, times(1)).replaceTag("kept", first, Duration.ofHours(1));
    verify(selected, times(1)).notifyCreation("kept", first);
    verify(selected, times(1)).close();
    verify(other, never()).notifyCreation(anyString(), anyLong());
    verify(other, never()).close();
    assertEquals(Collections.singletonList(7), rows(table));
  }

  @Test
  void deletionCallbackFailureReportsUnknownAfterRemovingNativeTag() throws Exception {
    PaimonTable owner = table("append-deletion-callback-failure", false);
    FileStoreTable table = (FileStoreTable) owner.originalTable();
    FileIO nativeIO = table.fileIO();
    long first = commit(table, 7);
    TableTagOperations normal = owner.tagOperations().get();
    TableTagInfo row = normal.createTag(create(normal, "kept", first));
    normal.createTag(create(normal, "other", first));
    Path kept = table.tagManager().tagPath("kept");
    Path other = table.tagManager().tagPath("other");
    Path marker = new Path(table.location(), ".amoro-tag-management.lock");
    String originalSnapshot = nativeIO.readFileUtf8(table.snapshotManager().snapshotPath(first));
    String originalOther = nativeIO.readFileUtf8(other);
    FileIO watched = spy(nativeIO);
    FileStoreTable refreshed = spy(table);
    doReturn(watched).when(refreshed).fileIO();
    doReturn(
            new org.apache.paimon.utils.TagManager(
                watched, table.location(), "main", table.coreOptions()))
        .when(refreshed)
        .tagManager();
    FileStore<?> store = spy(table.store());
    doReturn(store).when(refreshed).store();
    TagCallback callback = mock(TagCallback.class);
    doReturn(Collections.singletonList(callback)).when(store).createTagCallbacks(refreshed);
    doThrow(new IllegalStateException("HDFS Tag deletion callback failed"))
        .when(callback)
        .notifyDeletion("kept");
    TableTagOperations ops = new PaimonTagOperations(owner, () -> refreshed, false);
    DeleteTagRequest request = new DeleteTagRequest();
    request.setName(row.getName());
    request.setScope(row.getScope());
    request.setExpectedToken(row.getObservationToken());
    TagOperationException failure =
        assertThrows(TagOperationException.class, () -> ops.deleteTag(request));
    assertEquals(500, failure.getStatus());
    assertEquals("UNKNOWN", failure.getOutcome());
    assertFalse(nativeIO.exists(kept));
    assertFalse(nativeIO.exists(marker));
    verify(watched, times(1)).delete(kept, false);
    verify(callback, times(1)).notifyDeletion("kept");
    verify(callback, times(1)).close();
    assertEquals(
        originalSnapshot, nativeIO.readFileUtf8(table.snapshotManager().snapshotPath(first)));
    assertEquals(originalOther, nativeIO.readFileUtf8(other));
    assertEquals(Collections.singletonList(7), rows(table));
    assertEquals(
        Collections.singletonList(7),
        rows(table.copy(Collections.singletonMap("scan.tag-name", "other"))));
  }
}
