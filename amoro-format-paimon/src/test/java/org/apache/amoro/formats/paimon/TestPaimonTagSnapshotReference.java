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

package org.apache.amoro.formats.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.SnapshotDetailPage;
import org.apache.amoro.table.tag.TableTagInfo;
import org.apache.amoro.table.tag.TagQuery;
import org.apache.amoro.table.tag.TagScope;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class TestPaimonTagSnapshotReference {
  @TempDir java.nio.file.Path warehouse;

  @Test
  void explicitTagReferenceWinsOverSameNameBranchAndChecksVersion() throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      catalog.createTable(
          Identifier.create("db", "test"),
          Schema.newBuilder()
              .column("id", DataTypes.INT())
              .option("bucket", "-1")
              .option("write-only", "true")
              .build(),
          false);
      FileStoreTable table = (FileStoreTable) catalog.getTable(Identifier.create("db", "test"));
      org.apache.paimon.table.sink.BatchWriteBuilder builder = table.newBatchWriteBuilder();
      try (org.apache.paimon.table.sink.BatchTableWrite write = builder.newWrite();
          org.apache.paimon.table.sink.BatchTableCommit commit = builder.newCommit()) {
        write.write(org.apache.paimon.data.GenericRow.of(1));
        commit.commit(write.prepareCommit());
      }
      long id = table.snapshotManager().latestSnapshotId();
      table.createTag("same", id);
      table.branchManager().createBranch("same", "same");
      PaimonSnapshotReadDeadline deadline = new PaimonSnapshotReadDeadline();
      assertEquals(id, PaimonSnapshotMetadata.resolveTag(table, "same", id, deadline).id());
      assertEquals(id, PaimonSnapshotMetadata.resolveBranch(table, "same", id, deadline).id());
      table.fileIO().delete(table.snapshotManager().copyWithBranch("same").snapshotPath(id), false);
      assertEquals(id, PaimonSnapshotMetadata.resolveTag(table, "same", id, deadline).id());
      assertThrows(
          org.apache.amoro.table.descriptor.SnapshotReadException.class,
          () -> PaimonSnapshotMetadata.resolveBranch(table, "same", id, deadline));
      assertThrows(
          org.apache.amoro.table.descriptor.SnapshotReadException.class,
          () -> PaimonSnapshotMetadata.resolveTag(table, "same", id + 1, deadline));
      assertThrows(
          org.apache.amoro.table.descriptor.SnapshotReadException.class,
          () -> PaimonSnapshotMetadata.resolveBranch(table, "missing", id, deadline));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void mainTagDetailSurvivesMissingBranchTagAndExpiredSnapshot(boolean primaryKey)
      throws Exception {
    try (Catalog catalog = partitionedCatalog(primaryKey)) {
      FileStoreTable main = (FileStoreTable) catalog.getTable(Identifier.create("db", "test"));
      long id = commit(main, 1);
      main.createTag("release", id);
      SnapshotDetailPage expected = detail(owner(catalog, "test"), id, "release");
      main.createBranch("audit", "release");
      PaimonTable branchOwner = owner(catalog, "test$branch_audit");
      FileStoreTable branch = (FileStoreTable) branchOwner.originalTable();
      branch.deleteTag("release");
      main.snapshotManager().deleteSnapshot(id);
      assertTrue(main.tagManager().tagExists("release"));
      assertFalse(branch.tagManager().tagExists("release"));
      assertFalse(main.snapshotManager().snapshotExists(id));
      String original = main.tagManager().getOrThrow("release").toJson();

      TagQuery query = new TagQuery();
      query.setScope(TagScope.main());
      TableTagInfo listed =
          new PaimonTableDescriptor().getTableTags(branchOwner, query).getLeft().get(0);
      assertEquals("release", listed.getName());
      assertEquals("main", listed.getScope().getBranch());
      assertEquals(Long.toString(id), listed.getTarget().getId());
      assertSameFiles(main, expected, detail(branchOwner, id, listed.getName()));
      assertEquals(original, main.tagManager().getOrThrow("release").toJson());
      assertFalse(branch.tagManager().tagExists("release"));
    }
  }

  @Test
  void mainTagDetailDoesNotFollowSameNamedBranchTagToAnotherSnapshot() throws Exception {
    try (Catalog catalog = partitionedCatalog(false)) {
      FileStoreTable main = (FileStoreTable) catalog.getTable(Identifier.create("db", "test"));
      long id = commit(main, 1);
      main.createTag("release", id);
      SnapshotDetailPage expected = detail(owner(catalog, "test"), id, "release");
      main.createBranch("audit", "release");
      PaimonTable branchOwner = owner(catalog, "test$branch_audit");
      FileStoreTable branch = (FileStoreTable) branchOwner.originalTable();
      long branchId = commit(branch, 2);
      branch.replaceTag("release", branchId, null);
      assertNotEquals(id, branchId);
      String mainTag = main.tagManager().getOrThrow("release").toJson();
      String branchTag = branch.tagManager().getOrThrow("release").toJson();

      assertSameFiles(main, expected, detail(branchOwner, id, "release"));
      assertEquals(mainTag, main.tagManager().getOrThrow("release").toJson());
      assertEquals(branchTag, branch.tagManager().getOrThrow("release").toJson());
    }
  }

  @Test
  void mainTagDetailUsesMainStoreWhenBranchSchemaDiffers() throws Exception {
    try (Catalog catalog = partitionedCatalog(false)) {
      FileStoreTable main = (FileStoreTable) catalog.getTable(Identifier.create("db", "test"));
      long id = commit(main, 1);
      main.createTag("release", id);
      SnapshotDetailPage expected = detail(owner(catalog, "test"), id, "release");
      main.createBranch("audit", "release");
      catalog.alterTable(
          Identifier.create("db", "test$branch_audit"),
          SchemaChange.addColumn("audit_only", DataTypes.STRING()),
          false);
      FileStoreTable branch =
          spy((FileStoreTable) owner(catalog, "test$branch_audit").originalTable());
      assertNotEquals(main.schema().fields(), branch.schema().fields());
      // A main-scoped read must also avoid the unrelated branch's manifest and path context.
      // The real SDK switches the schema and catalog environment together with the branch.
      doThrow(new AssertionError("Main Tag detail must not read audit store")).when(branch).store();
      PaimonTable branchOwner =
          new PaimonTable(TableIdentifier.of("local", "db", "test$branch_audit"), branch);

      assertSameFiles(main, expected, detail(branchOwner, id, "release"));
      assertEquals("audit", branch.options().get("branch"));
      assertTrue(main.tagManager().tagExists("release"));
    }
  }

  private Catalog partitionedCatalog(boolean primaryKey) throws Exception {
    Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration());
    catalog.createDatabase("db", true);
    Schema.Builder schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("p", DataTypes.STRING())
            .partitionKeys("p")
            .option("bucket", primaryKey ? "1" : "-1")
            .option("write-only", "true");
    if (primaryKey) {
      schema.primaryKey("id", "p");
    }
    catalog.createTable(Identifier.create("db", "test"), schema.build(), false);
    return catalog;
  }

  private PaimonTable owner(Catalog catalog, String name) {
    return (PaimonTable)
        new PaimonCatalog(catalog, "local", Collections.emptyMap()).loadTable("db", name);
  }

  private long commit(FileStoreTable table, int value) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      write.write(GenericRow.of(value, BinaryString.fromString("p" + value)));
      commit.commit(write.prepareCommit());
    }
    return table.snapshotManager().latestSnapshotId();
  }

  private SnapshotDetailPage detail(PaimonTable table, long id, String tag) {
    return new PaimonTableDescriptor()
        .getSnapshotDetailPage(table, Long.toString(id), tag, "TAG", TagScope.main(), 25, null);
  }

  private void assertSameFiles(
      FileStoreTable table, SnapshotDetailPage expected, SnapshotDetailPage actual)
      throws Exception {
    assertFalse(expected.getList().isEmpty());
    assertNull(expected.getNextCursor());
    assertNull(actual.getNextCursor());
    List<String> expectedFiles =
        expected.getList().stream()
            .map(file -> file.getPartition() + "|" + file.getPath() + "|" + file.getOperation())
            .collect(Collectors.toList());
    assertEquals(
        expectedFiles,
        actual.getList().stream()
            .map(file -> file.getPartition() + "|" + file.getPath() + "|" + file.getOperation())
            .collect(Collectors.toList()));
    for (org.apache.amoro.table.descriptor.PartitionFileBaseInfo file : actual.getList()) {
      assertTrue(table.fileIO().exists(new org.apache.paimon.fs.Path(file.getPath())));
    }
  }
}
