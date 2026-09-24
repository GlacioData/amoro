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

import org.apache.amoro.AmoroTable;
import org.apache.amoro.formats.AmoroCatalogTestHelper;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TestServerTableDescriptor;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(Parameterized.class)
public class TestPaimonServerTableDescriptor extends TestServerTableDescriptor {

  public TestPaimonServerTableDescriptor(AmoroCatalogTestHelper<?> amoroCatalogTestHelper) {
    super(amoroCatalogTestHelper);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Object[] parameters() {
    return new Object[] {
      PaimonHadoopCatalogTestHelper.defaultHelper(), PaimonHiveCatalogTestHelper.defaultHelper()
    };
  }

  @Override
  protected void tableOperationsAddColumns() {
    try {
      getCatalog()
          .alterTable(
              Identifier.create(
                  TestServerTableDescriptor.TEST_DB, TestServerTableDescriptor.TEST_TABLE),
              SchemaChange.addColumn("new_col", DataTypes.INT()),
              false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsRenameColumns() {
    try {
      getCatalog()
          .alterTable(
              Identifier.create(
                  TestServerTableDescriptor.TEST_DB, TestServerTableDescriptor.TEST_TABLE),
              SchemaChange.renameColumn("new_col", "renamed_col"),
              false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsChangeColumnType() {
    try {
      getCatalog()
          .alterTable(
              Identifier.create(
                  TestServerTableDescriptor.TEST_DB, TestServerTableDescriptor.TEST_TABLE),
              SchemaChange.updateColumnType("renamed_col", DataTypes.BIGINT()),
              false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsChangeColumnComment() {
    try {
      getCatalog()
          .alterTable(
              Identifier.create(
                  TestServerTableDescriptor.TEST_DB, TestServerTableDescriptor.TEST_TABLE),
              SchemaChange.updateColumnComment("renamed_col", "new comment"),
              false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsChangeColumnRequired() {
    try {
      getCatalog()
          .alterTable(
              Identifier.create(
                  TestServerTableDescriptor.TEST_DB, TestServerTableDescriptor.TEST_TABLE),
              SchemaChange.updateColumnNullability("renamed_col", false),
              false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void tableOperationsDropColumn() {
    try {
      getCatalog()
          .alterTable(
              Identifier.create(
                  TestServerTableDescriptor.TEST_DB, TestServerTableDescriptor.TEST_TABLE),
              SchemaChange.dropColumn("renamed_col"),
              false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FormatTableDescriptor getTableDescriptor() {
    return new PaimonTableDescriptor();
  }

  @Test
  public void getTableDetailUsesSnapshotOnly() throws Exception {
    Identifier identifier = Identifier.create(TEST_DB, TEST_TABLE);
    Table table = getCatalog().getTable(identifier);
    writeRecords(table);

    FileStore<?> store = ((FileStoreTable) table).store();
    Snapshot snapshot = store.snapshotManager().latestSnapshot();
    long expectedRecords = snapshot != null ? snapshot.totalRecordCount() : 0;

    ServerTableMeta tableMeta =
        getTableDescriptor().getTableDetail(getAmoroCatalog().loadTable(TEST_DB, TEST_TABLE));

    Assert.assertNotNull(snapshot);
    Assert.assertEquals(
        Long.valueOf(snapshot.id()), tableMeta.getDetailObservation().getSnapshotId());
    Assert.assertEquals(
        ((FileStoreTable) table).schema().id(), tableMeta.getDetailObservation().getSchemaId());
    Assert.assertFalse(tableMeta.getDetailObservation().isEmptySnapshot());
    Assert.assertEquals(snapshot.timeMillis(), tableMeta.getBaseMetrics().get("lastCommitTime"));
    Assert.assertNull(tableMeta.getBaseMetrics().get("baseWatermark"));
    Assert.assertEquals(expectedRecords, tableMeta.getTableSummary().getRecords());
    Assert.assertNull(tableMeta.getTableSummary().getFile());
    Assert.assertNull(tableMeta.getBaseMetrics().get("fileCount"));
    Assert.assertNull(tableMeta.getBaseMetrics().get("totalSize"));
    Assert.assertNull(tableMeta.getBaseMetrics().get("averageFileSize"));
    Assert.assertNull(tableMeta.getTableSummary().getSize());
    Assert.assertNull(tableMeta.getTableSummary().getAverageFile());
  }

  @Test
  public void emptyTableObservationPreservesUnknownSizeAndAbsentCommitTime() throws Exception {
    FileStoreTable table =
        (FileStoreTable) getCatalog().getTable(Identifier.create(TEST_DB, TEST_TABLE));
    Assert.assertNull(table.snapshotManager().latestSnapshot());
    ServerTableMeta detail =
        getTableDescriptor().getTableDetail(getAmoroCatalog().loadTable(TEST_DB, TEST_TABLE));
    Assert.assertTrue(detail.getDetailObservation().isEmptySnapshot());
    Assert.assertNull(detail.getDetailObservation().getSnapshotId());
    Assert.assertEquals(table.schema().id(), detail.getDetailObservation().getSchemaId());
    Assert.assertEquals(0, detail.getBaseMetrics().get("fileCount"));
    Assert.assertEquals(0L, detail.getTableSummary().getRecords());
    Assert.assertNull(detail.getBaseMetrics().get("totalSize"));
    Assert.assertNull(detail.getBaseMetrics().get("averageFileSize"));
    Assert.assertNull(detail.getBaseMetrics().get("lastCommitTime"));
    Assert.assertNull(detail.getBaseMetrics().get("baseWatermark"));
  }

  @Test
  public void schemaOnlyAlterationKeepsSnapshotTimeAndWatermarkButObservesCurrentSchema()
      throws Exception {
    Identifier identifier = Identifier.create(TEST_DB, TEST_TABLE);
    FileStoreTable original = (FileStoreTable) getCatalog().getTable(identifier);
    long watermark = 1789603200000L;
    writeRecords(original, watermark);
    Snapshot snapshot = original.snapshotManager().latestSnapshot();
    Assert.assertEquals(Long.valueOf(watermark), snapshot.watermark());
    getCatalog().alterTable(identifier, SchemaChange.addColumn("added", DataTypes.STRING()), false);
    AmoroTable<?> current = getAmoroCatalog().loadTable(TEST_DB, TEST_TABLE);
    long currentSchema = ((FileStoreTable) current.originalTable()).schema().id();
    Assert.assertTrue(currentSchema > snapshot.schemaId());
    ServerTableMeta detail = getTableDescriptor().getTableDetail(current);
    Assert.assertEquals(Long.valueOf(snapshot.id()), detail.getDetailObservation().getSnapshotId());
    Assert.assertEquals(currentSchema, detail.getDetailObservation().getSchemaId());
    Assert.assertEquals(snapshot.timeMillis(), detail.getBaseMetrics().get("lastCommitTime"));
    Assert.assertEquals(watermark, detail.getBaseMetrics().get("baseWatermark"));
    Assert.assertNull(detail.getBaseMetrics().get("fileCount"));
    Assert.assertNull(detail.getBaseMetrics().get("totalSize"));
    Assert.assertNull(detail.getBaseMetrics().get("averageFileSize"));
  }

  @Test
  public void repeatedDetailsUseOnlyTheExistingSnapshotReadIo() throws Exception {
    FileStoreTable original =
        (FileStoreTable) getCatalog().getTable(Identifier.create(TEST_DB, TEST_TABLE));
    writeRecords(original);
    CountingFileIO io = new CountingFileIO();
    FileStoreTable tracked =
        FileStoreTableFactory.create(io, original.location(), original.schema());
    PaimonTable table =
        new PaimonTable(getAmoroCatalog().loadTable(TEST_DB, TEST_TABLE).id(), tracked);
    FileStore<?> store = tracked.store();
    Snapshot expected = original.snapshotManager().latestSnapshot();
    io.operations.clear();
    // Compare the pre-existing snapshot-read operation, not a separately built baseline binary.
    for (int i = 0; i < 100; i++) {
      Snapshot observed =
          PaimonSnapshotMetadata.latest(store.snapshotManager(), new PaimonSnapshotReadDeadline());
      Assert.assertEquals(expected.id(), observed.id());
    }
    Map<String, Integer> snapshotReads = new HashMap<>(io.operations);
    Assert.assertTrue(snapshotReads.getOrDefault("open:snapshot", 0) >= 100);
    Assert.assertTrue(snapshotReads.getOrDefault("open:hint", 0) >= 100);
    io.operations.clear();
    FormatTableDescriptor descriptor = getTableDescriptor();
    for (int i = 0; i < 100; i++) {
      ServerTableMeta detail = descriptor.getTableDetail(table);
      Assert.assertEquals(
          Long.valueOf(expected.id()), detail.getDetailObservation().getSnapshotId());
      Assert.assertEquals(expected.timeMillis(), detail.getBaseMetrics().get("lastCommitTime"));
      Assert.assertNull(detail.getBaseMetrics().get("fileCount"));
    }
    Assert.assertEquals(snapshotReads, io.operations);
    Assert.assertEquals(0, io.operations.getOrDefault("open:manifest", 0).intValue());
    Assert.assertEquals(0, io.operations.getOrDefault("open:data", 0).intValue());
    Assert.assertFalse(io.operations.keySet().stream().anyMatch(key -> key.startsWith("list:")));
  }

  private static final class CountingFileIO extends LocalFileIO {
    private static final long serialVersionUID = 1L;
    private final Map<String, Integer> operations = new ConcurrentHashMap<>();

    private void record(String operation, Path path) {
      String name = path.getName();
      String category =
          name.startsWith("snapshot-")
              ? "snapshot"
              : name.equals("LATEST") || name.equals("EARLIEST")
                  ? "hint"
                  : name.startsWith("manifest-")
                      ? "manifest"
                      : path.toString().contains("/bucket-") ? "data" : "other";
      operations.merge(operation + ":" + category, 1, Integer::sum);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
      record("open", path);
      return super.newInputStream(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
      record("exists", path);
      return super.exists(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
      record("list", path);
      return super.listStatus(path);
    }
  }

  private void writeRecords(Table table) throws Exception {
    writeRecords(table, null);
  }

  private void writeRecords(Table table, Long watermark) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite()) {
      write.write(GenericRow.of(1, BinaryString.fromString("alice"), 10), 0);
      write.write(GenericRow.of(2, BinaryString.fromString("bob"), 20), 0);
      List<CommitMessage> messages = write.prepareCommit();
      try (BatchTableCommit commit = builder.newCommit()) {
        if (watermark == null) {
          commit.commit(messages);
        } else {
          ((TableCommitImpl) commit)
              .commit(new ManifestCommittable(Long.MAX_VALUE, watermark, messages));
        }
      }
    }
  }

  private Catalog getCatalog() {
    return (Catalog) getOriginalCatalog();
  }
}
