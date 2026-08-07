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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.table.descriptor.PartitionBaseInfo;
import org.apache.amoro.table.descriptor.PartitionFileBaseInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Locks the Paimon Files-query correctness/performance fix in {@link PaimonTableDescriptor}.
 *
 * <p>Covers the contracts in the spec §9.1:
 *
 * <ul>
 *   <li>Canonical single-slash partition identifiers (single field, multi field, non-partitioned).
 *   <li>Legacy double-slash request input resolves to the same files as the canonical form.
 *   <li>Malformed / missing-field / non-numeric-bucket input returns an empty list without falling
 *       back to a full-table scan.
 *   <li>Append-only and primary-key tables scope the file list to the requested partition/bucket,
 *       preserving {@code plan().files(FileKind.ADD)} (whole-bucket, DELETE-excluded) semantics.
 *   <li>{@code commitId} is {@code null}, {@code commitTime} equals {@code
 *       creationTime().getMillisecond()} with no offset, and the physical {@code path} stays a
 *       single-slash Paimon path.
 *   <li>The file query runs exactly one current Scan and never touches the snapshot history.
 *   <li>{@code getSnapshotDetail} (BASE_FILE) shares the canonical-partition helper.
 * </ul>
 *
 * <p>Correctness cases build a real Paimon table on a temp warehouse via {@link
 * PaimonCatalogFactory}; bucketing is hash-fixed (a {@code bucket-key} plus {@code bucket=N}), so
 * tests discover the actual partition/bucket identifiers from {@code getTablePartitions} rather
 * than assuming a bucket number. The interaction-contract case uses Mockito to assert call counts
 * on a mocked store, which is the right tool for "did not invoke" assertions.
 */
class TestPaimonTableDescriptorFileListing {

  private static final String DB = "files_db";
  private static final Pattern CANONICAL_ID =
      Pattern.compile(".*/bucket-(-?\\d+)$|bucket-(-?\\d+)$");

  @TempDir Path warehouse;
  private Catalog catalog;
  private PaimonTableDescriptor descriptor;

  @BeforeEach
  void setUp() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(CatalogOptions.WAREHOUSE.key(), warehouse.toUri().toString());
    catalog = PaimonCatalogFactory.paimonCatalog(props, new Configuration());
    catalog.createDatabase(DB, true);
    descriptor = new PaimonTableDescriptor();
  }

  // ---- helpers ----------------------------------------------------------

  private FileStoreTable createTable(String name, Schema schema) throws Exception {
    Identifier id = Identifier.create(DB, name);
    catalog.createTable(id, schema, true);
    return (FileStoreTable) catalog.getTable(id);
  }

  /** Append-only, single STRING partition {@code dt}, hash-fixed 2 buckets on {@code id}. */
  private static Schema appendSchema() {
    return Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("dt", DataTypes.STRING())
        .partitionKeys("dt")
        .option("bucket", "2")
        .option("bucket-key", "id")
        .build();
  }

  private static Schema pkSchema() {
    return Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("v", DataTypes.INT())
        .column("dt", DataTypes.STRING())
        .primaryKey("id")
        .partitionKeys("dt")
        .option("bucket", "2")
        .build();
  }

  /**
   * Writes rows in one commit; buckets are auto-assigned (hash-fixed append or primary-key hash).
   */
  private void writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite()) {
      for (GenericRow row : rows) {
        write.write(row);
      }
      List<CommitMessage> messages = write.prepareCommit();
      try (BatchTableCommit commit = builder.newCommit()) {
        commit.commit(messages);
      }
    }
  }

  private static GenericRow row(int id, String dt) {
    return GenericRow.of(id, BinaryString.fromString(dt));
  }

  private static GenericRow pkRow(int id, int v, String dt) {
    return GenericRow.of(id, v, BinaryString.fromString(dt));
  }

  /**
   * Wraps a real FileStoreTable in a lightweight AmoroTable; doAs then runs the callable inline.
   */
  private static AmoroTable<?> wrap(FileStoreTable table) {
    AmoroTable<?> amoroTable = mock(AmoroTable.class);
    when(((AmoroTable) amoroTable).originalTable()).thenReturn(table);
    return amoroTable;
  }

  /** Mirrors {@code PaimonTableDescriptor#fullFilePath} so test paths line up with output paths. */
  private static String fullPath(FileStore<?> store, ManifestEntry e) {
    return store
        .pathFactory()
        .createDataFilePathFactory(e.partition(), e.bucket())
        .toPath(e.file())
        .toString();
  }

  /** Mirrors {@code canonicalPartitionBucket} for ground-truth comparisons. */
  private static String canonical(FileStore<?> store, ManifestEntry e) {
    String s = store.pathFactory().getPartitionString(e.partition());
    String base = s.isEmpty() ? "" : s.substring(0, s.length() - 1);
    return base.isEmpty() ? "bucket-" + e.bucket() : base + "/bucket-" + e.bucket();
  }

  private static Map<String, Long> rawAddCountsByPartition(FileStore<?> store) {
    Map<String, Long> counts = new HashMap<>();
    for (ManifestEntry e : store.newScan().plan().files(FileKind.ADD)) {
      counts.merge(canonical(store, e), 1L, Long::sum);
    }
    return counts;
  }

  private static long creationMillisForPath(FileStore<?> store, String path) {
    for (ManifestEntry e : store.newScan().plan().files(FileKind.ADD)) {
      if (fullPath(store, e).equals(path)) {
        return e.file().creationTime().getMillisecond();
      }
    }
    throw new AssertionError("no raw ADD entry for path " + path);
  }

  /**
   * Builds the legacy double-slash variant of a canonical id by inserting one extra {@code /} right
   * before the {@code /bucket-} separator. The canonical id has exactly one such separator.
   */
  private static String legacyDoubleSlash(String canonicalId) {
    int i = canonicalId.lastIndexOf("/bucket-");
    return canonicalId.substring(0, i) + "/" + canonicalId.substring(i);
  }

  private static void assertCanonical(String id) {
    assertNotNull(id);
    assertFalse(id.contains("//"), "partition id must not contain '//': " + id);
    Matcher m = CANONICAL_ID.matcher(id);
    assertTrue(m.matches(), "partition id must be canonical (<part>/bucket-N or bucket-N): " + id);
  }

  // ---- T1: single-field canonical partition list -----------------------

  @Test
  void partitionListUsesCanonicalSingleSlashIdentifier() throws Exception {
    FileStoreTable table = createTable("tpart", appendSchema());
    writeRows(
        table,
        row(1, "2026-01-22"),
        row(2, "2026-01-22"),
        row(3, "2026-01-22"),
        row(4, "2026-01-23"),
        row(5, "2026-01-23"),
        row(6, "2026-01-23"));

    List<PartitionBaseInfo> parts = descriptor.getTablePartitions(wrap(table));

    // T1: every id is canonical, and counts match the raw current-ADD scan (ground truth).
    Map<String, Long> raw = rawAddCountsByPartition(table.store());
    assertFalse(parts.isEmpty());
    parts.forEach(
        p -> {
          assertCanonical(p.getPartition());
          assertTrue(p.getPartition().startsWith("dt=2026-01-2"), p.getPartition());
          assertEquals(raw.get(p.getPartition()), p.getFileCount());
        });
    // two distinct dt partitions were written (bucket dimension is separate, so count dt values)
    java.util.Set<String> dtValues =
        parts.stream()
            .map(p -> p.getPartition().replaceAll("/bucket-.*$", ""))
            .collect(java.util.stream.Collectors.toSet());
    assertTrue(dtValues.contains("dt=2026-01-22"), dtValues.toString());
    assertTrue(dtValues.contains("dt=2026-01-23"), dtValues.toString());
  }

  // ---- T2: multi-field partition keeps Paimon field order ---------------

  @Test
  void multiFieldPartitionKeepsPaimonFieldOrder() throws Exception {
    FileStoreTable table =
        createTable(
            "tmulti",
            Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .column("region", DataTypes.STRING())
                .partitionKeys("dt", "region")
                .option("bucket", "2")
                .option("bucket-key", "id")
                .build());
    writeRows(
        table,
        GenericRow.of(1, BinaryString.fromString("2026-01-22"), BinaryString.fromString("us")),
        GenericRow.of(2, BinaryString.fromString("2026-01-22"), BinaryString.fromString("us")));

    List<PartitionBaseInfo> parts = descriptor.getTablePartitions(wrap(table));
    assertFalse(parts.isEmpty());
    parts.forEach(
        p -> {
          assertCanonical(p.getPartition());
          // Paimon field order is dt then region
          assertTrue(p.getPartition().startsWith("dt=2026-01-22/region=us/"), p.getPartition());
        });
  }

  // ---- T3: non-partitioned table -> bucket-N, no leading slash ----------

  @Test
  void nonPartitionedTableHasBucketOnlyIdentifier() throws Exception {
    FileStoreTable table =
        createTable(
            "tnopart",
            Schema.newBuilder()
                .column("id", DataTypes.INT())
                .option("bucket", "2")
                .option("bucket-key", "id")
                .build());
    writeRows(
        table,
        GenericRow.of(1),
        GenericRow.of(2),
        GenericRow.of(3),
        GenericRow.of(4),
        GenericRow.of(5));

    List<PartitionBaseInfo> parts = descriptor.getTablePartitions(wrap(table));
    assertFalse(parts.isEmpty());
    parts.forEach(
        p -> {
          // non-partitioned: no partition fields, no leading slash, no double slash
          assertFalse(p.getPartition().startsWith("/"), "no leading slash: " + p.getPartition());
          assertFalse(p.getPartition().contains("dt="), p.getPartition());
          assertTrue(p.getPartition().matches("bucket-\\d+"), p.getPartition());
        });
  }

  // ---- T4: legacy double-slash input resolves same as canonical --------

  @Test
  void legacyDoubleSlashInputResolvesSameAsCanonical() throws Exception {
    FileStoreTable table = createTable("tpart4", appendSchema());
    writeRows(table, row(1, "2026-01-22"), row(2, "2026-01-23"));

    String target = descriptor.getTablePartitions(wrap(table)).get(0).getPartition();
    assertCanonical(target);

    List<PartitionFileBaseInfo> fromNew = descriptor.getTableFiles(wrap(table), target, 0);
    List<PartitionFileBaseInfo> fromOld =
        descriptor.getTableFiles(wrap(table), legacyDoubleSlash(target), 0);

    assertEquals(fromNew.size(), fromOld.size());
    assertTrue(fromNew.size() >= 1);
    assertEquals(target, fromNew.get(0).getPartition());
    assertEquals(target, fromOld.get(0).getPartition());
    assertEquals(fromNew.get(0).getPath(), fromOld.get(0).getPath());
  }

  // ---- T5: malformed input -> empty list, no full-table scan -----------

  @Test
  void malformedInputReturnsEmptyWithoutFullTableScan() throws Exception {
    FileStoreTable table = createTable("tpart5", appendSchema());
    writeRows(table, row(1, "2026-01-22")); // table has a real file -> a fallback would return it
    AmoroTable<?> amoroTable = wrap(table);

    assertTrue(descriptor.getTableFiles(amoroTable, "garbage", 0).isEmpty());
    assertTrue(descriptor.getTableFiles(amoroTable, "bucket-0", 0).isEmpty());
    assertTrue(descriptor.getTableFiles(amoroTable, "dt=2026-01-22/bucket-x", 0).isEmpty());
    assertTrue(descriptor.getTableFiles(amoroTable, "unknown=1/bucket-0", 0).isEmpty());
    assertTrue(descriptor.getTableFiles(amoroTable, null, 0).isEmpty());
  }

  // ---- T6: append-only scoped to target partition/bucket ---------------

  @Test
  void appendOnlyFilesScopedToTargetPartitionBucket() throws Exception {
    FileStoreTable table = createTable("tpart6", appendSchema());
    writeRows(
        table,
        row(1, "2026-01-22"),
        row(2, "2026-01-22"),
        row(3, "2026-01-22"),
        row(4, "2026-01-23"),
        row(5, "2026-01-23"));

    List<PartitionBaseInfo> parts = descriptor.getTablePartitions(wrap(table));
    Map<String, Long> raw = rawAddCountsByPartition(table.store());
    assertFalse(parts.isEmpty());

    for (PartitionBaseInfo p : parts) {
      List<PartitionFileBaseInfo> files =
          descriptor.getTableFiles(wrap(table), p.getPartition(), 0);
      // scoped to exactly this partition/bucket, matching the raw ADD count
      assertEquals(raw.get(p.getPartition()).intValue(), files.size());
      files.forEach(
          f -> {
            assertEquals(p.getPartition(), f.getPartition());
            assertFalse(f.getPath().contains("//"), f.getPath());
          });
    }
  }

  // ---- T7/T8/T9: primary-key scoping, whole-bucket, DELETE excluded ----

  @Test
  void primaryKeyFilesScopedToTargetAndDeleteFilesExcluded() throws Exception {
    FileStoreTable table = createTable("tpk", pkSchema());
    // spread across partitions/buckets; then an in-place update of one primary key
    writeRows(table, pkRow(1, 10, "A"), pkRow(2, 20, "A"), pkRow(3, 30, "B"), pkRow(4, 40, "B"));
    writeRows(table, pkRow(1, 99, "A")); // update pk=1 -> a DELETE of the old version is produced

    List<PartitionBaseInfo> parts = descriptor.getTablePartitions(wrap(table));
    Map<String, Long> raw = rawAddCountsByPartition(table.store());
    assertFalse(parts.isEmpty());

    for (PartitionBaseInfo p : parts) {
      List<PartitionFileBaseInfo> files =
          descriptor.getTableFiles(wrap(table), p.getPartition(), 0);
      // T7/T8: whole-bucket, scoped to exactly this partition/bucket; T9: DELETE files excluded
      // (both sides use plan().files(FileKind.ADD), so the listing matches the raw ADD count).
      assertEquals(raw.get(p.getPartition()), (long) p.getFileCount());
      assertEquals(raw.get(p.getPartition()).intValue(), files.size());
      files.forEach(f -> assertEquals(p.getPartition(), f.getPartition()));
    }
  }

  // ---- T10/T11/T12/T15: commitId null, commitTime raw epoch, path clean --

  @Test
  void commitIdIsNullAndCommitTimeIsRawCreationMillis() throws Exception {
    FileStoreTable table = createTable("tpart10", appendSchema());
    writeRows(table, row(1, "2026-01-22"));

    List<PartitionBaseInfo> parts = descriptor.getTablePartitions(wrap(table));
    assertEquals(1, parts.size());
    String target = parts.get(0).getPartition();

    List<PartitionFileBaseInfo> files = descriptor.getTableFiles(wrap(table), target, 0);
    assertEquals(1, files.size());
    PartitionFileBaseInfo f = files.get(0);

    // T10: commitId null, fileType intact
    assertNull(f.getCommitId(), "commitId must be null");
    assertEquals("INSERT_FILE", f.getFileType());
    // T11/T12: commitTime is creationTime().getMillisecond() verbatim, no timezone offset
    assertEquals(creationMillisForPath(table.store(), f.getPath()), f.getCommitTime().longValue());
    assertNotNull(f.getCommitTime());
    // T15: path stays single-slash
    assertFalse(f.getPath().contains("//"), f.getPath());
  }

  // ---- T13: single current scan, no snapshot history -------------------

  @Test
  void fileListingRunsSingleCurrentScanWithoutHistory() {
    // Interaction contract: a valid partition/bucket triggers exactly one Scan and never touches
    // the snapshot history (snapshotManager). The parse path runs for real against a real RowType,
    // so this also exercises convertSpecToInternalRow end-to-end.
    FileStoreTable table = mock(FileStoreTable.class);
    FileStore<?> store = mock(FileStore.class);
    FileStoreScan scan = mock(FileStoreScan.class);
    FileStoreScan.Plan plan = mock(FileStoreScan.Plan.class);

    when(table.partitionKeys()).thenReturn(Collections.singletonList("dt"));
    // doReturn avoids the FileStore<?> capture-of-? mismatch that when().thenReturn() rejects.
    doReturn(store).when(table).store();
    when(store.partitionType())
        .thenReturn(RowType.builder().field("dt", DataTypes.STRING()).build());
    CoreOptions options = mock(CoreOptions.class);
    when(options.partitionDefaultName()).thenReturn("__DEFAULT__");
    when(store.options()).thenReturn(options);
    when(store.newScan()).thenReturn(scan);
    when(scan.withPartitionBucket(any(), anyInt())).thenReturn(scan);
    when(scan.plan()).thenReturn(plan);
    when(plan.files(FileKind.ADD)).thenReturn(Collections.emptyList());

    AmoroTable<?> amoroTable = mock(AmoroTable.class);
    when(((AmoroTable) amoroTable).originalTable()).thenReturn(table);

    List<PartitionFileBaseInfo> files =
        descriptor.getTableFiles(amoroTable, "dt=2026-01-22/bucket-0", 0);

    assertTrue(files.isEmpty());
    verify(store, times(1)).newScan();
    verify(store, never()).snapshotManager();
  }

  // ---- T14: empty table ------------------------------------------------

  @Test
  void emptyTableReturnsEmptyPartitionsAndFiles() throws Exception {
    FileStoreTable table = createTable("tempty", appendSchema());
    // no writes -> no snapshot

    assertTrue(descriptor.getTablePartitions(wrap(table)).isEmpty());
    assertTrue(descriptor.getTableFiles(wrap(table), "dt=2026-01-22/bucket-0", 0).isEmpty());
  }

  // ---- T16: snapshot detail shares canonical partition helper ----------

  @Test
  void snapshotDetailUsesCanonicalPartition() throws Exception {
    FileStoreTable table = createTable("tpart16", appendSchema());
    writeRows(table, row(1, "2026-01-22"));
    long snapshotId = table.store().snapshotManager().latestSnapshot().id();

    List<PartitionFileBaseInfo> details =
        descriptor.getSnapshotDetail(wrap(table), String.valueOf(snapshotId), "main");

    assertFalse(details.isEmpty());
    details.forEach(
        d -> {
          assertEquals("BASE_FILE", d.getFileType());
          assertNull(d.getCommitId());
          assertCanonical(d.getPartition());
        });
  }
}
