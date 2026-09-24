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

package org.apache.amoro.formats.paimon.optimizing.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.PlannerFile;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthEvaluationContext;
import org.apache.amoro.table.health.TableHealthStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class TestPaimonFixedBucketFileScanner {
  private static final Identifier IDENTIFIER = Identifier.create("db", "fixed_scan");

  @Test
  void capsUnproductiveWindowSearchWithoutTruncatingSnapshotFacts(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = new HashMap<>();
      options.put("bucket", "1");
      options.put("compaction.min.file-num", "3");
      options.put("source.split.open-file-cost", "0 b");
      options.put("self-optimizing.minor.trigger.interval", "-1");
      AppendOnlyFileStoreTable table = createTable(catalog, options);
      writeRounds(table, 1, 20);
      Map<String, String> planOptions = new HashMap<>(table.options());
      planOptions.put("compaction.file-num-limit", "2");
      PaimonPlanContext context =
          PaimonPlanContext.forOptions(
              CoreOptions.fromMap(planOptions),
              new OptimizingConfig()
                  .setMaxTaskSize(64L * 1024 * 1024)
                  .setFullTriggerInterval(-1)
                  .setMinorLeastInterval(-1),
              0L,
              0L,
              0L,
              1.0D,
              64L * 1024 * 1024,
              System.currentTimeMillis());
      PaimonAppendFileScanner scanner =
          new PaimonAppendFileScanner(
              table,
              PaimonHealthEvaluationContext.capture(table, table.fullName(), null),
              context,
              null);
      PaimonAppendSnapshotAnalysis analysis = scanner.scan().analysis();
      Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> windows =
          scanner.fixedBucketWindows(analysis);
      // Paimon FileStoreScan binds a snapshot, not a sequence cursor: each retry rereads scope.
      // One prefix scan plus two selection/proof pairs must stop even with more candidates.
      assertEquals(5, scanner.fixedScopeScanCount());
      assertEquals(20, analysis.fullSnapshotFileCount());
      assertEquals(1, windows.size());
      PaimonFixedBucketFileWindow window = windows.values().iterator().next();
      assertTrue(window.safe());
      assertTrue(window.canAdvance());
      assertEquals(2, window.files().size());
      assertTrue(
          new PaimonFixedBucketTaskPacker(context)
              .pack(window, new PaimonPartitionEvaluator(context).evaluate(window))
              .isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void preservesExactManifestBucketIdentityForBothAppendOrderingModes(
      boolean ordered, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      AppendOnlyFileStoreTable table =
          createTable(catalog, Collections.singletonMap("bucket-append-ordered", "" + ordered));
      writeRounds(table, 2, 2);
      List<ManifestEntry> nativeFiles = nativeFiles(table);
      assertEquals(8, nativeFiles.size());
      PaimonAppendFileScanner.ScanResult scan = scan(table, 100);
      PaimonAppendSnapshotAnalysis analysis = scan.analysis();

      // Paimon reference: ac223f47d916f4c8e9c07fd1745db448e4e7d0d8
      // paimon-core/src/main/java/org/apache/paimon/manifest/FileEntry.java:49-57.
      // Partition, bucket and totalBuckets are manifest facts, never synthetic partition defaults.
      assertTrue(analysis.plannerFactsAvailable());
      assertFalse(analysis.plannerFilesTruncated());
      assertEquals(nativeFiles.size(), analysis.plannerFiles().size());
      assertExactManifestFacts(nativeFiles, analysis.plannerFiles());
      assertEquals(4, unitKeys(table, analysis.plannerFiles()).size());
      assertEquals("4", analysis.healthDetails().getMetrics().get("effectiveUnitCount"));
      assertEquals("2", analysis.healthDetails().getMetrics().get("activePartitionCount"));
      assertEquals(8, analysis.fullSnapshotFileCount());
      assertEquals(1, analysis.fullSnapshotScanCount());
      assertEquals(table.latestSnapshot().get().id(), scan.snapshotId());
      assertTrue(scan.files().isEmpty(), "The legacy partition-only candidate map must stay empty");
    }
  }

  @Test
  void boundsPlannerFilesWithoutTruncatingHealthOrWithStructureFacts(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      AppendOnlyFileStoreTable table = createTable(catalog, Collections.emptyMap());
      writeRounds(table, 2, 3);
      List<ManifestEntry> nativeFiles = nativeFiles(table);
      assertEquals(12, nativeFiles.size());
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.capture(table, table.fullName(), null);
      PaimonAppendSnapshotAnalysis complete =
          new PaimonAppendFileScanner(table, health, context(table, 100), null).scan().analysis();
      PaimonAppendSnapshotAnalysis bounded =
          new PaimonAppendFileScanner(table, health, context(table, 3), null).scan().analysis();

      // Paimon reference: ac223f47, paimon-core/src/main/java/org/apache/paimon/
      // operation/AbstractFileStoreScan.java:395-407 supplies the complete merged iterator.
      // Bounded planner retention and complete health aggregates are separate Amoro concerns.
      assertEquals(12, complete.plannerFiles().size());
      assertFalse(complete.plannerFilesTruncated());
      assertEquals(3, bounded.plannerFiles().size());
      assertTrue(bounded.plannerFilesTruncated());
      assertTrue(bounded.plannerFactsAvailable());
      assertEquals(12, bounded.fullSnapshotFileCount());
      assertEquals(12, bounded.pendingInput().getDataFileCount());
      assertEquals(
          nativeFiles.stream().mapToLong(entry -> entry.file().fileSize()).sum(),
          bounded.pendingInput().getDataFileSize());
      assertTrue(bounded.pendingInput().getHealthScore() >= 0);
      assertEquals(
          complete.pendingInput().getHealthScore(), bounded.pendingInput().getHealthScore());
      assertEquals(complete.healthDetails().getMetrics(), bounded.healthDetails().getMetrics());
      assertEquals(
          complete.healthDetails().getComponents(), bounded.healthDetails().getComponents());
      assertStructureFactsEqual(
          complete.healthDetails().getStructure(), bounded.healthDetails().getStructure());

      PaimonAppendSnapshotAnalysis attached =
          bounded.withStructure(complete.healthDetails().getStructure());
      assertEquals(bounded.plannerFiles(), attached.plannerFiles());
      assertTrue(attached.plannerFilesTruncated());
      assertTrue(attached.plannerFactsAvailable());
      assertEquals(bounded.key(), attached.key());
      assertEquals(bounded.snapshot(), attached.snapshot());
      assertEquals(bounded.fullSnapshotFileCount(), attached.fullSnapshotFileCount());
      assertEquals(bounded.fullSnapshotScanCount(), attached.fullSnapshotScanCount());
      assertEquals(
          bounded.pendingInput().getHealthScore(), attached.pendingInput().getHealthScore());
      assertEquals(bounded.healthDetails().getMetrics(), attached.healthDetails().getMetrics());
      assertEquals(
          complete.healthDetails().getStructure(), attached.healthDetails().getStructure());
    }
  }

  @Test
  void retainsPlanningFactsForUnitsOutsideTheHealthProblemTopTwenty(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      AppendOnlyFileStoreTable table = createTable(catalog, Collections.emptyMap());
      writeRounds(table, 12, 2);
      List<ManifestEntry> nativeFiles = nativeFiles(table);
      assertEquals(48, nativeFiles.size());
      PaimonAppendSnapshotAnalysis analysis = scan(table, 100).analysis();
      TableHealthStructure structure = analysis.healthDetails().getStructure();
      assertNotNull(structure);

      // Paimon ac223f47, manifest/FileEntry.java:49-57 identifies every partition/bucket unit.
      // Top20 is an Amoro presentation bound; it must not restrict planner ownership facts.
      assertEquals("24", structure.getTotalUnits());
      assertEquals("24", structure.getAffectedUnits());
      assertEquals(20, structure.getProblems().size());
      assertEquals(48, analysis.plannerFiles().size());
      assertFalse(analysis.plannerFilesTruncated());
      assertExactManifestFacts(nativeFiles, analysis.plannerFiles());
      Set<String> plannedUnits = unitKeys(table, analysis.plannerFiles());
      Set<String> displayedUnits =
          structure.getProblems().stream()
              .map(problem -> problem.getPartition() + "|" + problem.getBucket())
              .collect(Collectors.toSet());
      assertEquals(24, plannedUnits.size());
      assertTrue(plannedUnits.containsAll(displayedUnits));
      Set<String> hiddenUnits = new HashSet<>(plannedUnits);
      hiddenUnits.removeAll(displayedUnits);
      assertEquals(4, hiddenUnits.size());
      assertEquals(
          8,
          analysis.plannerFiles().stream()
              .filter(file -> hiddenUnits.contains(unitKey(table, file.partition(), file.bucket())))
              .count());
      assertEquals(
          48L,
          structure.getBands().stream()
              .mapToLong(band -> Long.parseLong(band.getFileCount()))
              .sum());
    }
  }

  @Test
  void provesBoundedWindowsWithOneScopeScanAtTheOriginalSnapshot(@TempDir Path warehouse)
      throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      AppendOnlyFileStoreTable table = createTable(catalog, Collections.emptyMap());
      writeRounds(table, 2, 3);
      PaimonHealthEvaluationContext health =
          PaimonHealthEvaluationContext.capture(table, table.fullName(), null);
      PaimonAppendFileScanner scanner =
          new PaimonAppendFileScanner(table, health, context(table, 8), null);
      PaimonAppendSnapshotAnalysis analysis = scanner.scan().analysis();
      assertEquals(8, analysis.plannerFiles().size());
      assertTrue(analysis.plannerFilesTruncated());
      // Paimon ac223f47, operation/FileStoreScan.java:68-74 supports a fixed Snapshot and one
      // total-aware bucket predicate. Later writes must not expand the scope proof to latest.
      writeRounds(table, 2, 1);
      assertEquals(16, nativeFiles(table).size());
      Map<PaimonAppendUnitKey, PaimonFixedBucketFileWindow> windows =
          scanner.fixedBucketWindows(analysis);
      assertEquals(1, scanner.fixedScopeScanCount());
      assertEquals(4, windows.size());
      assertEquals(12L, windows.values().stream().mapToLong(w -> w.totalFileCount()).sum());
      assertEquals(8L, windows.values().stream().mapToLong(w -> w.files().size()).sum());
      assertEquals(4L, windows.values().stream().mapToLong(w -> w.omittedFileCount()).sum());
      for (PaimonFixedBucketFileWindow window : windows.values()) {
        assertTrue(window.safe());
        assertFalse(window.complete());
        assertTrue(window.provenPrefix());
        assertEquals(2, window.files().size());
        assertEquals(2, window.totalBuckets());
        assertEquals(0L, window.files().get(0).file().minSequenceNumber());
      }
      assertEquals(12, analysis.fullSnapshotFileCount());
      assertEquals(1, analysis.fullSnapshotScanCount());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"deletion-vectors", "clustering"})
  void unsupportedFixedRewriteModesKeepCompleteHealthWithoutPlannerFacts(
      String mode, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = new HashMap<>();
      if ("deletion-vectors".equals(mode)) {
        options.put("deletion-vectors.enabled", "true");
      } else {
        options.put("bucket-append-ordered", "false");
        options.put("clustering.incremental", "true");
        options.put("clustering.columns", "id");
      }
      AppendOnlyFileStoreTable table = createTable(catalog, options);
      writeRounds(table, 2, 2);
      List<ManifestEntry> nativeFiles = nativeFiles(table);
      assertEquals(8, nativeFiles.size());
      PaimonAppendFileScanner.ScanResult scan = scan(table, 100);
      PaimonAppendSnapshotAnalysis analysis = scan.analysis();

      // Paimon reference: ac223f47, paimon-api/src/main/java/org/apache/paimon/
      // CoreOptions.java:3654-3658 and paimon-core/.../schema/SchemaValidation.java:754-768.
      // These are legal native tables; rejecting Amoro rewrite capability must not erase health.
      assertTrue(
          "deletion-vectors".equals(mode)
              ? table.coreOptions().deletionVectorsEnabled()
              : table.coreOptions().bucketClusterEnabled());
      assertFalse(analysis.plannerFactsAvailable());
      assertTrue(analysis.plannerFiles().isEmpty());
      assertFalse(analysis.plannerFilesTruncated());
      assertTrue(scan.files().isEmpty());
      assertEquals(8, analysis.pendingInput().getDataFileCount());
      assertEquals(8, analysis.fullSnapshotFileCount());
      assertEquals("8", analysis.healthDetails().getMetrics().get("totalFileCount"));
      assertTrue(analysis.pendingInput().getHealthScore() >= 0);
      assertEquals("COMPLETE", analysis.healthDetails().getStructure().getAvailability());
      assertEquals("4", analysis.healthDetails().getStructure().getTotalUnits());
      assertEquals(4, analysis.healthDetails().getStructure().getProblems().size());
    }
  }

  private static Catalog catalog(Path warehouse) {
    return PaimonCatalogFactory.paimonCatalog(
        Collections.singletonMap("warehouse", warehouse.toUri().toString()), new Configuration());
  }

  private static AppendOnlyFileStoreTable createTable(Catalog catalog, Map<String, String> options)
      throws Exception {
    catalog.createDatabase(IDENTIFIER.getDatabaseName(), true);
    Schema.Builder schema =
        Schema.newBuilder()
            .column("dt", DataTypes.INT())
            .column("id", DataTypes.INT())
            .column("payload", DataTypes.STRING())
            .partitionKeys("dt")
            .option("bucket", "2")
            .option("bucket-key", "id")
            .option("bucket-append-ordered", "true")
            .option("write-only", "true")
            .option("self-optimizing.enabled", "true")
            .option("target-file-size", "64 mb");
    options.forEach(schema::option);
    catalog.createTable(IDENTIFIER, schema.build(), false);
    return (AppendOnlyFileStoreTable) catalog.getTable(IDENTIFIER);
  }

  private static void writeRounds(AppendOnlyFileStoreTable table, int partitions, int rounds)
      throws Exception {
    for (int round = 0; round < rounds; round++) {
      BatchWriteBuilder builder = table.newBatchWriteBuilder();
      try (BatchTableWrite write = builder.newWrite();
          BatchTableCommit commit = builder.newCommit()) {
        for (int partition = 0; partition < partitions; partition++) {
          for (int key = 0; key < 32; key++) {
            write.write(
                GenericRow.of(
                    partition, key, BinaryString.fromString("round-" + round + "-key-" + key)));
          }
        }
        commit.commit(write.prepareCommit());
      }
    }
  }

  private static PaimonAppendFileScanner.ScanResult scan(
      AppendOnlyFileStoreTable table, int limit) {
    return new PaimonAppendFileScanner(table, context(table, limit), null).scan();
  }

  private static PaimonPlanContext context(AppendOnlyFileStoreTable table, int limit) {
    Map<String, String> options = new HashMap<>(table.options());
    options.put("compaction.file-num-limit", Integer.toString(limit));
    return PaimonPlanContext.forOptions(
        CoreOptions.fromMap(options),
        new OptimizingConfig().setMaxTaskSize(64L * 1024 * 1024).setFullTriggerInterval(-1),
        0L,
        0L,
        0L,
        1.0D,
        64L * 1024 * 1024,
        System.currentTimeMillis());
  }

  private static List<ManifestEntry> nativeFiles(AppendOnlyFileStoreTable table) {
    return table.store().newScan().withSnapshot(table.latestSnapshot().get()).plan().files()
        .stream()
        .filter(entry -> entry.kind() == FileKind.ADD)
        .collect(Collectors.toList());
  }

  private static void assertExactManifestFacts(
      List<ManifestEntry> entries, List<PlannerFile> facts) {
    Map<String, ManifestEntry> expected =
        entries.stream().collect(Collectors.toMap(ManifestEntry::fileName, entry -> entry));
    assertEquals(
        expected.keySet(),
        facts.stream().map(f -> f.file().fileName()).collect(Collectors.toSet()));
    for (PlannerFile fact : facts) {
      ManifestEntry entry = expected.get(fact.file().fileName());
      assertNotNull(entry);
      assertEquals(entry.partition(), fact.partition());
      assertEquals(entry.bucket(), fact.bucket());
      assertEquals(entry.totalBuckets(), fact.totalBuckets());
      assertEquals(entry.file().fileSize(), fact.file().fileSize());
      assertEquals(entry.file().rowCount(), fact.file().rowCount());
      assertEquals(entry.file().schemaId(), fact.file().schemaId());
      assertEquals(entry.file().minSequenceNumber(), fact.file().minSequenceNumber());
      assertEquals(entry.file().maxSequenceNumber(), fact.file().maxSequenceNumber());
    }
  }

  private static Set<String> unitKeys(AppendOnlyFileStoreTable table, List<PlannerFile> facts) {
    return facts.stream()
        .map(file -> unitKey(table, file.partition(), file.bucket()))
        .collect(Collectors.toSet());
  }

  private static String unitKey(AppendOnlyFileStoreTable table, BinaryRow partition, int bucket) {
    return table.store().pathFactory().getPartitionString(partition) + "|" + bucket;
  }

  private static void assertStructureFactsEqual(
      TableHealthStructure expected, TableHealthStructure actual) {
    assertNotNull(actual);
    assertEquals(expected.getTableShape(), actual.getTableShape());
    assertEquals(expected.getAvailability(), actual.getAvailability());
    assertEquals(expected.getSnapshotTimeMillis(), actual.getSnapshotTimeMillis());
    assertEquals(expected.getTotalUnits(), actual.getTotalUnits());
    assertEquals(expected.getAffectedUnits(), actual.getAffectedUnits());
    assertEquals(expected.getSevereUnits(), actual.getSevereUnits());
    assertEquals(expected.getBands(), actual.getBands());
    assertEquals(expected.getProblems(), actual.getProblems());
  }
}
