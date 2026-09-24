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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthComparisonState;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthComparisonState.Entry;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthSupport;
import org.apache.amoro.formats.paimon.optimizing.health.TestPaimonHealthSupport;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonOptimizingPlanner;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.TableRuntimeStore;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Historical health comparison driven by a real fixed-bucket rewrite commit. */
class TestPaimonFixedBucketHealthComparison {
  private static final long TABLE_ID = 301L;
  private static final long PROCESS_ID = 401L;
  private static final long MAX_TASK_BYTES = 64L * 1024 * 1024;

  @Test
  void matchesCompactIdentityAndMarksConcurrentAppend(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PaimonTable wrapper = wrap(fixture);
      long beforeId = fixture.table.latestSnapshot().get().id();
      FormatTableAnalysis before =
          TestPaimonHealthSupport.evaluateHistorical(
              fixture.table, wrapper.id().toString(), beforeId);
      PlannedRewrite rewrite = planAndExecute(wrapper);
      Entry entry = capture(wrapper, rewrite.plan, before);
      PaimonCompactionInput firstInput = rewrite.tasks.get(0).getInput();
      assertEquals(PROCESS_ID, entry.getProcessId());
      assertEquals(beforeId, entry.getInputSnapshotId());
      assertEquals(fixture.table.schema().id(), entry.getSchemaId());
      assertEquals(rewrite.plan.getOptimizingType().name(), entry.getOptimizingType());
      assertEquals(firstInput.getCommitUser(), entry.getCommitUser());
      assertEquals(firstInput.getCommitIdentifier(), entry.getCommitIdentifier());
      assertAnalysisMatchesManifest((PaimonAppendSnapshotAnalysis) before, fixture.table, beforeId);

      fixture.append(10, 1);
      Set<String> concurrentFiles =
          manifestFiles(fixture.table, fixture.table.latestSnapshot().get().id()).stream()
              .map(ManifestEntry::fileName)
              .collect(Collectors.toSet());
      concurrentFiles.removeAll(
          manifestFiles(fixture.table, beforeId).stream()
              .map(ManifestEntry::fileName)
              .collect(Collectors.toSet()));
      assertFalse(concurrentFiles.isEmpty());
      new PaimonTableCommit(
              fixture.table, rewrite.tasks, rewrite.planner.getCommitUser(), PROCESS_ID)
          .commit();
      Snapshot committed = fixture.table.latestSnapshot().get();
      // Paimon ac223f47d916f4c8e9c07fd1745db448e4e7d0d8,
      // paimon-core/.../Snapshot.java and operation/FileStoreCommit.java: COMPACT identity is
      // persisted on the native snapshot and is the authoritative historical join key.
      assertEquals(Snapshot.CommitKind.COMPACT, committed.commitKind());
      assertEquals(rewrite.planner.getCommitUser(), committed.commitUser());
      assertEquals(PROCESS_ID, committed.commitIdentifier());

      FormatTableAnalysis after =
          TestPaimonHealthSupport.evaluateHistorical(
              fixture.table, wrapper.id().toString(), committed.id());
      assertAnalysisMatchesManifest(
          (PaimonAppendSnapshotAnalysis) after, fixture.table, committed.id());
      Set<String> afterFiles =
          manifestFiles(fixture.table, committed.id()).stream()
              .map(ManifestEntry::fileName)
              .collect(Collectors.toSet());
      assertTrue(afterFiles.containsAll(concurrentFiles));
      TestPaimonHealthSupport.buildComparison(
          entry,
          fixture.table,
          wrapper.id().toString(),
          after.healthDetails(),
          after.pendingInput().getHealthScore());

      assertEquals("READY", entry.getStatus());
      assertEquals("OBSERVED_STRUCTURE", entry.getReason());
      assertTrue(entry.getConcurrentChanges());
      assertNotNull(entry.getBefore().getStructure());
      assertNotNull(entry.getAfter().getStructure());
      assertEquals("COMPLETE", entry.getBefore().getStructure().getAvailability());
      assertEquals("COMPLETE", entry.getAfter().getStructure().getAvailability());
      assertEquals(
          Integer.toString(fixture.units().size()),
          entry.getAfter().getStructure().getTotalUnits());
      assertEquals(beforeId, entry.getBefore().getSnapshotId());
      assertEquals(committed.id(), entry.getAfter().getSnapshotId());
    }
  }

  @Test
  void expiredOrConfigurationMismatchedInputIsUnavailable(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PaimonTable wrapper = wrap(fixture);
      long beforeId = fixture.table.latestSnapshot().get().id();
      PlannedRewrite rewrite = planAndExecute(wrapper);
      new PaimonTableCommit(
              fixture.table, rewrite.tasks, rewrite.planner.getCommitUser(), PROCESS_ID)
          .commit();

      Entry mismatched =
          entry(beforeId, fixture.table.schema().id() + 1, rewrite.planner.getCommitUser());
      TestPaimonHealthSupport.buildComparison(
          mismatched, fixture.table, wrapper.id().toString(), null, -1);
      assertEquals("UNAVAILABLE", mismatched.getStatus());
      assertEquals("CONFIGURATION_CHANGED", mismatched.getReason());
      assertFalse("READY".equals(mismatched.getStatus()));

      ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) fixture.table.newExpireSnapshots();
      assertTrue(
          expire.expireUntil(fixture.table.snapshotManager().earliestSnapshotId(), beforeId + 1)
              > 0);
      Entry expired = entry(beforeId, fixture.table.schema().id(), rewrite.planner.getCommitUser());
      TestPaimonHealthSupport.buildComparison(
          expired, fixture.table, wrapper.id().toString(), null, -1);
      assertEquals("UNAVAILABLE", expired.getStatus());
      assertEquals("SNAPSHOT_EXPIRED", expired.getReason());
      assertFalse("READY".equals(expired.getStatus()));
    }
  }

  private static PaimonFixedBucketTestSupport fixture(Path warehouse) throws Exception {
    Map<String, String> options = new java.util.HashMap<>();
    options.put("compaction.min.file-num", "2");
    options.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, "false");
    PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true, options);
    fixture.append(0, 1);
    fixture.append(1, 1);
    fixture.append(2, 1);
    return fixture;
  }

  private static PaimonTable wrap(PaimonFixedBucketTestSupport fixture) {
    return new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), fixture.table);
  }

  private static PlannedRewrite planAndExecute(PaimonTable table) {
    PaimonOptimizingPlanner planner =
        new PaimonOptimizingPlanner(
            table,
            TABLE_ID,
            PROCESS_ID,
            4.0D,
            MAX_TASK_BYTES,
            new OptimizingConfig()
                .setEnabled(true)
                .setMinorLeastInterval(-1)
                .setFullTriggerInterval(-1)
                .setFullRewriteAllFiles(false)
                .setMaxTaskSize(MAX_TASK_BYTES),
            0L,
            0L,
            0L,
            null);
    OptimizingPlanResult<PaimonCompactionTask> plan = planner.plan();
    assertFalse(plan.getTasks().isEmpty());
    List<PaimonCompactionTask> tasks = new ArrayList<>(plan.getTasks());
    tasks.forEach(
        task -> {
          PaimonCompactionOutput output = new PaimonCompactionExecutor(task.getInput()).execute();
          ByteBuffer buffer = SerializationUtil.simpleSerialize(output);
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          task.setOutputBytes(bytes);
        });
    return new PlannedRewrite(planner, plan, tasks);
  }

  private static Entry capture(
      PaimonTable table,
      OptimizingPlanResult<PaimonCompactionTask> plan,
      FormatTableAnalysis analysis) {
    TableRuntimeStore store = org.mockito.Mockito.mock(TableRuntimeStore.class);
    TableRuntimeStore.TableRuntimeOperation operation =
        org.mockito.Mockito.mock(TableRuntimeStore.TableRuntimeOperation.class);
    PaimonHealthComparisonState state = new PaimonHealthComparisonState();
    org.mockito.Mockito.when(store.getState(PaimonHealthComparisonState.KEY)).thenReturn(state);
    org.mockito.Mockito.when(store.begin()).thenReturn(operation);
    org.mockito.Mockito.when(
            operation.updateState(
                org.mockito.Mockito.eq(PaimonHealthComparisonState.KEY), org.mockito.Mockito.any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<PaimonHealthComparisonState, PaimonHealthComparisonState> update =
                  invocation.getArgument(1);
              update.apply(state);
              return operation;
            });
    new PaimonHealthSupport().capture(store, table, plan, analysis);
    assertNotNull(state.getPending());
    return state.getPending();
  }

  private static void assertAnalysisMatchesManifest(
      PaimonAppendSnapshotAnalysis analysis,
      org.apache.paimon.table.AppendOnlyFileStoreTable table,
      long snapshotId) {
    // Paimon ac223f47d916f4c8e9c07fd1745db448e4e7d0d8,
    // paimon-core/.../operation/FileStoreCommit.java and manifest/ManifestEntry.java: partition and
    // bucket in the fixed snapshot manifest, rather than directory names, define each unit.
    Map<String, Long> manifestCounts =
        manifestFiles(table, snapshotId).stream()
            .collect(
                Collectors.groupingBy(
                    entry ->
                        table.store().pathFactory().getPartitionString(entry.partition())
                            + ":"
                            + entry.bucket(),
                    Collectors.counting()));
    // Historical diagnostics retain complete health facts, not reusable planning payloads.
    assertTrue(analysis.plannerFiles().isEmpty());
    assertFalse(analysis.plannerFactsAvailable());
    assertFalse(analysis.plannerFilesTruncated());
    assertEquals(null, analysis.snapshot());
    Map<String, Long> problemCounts =
        analysis.healthDetails().getStructure().getProblems().stream()
            .collect(
                Collectors.toMap(
                    problem -> problem.getPartition() + ":" + problem.getBucket(),
                    problem -> Long.parseLong(problem.getFileCount())));
    assertEquals(
        manifestCounts.size(), analysis.healthDetails().getStructure().getProblems().size());
    assertEquals(manifestCounts, problemCounts);
    assertEquals(
        Long.toString(manifestCounts.values().stream().mapToLong(Long::longValue).sum()),
        analysis.healthDetails().getMetrics().get("totalFileCount"));
    assertEquals(
        Integer.toString(manifestCounts.size()),
        analysis.healthDetails().getStructure().getTotalUnits());
    assertEquals(
        new HashSet<>(java.util.Arrays.asList(0, 1)),
        analysis.healthDetails().getStructure().getProblems().stream()
            .map(problem -> problem.getBucket())
            .collect(Collectors.toSet()));
  }

  private static List<ManifestEntry> manifestFiles(
      org.apache.paimon.table.AppendOnlyFileStoreTable table, long snapshotId) {
    return table.store().newScan().withSnapshot(snapshotId).plan().files().stream()
        .filter(entry -> entry.kind() == FileKind.ADD)
        .collect(Collectors.toList());
  }

  private static Entry entry(long beforeId, long schemaId, String commitUser) {
    Entry entry = new Entry();
    entry.setProcessId(PROCESS_ID);
    entry.setInputSnapshotId(beforeId);
    entry.setSchemaId(schemaId);
    entry.setCommitUser(commitUser);
    entry.setCommitIdentifier(PROCESS_ID);
    entry.setOptimizingType("MINOR");
    return entry;
  }

  private static final class PlannedRewrite {
    private final PaimonOptimizingPlanner planner;
    private final OptimizingPlanResult<PaimonCompactionTask> plan;
    private final List<PaimonCompactionTask> tasks;

    private PlannedRewrite(
        PaimonOptimizingPlanner planner,
        OptimizingPlanResult<PaimonCompactionTask> plan,
        List<PaimonCompactionTask> tasks) {
      this.planner = planner;
      this.plan = plan;
      this.tasks = tasks;
    }
  }
}
