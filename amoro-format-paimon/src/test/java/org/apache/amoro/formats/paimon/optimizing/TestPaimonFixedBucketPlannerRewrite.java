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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.commit.PaimonTableCommit;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonOptimizingPlanner;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Real-file planner to executor to native commit contract for fixed-bucket Append tables. */
class TestPaimonFixedBucketPlannerRewrite {
  private static final long TABLE_ID = 101L;
  private static final long PROCESS_ID = 202L;
  private static final long MAX_TASK_BYTES = 64L * 1024 * 1024;

  @ParameterizedTest(name = "ordered={0}")
  @ValueSource(booleans = {true, false})
  void quotaPublishesExactUnitsAndPipelinePreservesRows(boolean ordered, @TempDir Path warehouse)
      throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("compaction.min.file-num", "3");
    options.put("compaction.small-file-ratio", "0.7");
    options.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, "false");
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, ordered, options)) {
      fixture.append(0, 1);
      fixture.append(1, 1);
      fixture.append(2, 1);
      List<String> rowsBefore = fixture.snapshotRows();
      Map<String, List<String>> rowsByUnitBefore = fixture.rowsByUnit();
      Map<String, Set<String>> filesByUnitBefore = fileNamesByUnit(fixture);
      long sourceSnapshot = fixture.table.latestSnapshot().get().id();
      PaimonTable table =
          new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), fixture.table);
      PaimonOptimizingPlanner planner =
          new PaimonOptimizingPlanner(
              table, TABLE_ID, PROCESS_ID, 2.0D, MAX_TASK_BYTES, config(), 0L, 0L, 0L, null);

      OptimizingPlanResult<PaimonCompactionTask> plan = planner.plan();

      assertEquals(sourceSnapshot, plan.getTargetSnapshotId());
      assertEquals(2, plan.getTasks().size(), "quota must release exactly two complete units");
      Set<String> selectedUnits = new HashSet<>();
      List<PaimonCompactionTask> executed = new ArrayList<>();
      for (PaimonCompactionTask task : plan.getTasks()) {
        PaimonFixedBucketTask fixed = decode(task);
        String unit = unit(fixed);
        selectedUnits.add(unit);
        assertEquals(filesByUnitBefore.get(unit), fileNames(fixed.files()));
        PaimonCompactionOutput output = new PaimonCompactionExecutor(task.getInput()).execute();
        assertEquals(
            fileNames(fixed.files()),
            fileNames(message(output).compactIncrement().compactBefore()));
        setOutput(task, output);
        executed.add(task);
      }
      assertEquals(2, selectedUnits.size(), "quota must not duplicate or split one unit");

      new PaimonTableCommit(fixture.table, executed, planner.getCommitUser(), PROCESS_ID).commit();

      assertEquals(
          rowsBefore, fixture.snapshotRows(), "published rewrite must preserve row multiset");
      Map<String, Set<String>> filesByUnitAfter = fileNamesByUnit(fixture);
      for (Map.Entry<String, Set<String>> entry : filesByUnitBefore.entrySet()) {
        if (!selectedUnits.contains(entry.getKey())) {
          assertEquals(entry.getValue(), filesByUnitAfter.get(entry.getKey()));
        }
      }
      if (ordered) {
        Map<String, List<String>> rowsByUnitAfter = fixture.rowsByUnit();
        for (String selected : selectedUnits) {
          assertEquals(rowsByUnitBefore.get(selected), rowsByUnitAfter.get(selected));
        }
      }
      long committedSnapshot = fixture.table.latestSnapshot().get().id();
      new PaimonTableCommit(fixture.table, executed, planner.getCommitUser(), PROCESS_ID)
          .commit(CommitMode.RECOVERY_REPLAY);
      assertEquals(committedSnapshot, fixture.table.latestSnapshot().get().id());
      assertEquals(rowsBefore, fixture.snapshotRows());
    }
  }

  @Test
  void concurrentAppendSurvivesCommit(@TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PlannedRewrite rewrite = planAndExecute(fixture);
      fixture.append(10, 1);
      List<String> rowsAfterConcurrentAppend = fixture.snapshotRows();

      new PaimonTableCommit(
              fixture.table, rewrite.tasks, rewrite.planner.getCommitUser(), PROCESS_ID)
          .commit();

      assertEquals(rowsAfterConcurrentAppend, fixture.snapshotRows());
    }
  }

  @Test
  void nativeCompactionDeletingPlannedSourceRejectsCommit(@TempDir Path warehouse)
      throws Exception {
    try (PaimonFixedBucketTestSupport fixture = fixture(warehouse)) {
      PlannedRewrite rewrite = planAndExecute(fixture);
      PaimonFixedBucketTask first = decode(rewrite.tasks.get(0));
      Set<String> selected = fileNames(first.files());
      List<ManifestEntry> selectedEntries =
          fixture.units().values().stream()
              .flatMap(List::stream)
              .filter(entry -> selected.contains(entry.fileName()))
              .collect(Collectors.toList());
      assertEquals(selected.size(), selectedEntries.size());
      fixture.commit(fixture.rewrite(selectedEntries));

      assertThrows(
          OptimizingCommitException.class,
          () ->
              new PaimonTableCommit(
                      fixture.table, rewrite.tasks, rewrite.planner.getCommitUser(), PROCESS_ID)
                  .commit());
    }
  }

  @ParameterizedTest(name = "orderedRange={0}")
  @ValueSource(booleans = {true, false})
  void fullQuotaKeepsNonOverlappingRetainedRange(boolean ordered, @TempDir Path warehouse)
      throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("compaction.min.file-num", "2");
    options.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, "true");
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, ordered, options)) {
      for (int round = 0; round < 6; round++) {
        fixture.append(round, 1);
      }
      Map<String, List<ManifestEntry>> unitsBefore = fixture.units();
      assertTrue(unitsBefore.values().stream().allMatch(files -> files.size() == 6));
      long largestInputFile =
          unitsBefore.values().stream()
              .flatMap(List::stream)
              .mapToLong(entry -> entry.file().fileSize())
              .max()
              .orElseThrow(() -> new AssertionError("missing input file"));
      long taskBytes = largestInputFile * 5 / 2;
      Map<String, List<String>> rowsBefore = fixture.rowsByUnit();
      PaimonTable table =
          new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), fixture.table);
      OptimizingConfig fullConfig =
          new OptimizingConfig()
              .setEnabled(true)
              .setMinorLeastInterval(-1)
              .setFullTriggerInterval(0)
              .setFullRewriteAllFiles(true)
              .setMaxTaskSize(taskBytes);
      PaimonOptimizingPlanner planner =
          new PaimonOptimizingPlanner(
              table, TABLE_ID, PROCESS_ID, 1.0D, taskBytes, fullConfig, 0L, 0L, 0L, null);

      OptimizingPlanResult<PaimonCompactionTask> plan = planner.plan();

      assertEquals(1, plan.getTasks().size());
      PaimonCompactionTask task = plan.getTasks().get(0);
      PaimonFixedBucketTask fixed = decode(task);
      String selectedUnit = unit(fixed);
      Set<String> selectedNames = fileNames(fixed.files());
      assertEquals(fixed.files().size(), selectedNames.size(), "exact inputs must be unique");
      List<ManifestEntry> retained =
          unitsBefore.get(selectedUnit).stream()
              .filter(entry -> !selectedNames.contains(entry.fileName()))
              .collect(Collectors.toList());
      assertFalse(retained.isEmpty(), "the selected bucket must retain at least one input file");

      PaimonCompactionOutput output = new PaimonCompactionExecutor(task.getInput()).execute();
      CommitMessageImpl message = message(output);
      assertEquals(selectedNames, fileNames(message.compactIncrement().compactBefore()));
      setOutput(task, output);
      new PaimonTableCommit(
              fixture.table, Collections.singletonList(task), planner.getCommitUser(), PROCESS_ID)
          .commit();

      Set<String> liveNames =
          fixture.units().values().stream()
              .flatMap(List::stream)
              .map(ManifestEntry::fileName)
              .collect(Collectors.toSet());
      assertTrue(
          retained.stream().map(ManifestEntry::fileName).allMatch(liveNames::contains),
          "all unselected metadata identifiers must remain live");
      List<String> beforeRows = new ArrayList<>(rowsBefore.get(selectedUnit));
      List<String> afterRows = new ArrayList<>(fixture.rowsByUnit().get(selectedUnit));
      if (ordered) {
        assertEquals(beforeRows, afterRows, "ordered unit read order must remain stable");
        for (DataFileMeta produced : message.compactIncrement().compactAfter()) {
          for (ManifestEntry untouched : retained) {
            assertTrue(
                produced.maxSequenceNumber() < untouched.file().minSequenceNumber()
                    || untouched.file().maxSequenceNumber() < produced.minSequenceNumber(),
                "rewritten and retained sequence ranges must be strictly disjoint");
          }
        }
      } else {
        Collections.sort(beforeRows);
        Collections.sort(afterRows);
        assertEquals(beforeRows, afterRows, "unordered unit row multiset must remain equal");
      }
    }
  }

  private static OptimizingConfig config() {
    return new OptimizingConfig()
        .setEnabled(true)
        .setMinorLeastInterval(-1)
        .setFullTriggerInterval(-1)
        .setFullRewriteAllFiles(false)
        .setMaxTaskSize(MAX_TASK_BYTES);
  }

  private static PaimonFixedBucketTestSupport fixture(Path warehouse) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("compaction.min.file-num", "3");
    options.put("compaction.small-file-ratio", "0.7");
    options.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, "false");
    PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true, options);
    fixture.append(0, 1);
    fixture.append(1, 1);
    fixture.append(2, 1);
    return fixture;
  }

  private static PlannedRewrite planAndExecute(PaimonFixedBucketTestSupport fixture) {
    PaimonTable table =
        new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), fixture.table);
    PaimonOptimizingPlanner planner =
        new PaimonOptimizingPlanner(
            table, TABLE_ID, PROCESS_ID, 2.0D, MAX_TASK_BYTES, config(), 0L, 0L, 0L, null);
    List<PaimonCompactionTask> tasks = new ArrayList<>(planner.plan().getTasks());
    assertFalse(tasks.isEmpty());
    tasks.forEach(task -> setOutput(task, new PaimonCompactionExecutor(task.getInput()).execute()));
    return new PlannedRewrite(planner, tasks);
  }

  private static final class PlannedRewrite {
    private final PaimonOptimizingPlanner planner;
    private final List<PaimonCompactionTask> tasks;

    private PlannedRewrite(PaimonOptimizingPlanner planner, List<PaimonCompactionTask> tasks) {
      this.planner = planner;
      this.tasks = tasks;
    }
  }

  private static PaimonFixedBucketTask decode(PaimonCompactionTask task) {
    PaimonCompactionInput input = task.getInput();
    return new PaimonFixedBucketTaskSerializer()
        .deserialize(input.getSerializerVersion(), input.getTaskBytes());
  }

  private static String unit(PaimonFixedBucketTask task) {
    return task.partition().getInt(0) + ":" + task.bucket();
  }

  private static Map<String, Set<String>> fileNamesByUnit(PaimonFixedBucketTestSupport fixture) {
    Map<String, Set<String>> result = new HashMap<>();
    fixture
        .units()
        .forEach(
            (unit, files) ->
                result.put(
                    unit,
                    fileNames(
                        files.stream().map(ManifestEntry::file).collect(Collectors.toList()))));
    return result;
  }

  private static Set<String> fileNames(List<DataFileMeta> files) {
    return files.stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
  }

  private static CommitMessageImpl message(PaimonCompactionOutput output) throws Exception {
    return (CommitMessageImpl)
        new CommitMessageSerializer()
            .deserialize(output.getCommitMessageVersion(), output.getCommitMessageBytes());
  }

  private static void setOutput(PaimonCompactionTask task, PaimonCompactionOutput output) {
    ByteBuffer buffer = SerializationUtil.simpleSerialize(output);
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    task.setOutputBytes(bytes);
  }
}
