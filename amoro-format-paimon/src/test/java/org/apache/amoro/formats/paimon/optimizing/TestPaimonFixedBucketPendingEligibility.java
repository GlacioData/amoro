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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis;
import org.apache.amoro.formats.paimon.process.PaimonProcessFactory;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.OptimizingPlanResult;
import org.apache.amoro.optimizing.PendingInputResult;
import org.apache.amoro.optimizing.TableOptimizingPlanner;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Automatic-admission contract for ordinary fixed-bucket append optimization. */
class TestPaimonFixedBucketPendingEligibility {
  private static final long MAX_TASK_BYTES = 64L * 1024 * 1024;

  @ParameterizedTest(name = "ordered={0}")
  @ValueSource(booleans = {true, false})
  void eligibleFixedTableFlowsFromDemandToVersionedAndRecoverableTasks(
      boolean ordered, @TempDir Path warehouse) throws Exception {
    try (PaimonFixedBucketTestSupport fixture = eligibleFixture(warehouse, ordered)) {
      PaimonTable table = wrap(fixture.table);
      TableRuntime runtime = runtime(true);

      PendingInputResult pending =
          table
              .evaluatePendingInput((OptimizationContext) runtime, 100)
              .orElseThrow(AssertionError::new);
      assertTrue(pending.optimizingNecessary());
      assertTrue(pending.tableAnalysis().isPresent());
      assertTrue(((PaimonPendingInput) pending.pendingInput()).getHealthScore() >= 0);

      PaimonProcessFactory disabled = new PaimonProcessFactory();
      disabled.open(null);
      assertFalse(disabled.supportedFormats().contains(TableFormat.PAIMON));

      PaimonProcessFactory enabled = new PaimonProcessFactory();
      enabled.open(Collections.singletonMap(PaimonProcessFactory.OPTIMIZER_ENABLED.key(), "true"));
      assertTrue(enabled.supportedFormats().contains(TableFormat.PAIMON));
      TableOptimizingPlanner planner =
          enabled.createPlanner(runtime, table, 8.0D, MAX_TASK_BYTES, pending.tableAnalysis());
      OptimizingPlanResult<?> plan = planner.plan();
      assertEquals(fixture.table.latestSnapshot().get().id(), plan.getTargetSnapshotId());
      assertFalse(plan.getTasks().isEmpty());
      for (Object planned : plan.getTasks()) {
        PaimonCompactionTask task = (PaimonCompactionTask) planned;
        PaimonCompactionInput input = task.getInput();
        assertEquals(PaimonFixedBucketTaskSerializer.VERSION, input.getSerializerVersion());
        PaimonFixedBucketTask fixed =
            new PaimonFixedBucketTaskSerializer()
                .deserialize(input.getSerializerVersion(), input.getTaskBytes());
        assertEquals(ordered, fixed.ordered());
        assertEquals(plan.getTargetSnapshotId(), fixed.snapshotId());

        ByteBuffer bytes = SerializationUtil.simpleSerialize(input);
        PaimonCompactionInput restored = SerializationUtil.simpleDeserialize(bytes.array());
        assertEquals(PaimonFixedBucketTaskSerializer.VERSION, restored.getSerializerVersion());
        assertArrayEquals(input.getTaskBytes(), restored.getTaskBytes());
      }
    }
  }

  @ParameterizedTest(name = "unsupported={0}")
  @ValueSource(strings = {"dv", "clustering", "row-tracking"})
  void unsupportedFixedCapabilitiesKeepHealthButRejectDemand(
      String capability, @TempDir Path warehouse) throws Exception {
    Map<String, String> options = baseOptions();
    if ("dv".equals(capability)) {
      options.put("deletion-vectors.enabled", "true");
    } else if ("clustering".equals(capability)) {
      options.put("bucket-append-ordered", "false");
      options.put("clustering.incremental", "true");
      options.put("clustering.columns", "hm");
    }
    try (PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, true, options)) {
      fixture.append(0, 1);
      AppendOnlyFileStoreTable evaluated = fixture.table;
      if ("row-tracking".equals(capability)) {
        // Native DDL rejects this append shape. Overlay only the effective option on a delegate so
        // the capability gate is isolated while health still reads a real snapshot and manifests.
        Map<String, String> overlaid = new HashMap<>(fixture.table.options());
        overlaid.put("row-tracking.enabled", "true");
        evaluated =
            mock(
                AppendOnlyFileStoreTable.class,
                withSettings().defaultAnswer(delegatesTo(fixture.table)));
        when(evaluated.options()).thenReturn(overlaid);
        when(evaluated.coreOptions()).thenReturn(CoreOptions.fromMap(overlaid));
      }

      PendingInputResult result =
          wrap(evaluated)
              .evaluatePendingInput((OptimizationContext) runtime(true), 100)
              .orElseThrow(AssertionError::new);
      assertFalse(result.optimizingNecessary());
      assertTrue(((PaimonPendingInput) result.pendingInput()).getHealthScore() >= 0);
      PaimonAppendSnapshotAnalysis analysis =
          (PaimonAppendSnapshotAnalysis) result.tableAnalysis().orElseThrow(AssertionError::new);
      assertFalse(analysis.plannerFactsAvailable());
    }
  }

  @Test
  void dualSwitchDefaultsAndDisabledContextCannotCreateDemand(@TempDir Path warehouse)
      throws Exception {
    Map<String, String> missingWriteOnly = baseOptions();
    missingWriteOnly.remove("write-only");
    assertNoEvaluation(missingWriteOnly);

    Map<String, String> missingSelfOptimizing = baseOptions();
    missingSelfOptimizing.remove("self-optimizing.enabled");
    assertNoEvaluation(missingSelfOptimizing);

    try (PaimonFixedBucketTestSupport fixture =
        eligibleFixture(warehouse.resolve("disabled"), true)) {
      PendingInputResult result =
          wrap(fixture.table)
              .evaluatePendingInput((OptimizationContext) runtime(false), 100)
              .orElseThrow(AssertionError::new);
      assertFalse(result.optimizingNecessary());
      assertTrue(((PaimonPendingInput) result.pendingInput()).getHealthScore() >= 0);
    }
  }

  private static void assertNoEvaluation(Map<String, String> options) {
    AppendOnlyFileStoreTable table = mock(AppendOnlyFileStoreTable.class);
    when(table.options()).thenReturn(options);
    Optional<PendingInputResult> result =
        wrap(table).evaluatePendingInput((OptimizationContext) runtime(true), 100);
    assertFalse(result.isPresent());
  }

  private static PaimonFixedBucketTestSupport eligibleFixture(Path warehouse, boolean ordered)
      throws Exception {
    PaimonFixedBucketTestSupport fixture =
        new PaimonFixedBucketTestSupport(warehouse, true, ordered, baseOptions());
    fixture.append(0, 1);
    fixture.append(1, 1);
    fixture.append(2, 1);
    return fixture;
  }

  private static Map<String, String> baseOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("write-only", "true");
    options.put("self-optimizing.enabled", "true");
    options.put("compaction.min.file-num", "3");
    options.put("compaction.small-file-ratio", "0.7");
    options.put(PaimonAppendRewriteOptions.AMORO_FULL_REWRITE, "false");
    return options;
  }

  private static PaimonTable wrap(AppendOnlyFileStoreTable table) {
    return new PaimonTable(TableIdentifier.of("test", "db", "fixed_append"), table);
  }

  private static TableRuntime runtime(boolean enabled) {
    OptimizingConfig config =
        new OptimizingConfig()
            .setEnabled(enabled)
            .setMinorLeastInterval(-1)
            .setFullTriggerInterval(-1)
            .setFullRewriteAllFiles(false)
            .setMaxTaskSize(MAX_TASK_BYTES);
    TableRuntime runtime =
        mock(TableRuntime.class, withSettings().extraInterfaces(OptimizationContext.class));
    when(runtime.getTableConfiguration())
        .thenReturn(new TableConfiguration().setOptimizingConfig(config));
    when(((OptimizationContext) runtime).getOptimizingConfig()).thenReturn(config);
    return runtime;
  }
}
