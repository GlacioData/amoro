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

package org.apache.amoro.optimizing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.Action;
import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.TableSnapshot;
import org.apache.amoro.process.ProcessFactory;
import org.apache.amoro.process.RecoverProcessFailedException;
import org.apache.amoro.process.TableProcess;
import org.apache.amoro.process.TableProcessStore;
import org.apache.amoro.table.FormatPendingInput;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthComponent;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.utils.SerializationUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@DisplayName("Test optimizing SPI interfaces after moving to amoro-common")
public class TestOptimizingSpiInterfaces {

  @Test
  @DisplayName("BaseOptimizingInput.option() should store and retrieve a single option")
  void testBaseOptimizingInputOption() {
    BaseOptimizingInput input = createTestInput();
    input.option("key1", "value1");
    assertEquals("value1", input.getOptions().get("key1"));
  }

  @Test
  @DisplayName("BaseOptimizingInput.options() should merge multiple options")
  void testBaseOptimizingInputOptions() {
    BaseOptimizingInput input = createTestInput();
    input.option("existing", "v0");

    Map<String, String> batch = new HashMap<>();
    batch.put("key1", "value1");
    batch.put("key2", "value2");
    input.options(batch);

    assertEquals(3, input.getOptions().size());
    assertEquals("v0", input.getOptions().get("existing"));
    assertEquals("value1", input.getOptions().get("key1"));
    assertEquals("value2", input.getOptions().get("key2"));
  }

  @Test
  @DisplayName("OptimizingOutput.summary() should return a map")
  void testOptimizingOutputSummary() {
    TableOptimizing.OptimizingOutput output =
        new TableOptimizing.OptimizingOutput() {
          @Override
          public Map<String, String> summary() {
            return Collections.singletonMap("files", "10");
          }
        };
    assertNotNull(output.summary());
    assertEquals("10", output.summary().get("files"));
  }

  @Test
  @DisplayName("TaskProperties constants should have expected values")
  void testTaskPropertyConstants() {
    assertEquals("task-executor-factory-impl", TaskProperties.TASK_EXECUTOR_FACTORY_IMPL);
    assertEquals("process-id", TaskProperties.PROCESS_ID);
    assertEquals("unknown", TaskProperties.UNKNOWN_PROCESS_ID);
  }

  @Test
  @DisplayName("BaseOptimizingInput subclass should be serializable via SerializationUtil")
  void testBaseOptimizingInputSerialization() {
    TestSerializableInput input = new TestSerializableInput();
    input.option("key1", "value1");
    input.option("key2", "value2");

    ByteBuffer buffer = SerializationUtil.simpleSerialize(input);
    assertNotNull(buffer);

    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    TestSerializableInput deserialized = SerializationUtil.simpleDeserialize(bytes);
    assertNotNull(deserialized);
    assertEquals("value1", deserialized.getOptions().get("key1"));
    assertEquals("value2", deserialized.getOptions().get("key2"));
  }

  @Test
  @DisplayName("Commit mode overload should preserve legacy committer behavior")
  void testCommitModeOverloadPreservesLegacyCommitterBehavior() throws Exception {
    AtomicInteger commitCount = new AtomicInteger();
    TableOptimizingCommitter legacyCommitter = commitCount::incrementAndGet;

    legacyCommitter.commit(TableOptimizingCommitter.CommitMode.NORMAL);
    assertEquals(1, commitCount.get());

    legacyCommitter.commit(TableOptimizingCommitter.CommitMode.RECOVERY_REPLAY);
    assertEquals(2, commitCount.get());
  }

  @Test
  @DisplayName("Legacy pending input result constructors should expose no table analysis")
  void testPendingInputResultConstructorsRemainCompatible() {
    FormatPendingInput pendingInput = new TestPendingInput(72);
    FormatPendingInput optimizingInput = new TestPendingInput(65);

    PendingInputResult twoArgumentResult = new PendingInputResult(pendingInput, true);
    assertSame(pendingInput, twoArgumentResult.pendingInput());
    assertSame(pendingInput, twoArgumentResult.optimizingPendingInput());
    assertTrue(twoArgumentResult.optimizingNecessary());
    assertFalse(twoArgumentResult.tableAnalysis().isPresent());

    PendingInputResult threeArgumentResult =
        new PendingInputResult(pendingInput, optimizingInput, false);
    assertSame(pendingInput, threeArgumentResult.pendingInput());
    assertSame(optimizingInput, threeArgumentResult.optimizingPendingInput());
    assertFalse(threeArgumentResult.optimizingNecessary());
    assertFalse(threeArgumentResult.tableAnalysis().isPresent());
  }

  @Test
  @DisplayName("Pending input result should optionally carry a format table analysis")
  void testPendingInputResultCarriesAnalysis() {
    TestFormatTableAnalysis analysis = new TestFormatTableAnalysis();
    PendingInputResult result = new PendingInputResult(analysis.pendingInput(), true, analysis);

    assertSame(analysis, result.tableAnalysis().get());
    assertSame(analysis.pendingInput(), result.pendingInput());
    assertEquals(analysis.key(), result.tableAnalysis().get().key());
    assertEquals(analysis.healthDetails(), result.tableAnalysis().get().healthDetails());
  }

  @Test
  @DisplayName("AmoroTable analysis key preflight should be opt-in")
  void testAmoroTableCurrentAnalysisKeyDefaultsToEmpty() {
    assertFalse(new TestAmoroTable().currentAnalysisKey(null).isPresent());
  }

  @Test
  @DisplayName("Forced health evaluation should delegate to legacy AmoroTable implementation")
  void testForcedHealthEvaluationDefaultsToLegacyMethod() {
    AtomicInteger invocationCount = new AtomicInteger();
    TestAmoroTable legacyTable =
        new TestAmoroTable() {
          @Override
          public Optional<PendingInputResult> evaluatePendingInput(
              OptimizationContext context, int maxPendingPartitions) {
            invocationCount.incrementAndGet();
            return Optional.empty();
          }
        };

    assertFalse(legacyTable.evaluatePendingInput(null, 3, true).isPresent());
    assertEquals(1, invocationCount.get());
  }

  @Test
  @DisplayName("Five argument planner factory should delegate to the legacy overload")
  void testProcessFactoryPlannerOverloadDelegatesToLegacyMethod() {
    AtomicInteger legacyInvocationCount = new AtomicInteger();
    TableOptimizingPlanner planner = new TestPlanner();
    ProcessFactory factory = new TestProcessFactory(legacyInvocationCount, planner);
    TestFormatTableAnalysis analysis = new TestFormatTableAnalysis();

    TableOptimizingPlanner actual =
        factory.createPlanner(null, null, 1.0D, 1024L, Optional.of(analysis));

    assertSame(planner, actual);
    assertEquals(1, legacyInvocationCount.get());
    assertFalse(planner.tableAnalysis().isPresent());
  }

  @Test
  @DisplayName("Process factory optimizing eligibility should default to true")
  void testProcessFactoryOptimizingEligibilityDefaultsToTrue() {
    ProcessFactory factory = new TestProcessFactory(new AtomicInteger(), new TestPlanner());

    assertTrue(factory.isOptimizingEligible(null));
  }

  private BaseOptimizingInput createTestInput() {
    return new BaseOptimizingInput() {};
  }

  /** A concrete subclass for serialization testing. */
  public static class TestSerializableInput extends BaseOptimizingInput {
    private static final long serialVersionUID = 1L;
  }

  private static class TestPendingInput implements FormatPendingInput {
    private final int healthScore;

    private TestPendingInput(int healthScore) {
      this.healthScore = healthScore;
    }

    @Override
    public int getHealthScore() {
      return healthScore;
    }

    @Override
    public int getTotalFileCount() {
      return 0;
    }

    @Override
    public long getTotalFileSize() {
      return 0L;
    }

    @Override
    public long getDataFileSize() {
      return 0L;
    }

    @Override
    public int getDataFileCount() {
      return 0;
    }
  }

  private static class TestFormatTableAnalysis implements FormatTableAnalysis {
    private final FormatPendingInput pendingInput = new TestPendingInput(72);
    private final TableAnalysisKey key =
        new TableAnalysisKey(
            "catalog.db.table",
            TableFormat.PAIMON,
            42L,
            TableAnalysisKey.NO_CHANGE_SNAPSHOT,
            7L,
            "config",
            "formula-v1",
            TableAnalysisKey.NO_BASELINE,
            TableAnalysisKey.NO_BASELINE_TIME);
    private final TableHealthDetails healthDetails =
        new TableHealthDetails(
            "formula-v1",
            42L,
            null,
            7L,
            "config",
            key.encoded(),
            Collections.singletonList(
                new TableHealthComponent("FILE_ORGANIZATION", 72, 100, "WEIGHTED", null)),
            Collections.emptyMap(),
            Collections.emptyList());

    @Override
    public TableAnalysisKey key() {
      return key;
    }

    @Override
    public FormatPendingInput pendingInput() {
      return pendingInput;
    }

    @Override
    public TableHealthDetails healthDetails() {
      return healthDetails;
    }
  }

  private static class TestAmoroTable implements AmoroTable<Object> {
    @Override
    public TableIdentifier id() {
      return TableIdentifier.of("catalog", "db", "table");
    }

    @Override
    public TableFormat format() {
      return TableFormat.PAIMON;
    }

    @Override
    public Map<String, String> properties() {
      return Collections.emptyMap();
    }

    @Override
    public Object originalTable() {
      return new Object();
    }

    @Override
    public TableSnapshot currentSnapshot() {
      return null;
    }
  }

  private static class TestPlanner implements TableOptimizingPlanner {
    @Override
    public boolean isNecessary() {
      return false;
    }

    @Override
    public OptimizingPlanResult<?> plan() {
      return null;
    }

    @Override
    public OptimizingType getOptimizingType() {
      return OptimizingType.MINOR;
    }

    @Override
    public long getProcessId() {
      return 1L;
    }

    @Override
    public long getPlanTime() {
      return 1L;
    }

    @Override
    public long getTargetSnapshotId() {
      return 1L;
    }

    @Override
    public long getTargetChangeSnapshotId() {
      return -1L;
    }

    @Override
    public Map<String, Long> getFromSequence() {
      return Collections.emptyMap();
    }

    @Override
    public Map<String, Long> getToSequence() {
      return Collections.emptyMap();
    }
  }

  private static class TestProcessFactory implements ProcessFactory {
    private final AtomicInteger legacyInvocationCount;
    private final TableOptimizingPlanner planner;

    private TestProcessFactory(
        AtomicInteger legacyInvocationCount, TableOptimizingPlanner planner) {
      this.legacyInvocationCount = legacyInvocationCount;
      this.planner = planner;
    }

    @Override
    public String name() {
      return "test";
    }

    @Override
    public void open(Map<String, String> properties) {}

    @Override
    public void close() {}

    @Override
    public Map<TableFormat, Set<Action>> supportedActions() {
      return Collections.emptyMap();
    }

    @Override
    public TableOptimizingPlanner createPlanner(
        TableRuntime tableRuntime,
        AmoroTable<?> table,
        double availableCore,
        long maxInputSizePerThread) {
      legacyInvocationCount.incrementAndGet();
      return planner;
    }

    @Override
    public Optional<TableProcess> trigger(TableRuntime tableRuntime, Action action) {
      return Optional.empty();
    }

    @Override
    public TableProcess recover(TableRuntime tableRuntime, TableProcessStore store)
        throws RecoverProcessFailedException {
      return null;
    }
  }
}
