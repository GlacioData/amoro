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

package org.apache.amoro.formats.paimon.optimizing.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.formats.paimon.PaimonCatalogFactory;
import org.apache.amoro.formats.paimon.optimizing.plan.PaimonAppendFileScanner;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeySnapshotAnalysis;
import org.apache.amoro.optimizing.FormatTableAnalysis;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;

class TestPaimonHealthFingerprint {
  @TempDir Path warehouse;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void runtimeFactsAreReusedWhilePlanningKeysRemainStrict(boolean primaryKey) throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("d", false);
      Identifier id = Identifier.create("d", "t");
      Schema.Builder schema =
          Schema.newBuilder().column("id", DataTypes.INT()).option("write-only", "true");
      if (primaryKey) {
        schema.primaryKey("id").option("bucket", "1");
      }
      catalog.createTable(id, schema.build(), false);
      FileStoreTable table = (FileStoreTable) catalog.getTable(id);
      OptimizationContext runtime = mock(OptimizationContext.class);
      OptimizingConfig config = new OptimizingConfig();
      when(runtime.getOptimizingConfig()).thenReturn(config);
      write(table, 1);
      FormatTableAnalysis before = analyze(table, runtime);
      write(table, 2);
      FormatTableAnalysis after = analyze(table, runtime);
      Snapshot committed = table.snapshotManager().latestSnapshot();
      PaimonHealthComparisonState.Entry entry = new PaimonHealthComparisonState.Entry();
      entry.setInputSnapshotId(before.key().getSnapshotId());
      entry.setSchemaId(committed.schemaId());
      entry.setCommitUser(committed.commitUser());
      entry.setCommitIdentifier(committed.commitIdentifier());
      entry.setBefore(before.healthDetails());
      entry.setBeforeScore(before.pendingInput().getHealthScore());
      PaimonHealthSupport.build(
          entry, table, "t", after.healthDetails(), after.pendingInput().getHealthScore());
      assertEquals("READY", entry.getStatus());
      // A rescan creates new immutable details, so identity proves both native scans were skipped.
      assertSame(before.healthDetails(), entry.getBefore());
      assertSame(after.healthDetails(), entry.getAfter());

      PaimonHealthSupport support = new PaimonHealthSupport();
      config.setMinorLeastInterval(123);
      config.setFullTriggerInterval(456);
      config.setFullRewriteAllFiles(true);
      config.setFilter("id > 0");
      PaimonHealthEvaluationContext changed =
          PaimonHealthEvaluationContext.capture(table, "t", runtime);
      assertNotEquals(after.key(), changed.key());
      assertTrue(support.sameInput(changed.key(), after.healthDetails()));
      assertTrue(
          PaimonHealthSupport.reusable(
              after.healthDetails(), after.pendingInput().getHealthScore(), table, "t", committed));

      FileStoreTable differentScore =
          table.copy(Collections.singletonMap("target-file-size", "3 mb"));
      PaimonHealthEvaluationContext different =
          PaimonHealthEvaluationContext.capture(differentScore, "t", runtime);
      assertFalse(support.sameInput(different.key(), after.healthDetails()));
      FormatTableAnalysis differentFacts = analyze(differentScore, runtime);
      assertFalse(
          PaimonHealthSupport.reusable(
              differentFacts.healthDetails(),
              differentFacts.pendingInput().getHealthScore(),
              table,
              "t",
              committed));

      TableHealthDetails d = after.healthDetails();
      String legacyDigest =
          d.getScoringConfigFingerprint()
              .substring(d.getScoringConfigFingerprint().lastIndexOf(':') + 1);
      TableHealthDetails legacy =
          new TableHealthDetails(
              d.getFormulaVersion(),
              d.getSnapshotId(),
              d.getChangeSnapshotId(),
              d.getSchemaId(),
              legacyDigest,
              d.getEvaluationKey(),
              d.getComponents(),
              d.getMetrics(),
              d.getReasonCodes(),
              d.getStructure());
      assertFalse(support.sameInput(after.key(), legacy));
      assertFalse(
          PaimonHealthSupport.reusable(
              legacy, after.pendingInput().getHealthScore(), table, "t", committed));
    }
  }

  private static FormatTableAnalysis analyze(FileStoreTable table, OptimizationContext runtime) {
    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.capture(table, "t", runtime);
    return table instanceof PrimaryKeyFileStoreTable
        ? PaimonPrimaryKeySnapshotAnalysis.analyze(table, context)
        : PaimonAppendFileScanner.analyze((AppendOnlyFileStoreTable) table, context, null);
  }

  private static void write(FileStoreTable table, int value) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite write = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      write.write(GenericRow.of(value));
      commit.commit(write.prepareCommit());
    }
  }
}
