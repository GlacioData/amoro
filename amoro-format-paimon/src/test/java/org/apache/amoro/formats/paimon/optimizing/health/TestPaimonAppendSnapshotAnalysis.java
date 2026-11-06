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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.PlannerFile;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.ScanTotals;
import org.apache.amoro.table.health.TableHealthStructure;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestPaimonAppendSnapshotAnalysis {

  @ParameterizedTest
  @EnumSource(
      value = BucketMode.class,
      names = {"BUCKET_UNAWARE", "HASH_FIXED"})
  void derivedAnalysesShareIsolatedFileSnapshotAndPreserveOrder(BucketMode mode) {
    PaimonHealthEvaluationContext context = context(mode);
    BinaryRow partition = BinaryRow.singleColumn(7);
    PlannerFile second = new PlannerFile(partition, mock(DataFileMeta.class), null, null);
    PlannerFile first = new PlannerFile(partition, mock(DataFileMeta.class), null, null);
    List<PlannerFile> input = new ArrayList<>(Arrays.asList(second, first));
    PaimonAppendHealthEvaluator.Result health =
        PaimonAppendHealthEvaluator.Result.invalid("fixture");
    PaimonAppendSnapshotAnalysis original =
        PaimonAppendSnapshotAnalysis.create(
            context, health, health, new ScanTotals(), input, true, context.snapshot().get(), 1);
    TableHealthStructure structure =
        new TableHealthStructure(
            "APPEND",
            "COMPLETE",
            1L,
            1L,
            "1",
            "0",
            "0",
            Collections.emptyList(),
            Collections.emptyList());

    PaimonAppendSnapshotAnalysis truncated = original.withPlannerFilesTruncated(true);
    PaimonAppendSnapshotAnalysis attached = truncated.withStructure(structure);
    input.clear();
    partition.setInt(0, 99);
    attached.plannerFiles().get(0).partition().setInt(0, 100);

    assertNotSame(input, original.plannerFiles());
    assertEquals(Arrays.asList(second, first), attached.plannerFiles());
    assertSame(original.plannerFiles(), truncated.plannerFiles());
    assertSame(truncated.plannerFiles(), attached.plannerFiles());
    assertEquals(7, attached.plannerFiles().get(0).partition().getInt(0));
    assertFalse(original.plannerFilesTruncated());
    assertTrue(attached.plannerFilesTruncated());
    assertTrue(attached.plannerFactsAvailable());
    assertNull(original.healthDetails().getStructure());
    assertSame(structure, attached.healthDetails().getStructure());
    assertEquals(original.healthDetails().getMetrics(), attached.healthDetails().getMetrics());
    assertEquals(
        original.healthDetails().getComponents(), attached.healthDetails().getComponents());
    assertEquals(
        original.healthDetails().getReasonCodes(), attached.healthDetails().getReasonCodes());
    assertSame(original.pendingInput(), attached.pendingInput());
    assertSame(original.key(), attached.key());
    assertSame(original.snapshot(), attached.snapshot());
    assertEquals(original.fullSnapshotFileCount(), attached.fullSnapshotFileCount());
    assertEquals(original.fullSnapshotScanCount(), attached.fullSnapshotScanCount());
    assertThrows(UnsupportedOperationException.class, () -> attached.plannerFiles().clear());

    PaimonAppendSnapshotAnalysis historical = attached.withoutPlannerFacts();
    assertTrue(historical.plannerFiles().isEmpty());
    assertFalse(historical.plannerFactsAvailable());
    assertFalse(historical.plannerFilesTruncated());
    assertNull(historical.snapshot());
    assertEquals(attached.healthDetails(), historical.healthDetails());
  }

  private static PaimonHealthEvaluationContext context(BucketMode mode) {
    AppendOnlyFileStoreTable table = mock(AppendOnlyFileStoreTable.class);
    TableSchema schema = mock(TableSchema.class);
    SnapshotManager snapshots = mock(SnapshotManager.class);
    Snapshot snapshot = mock(Snapshot.class);
    Map<String, String> options = new HashMap<>();
    options.put("bucket", mode == BucketMode.HASH_FIXED ? "1" : "-1");
    when(table.bucketMode()).thenReturn(mode);
    when(table.schema()).thenReturn(schema);
    when(schema.id()).thenReturn(1L);
    when(schema.options()).thenReturn(options);
    when(table.coreOptions()).thenReturn(new CoreOptions(options));
    when(table.snapshotManager()).thenReturn(snapshots);
    when(snapshots.latestSnapshot()).thenReturn(snapshot);
    when(snapshot.id()).thenReturn(1L);
    when(snapshot.timeMillis()).thenReturn(1L);
    return PaimonHealthEvaluationContext.capture(table, "catalog.db.append", null);
  }

  @Test
  public void pendingCountsSaturateInsteadOfBecomingEmpty() {
    assertEquals(Integer.MAX_VALUE, PaimonAppendSnapshotAnalysis.safeInt(Integer.MAX_VALUE + 1L));
    assertEquals(Integer.MAX_VALUE, PaimonAppendSnapshotAnalysis.safeInt(Long.MAX_VALUE));
    assertEquals(0, PaimonAppendSnapshotAnalysis.safeInt(0L));
    assertThrows(IllegalArgumentException.class, () -> PaimonAppendSnapshotAnalysis.safeInt(-1L));
  }

  @Test
  public void scanTotalsRejectNegativeCountsAndLongOverflow() {
    ScanTotals totals = new ScanTotals();
    assertThrows(IllegalArgumentException.class, () -> totals.setPartitionCount(-1L));
    assertThrows(IllegalArgumentException.class, () -> totals.setUnitCount(-1L));

    DataFileMeta huge = mock(DataFileMeta.class);
    when(huge.fileSize()).thenReturn(Long.MAX_VALUE);
    when(huge.rowCount()).thenReturn(0L);
    totals.addFile(huge, 0L, 0L, 0L);
    assertThrows(ArithmeticException.class, () -> totals.addFile(huge, 0L, 0L, 0L));
  }

  @Test
  public void deleteCountAboveRowCountIsIncompleteAndNotAccumulated() {
    ScanTotals totals = new ScanTotals();
    DataFileMeta file = mock(DataFileMeta.class);
    when(file.fileSize()).thenReturn(10L);
    when(file.rowCount()).thenReturn(5L);

    totals.addFile(file, 0L, 10L, 6L);

    assertFalse(totals.deleteMetadataComplete());
    assertEquals(0L, totals.deleteRecordCount());
  }

  @Test
  public void invalidConfigurationUsesNAInsteadOfNegativeSentinels() {
    AppendOnlyFileStoreTable table = mock(AppendOnlyFileStoreTable.class);
    TableSchema schema = mock(TableSchema.class);
    SnapshotManager snapshotManager = mock(SnapshotManager.class);
    when(table.bucketMode()).thenReturn(BucketMode.BUCKET_UNAWARE);
    when(table.schema()).thenReturn(schema);
    when(schema.id()).thenReturn(1L);
    when(schema.options()).thenReturn(java.util.Collections.emptyMap());
    when(table.coreOptions()).thenThrow(new IllegalArgumentException("invalid options"));
    when(table.snapshotManager()).thenReturn(snapshotManager);
    PaimonHealthEvaluationContext context =
        PaimonHealthEvaluationContext.capture(table, "catalog.db.invalid_append", null);

    PaimonAppendSnapshotAnalysis analysis =
        PaimonAppendSnapshotAnalysis.invalid(
            context, PaimonAppendHealthEvaluator.INVALID_SCORING_CONFIG);

    assertEquals("N/A", analysis.healthDetails().getMetrics().get("targetFileSize"));
    assertEquals("N/A", analysis.healthDetails().getMetrics().get("smallFileBoundary"));
  }
}
