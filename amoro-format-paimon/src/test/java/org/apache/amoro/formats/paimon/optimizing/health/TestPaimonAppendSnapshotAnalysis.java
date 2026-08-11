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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.formats.paimon.optimizing.health.PaimonAppendSnapshotAnalysis.ScanTotals;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;

public class TestPaimonAppendSnapshotAnalysis {

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
