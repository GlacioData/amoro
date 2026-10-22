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

package org.apache.amoro.formats.paimon.optimizing.primary;

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.append;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.commit;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.input;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.amoro.exception.OptimizingCommitException;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class TestPaimonPrimaryKeyRetentionGap {
  @ParameterizedTest
  @CsvSource({"1,missing", "-1,missing", "1,1", "-1,1", "1,0", "-1,0", "1,broken", "-1,broken"})
  void expiresPlannedSnapshotAndSuccessorsBeforeRealExecution(
      String bucket, String hint, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      Map<String, String> options = options(false);
      options.put("bucket", bucket);
      options.put("deletion-vectors.enabled", "true");
      FileStoreTable raw = table(catalog, "retention_gap", options);
      append(raw, GenericRow.of(1, 1, 1, 1L));
      SnapshotManager manager = spy(raw.snapshotManager());
      FileStoreTable observed = spy(raw);
      doReturn(manager).when(observed).snapshotManager();
      PaimonPrimaryKeyCompactionInput planned = input(observed, OptimizingType.MINOR, 701L);
      for (int i = 2; i <= 41; i++) {
        append(raw, GenericRow.of(1, i, i, (long) i));
      }
      for (long i = 1; i < 40; i++) {
        manager.deleteSnapshot(i);
      }
      HintFileUtils.deleteLatestHint(manager.fileIO(), manager.snapshotDirectory());
      if (!hint.equals("missing")) {
        manager
            .fileIO()
            .overwriteHintFile(
                new org.apache.paimon.fs.Path(manager.snapshotDirectory(), HintFileUtils.LATEST),
                hint);
      }
      PaimonPrimaryKeyCompactionOutput output =
          new PaimonPrimaryKeyCompactionExecutor(planned).execute();
      verify(manager).snapshotIdStream();
      verify(manager).snapshot(41L);
      if (hint.equals("1")) {
        // Restore is fixed locally, but unmodified 1.4.2 commit still trusts an expired positive
        // LATEST when its successor is also absent (HintFileUtils.findLatest). Keep that native
        // limitation visible: no hint rewrite or patched dependency may make this test succeed.
        assertEquals(Collections.singletonList("1|41|41|41"), snapshotRows(raw, 41L));
        OptimizingCommitException failure =
            assertThrows(OptimizingCommitException.class, () -> commit(raw, planned, output));
        assertTrue(ExceptionUtils.indexOfType(failure, FileNotFoundException.class) >= 0);
        assertTrue(ExceptionUtils.getRootCauseMessage(failure).contains("snapshot-1 "));
        assertFalse(manager.snapshotExists(42L));
        assertEquals(Long.valueOf(1L), manager.latestSnapshotId());
        assertEquals(Collections.singletonList("1|41|41|41"), snapshotRows(raw, 41L));
      } else {
        commit(raw, planned, output);
        assertEquals(Collections.singletonList("1|41|41|41"), rows(raw));
      }
    }
  }

  private static List<String> snapshotRows(FileStoreTable table, long snapshotId) throws Exception {
    List<String> result = new ArrayList<>();
    try (RecordReader<InternalRow> reader =
        table
            .newReadBuilder()
            .newRead()
            .createReader(table.newSnapshotReader().withSnapshot(snapshotId).read())) {
      reader.forEachRemaining(
          row ->
              result.add(
                  row.getInt(0)
                      + "|"
                      + row.getInt(1)
                      + "|"
                      + row.getInt(2)
                      + "|"
                      + row.getLong(3)));
    }
    return result;
  }
}
