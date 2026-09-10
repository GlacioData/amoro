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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.utils.SerializationUtil;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class TestPaimonPrimaryKeyCompactionIntervalRecovery {
  @Test
  void retriesDerivePolicyFromSerializedRawInput(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      for (OptimizingType type : Arrays.asList(OptimizingType.MINOR, OptimizingType.MAJOR)) {
        FileStoreTable table =
            PaimonPrimaryKeyIntervalTestSupport.table(
                catalog, type.name(), PaimonPrimaryKeyIntervalTestSupport.options(true));
        Set<String> history = PaimonPrimaryKeyIntervalTestSupport.history(table);
        PaimonPrimaryKeyIntervalTestSupport.increments(table, 3, 1);
        Map<String, String> before = new HashMap<>(table.options());
        PaimonPrimaryKeyCompactionInput original =
            PaimonPrimaryKeyIntervalTestSupport.input(table, type, 2L);
        PaimonPrimaryKeyCompactionOutput first =
            new PaimonPrimaryKeyCompactionExecutor(original).execute();
        // 模拟输出尚未提交时的重试，恢复新对象；只提交第二次结果，不重复提交旧 snapshot。
        PaimonPrimaryKeyCompactionInput recovered =
            SerializationUtil.simpleDeserialize(
                SerializationUtil.simpleSerialize(original).array());
        assertEquals(original.getTargetSnapshotId(), recovered.getTargetSnapshotId());
        assertEquals(original.getCommitUser(), recovered.getCommitUser());
        assertEquals(original.getCommitIdentifier(), recovered.getCommitIdentifier());
        assertEquals(original.getOptimizingType(), recovered.getOptimizingType());
        assertEquals(original.isFullCompaction(), recovered.isFullCompaction());
        assertEquals(before, ((FileStoreTable) recovered.getTable().originalTable()).options());
        PaimonPrimaryKeyCompactionOutput retry =
            new PaimonPrimaryKeyCompactionExecutor(recovered).execute();
        assertTrue(retry.getCompactedFileCount() > 1);
        assertEquals(first.getCompactedFileCount(), retry.getCompactedFileCount());
        assertEquals(first.getCompactedRecordCount(), retry.getCompactedRecordCount());
        PaimonPrimaryKeyIntervalTestSupport.commit(table, recovered, retry);
        assertTrue(PaimonPrimaryKeyIntervalTestSupport.fileNames(table).containsAll(history));
        assertEquals(before, table.options());
        assertTrue(PaimonPrimaryKeyIntervalTestSupport.rows(table).contains("1|2|2|12"));
      }
    }
  }

  @Test
  void newUnmanagedWriterStillReadsOriginalInterval(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      FileStoreTable table =
          PaimonPrimaryKeyIntervalTestSupport.table(
              catalog, "exit", PaimonPrimaryKeyIntervalTestSupport.options(true));
      PaimonPrimaryKeyIntervalTestSupport.history(table);
      PaimonPrimaryKeyIntervalTestSupport.increments(table, 1, 1);
      PaimonPrimaryKeyCompactionOutput managed =
          new PaimonPrimaryKeyCompactionExecutor(
                  PaimonPrimaryKeyIntervalTestSupport.input(table, OptimizingType.MINOR, 2L))
              .execute();
      assertEquals(0, managed.getCompactedFileCount());
      FileStoreTable unmanaged =
          table.copy(Collections.singletonMap("self-optimizing.enabled", "false"));
      PaimonPrimaryKeyCompactionOutput nativeOutput =
          new PaimonPrimaryKeyCompactionExecutor(
                  PaimonPrimaryKeyIntervalTestSupport.input(unmanaged, OptimizingType.MINOR, 2L))
              .execute();
      assertTrue(nativeOutput.getCompactedFileCount() > 1);
      assertEquals("1h", table.options().get("compaction.optimization-interval"));
      assertEquals("true", table.options().get("self-optimizing.enabled"));
    }
  }
}
