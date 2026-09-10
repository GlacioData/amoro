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

import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.catalog;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.compact;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.evaluate;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.fileNames;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.history;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.increments;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.options;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.rows;
import static org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyIntervalTestSupport.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.optimizing.OptimizingType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;

class TestPaimonPrimaryKeyCompactionInterval {
  @Test
  void intervalAloneDoesNotCreateRepeatedCandidates(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      FileStoreTable control = table(catalog, "control", options(false));
      FileStoreTable managed = table(catalog, "managed", options(true));
      for (FileStoreTable table : Arrays.asList(control, managed)) {
        history(table);
        increments(table, 1, 1);
      }
      assertFalse(evaluate(control).necessary(), "无 interval 对照不能有其它触发");
      for (int i = 0; i < 3; i++) {
        assertFalse(evaluate(managed).necessary(), "托管表不能因 interval 新建候选");
      }
    }
  }

  @Test
  void normalTasksKeepHistoryAcrossTwoCommittedRounds(@TempDir Path warehouse) throws Exception {
    try (Catalog catalog = catalog(warehouse)) {
      for (String bucket : Arrays.asList("1", "-1")) {
        for (OptimizingType type : Arrays.asList(OptimizingType.MINOR, OptimizingType.MAJOR)) {
          String name = type.name().toLowerCase() + (bucket.equals("1") ? "fixed" : "dynamic");
          java.util.Map<String, String> controlOptions = options(false);
          controlOptions.put("bucket", bucket);
          java.util.Map<String, String> managedOptions = options(true);
          managedOptions.put("bucket", bucket);
          FileStoreTable control = table(catalog, name + "_control", controlOptions);
          FileStoreTable managed = table(catalog, name + "_managed", managedOptions);
          Set<String> controlHistory = history(control);
          Set<String> managedHistory = history(managed);
          for (int round = 1; round <= 2; round++) {
            increments(control, 3, round);
            increments(managed, 3, round);
            assertTrue(evaluate(control).necessary(), "必须存在非时间候选");
            PaimonPrimaryKeyCompactionOutput reference = compact(control, type, round + 1);
            PaimonPrimaryKeyCompactionOutput actual = compact(managed, type, round + 1);
            assertTrue(reference.getCompactedFileCount() > 1);
            assertTrue(actual.getProducedFileCount() > 0);
            assertTrue(fileNames(control).containsAll(controlHistory), "对照必须保留历史大 run");
            assertTrue(fileNames(managed).containsAll(managedHistory), "interval 不得额外纳入历史大 run");
            assertEquals(reference.getCompactedFileCount(), actual.getCompactedFileCount());
            assertEquals(reference.getCompactedRecordCount(), actual.getCompactedRecordCount());
            assertEquals(rows(control), rows(managed));
          }
        }
      }
    }
  }
}
