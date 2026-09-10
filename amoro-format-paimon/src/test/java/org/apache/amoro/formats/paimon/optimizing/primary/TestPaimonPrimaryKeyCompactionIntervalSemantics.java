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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class TestPaimonPrimaryKeyCompactionIntervalSemantics {
  static Stream<Arguments> cases() {
    List<Arguments> cases = new ArrayList<>();
    for (String bucket : Arrays.asList("1", "-1")) {
      for (String engine :
          Arrays.asList("deduplicate", "partial-update", "aggregation", "first-row")) {
        for (OptimizingType type :
            Arrays.asList(OptimizingType.MINOR, OptimizingType.MAJOR, OptimizingType.FULL)) {
          cases.add(Arguments.of(bucket, engine, type));
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest(name = "bucket={0}, engine={1}, type={2}")
  @MethodSource("cases")
  void managedIntervalMatchesNativeUnsetSemantics(
      String bucket, String engine, OptimizingType type, @TempDir Path warehouse) throws Exception {
    try (Catalog catalog = PaimonPrimaryKeyIntervalTestSupport.catalog(warehouse)) {
      FileStoreTable control = create(catalog, "control", bucket, engine, false);
      FileStoreTable managed = create(catalog, "managed", bucket, engine, true);
      List<String> expected = seed(control, engine);
      assertEquals(expected, seed(managed, engine));
      List<String> visibleBefore = engine.equals("first-row") ? Collections.emptyList() : expected;
      assertEquals(visibleBefore, PaimonPrimaryKeyIntervalTestSupport.rows(control));
      assertEquals(visibleBefore, PaimonPrimaryKeyIntervalTestSupport.rows(managed));
      PaimonPrimaryKeyCompactionOutput reference =
          PaimonPrimaryKeyIntervalTestSupport.compact(control, type, 1L);
      PaimonPrimaryKeyCompactionOutput actual =
          PaimonPrimaryKeyIntervalTestSupport.compact(managed, type, 1L);
      assertTrue(reference.getCompactedFileCount() > 1);
      assertTrue(actual.getProducedFileCount() > 0);
      assertEquals(reference.getCompactedFileCount(), actual.getCompactedFileCount());
      assertEquals(reference.getCompactedRecordCount(), actual.getCompactedRecordCount());
      assertEquals(
          PaimonPrimaryKeyIntervalTestSupport.files(control).size(),
          PaimonPrimaryKeyIntervalTestSupport.files(managed).size());
      assertEquals(expected, PaimonPrimaryKeyIntervalTestSupport.rows(control));
      assertEquals(expected, PaimonPrimaryKeyIntervalTestSupport.rows(managed));
      assertEquals("1h", managed.options().get("compaction.optimization-interval"));
    }
  }

  private static FileStoreTable create(
      Catalog catalog, String name, String bucket, String engine, boolean interval)
      throws Exception {
    Map<String, String> options = PaimonPrimaryKeyIntervalTestSupport.options(interval);
    options.put("bucket", bucket);
    options.put("merge-engine", engine);
    options.put("num-sorted-run.compaction-trigger", "2");
    if (engine.equals("deduplicate")) {
      options.put("sequence.field", "seq");
    }
    if (engine.equals("aggregation")) {
      options.put("fields.a.aggregate-function", "sum");
      options.put("fields.b.aggregate-function", "sum");
      options.put("fields.seq.aggregate-function", "max");
    }
    return PaimonPrimaryKeyIntervalTestSupport.table(catalog, name, options);
  }

  private static List<String> seed(FileStoreTable table, String engine) throws Exception {
    if (engine.equals("deduplicate")) {
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(1, 10, 10, 20L));
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(1, 99, 99, 10L));
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(2, 22, 22, 20L));
      GenericRow deleted = GenericRow.of(1, 10, 10, 30L);
      deleted.setRowKind(RowKind.DELETE);
      PaimonPrimaryKeyIntervalTestSupport.append(table, deleted);
      // 存活键独立验证 sequence 20 胜过后到达的 10，避免 DELETE 遮蔽错误选择。
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(3, 30, 30, 20L));
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(3, 99, 99, 10L));
      return Arrays.asList("2|22|22|20", "3|30|30|20");
    }
    if (engine.equals("partial-update")) {
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(1, 10, null, 1L));
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(1, null, 20, 2L));
      return Collections.singletonList("1|10|20|2");
    }
    if (engine.equals("aggregation")) {
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(1, 2, 4, 1L));
      PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(1, 3, 5, 2L));
      return Collections.singletonList("1|5|9|2");
    }
    PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(1, 10, 11, 1L));
    PaimonPrimaryKeyIntervalTestSupport.append(table, GenericRow.of(1, 99, 99, 2L));
    return Collections.singletonList("1|10|11|1");
  }
}
