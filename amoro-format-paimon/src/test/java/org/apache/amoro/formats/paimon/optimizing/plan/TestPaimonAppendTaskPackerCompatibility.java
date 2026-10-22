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

package org.apache.amoro.formats.paimon.optimizing.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.utils.DigestUtil;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DeletionFile;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;

class TestPaimonAppendTaskPackerCompatibility {
  @ParameterizedTest
  @MethodSource("cases")
  void preservesBaselineTaskGroupsAndFileOrder(OptimizingType type, long limit, int seed)
      throws Exception {
    Properties expected = new Properties();
    try (InputStream resource =
        getClass().getResourceAsStream("/paimon-append-packer-f3198e58c.properties")) {
      assertNotNull(resource);
      expected.load(resource);
    }
    String key = type + "." + limit + "." + seed;
    // Captured from the pre-R4 f3198e58c packer. Hash task boundaries and ordered file names,
    // rather than checking only coverage, counts, or the new implementation's internal state.
    assertEquals(expected.getProperty(key), digest(pack(type, limit, seed)), key);
  }

  static Stream<Arguments> cases() {
    Stream.Builder<Arguments> cases = Stream.builder();
    for (OptimizingType type : new OptimizingType[] {OptimizingType.MAJOR, OptimizingType.FULL}) {
      for (long limit : new long[] {100, 700, 900, 1000, 4096, Long.MAX_VALUE / 2}) {
        for (int seed = 0; seed < 16; seed++) {
          cases.add(Arguments.of(type, limit, seed));
        }
      }
    }
    return cases.build();
  }

  static List<AppendCompactTask> pack(OptimizingType type, long limit, int seed) {
    Map<String, String> options = new HashMap<>();
    options.put("target-file-size", "1000 b");
    options.put("compaction.small-file-ratio", "0.7");
    options.put("compaction.min.file-num", "3");
    options.put("source.split.open-file-cost", "10 b");
    options.put("deletion-vectors.enabled", "true");
    PaimonPlanContext context =
        PaimonPlanContext.forOptions(
            CoreOptions.fromMap(options),
            new OptimizingConfig().setMaxTaskSize(limit),
            0,
            0,
            0,
            4,
            limit,
            10000);
    Random random = new Random(seed);
    long[] sizes = {0, 1, 100, 450, 700, 899, 900, 901, 1000, 10000};
    List<PaimonFileCandidate> files = new ArrayList<>();
    int count = seed == 0 ? 0 : 16 + random.nextInt(112);
    for (int id = 0; id < count; id++) {
      long size = sizes[random.nextInt(sizes.length)];
      boolean dv = seed % 4 == 1 || (seed % 4 != 0 && random.nextBoolean());
      String group = dv ? "dv-" + random.nextInt(5) : null;
      DataFileMeta file =
          DataFileMeta.create(
              "file-" + id,
              size,
              100,
              DataFileMeta.EMPTY_MIN_KEY,
              DataFileMeta.EMPTY_MAX_KEY,
              SimpleStats.EMPTY_STATS,
              SimpleStats.EMPTY_STATS,
              0L,
              0L,
              0L,
              DataFileMeta.DUMMY_LEVEL,
              null,
              null,
              null,
              null,
              null,
              null);
      files.add(
          PaimonFileCandidate.from(
              BinaryRow.EMPTY_ROW,
              file,
              context,
              dv ? new DeletionFile(group, 0L, 1L, 1L) : null,
              group));
    }
    return new PaimonAppendTaskPacker(context)
        .pack(
            new PaimonPartitionEvaluation(
                BinaryRow.EMPTY_ROW, type, files, files, files.size(), 0, 0));
  }

  static String digest(List<AppendCompactTask> tasks) {
    StringBuilder encoded = new StringBuilder();
    for (AppendCompactTask task : tasks) {
      encoded.append('[');
      for (DataFileMeta file : task.compactBefore()) {
        encoded.append(file.fileName().length()).append(':').append(file.fileName());
      }
      encoded.append(']');
    }
    return DigestUtil.sha256Hex(encoded.toString());
  }
}
