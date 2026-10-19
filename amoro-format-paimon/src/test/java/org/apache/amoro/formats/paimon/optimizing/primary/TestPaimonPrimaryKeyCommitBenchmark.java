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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.amoro.optimizing.TableOptimizingCommitter.CommitMode;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Opt-in local experiments; fixture preparation and logical validation are outside the timer. */
@EnabledIfSystemProperty(named = "paimon.commit.benchmark", matches = "true")
class TestPaimonPrimaryKeyCommitBenchmark {
  @Test
  void equivalentFreshFixtures(@TempDir Path warehouse) throws Exception {
    int samples = Integer.parseInt(System.getProperty("paimon.commit.benchmark.samples", "5"));
    int rounds = Integer.parseInt(System.getProperty("paimon.commit.benchmark.rounds", "1"));
    CommitMode mode =
        CommitMode.valueOf(System.getProperty("paimon.commit.benchmark.mode", "NORMAL"));
    Path output =
        Paths.get(
            System.getProperty(
                "paimon.commit.benchmark.output", "target/commit-benchmark-samples.csv"));
    Files.createDirectories(output.toAbsolutePath().getParent());
    String revision = System.getProperty("paimon.commit.benchmark.revision", "unavailable");
    String jar = CoreOptions.class.getProtectionDomain().getCodeSource().getLocation().toString();
    // name, buckets, generations, maintenance backlog, dynamic buckets
    int[][] cases = {
      {32, 3, 0, 0},
      {32, 20, 1, 0},
      {8, 8, 0, 0},
      {32, 2, 0, 0},
      {8, 3, 0, 0},
      {32, 3, 0, 0},
      {8, 3, 0, 1},
      {32, 3, 0, 1}
    };
    String[] names = {
      "no_maintenance",
      "backlog",
      "fixed_total_8",
      "fixed_total_32",
      "fixed_per_bucket_8",
      "fixed_per_bucket_32",
      "dynamic_8",
      "dynamic_32"
    };
    try (BufferedWriter csv = Files.newBufferedWriter(output)) {
      csv.write(
          "revision,paimon_jar,jdk,round,sample,scenario,policy,mode,cache,concurrency,configured_bucket,active_units,input_files,message_count,message_bytes,metadata_files,metadata_bytes,snapshot_count_before,native_attempt_count,fallback_count,native_retry_count,total_ms,prepare_ms,native_ms,full_inspection_ms,outcome,options\n");
      for (int round = 0; round < rounds; round++) {
        for (int scenario = 0; scenario < cases.length; scenario++) {
          Path templatePath = warehouse.resolve("template");
          int[] config = cases[scenario];
          Map<String, String> options = new HashMap<>();
          options.put(PaimonPrimaryKeyOptions.MAJOR_MAX_BUCKET_RATIO, "1.00");
          options.put(CoreOptions.BUCKET.key(), config[3] == 1 ? "-1" : String.valueOf(config[0]));
          options.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
          options.put(
              CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), config[2] == 1 ? "1" : "2147483647");
          try (PaimonPrimaryKeyCommitTestSupport fixture =
              new PaimonPrimaryKeyCommitTestSupport(templatePath, config[3] == 1, options)) {
            for (int generation = 0; generation < config[1]; generation++) {
              fixture.writeGeneration(generation, config[0] * 16, config[0]);
            }
            List<PaimonPrimaryKeyCompactionTask> tasks = fixture.prepareTasks();
            FileStoreTable template = fixture.table();
            List<String> expected = PaimonPrimaryKeyCommitTestSupport.rows(template);
            long[] metadata = metadata(template);
            long fileCount =
                tasks.stream()
                    .flatMap(t -> t.getInput().getUnits().stream())
                    .mapToLong(PaimonBucketCompactionUnit::getFileCount)
                    .sum();
            int units = tasks.stream().mapToInt(t -> t.getInput().getUnits().size()).sum();
            assertEquals(config[0], units, "Every configured bucket must be active and selected");
            assertEquals((long) config[0] * config[1], fileCount, "Controlled input file count");
            long messageBytes =
                tasks.stream()
                    .flatMap(t -> t.getOutput().getCommitMessageBytesList().stream())
                    .mapToLong(bytes -> bytes.length)
                    .sum();
            long messageCount =
                tasks.stream()
                    .mapToLong(t -> t.getOutput().getCommitMessageBytesList().size())
                    .sum();
            for (int sample = -1; sample < samples; sample++) {
              // Each group/round gets an independent native template; samples get byte-for-byte
              // independent copies, including prepared output files. Never hard-link mutable files.
              for (int order = 0; order < 2; order++) {
                boolean candidate = ((sample + round + order) & 1) == 0;
                Path fixturePath =
                    warehouse.resolve(
                        "sample-" + round + "-" + scenario + "-" + sample + "-" + order);
                try {
                  copyFixture(Paths.get(template.location().toUri()), fixturePath);
                  FileStoreTable table =
                      FileStoreTableFactory.create(
                          template.fileIO(),
                          new org.apache.paimon.fs.Path(fixturePath.toUri()),
                          template.schema());
                  Map<String, String> timings = new HashMap<>();
                  Logger logger =
                      mock(
                          Logger.class,
                          call -> {
                            if ("info".equals(call.getMethod().getName())) {
                              String event =
                                  MessageFormatter.arrayFormat(
                                          call.getArgument(0),
                                          Arrays.copyOfRange(
                                              call.getArguments(), 1, call.getArguments().length))
                                      .getMessage();
                              Map<String, String> fields = new HashMap<>();
                              for (String field : event.split(" ")) {
                                int equals = field.indexOf('=');
                                if (equals > 0) {
                                  fields.put(
                                      field.substring(0, equals), field.substring(equals + 1));
                                }
                              }
                              timings.put(fields.get("stage"), fields.get("elapsedMs"));
                              if ("native_attempt".equals(fields.get("stage"))) {
                                timings.merge(
                                    "attempts",
                                    "1",
                                    (a, b) -> String.valueOf(Integer.parseInt(a) + 1));
                                if (fields.get("phase").startsWith("fallback")) {
                                  timings.merge(
                                      "fallbacks",
                                      "1",
                                      (a, b) -> String.valueOf(Integer.parseInt(a) + 1));
                                }
                              }
                              if ("committer_total".equals(fields.get("stage"))) {
                                timings.put("outcome", fields.get("outcome"));
                              }
                            }
                            return RETURNS_DEFAULTS.answer(call);
                          });
                  when(logger.isInfoEnabled()).thenReturn(true);
                  // Both sides run the same observation patch; only this local copy policy differs.
                  // Control reproduces cf7d1a9's false override without editing native Paimon code.
                  FileStoreTable source = spy(table);
                  doReturn(
                          table.copy(
                              Collections.singletonMap("write-only", candidate ? "true" : "false")))
                      .when(source)
                      .copy(anyMap());
                  PaimonPrimaryKeyTableCommit committer =
                      new PaimonPrimaryKeyTableCommit(null, source, tasks, logger);
                  long start = System.nanoTime();
                  committer.commit(mode);
                  double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
                  assertEquals(expected, PaimonPrimaryKeyCommitTestSupport.rows(table));
                  assertEquals(
                      candidate || config[2] == 0, table.snapshotManager().snapshotExists(1));
                  if (sample >= 0) {
                    csv.write(
                        String.join(
                            ",",
                            quote(revision),
                            quote(jar),
                            quote(System.getProperty("java.version")),
                            String.valueOf(round),
                            String.valueOf(sample),
                            names[scenario],
                            candidate ? "candidate" : "old_policy_control",
                            mode.name(),
                            "warm_os_new_instance",
                            "none",
                            options.get("bucket"),
                            String.valueOf(units),
                            String.valueOf(fileCount),
                            String.valueOf(messageCount),
                            String.valueOf(messageBytes),
                            String.valueOf(metadata[0]),
                            String.valueOf(metadata[1]),
                            String.valueOf(config[1]),
                            timings.getOrDefault("attempts", "0"),
                            timings.getOrDefault("fallbacks", "0"),
                            "unavailable",
                            String.format(Locale.ROOT, "%.6f", elapsedMs),
                            timings.getOrDefault("prepare_messages", "unavailable"),
                            timings.getOrDefault("native_attempt", "unavailable"),
                            timings.getOrDefault("full_inspection", "unavailable"),
                            timings.getOrDefault("outcome", "unavailable"),
                            quote(new java.util.TreeMap<>(table.options()).toString())));
                    csv.newLine();
                    csv.flush();
                  }
                  assertEquals(
                      (long) config[1],
                      template.snapshotManager().latestSnapshotId(),
                      "Sample must not mutate the template");
                } finally {
                  deleteFixture(fixturePath);
                }
              }
            }
          } finally {
            deleteFixture(templatePath);
          }
        }
      }
    }
  }

  private static void copyFixture(Path source, Path target) throws Exception {
    try (Stream<Path> paths = Files.walk(source)) {
      for (Path path : paths.collect(Collectors.toList())) {
        Path destination = target.resolve(source.relativize(path));
        if (Files.isDirectory(path)) {
          Files.createDirectories(destination);
        } else {
          Files.copy(path, destination);
        }
      }
    }
  }

  private static void deleteFixture(Path directory) throws Exception {
    if (Files.exists(directory)) {
      try (Stream<Path> paths = Files.walk(directory)) {
        for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
          Files.delete(path);
        }
      }
    }
  }

  private static long[] metadata(FileStoreTable table) throws Exception {
    long count = 0;
    long bytes = 0;
    try (Stream<Path> paths = Files.walk(Paths.get(table.location().toUri()))) {
      for (Path path : paths.filter(Files::isRegularFile).collect(Collectors.toList())) {
        String parent = path.getParent().getFileName().toString();
        if (parent.equals("manifest") || parent.equals("index") || parent.equals("snapshot")) {
          count++;
          bytes += Files.size(path);
        }
      }
    }
    return new long[] {count, bytes};
  }

  private static String quote(String value) {
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }
}
