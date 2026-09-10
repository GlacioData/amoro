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

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Counts physical data-file rewrites, excluding filenames retained within the same partition and
 * bucket.
 */
public final class PaimonRewriteMetrics {
  private final long[] before = new long[3];
  private final long[] after = new long[3];

  private PaimonRewriteMetrics() {}

  public static PaimonRewriteMetrics collect(List<CommitMessage> messages) {
    Map<BinaryRow, Map<Integer, Map<String, DataFileMeta>>> inputs = new HashMap<>();
    Map<BinaryRow, Map<Integer, Map<String, DataFileMeta>>> outputs = new HashMap<>();
    for (CommitMessage message : messages) {
      if (!(message instanceof CommitMessageImpl)) {
        throw new IllegalArgumentException("Expected Paimon CommitMessageImpl");
      }
      CommitMessageImpl impl = (CommitMessageImpl) message;
      add(inputs, impl, impl.compactIncrement().compactBefore());
      add(outputs, impl, impl.compactIncrement().compactAfter());
    }
    PaimonRewriteMetrics metrics = new PaimonRewriteMetrics();
    countDifference(inputs, outputs, metrics.before);
    countDifference(outputs, inputs, metrics.after);
    return metrics;
  }

  private static void add(
      Map<BinaryRow, Map<Integer, Map<String, DataFileMeta>>> groups,
      CommitMessageImpl message,
      List<DataFileMeta> files) {
    Map<String, DataFileMeta> bucket =
        groups
            .computeIfAbsent(message.partition().copy(), k -> new HashMap<>())
            .computeIfAbsent(message.bucket(), k -> new HashMap<>());
    for (DataFileMeta file : files) {
      bucket.putIfAbsent(file.fileName(), file);
    }
  }

  private static void countDifference(
      Map<BinaryRow, Map<Integer, Map<String, DataFileMeta>>> source,
      Map<BinaryRow, Map<Integer, Map<String, DataFileMeta>>> retained,
      long[] totals) {
    source.forEach(
        (partition, buckets) ->
            buckets.forEach(
                (bucket, files) -> {
                  Map<Integer, Map<String, DataFileMeta>> retainedBuckets = retained.get(partition);
                  Map<String, DataFileMeta> retainedFiles =
                      retainedBuckets == null ? null : retainedBuckets.get(bucket);
                  files.forEach(
                      (name, file) -> {
                        // Paimon 1.4.2 retains filenames during level upgrades; see
                        // AbstractCompactRewriter.upgrade。
                        if (retainedFiles == null || !retainedFiles.containsKey(name)) {
                          totals[0] = Math.addExact(totals[0], 1L);
                          totals[1] = Math.addExact(totals[1], file.fileSize());
                          totals[2] = Math.addExact(totals[2], file.rowCount());
                        }
                      });
                }));
  }

  public long inputFiles() {
    return before[0];
  }

  public long inputBytes() {
    return before[1];
  }

  public long inputRecords() {
    return before[2];
  }

  public long outputFiles() {
    return after[0];
  }

  public long outputBytes() {
    return after[1];
  }

  public long outputRecords() {
    return after[2];
  }
}
