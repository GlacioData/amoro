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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class TestPaimonRewriteMetrics {
  @Test
  void changelogDoesNotInflatePhysicalDataRows() {
    CommitMessageImpl message = mock(CommitMessageImpl.class);
    when(message.partition()).thenReturn(BinaryRow.EMPTY_ROW);
    when(message.bucket()).thenReturn(0);
    CompactIncrement increment =
        new CompactIncrement(
            Collections.singletonList(file("data-before", 100, 10)),
            Collections.singletonList(file("data-after", 60, 6)),
            Collections.singletonList(file("changelog", 1000, 100)));
    when(message.compactIncrement()).thenReturn(increment);
    PaimonRewriteMetrics metrics = PaimonRewriteMetrics.collect(Collections.singletonList(message));
    assertEquals(10, metrics.inputRecords());
    assertEquals(6, metrics.outputRecords());
    assertEquals(60, metrics.outputBytes());
  }

  @Test
  void excludesLevelUpgradesAndDeduplicatesWithinPhysicalBucket() {
    DataFileMeta retained = file("retained", 500, 50);
    DataFileMeta before = file("old", 100, 10);
    DataFileMeta after = file("new", 60, 6);
    CommitMessage message =
        message(
            BinaryRow.EMPTY_ROW,
            0,
            Arrays.asList(retained, before, before),
            Arrays.asList(retained, after));
    PaimonRewriteMetrics metrics = PaimonRewriteMetrics.collect(Arrays.asList(message, message));
    assertEquals(1, metrics.inputFiles());
    assertEquals(100, metrics.inputBytes());
    assertEquals(10, metrics.inputRecords());
    assertEquals(1, metrics.outputFiles());
    assertEquals(60, metrics.outputBytes());
    assertEquals(6, metrics.outputRecords());
  }

  @Test
  void fileIdentityIncludesPartitionAndBucket() {
    BinaryRow partition = new BinaryRow(1);
    BinaryRowWriter writer = new BinaryRowWriter(partition);
    writer.writeInt(0, 9);
    writer.complete();
    DataFileMeta file = file("same", 10, 2);
    PaimonRewriteMetrics metrics =
        PaimonRewriteMetrics.collect(
            Arrays.asList(
                message(
                    BinaryRow.EMPTY_ROW,
                    0,
                    Collections.singletonList(file),
                    Collections.emptyList()),
                message(
                    BinaryRow.EMPTY_ROW,
                    1,
                    Collections.emptyList(),
                    Collections.singletonList(file)),
                message(partition, 0, Collections.emptyList(), Collections.singletonList(file))));
    assertEquals(1, metrics.inputFiles());
    assertEquals(2, metrics.outputFiles());
    assertEquals(4, metrics.outputRecords());
  }

  @Test
  void upgradeOnlyAndEmptyResultAreKnownZeros() {
    DataFileMeta file = file("upgraded", 100, 10);
    PaimonRewriteMetrics metrics =
        PaimonRewriteMetrics.collect(
            Collections.singletonList(
                message(
                    BinaryRow.EMPTY_ROW,
                    0,
                    Collections.singletonList(file),
                    Collections.singletonList(file))));
    assertEquals(0, metrics.inputFiles());
    assertEquals(0, metrics.outputRecords());
    assertEquals(0, PaimonRewriteMetrics.collect(Collections.emptyList()).inputRecords());
  }

  private static DataFileMeta file(String name, long bytes, long rows) {
    DataFileMeta file = mock(DataFileMeta.class);
    when(file.fileName()).thenReturn(name);
    when(file.fileSize()).thenReturn(bytes);
    when(file.rowCount()).thenReturn(rows);
    return file;
  }

  private static CommitMessage message(
      BinaryRow partition, int bucket, List<DataFileMeta> before, List<DataFileMeta> after) {
    CommitMessageImpl message = mock(CommitMessageImpl.class);
    when(message.partition()).thenReturn(partition);
    when(message.bucket()).thenReturn(bucket);
    when(message.compactIncrement())
        .thenReturn(new CompactIncrement(before, after, Collections.emptyList()));
    return message;
  }
}
