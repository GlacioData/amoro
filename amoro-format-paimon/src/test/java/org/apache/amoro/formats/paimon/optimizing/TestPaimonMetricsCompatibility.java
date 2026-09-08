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

import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionOutput;
import org.apache.amoro.formats.paimon.optimizing.primary.PaimonPrimaryKeyCompactionTask;
import org.apache.amoro.utils.SerializationUtil;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;

class TestPaimonMetricsCompatibility {
  private static final String LEGACY_PRIMARY =
      "rO0ABXNyAFNvcmcuYXBhY2hlLmFtb3JvLmZvcm1hdHMucGFpbW9uLm9wdGltaXppbmcucHJpbWFyeS5QYWltb25QcmltYXJ5S2V5Q29tcGFjdGlvbk91dHB1dAAAAAAAAAABAgAHSQAUY29tcGFjdGVkQnVja2V0Q291bnRKABJjb21wYWN0ZWRGaWxlQ291bnRKABFjb21wYWN0ZWRGaWxlU2l6ZUoAFGNvbXBhY3RlZFJlY29yZENvdW50SgARcHJvZHVjZWRGaWxlQ291bnRKABBwcm9kdWNlZEZpbGVTaXplTAAWY29tbWl0TWVzc2FnZUJ5dGVzTGlzdHQAEExqYXZhL3V0aWwvTGlzdDt4cAAAAAEAAAAAAAAABwAAAAAAAAK8AAAAAAAAAEYAAAAAAAAAAgAAAAAAAADIc3IAH2phdmEudXRpbC5Db2xsZWN0aW9ucyRFbXB0eUxpc3R6uBe0PKee3gIAAHhw";

  private static final String LEGACY_APPEND =
      "rO0ABXNyAEFvcmcuYXBhY2hlLmFtb3JvLmZvcm1hdHMucGFpbW9uLm9wdGltaXppbmcuUGFpbW9uQ29tcGFjdGlvbk91dHB1dAAAAAAAAAABAgAGSQAUY29tbWl0TWVzc2FnZVZlcnNpb25KABJjb21wYWN0ZWRGaWxlQ291bnRKABFjb21wYWN0ZWRGaWxlU2l6ZUoAEXByb2R1Y2VkRmlsZUNvdW50SgAQcHJvZHVjZWRGaWxlU2l6ZVsAEmNvbW1pdE1lc3NhZ2VCeXRlc3QAAltCeHAAAAABAAAAAAAAAAcAAAAAAAACvAAAAAAAAAACAAAAAAAAAMhw";

  @Test
  void longPartitionLabelsPreserveFullDisplayWithoutExceedingDatabaseLimit() {
    String partition = String.join("", Collections.nCopies(200, "x")) + " [bucket=0]";
    PaimonPrimaryKeyCompactionTask task =
        new PaimonPrimaryKeyCompactionTask(1, partition, null, Collections.emptyMap());
    assertEquals(true, task.getPartition().length() <= 128);
    assertEquals(
        partition, task.getProperties().get(PaimonPrimaryKeyCompactionTask.PARTITION_DISPLAY));
  }

  @Test
  void primaryRetryReplacesPreviousOutput() {
    PaimonPrimaryKeyCompactionTask task =
        new PaimonPrimaryKeyCompactionTask(1, "", null, new HashMap<>());
    PaimonPrimaryKeyCompactionOutput first =
        new PaimonPrimaryKeyCompactionOutput(Collections.emptyList(), 1, 7, 700, 70, 2, 200);
    first.setRewriteRecords(70, 20);
    task.setOutput(SerializationUtil.simpleSerialize(first));
    assertEquals(70, task.getSummary().getCompactedRecordCount());
    task.reset();
    assertEquals(0, task.getSummary().getCompactedFileCount());
    PaimonPrimaryKeyCompactionOutput retry =
        new PaimonPrimaryKeyCompactionOutput(Collections.emptyList(), 1, 3, 300, 30, 1, 100);
    retry.setRewriteRecords(30, 10);
    task.setOutput(SerializationUtil.simpleSerialize(retry));
    assertEquals(3, task.getSummary().getCompactedFileCount());
    assertEquals(30, task.getSummary().getCompactedRecordCount());
    assertEquals(10, task.getSummary().getProducedRecordCount());
  }

  // Samples were serialized by the original pre-fix Output classes (serialVersionUID=1).
  @Test
  void oldAppendBytesKeepFileCountersWithoutInventingRecords() throws Exception {
    try (ObjectInputStream in =
        new ObjectInputStream(
            new ByteArrayInputStream(Base64.getDecoder().decode(LEGACY_APPEND)))) {
      PaimonCompactionOutput output = (PaimonCompactionOutput) in.readObject();
      assertEquals(7, output.getCompactedFileCount());
      assertEquals(200, output.getProducedFileSize());
      assertEquals("0", output.summary().get("output-data-records"));
    }
  }

  @Test
  void oldPrimaryBytesKeepHistoricalCounters() throws Exception {
    try (ObjectInputStream in =
        new ObjectInputStream(
            new ByteArrayInputStream(Base64.getDecoder().decode(LEGACY_PRIMARY)))) {
      PaimonPrimaryKeyCompactionOutput output = (PaimonPrimaryKeyCompactionOutput) in.readObject();
      assertEquals(7, output.getCompactedFileCount());
      assertEquals(70, output.getCompactedRecordCount());
      assertEquals("0", output.summary().get("output-data-records"));
    }
  }
}
