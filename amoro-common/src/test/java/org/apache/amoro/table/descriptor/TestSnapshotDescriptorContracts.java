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

package org.apache.amoro.table.descriptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class TestSnapshotDescriptorContracts {
  @Test
  void unsupportedFormatsNeverFallBackToUnlimitedDetail() {
    FormatTableDescriptor descriptor = mock(FormatTableDescriptor.class, CALLS_REAL_METHODS);
    assertFalse(descriptor.supportsBoundedSnapshotRead());
    assertThrows(
        UnsupportedOperationException.class,
        () -> descriptor.getSnapshotDetailPage(null, "1", "main", 25, null));
  }

  @Test
  void unknownCountsStayNullAndLegacyCountsKeepTheirNumericValues() throws Exception {
    AmoroSnapshotsOfTable snapshot =
        new AmoroSnapshotsOfTable(
            "1", 130, 4096L, 10, 0, "APPEND", "INGESTION", Collections.emptyMap());
    ObjectMapper mapper = new ObjectMapper();
    assertTrue(mapper.writeValueAsString(snapshot).contains("\"fileCount\":130"));
    snapshot.setFileCount(3000000000L);
    assertTrue(mapper.writeValueAsString(snapshot).contains("\"fileCount\":3000000000"));
    assertEquals(
        3000000000L,
        mapper.readValue("{\"fileCount\":3000000000}", AmoroSnapshotsOfTable.class).getFileCount());
    snapshot.setFileCount(null);
    snapshot.setFileSize(null);
    String json = mapper.writeValueAsString(snapshot);
    assertTrue(json.contains("\"fileCount\":null"));
    assertTrue(json.contains("\"fileSize\":null"));
    assertTrue(json.contains("\"originalFileSize\":null"));
    TableSummary summary = new TableSummary(0, null, null, 10, "paimon");
    summary.setFile(null);
    assertTrue(mapper.writeValueAsString(summary).contains("\"file\":null"));
    assertTrue(
        mapper.writeValueAsString(new ServerTableMeta()).contains("\"boundedSnapshotRead\":false"));
  }

  @Test
  void boxedCountsCompareValuesNotIdentity() {
    AmoroSnapshotsOfTable a =
        new AmoroSnapshotsOfTable(
            "1", 300, 9999L, 10, 0, "APPEND", "INGESTION", Collections.emptyMap());
    AmoroSnapshotsOfTable b =
        new AmoroSnapshotsOfTable(
            "1", 300, 9999L, 10, 0, "APPEND", "INGESTION", Collections.emptyMap());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }
}
