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

package org.apache.amoro.table.tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.TagOrBranchInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class TestTagContracts {
  @Test
  void defaultsPreserveReadOnlyPluginsAndPagedLegacyValues() {
    AmoroTable<?> table = mock(AmoroTable.class, CALLS_REAL_METHODS);
    assertFalse(table.tagOperations().isPresent());
    FormatTableDescriptor descriptor = mock(FormatTableDescriptor.class, CALLS_REAL_METHODS);
    List<TagOrBranchInfo> tags = new ArrayList<>();
    for (int i = 0; i < 23; i++) {
      tags.add(new TagOrBranchInfo(String.format("tag%02d", i), i + 1, 7, 100L, 200L, "tag"));
    }
    when(descriptor.getTableTags(table)).thenReturn(tags);
    TagQuery query = new TagQuery();
    query.setPage(2);
    org.apache.commons.lang3.tuple.Pair<List<TableTagInfo>, Long> page =
        descriptor.getTableTags(table, query);
    assertEquals(23L, page.getRight());
    assertEquals(3, page.getLeft().size());
    TableTagInfo row = page.getLeft().get(0);
    assertEquals("tag20", row.getName());
    assertEquals("21", row.getTarget().getId());
    assertEquals(7, row.getMinSnapshotsToKeep());
    assertEquals(200L, row.getMaxRefAgeMs());
    assertEquals("UNKNOWN", row.getRetention().getState());
    assertEquals("DEFAULT", row.getScope().getKind());
    query.getScope().setKind("TABLE");
    assertThrows(TagOperationException.class, () -> descriptor.getTableTags(table, query));
  }

  @Test
  void opaqueVersionsAndOverflowAreNotCoerced() {
    TableVersionRef version = TableVersionRef.of("019c6e27-e55b-73d1-87d8-4e01f1f75043", null);
    assertEquals("019c6e27-e55b-73d1-87d8-4e01f1f75043", version.getId());
    TagQuery query = new TagQuery();
    query.setPage(Integer.MAX_VALUE);
    query.setPageSize(100);
    assertThrows(TagOperationException.class, query::offset);
  }

  @Test
  void newRetentionOperationDefaultsToUnsupportedWithoutChangingExistingPlugins() {
    TableTagOperations operations = mock(TableTagOperations.class, CALLS_REAL_METHODS);
    TagOperationException failure =
        assertThrows(
            TagOperationException.class,
            () -> operations.updateTagRetention(new UpdateTagRetentionRequest()));
    assertEquals(422, failure.getStatus());
    assertEquals("NOT_APPLIED", failure.getOutcome());
    assertEquals("UNKNOWN", new TagWriteCapabilities().getRetention().getUpdateClock());
  }
}
