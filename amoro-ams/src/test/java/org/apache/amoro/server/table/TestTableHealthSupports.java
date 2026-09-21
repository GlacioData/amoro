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

package org.apache.amoro.server.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.table.health.TableHealthSupport;
import org.junit.jupiter.api.Test;

class TestTableHealthSupports {
  @Test
  void legacyFormatsDoNotAcquireHealthStateOrPerformComparisonWork() {
    for (TableFormat format :
        new TableFormat[] {
          TableFormat.ICEBERG, TableFormat.MIXED_ICEBERG, TableFormat.MIXED_HIVE
        }) {
      TableHealthSupport support = TableHealthSupports.forFormat(format);
      assertFalse(support.enabled());
      assertTrue(support.requiredStates().isEmpty());
      DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
      when(runtime.getFormat()).thenReturn(format);
      when(runtime.getTableIdentifier())
          .thenReturn(ServerTableIdentifier.of("c", "d", "t", format));
      AmoroTable<?> table = mock(AmoroTable.class);
      TableHealthComparisonService.instance().refresh(runtime, table);
      TableHealthComparisonService.instance().capture(runtime, table, null, null);
      assertNull(TableHealthComparisonService.instance().view(runtime));
      verifyNoInteractions(table);
      verify(runtime, never()).runHealthComparison(any());
    }
  }

  @Test
  void catalogSpiProvidesHealthWithoutRequiringOptimizerActions() {
    TableHealthSupport support = TableHealthSupports.forFormat(TableFormat.PAIMON);
    assertTrue(support.enabled());
    assertEquals("paimon_health_comparison_v1", support.requiredStates().get(0).getKey());
  }
}
