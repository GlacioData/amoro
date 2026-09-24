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

package org.apache.amoro.server.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.TableModel;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;

class TestTableModel {
  @TempDir Path warehouse;

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void readsRealIcebergVersionWithoutAdditionalMetadataReads(int version) {
    Table nativeTable =
        new HadoopTables()
            .create(
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", String.valueOf(version)),
                warehouse.resolve("v" + version).toString());
    TableOperations operations = mock(TableOperations.class);
    when(operations.current())
        .thenReturn(((HasTableOperations) nativeTable).operations().current());
    org.apache.iceberg.BaseTable cachedTable =
        new org.apache.iceberg.BaseTable(operations, "cached");
    AmoroTable<?> table = mock(AmoroTable.class);
    when(table.format()).thenReturn(TableFormat.ICEBERG);
    doReturn(cachedTable).when(table).originalTable();

    TableModel model = new MixedAndIcebergTableDescriptor().getTableModel(table);

    assertEquals(TableModel.FORMAT_VERSION, model.getKind());
    assertEquals("V" + version, model.getCode());
    verify(operations).current();
    verifyNoMoreInteractions(operations);
    verify(table).format();
    verify(table).originalTable();
    verifyNoMoreInteractions(table);

    org.apache.amoro.formats.iceberg.IcebergTable wrapped =
        org.apache.amoro.formats.iceberg.IcebergTable.newIcebergTable(
            org.apache.amoro.table.TableIdentifier.of("local", "db", "v" + version),
            nativeTable,
            org.apache.amoro.table.TableMetaStore.EMPTY,
            Collections.emptyMap());
    ServerTableMeta detail = new MixedAndIcebergTableDescriptor().getTableDetail(wrapped);
    assertEquals("ICEBERG", detail.getTableType());
    assertEquals("V" + version, detail.getTableModel().getCode());
    assertEquals("Iceberg(V" + version + ")", detail.getTableSummary().getTableFormat());
  }

  @ParameterizedTest
  @ValueSource(strings = {"MIXED_ICEBERG", "MIXED_HIVE", "PAIMON"})
  void doesNotInferLogicalModelFromAnUnderlyingTable(String format) {
    AmoroTable<?> table = mock(AmoroTable.class);
    when(table.format()).thenReturn(TableFormat.valueOf(format));
    assertNull(new MixedAndIcebergTableDescriptor().getTableModel(table));
    verify(table).format();
    verifyNoMoreInteractions(table);
  }

  @Test
  void serializesOpenContractWithoutClaimingV3ReadSupport() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ServerTableMeta meta = new ServerTableMeta();
    assertNull(meta.getTableModel());
    JsonNode unavailable = mapper.valueToTree(meta);
    assertTrue(unavailable.has("tableModel"));
    assertTrue(unavailable.get("tableModel").isNull());
    for (String code : new String[] {"V3", "V4", "FUTURE"}) {
      meta.setTableModel(new TableModel(TableModel.FORMAT_VERSION, code));
      JsonNode json = mapper.valueToTree(meta);
      assertEquals("FORMAT_VERSION", json.path("tableModel").path("kind").asText());
      assertEquals(code, json.path("tableModel").path("code").asText());
      assertEquals(2, json.path("tableModel").size());
    }
  }

  @Test
  void rejectsMissingModelIdentity() {
    assertThrows(NullPointerException.class, () -> new TableModel(null, "V2"));
    assertThrows(NullPointerException.class, () -> new TableModel("FORMAT_VERSION", null));
    assertThrows(IllegalArgumentException.class, () -> new TableModel(" ", "V2"));
    assertThrows(IllegalArgumentException.class, () -> new TableModel("FORMAT_VERSION", ""));
  }
}
