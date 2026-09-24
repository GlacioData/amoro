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

package org.apache.amoro.formats.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.paimon.FileStore;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.Optional;

class TestPaimonDetailBucketMode {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path warehouse;

  @ParameterizedTest
  @EnumSource(BucketMode.class)
  void returnsNativeModeWithoutReadingSchemasOrManifests(BucketMode mode) {
    FileStoreTable table = mock(FileStoreTable.class);
    FileStore<?> store = mock(FileStore.class);
    TableSchema schema = mock(TableSchema.class);
    when(schema.id()).thenReturn(7L);
    when(table.schema()).thenReturn(schema);
    SnapshotManager snapshots =
        new SnapshotManager(
            org.apache.paimon.fs.local.LocalFileIO.create(),
            new org.apache.paimon.fs.Path(warehouse.toUri()),
            null,
            null,
            null);
    doReturn(store).when(table).store();
    when(table.rowType()).thenReturn(RowType.of());
    when(table.primaryKeys()).thenReturn(Collections.emptyList());
    when(table.options()).thenReturn(Collections.emptyMap());
    when(table.comment()).thenReturn(Optional.empty());
    when(store.bucketMode()).thenReturn(mode);
    when(store.partitionType()).thenReturn(RowType.of());
    when(store.snapshotManager()).thenReturn(snapshots);
    ServerTableMeta response =
        new PaimonTableDescriptor()
            .getTableDetail(new PaimonTable(TableIdentifier.of("catalog", "db", "table"), table));
    assertEquals(mode.name(), response.getBucketMode());
    assertEquals("BUCKET_MODE", response.getTableModel().getKind());
    assertEquals(mode.name(), response.getTableModel().getCode());
    assertEquals(0L, response.getCreateTime());
    assertEquals(7L, response.getDetailObservation().getSchemaId());
    verify(table, never()).schemaManager();
    verify(table).store();
    verify(table).rowType();
    verify(table).primaryKeys();
    verify(table).options();
    verify(table).comment();
    verify(table).schema();
    verifyNoMoreInteractions(table);
    verify(store).bucketMode();
    verify(store).partitionType();
    verify(store).snapshotManager();
    verifyNoMoreInteractions(store);
  }
}
