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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.TableIdentifier;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.server.table.TableManager;
import org.apache.amoro.table.descriptor.FormatTableDescriptor;
import org.apache.amoro.table.descriptor.OperationType;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.descriptor.SnapshotDetailPage;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class TestSnapshotRequestGate {
  private ServerTableDescriptor server;
  private final FormatTableDescriptor format = mock(FormatTableDescriptor.class);
  private final AmoroTable<?> table = mock(AmoroTable.class);
  private final TableIdentifier id = new TableIdentifier("catalog", "db", "tbl");

  @BeforeEach
  void setup() throws Exception {
    CatalogManager catalogs = mock(CatalogManager.class);
    ServerCatalog catalog = mock(ServerCatalog.class);
    when(catalogs.getServerCatalog("catalog")).thenReturn(catalog);
    doReturn(table).when(catalog).loadTable("db", "tbl");
    when(table.format()).thenReturn(TableFormat.PAIMON);
    server = new ServerTableDescriptor(catalogs, mock(TableManager.class), new Configurations());
    Field field = ServerTableDescriptor.class.getDeclaredField("formatDescriptorMap");
    field.setAccessible(true);
    ((Map<TableFormat, FormatTableDescriptor>) field.get(server)).put(TableFormat.PAIMON, format);
    when(format.supportsBoundedSnapshotRead()).thenReturn(true);
    when(format.getSnapshotDetailPage(table, "1", "main", 25, null))
        .thenReturn(new SnapshotDetailPage(Collections.emptyList(), null));
  }

  @Test
  void detailConsumerAndListShareAdmission() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch cleanup = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    when(format.getSnapshots(table, "main", OperationType.ALL, 25, 0, null))
        .thenReturn(org.apache.commons.lang3.tuple.Pair.of(Collections.emptyList(), 0L));
    when(format.getSnapshotDetailPage(table, "1", "main", 25, null))
        .thenAnswer(
            i -> {
              entered.countDown();
              assertTrue(cleanup.await(5, TimeUnit.SECONDS));
              throw new SnapshotReadException(
                  SnapshotReadException.Reason.READ_FAILED, "close failed");
            });
    Thread first =
        new Thread(
            () -> {
              try {
                server.getSnapshotDetailResponse(id, "1", "main", null, 25, null, false);
              } catch (Throwable e) {
                failure.set(e);
              }
            });
    first.start();
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertEquals(
          SnapshotReadException.Reason.BUSY,
          assertThrows(
                  SnapshotReadException.class,
                  () -> server.getSnapshots(id, "main", OperationType.ALL, 25, 0, null))
              .getReason());
      assertEquals(
          SnapshotReadException.Reason.BUSY,
          assertThrows(
                  SnapshotReadException.class,
                  () -> server.getSnapshotListResponse(id, "main", OperationType.ALL, 1, 25, null))
              .getReason());
      assertEquals(
          SnapshotReadException.Reason.BUSY,
          assertThrows(
                  SnapshotReadException.class,
                  () -> server.getSnapshotDetailResponse(id, "1", "main", null, 25, null, false))
              .getReason());
      assertEquals(
          SnapshotReadException.Reason.BUSY,
          assertThrows(SnapshotReadException.class, () -> server.getTableConsumersInfos(id))
              .getReason());
      verify(format).getSnapshotDetailPage(any(), anyString(), anyString(), anyInt(), any());
    } finally {
      cleanup.countDown();
      first.join(5000);
    }
    assertTrue(failure.get() instanceof SnapshotReadException);
    doReturn(new SnapshotDetailPage(Collections.emptyList(), null))
        .when(format)
        .getSnapshotDetailPage(table, "1", "main", 25, null);
    assertTrue(
        server.getSnapshotDetailResponse(id, "1", "main", null, 25, null, false)
            instanceof SnapshotDetailPage);
  }

  @Test
  void listHoldsAdmissionAndReleasesItAfterFailure() {
    when(format.getSnapshots(table, "main", OperationType.ALL, 25, 0, null))
        .thenAnswer(
            i -> {
              assertEquals(
                  SnapshotReadException.Reason.BUSY,
                  assertThrows(
                          SnapshotReadException.class,
                          () ->
                              server.getSnapshotDetailResponse(
                                  id, "1", "main", null, 25, null, false))
                      .getReason());
              throw new SnapshotReadException(
                  SnapshotReadException.Reason.READ_FAILED, "list failed");
            });
    assertEquals(
        SnapshotReadException.Reason.READ_FAILED,
        assertThrows(
                SnapshotReadException.class,
                () -> server.getSnapshotListResponse(id, "main", OperationType.ALL, 1, 25, null))
            .getReason());
    assertTrue(
        server.getSnapshotDetailResponse(id, "1", "main", null, 25, null, false)
            instanceof SnapshotDetailPage);
  }

  @Test
  void legacyCannotReceiveCursorAndBoundedCannotReceiveOffset() {
    assertThrows(
        SnapshotReadException.class,
        () -> server.getSnapshotDetailResponse(id, "1", "main", 1, 25, null, false));
    assertThrows(
        SnapshotReadException.class,
        () -> server.getSnapshotDetailResponse(id, "1", "main", null, 26, null, false));
    when(format.supportsBoundedSnapshotRead()).thenReturn(false);
    assertThrows(
        SnapshotReadException.class,
        () -> server.getSnapshotDetailResponse(id, "1", "main", null, 25, "cursor", false));
    when(format.getSnapshotDetail(table, "1", "main")).thenReturn(Collections.emptyList());
    assertTrue(
        server.getSnapshotDetailResponse(id, "1", "main", 1, 20, null, false)
            instanceof org.apache.amoro.server.dashboard.response.PageResult);
  }

  @Test
  void metadataCarriesActualCapabilityAndOffsetsCannotOverflow() {
    when(format.getTableDetail(table)).thenReturn(new ServerTableMeta());
    assertTrue(server.getTableDetail(id).isBoundedSnapshotRead());
    assertThrows(
        SnapshotReadException.class,
        () -> ServerTableDescriptor.snapshotOffset(Integer.MAX_VALUE, 25));
    assertThrows(SnapshotReadException.class, () -> ServerTableDescriptor.snapshotOffset(0, 25));
  }

  @Test
  void listDefaultsAreChosenFromTheActualFormatCapability() {
    when(format.getSnapshots(table, "main", OperationType.ALL, 25, 0, null))
        .thenReturn(org.apache.commons.lang3.tuple.Pair.of(Collections.emptyList(), 0L));
    assertEquals(
        0L,
        server.getSnapshotListResponse(id, "main", OperationType.ALL, null, null, null).getTotal());
    verify(format).getSnapshots(table, "main", OperationType.ALL, 25, 0, null);
    when(format.supportsBoundedSnapshotRead()).thenReturn(false);
    when(format.getSnapshots(table, "main", OperationType.ALL, 20, 0, null))
        .thenReturn(org.apache.commons.lang3.tuple.Pair.of(Collections.emptyList(), 0L));
    assertEquals(
        0L,
        server.getSnapshotListResponse(id, "main", OperationType.ALL, null, null, null).getTotal());
    verify(format).getSnapshots(table, "main", OperationType.ALL, 20, 0, null);
  }
}
