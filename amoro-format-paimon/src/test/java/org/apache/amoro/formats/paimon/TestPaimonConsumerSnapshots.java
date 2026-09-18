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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.table.descriptor.ConsumerInfo;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class TestPaimonConsumerSnapshots {
  @TempDir Path warehouse;
  FileStoreTable table;
  ConsumerManager consumers;
  SnapshotManager snapshots = mock(SnapshotManager.class);
  AmoroTable<?> amoro = mock(AmoroTable.class);
  PaimonTableDescriptor descriptor = new PaimonTableDescriptor();

  @BeforeEach
  void setup() throws Exception {
    try (Catalog catalog =
        PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new Configuration())) {
      catalog.createDatabase("db", true);
      Identifier id = Identifier.create("db", "consumers");
      catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
      table = spy((FileStoreTable) catalog.getTable(id));
    }
    consumers = new ConsumerManager(table.fileIO(), table.location());
    doReturn(snapshots).when(table).snapshotManager();
    doReturn(table).when(amoro).originalTable();
    when(snapshots.tryGetSnapshot(anyLong())).thenThrow(new FileNotFoundException("missing"));
  }

  Snapshot available(long id) throws Exception {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.id()).thenReturn(id);
    when(snapshot.commitKind()).thenReturn(Snapshot.CommitKind.APPEND);
    when(snapshots.snapshotExists(id)).thenReturn(true);
    doReturn(snapshot).when(snapshots).tryGetSnapshot(id);
    return snapshot;
  }

  @Test
  void missingConsumerDoesNotHideHealthyConsumerOrChangeProgress() throws Exception {
    consumers.resetConsumer("missing", new Consumer(10));
    consumers.resetConsumer("healthy", new Consumer(20));
    available(20);
    List<ConsumerInfo> result = descriptor.getTableConsumerInfos(amoro);
    assertEquals(2, result.size());
    java.util.Map<String, ConsumerInfo> byId =
        result.stream().collect(Collectors.toMap(ConsumerInfo::getConsumerId, c -> c));
    assertEquals(10, byId.get("missing").getNextSnapshotId());
    assertNull(byId.get("missing").getAmoroCurrentSnapshotsOfTable());
    assertEquals("20", byId.get("healthy").getAmoroCurrentSnapshotsOfTable().getSnapshotId());
    verify(table, never()).store();
    verify(snapshots, never()).snapshots();
    verify(snapshots, never()).latestSnapshotId();
  }

  @Test
  void waitingForNextSnapshotUsesPrevious() throws Exception {
    consumers.resetConsumer("waiting", new Consumer(21));
    available(20);
    ConsumerInfo result = descriptor.getTableConsumerInfos(amoro).get(0);
    assertEquals(21, result.getNextSnapshotId());
    assertEquals("20", result.getAmoroCurrentSnapshotsOfTable().getSnapshotId());
  }

  @Test
  void disappearanceAfterExistsCheckIsAlsoHandled() throws Exception {
    consumers.resetConsumer("race", new Consumer(21));
    when(snapshots.snapshotExists(21)).thenReturn(true);
    available(20);
    assertEquals(
        "20",
        descriptor
            .getTableConsumerInfos(amoro)
            .get(0)
            .getAmoroCurrentSnapshotsOfTable()
            .getSnapshotId());
  }

  @Test
  void firstMissingSnapshotNeverReadsZero() {
    consumers.resetConsumer("first", new Consumer(1));
    assertNull(descriptor.getTableConsumerInfos(amoro).get(0).getAmoroCurrentSnapshotsOfTable());
    verify(snapshots, never()).snapshotExists(0);
  }

  @Test
  void invalidConsumerIdDoesNotUnderflowOrScan() {
    consumers.resetConsumer("invalid", new Consumer(Long.MIN_VALUE));
    assertThrows(SnapshotReadException.class, () -> descriptor.getTableConsumerInfos(amoro));
    verify(snapshots, never()).snapshotExists(anyLong());
  }

  @Test
  void existenceCheckFailureIsNotTreatedAsMissing() {
    consumers.resetConsumer("denied", new Consumer(21));
    when(snapshots.snapshotExists(21)).thenThrow(new SecurityException("denied"));
    assertThrows(SnapshotReadException.class, () -> descriptor.getTableConsumerInfos(amoro));
    verify(snapshots, never()).snapshotExists(20);
  }

  @Test
  void storageErrorsAreNotReportedAsMissing() throws Exception {
    consumers.resetConsumer("broken", new Consumer(21));
    when(snapshots.snapshotExists(21)).thenReturn(true);
    doThrow(new IllegalStateException("storage failure")).when(snapshots).tryGetSnapshot(21);
    SnapshotReadException error =
        assertThrows(SnapshotReadException.class, () -> descriptor.getTableConsumerInfos(amoro));
    assertEquals(SnapshotReadException.Reason.READ_FAILED, error.getReason());
  }
}
