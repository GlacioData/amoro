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

package org.apache.amoro.formats.paimon.optimizing.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthComparisonState.Entry;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class TestPaimonHealthSupport {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path warehouse;

  @Test
  void oneTimeCaptureMergeRetriesAConflictWithoutLosingThePreviousResult() {
    org.apache.amoro.table.TableRuntimeStore store =
        org.mockito.Mockito.mock(org.apache.amoro.table.TableRuntimeStore.class);
    org.apache.amoro.table.TableRuntimeStore.TableRuntimeOperation operation =
        org.mockito.Mockito.mock(
            org.apache.amoro.table.TableRuntimeStore.TableRuntimeOperation.class);
    java.util.concurrent.atomic.AtomicReference<
            java.util.function.Function<PaimonHealthComparisonState, PaimonHealthComparisonState>>
        merge = new java.util.concurrent.atomic.AtomicReference<>();
    PaimonHealthComparisonState persisted = new PaimonHealthComparisonState();
    java.util.concurrent.atomic.AtomicInteger attempts =
        new java.util.concurrent.atomic.AtomicInteger();
    org.mockito.Mockito.when(store.begin()).thenReturn(operation);
    org.mockito.Mockito.when(
            operation.updateState(
                org.mockito.Mockito.eq(PaimonHealthComparisonState.KEY), org.mockito.Mockito.any()))
        .thenAnswer(
            call -> {
              merge.set(call.getArgument(1));
              return operation;
            });
    org.mockito.Mockito.doAnswer(
            call -> {
              if (attempts.incrementAndGet() == 1) {
                Entry previous = new Entry();
                previous.setProcessId(100);
                previous.setStatus("READY");
                persisted.setLatest(previous);
                throw new org.apache.amoro.exception.RuntimeStateConflictException(
                    1, PaimonHealthComparisonState.KEY.getKey());
              }
              merge.get().apply(persisted);
              return null;
            })
        .when(operation)
        .commit();
    Entry pending = new Entry();
    pending.setProcessId(101);
    pending.setCommitUser("new-plan");
    pending.setCommitIdentifier(77);
    new PaimonHealthSupport().savePending(store, pending);
    assertEquals(2, attempts.get());
    assertEquals(100, persisted.getLatest().getProcessId());
    assertEquals(101, persisted.getPending().getProcessId());
    assertEquals("new-plan", persisted.getPending().getCommitUser());
    assertEquals(77, persisted.getPending().getCommitIdentifier());
  }

  @Test
  void comparisonStateConflictRetriesAreBounded() {
    org.apache.amoro.table.TableRuntimeStore store =
        org.mockito.Mockito.mock(org.apache.amoro.table.TableRuntimeStore.class);
    org.apache.amoro.table.TableRuntimeStore.TableRuntimeOperation operation =
        org.mockito.Mockito.mock(
            org.apache.amoro.table.TableRuntimeStore.TableRuntimeOperation.class);
    org.mockito.Mockito.when(store.begin()).thenReturn(operation);
    org.mockito.Mockito.when(
            operation.updateState(
                org.mockito.Mockito.eq(PaimonHealthComparisonState.KEY), org.mockito.Mockito.any()))
        .thenReturn(operation);
    org.mockito.Mockito.doThrow(
            new org.apache.amoro.exception.RuntimeStateConflictException(
                1, PaimonHealthComparisonState.KEY.getKey()))
        .when(operation)
        .commit();
    org.junit.jupiter.api.Assertions.assertThrows(
        org.apache.amoro.exception.RuntimeStateConflictException.class,
        () -> new PaimonHealthSupport().savePending(store, new Entry()));
    org.mockito.Mockito.verify(operation, org.mockito.Mockito.times(3)).commit();
  }

  @Test
  void restoresJsonProducedByThePreviousAmsOwnedState() {
    // Literal generated with the state class from commit 69995123a before moving its package.
    String json =
        "{\"version\":1,\"pending\":{\"processId\":100,\"inputSnapshotId\":17,\"schemaId\":1,\"commitUser\":\"old-ams\",\"commitIdentifier\":77,\"optimizingType\":\"MAJOR\",\"status\":\"READY\",\"reason\":\"OBSERVED_STRUCTURE\",\"concurrentChanges\":false,\"beforeScore\":49,\"afterScore\":100,\"before\":null,\"after\":null,\"completedAtMillis\":1000,\"attempts\":1,\"retryAtMillis\":0},\"latest\":{\"processId\":100,\"inputSnapshotId\":17,\"schemaId\":1,\"commitUser\":\"old-ams\",\"commitIdentifier\":77,\"optimizingType\":\"MAJOR\",\"status\":\"READY\",\"reason\":\"OBSERVED_STRUCTURE\",\"concurrentChanges\":false,\"beforeScore\":49,\"afterScore\":100,\"before\":null,\"after\":null,\"completedAtMillis\":1000,\"attempts\":1,\"retryAtMillis\":0}}";
    PaimonHealthComparisonState state = PaimonHealthComparisonState.KEY.deserialize(json);
    assertEquals(1, state.getVersion());
    assertEquals(100, state.getPending().getProcessId());
    assertEquals("old-ams", state.getPending().getCommitUser());
    assertEquals(77, state.getLatest().getCommitIdentifier());
    assertEquals(17, state.getLatest().getInputSnapshotId());
    assertEquals(1, state.getLatest().getSchemaId());
    assertEquals(json, PaimonHealthComparisonState.KEY.serialize(state));
    org.apache.amoro.table.TableRuntimeStore store =
        org.mockito.Mockito.mock(org.apache.amoro.table.TableRuntimeStore.class);
    org.mockito.Mockito.when(store.getState(PaimonHealthComparisonState.KEY)).thenReturn(state);
    org.apache.amoro.table.descriptor.TableHealthComparisonView view =
        new PaimonHealthSupport().comparisonView(store);
    assertEquals("READY", view.getStatus());
    assertEquals(49, view.getBeforeScore());
    assertEquals(100, view.getAfterScore());
    assertEquals("100", view.getProcessId());
  }

  @Test
  void olderCompletionsCannotReplaceNewerOrTerminalResults() {
    org.apache.amoro.table.TableRuntimeStore store =
        org.mockito.Mockito.mock(org.apache.amoro.table.TableRuntimeStore.class);
    org.apache.amoro.table.TableRuntimeStore.TableRuntimeOperation operation =
        org.mockito.Mockito.mock(
            org.apache.amoro.table.TableRuntimeStore.TableRuntimeOperation.class);
    PaimonHealthComparisonState state = new PaimonHealthComparisonState();
    org.mockito.Mockito.when(store.begin()).thenReturn(operation);
    org.mockito.Mockito.when(
            operation.updateState(
                org.mockito.Mockito.eq(PaimonHealthComparisonState.KEY), org.mockito.Mockito.any()))
        .thenAnswer(
            call -> {
              java.util.function.Function<PaimonHealthComparisonState, PaimonHealthComparisonState>
                  updater = call.getArgument(1);
              updater.apply(state);
              return operation;
            });
    PaimonHealthSupport support = new PaimonHealthSupport();
    Entry latest = new Entry();
    latest.setProcessId(102);
    latest.setStatus("UNAVAILABLE");
    support.saveLatest(store, latest);
    Entry older = new Entry();
    older.setProcessId(100);
    older.setStatus("READY");
    support.saveLatest(store, older);
    assertEquals(102, state.getLatest().getProcessId());
    older.setProcessId(102);
    older.setStatus("PREPARING");
    support.saveLatest(store, older);
    assertEquals("UNAVAILABLE", state.getLatest().getStatus());
  }

  @Test
  void exactSnapshotFactsAreReusedAndExpiredInputIsExplicit() throws Exception {
    try (org.apache.paimon.catalog.Catalog catalog =
        org.apache.amoro.formats.paimon.PaimonCatalogFactory.paimonCatalog(
            Collections.singletonMap("warehouse", warehouse.toUri().toString()),
            new org.apache.hadoop.conf.Configuration())) {
      catalog.createDatabase("d", false);
      org.apache.paimon.catalog.Identifier id =
          org.apache.paimon.catalog.Identifier.create("d", "t");
      catalog.createTable(
          id,
          org.apache.paimon.schema.Schema.newBuilder()
              .column("id", org.apache.paimon.types.DataTypes.INT())
              .option("write-only", "true")
              .build(),
          false);
      org.apache.paimon.table.FileStoreTable table =
          (org.apache.paimon.table.FileStoreTable) catalog.getTable(id);
      for (int i = 0; i < 2; i++) {
        org.apache.paimon.table.sink.BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (org.apache.paimon.table.sink.BatchTableWrite write = builder.newWrite();
            org.apache.paimon.table.sink.BatchTableCommit commit = builder.newCommit()) {
          write.write(org.apache.paimon.data.GenericRow.of(i));
          commit.commit(write.prepareCommit());
        }
      }
      org.apache.amoro.optimizing.FormatTableAnalysis before =
          org.apache.amoro.formats.paimon.optimizing.health.PaimonHistoricalHealth.evaluate(
              table, "t", 1);
      org.apache.amoro.optimizing.FormatTableAnalysis after =
          org.apache.amoro.formats.paimon.optimizing.health.PaimonHistoricalHealth.evaluate(
              table, "t", 2);
      org.apache.paimon.Snapshot committed = table.snapshotManager().snapshot(2);
      Entry entry = new Entry();
      entry.setInputSnapshotId(1);
      entry.setSchemaId(committed.schemaId());
      entry.setCommitUser(committed.commitUser());
      entry.setCommitIdentifier(committed.commitIdentifier());
      entry.setBefore(before.healthDetails());
      entry.setBeforeScore(before.pendingInput().getHealthScore());
      PaimonHealthSupport.build(
          entry, table, "t", after.healthDetails(), after.pendingInput().getHealthScore());
      assertEquals("READY", entry.getStatus());
      assertSame(before.healthDetails(), entry.getBefore());
      assertSame(after.healthDetails(), entry.getAfter());
      entry.setSchemaId(999);
      PaimonHealthSupport.build(entry, table, "t");
      assertEquals("CONFIGURATION_CHANGED", entry.getReason());
      entry.setSchemaId(committed.schemaId());
      table.fileIO().delete(table.snapshotManager().snapshotPath(1), false);
      PaimonHealthSupport.build(entry, table, "t");
      assertEquals("SNAPSHOT_EXPIRED", entry.getReason());
    }
  }
}
