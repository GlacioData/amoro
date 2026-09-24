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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthComparisonState.Entry;
import org.apache.amoro.table.health.TableFileStatisticsSnapshot;
import org.apache.amoro.table.health.TableHealthDetails;
import org.apache.amoro.table.health.TableHealthStructure;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class TestPaimonHealthSupport {
  @org.junit.jupiter.api.io.TempDir java.nio.file.Path warehouse;

  @Test
  void extractsCompleteFileStatisticsDespitePartialHealthEvaluation() {
    TableHealthDetails details = fileDetails(101L, 0L, "3", "10", "PARTIAL", 1234);
    TableFileStatisticsSnapshot statistics =
        new PaimonHealthSupport().extractFileStatistics(details).get();
    assertEquals(Long.valueOf(101), statistics.getSnapshotId());
    assertEquals(0, statistics.getSchemaId());
    assertEquals(3, statistics.getFileCount());
    assertEquals(10, statistics.getTotalSizeBytes());
    assertEquals(3, statistics.getAverageFileSizeBytes());
    assertEquals(1234, statistics.getEvaluatedAtMillis());
  }

  @Test
  void rejectsMissingStructureAndInvalidFileMetrics() {
    PaimonHealthSupport support = new PaimonHealthSupport();
    assertFalse(support.extractFileStatistics(null).isPresent());
    assertFalse(
        support
            .extractFileStatistics(
                fileDetails(1L, 0L, "1", "10", "COMPLETE", 1234).withStructure(null))
            .isPresent());
    for (String invalid : new String[] {null, "", "N/A", "-1", "1.5", "9223372036854775808"}) {
      assertFalse(
          support
              .extractFileStatistics(fileDetails(1L, 0L, invalid, "10", "COMPLETE", 1234))
              .isPresent(),
          "Invalid file count: " + invalid);
      assertFalse(
          support
              .extractFileStatistics(fileDetails(1L, 0L, "1", invalid, "COMPLETE", 1234))
              .isPresent(),
          "Invalid file size: " + invalid);
    }
    assertFalse(
        support.extractFileStatistics(fileDetails(1L, 0L, "0", "1", "COMPLETE", 1234)).isPresent());
  }

  @Test
  void rejectsUnknownOrInvalidSnapshotSchemaAndEvaluationTime() {
    PaimonHealthSupport support = new PaimonHealthSupport();
    for (Long snapshot : new Long[] {null, 0L, -1L}) {
      assertFalse(
          support
              .extractFileStatistics(fileDetails(snapshot, 0L, "1", "10", "COMPLETE", 1234))
              .isPresent());
    }
    for (Long schema : new Long[] {null, -1L}) {
      assertFalse(
          support
              .extractFileStatistics(fileDetails(1L, schema, "1", "10", "COMPLETE", 1234))
              .isPresent());
    }
    for (long evaluatedAt : new long[] {0, -1}) {
      assertFalse(
          support
              .extractFileStatistics(fileDetails(1L, 0L, "1", "10", "COMPLETE", evaluatedAt))
              .isPresent());
    }
  }

  @Test
  void acceptsNullSnapshotOnlyForCompleteEmptyStatistics() {
    PaimonHealthSupport support = new PaimonHealthSupport();
    TableFileStatisticsSnapshot statistics =
        support.extractFileStatistics(fileDetails(null, 0L, "0", "0", "COMPLETE", 1234)).get();
    assertNull(statistics.getSnapshotId());
    assertEquals(0, statistics.getFileCount());
    assertEquals(0, statistics.getTotalSizeBytes());
    assertEquals(0, statistics.getAverageFileSizeBytes());
    for (String availability : new String[] {null, "PARTIAL", "UNAVAILABLE"}) {
      assertFalse(
          support
              .extractFileStatistics(fileDetails(null, 0L, "0", "0", availability, 1234))
              .isPresent());
    }
    for (Long snapshot : new Long[] {0L, -1L}) {
      assertFalse(
          support
              .extractFileStatistics(fileDetails(snapshot, 0L, "0", "0", "COMPLETE", 1234))
              .isPresent());
    }
    assertFalse(
        support
            .extractFileStatistics(fileDetails(null, null, "0", "0", "COMPLETE", 1234))
            .isPresent());
    assertFalse(
        support.extractFileStatistics(fileDetails(null, 0L, "0", "0", "COMPLETE", 0)).isPresent());
  }

  @Test
  void preservesRawLongStatisticsAndAllowsZeroByteFiles() {
    PaimonHealthSupport support = new PaimonHealthSupport();
    TableFileStatisticsSnapshot large =
        support
            .extractFileStatistics(
                fileDetails(1L, 0L, "9007199254740993", "9223372036854775807", "COMPLETE", 1234))
            .get();
    assertEquals(9007199254740993L, large.getFileCount());
    assertEquals(Long.MAX_VALUE, large.getTotalSizeBytes());
    assertEquals(Long.MAX_VALUE / 9007199254740993L, large.getAverageFileSizeBytes());
    TableFileStatisticsSnapshot zeroBytes =
        support.extractFileStatistics(fileDetails(1L, 0L, "1", "0", "PARTIAL", 1234)).get();
    assertEquals(1, zeroBytes.getFileCount());
    assertEquals(0, zeroBytes.getTotalSizeBytes());
    assertEquals(0, zeroBytes.getAverageFileSizeBytes());
  }

  private static TableHealthDetails fileDetails(
      Long snapshot,
      Long schema,
      String count,
      String bytes,
      String availability,
      long evaluatedAt) {
    Map<String, String> metrics = new HashMap<>();
    if (count != null) {
      metrics.put("totalFileCount", count);
    }
    if (bytes != null) {
      metrics.put("totalFileSize", bytes);
    }
    metrics.put("averageFileSize", "999");
    return new TableHealthDetails(
            "test", snapshot, null, schema, "config", "evaluation", null, metrics, null)
        .withStructure(
            new TableHealthStructure(
                "APPEND", availability, evaluatedAt, 100, "0", "0", "0", null, null));
  }

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
      TableFileStatisticsSnapshot statistics =
          new PaimonHealthSupport().extractFileStatistics(after.healthDetails()).get();
      assertEquals(Long.valueOf(2), statistics.getSnapshotId());
      assertEquals(table.schema().id(), statistics.getSchemaId());
      assertEquals(2, statistics.getFileCount());
      assertEquals(
          Long.parseLong(after.healthDetails().getMetrics().get("totalFileSize")),
          statistics.getTotalSizeBytes());
      assertEquals(statistics.getTotalSizeBytes() / 2, statistics.getAverageFileSizeBytes());
      assertEquals(
          after.healthDetails().getStructure().getEvaluatedAtMillis(),
          statistics.getEvaluatedAtMillis());
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
