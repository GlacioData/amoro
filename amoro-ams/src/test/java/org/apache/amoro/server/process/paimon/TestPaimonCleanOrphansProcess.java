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

package org.apache.amoro.server.process.paimon;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.TableSnapshot;
import org.apache.amoro.process.HttpRemoteSparkStandAloneSubmit;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.cleanup.TableRuntimeCleanupState;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class TestPaimonCleanOrphansProcess {

  private static final Duration CLEAN_ORPHANS_INTERVAL = Duration.ofHours(48);
  private static final Duration STATIC_TABLE_THRESHOLD = Duration.ofDays(6);

  @Test
  public void testGetProcessParametersUseExecuteUser() {
    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    Map<String, String> engineProps = new HashMap<>();
    engineProps.put("execute.user", "sl_real_time_merger");
    engine.open(engineProps);

    TableRuntime runtime = Mockito.mock(TableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));
    Mockito.when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);

    PaimonCleanOrphansProcess process = new PaimonCleanOrphansProcess(runtime, engine, 354);

    Map<String, String> params = process.getProcessParameters();
    Assert.assertEquals("sl_real_time_merger", params.get("curUser"));
    Assert.assertEquals("sl_real_time_merger", params.get("logUser"));
    Assert.assertEquals("AMORO", params.get("sourceTag"));
    Assert.assertEquals("354", params.get("sparkVersion"));
    Assert.assertEquals(
        "{\"sparkVersion\":\"354\",\"paimon.version\":\"1.3\"}", params.get("conf"));
  }

  @Test
  public void testActionNameUsesCleanOrphans() {
    TableRuntime runtime = Mockito.mock(TableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    PaimonCleanOrphansProcess process = new PaimonCleanOrphansProcess(runtime, engine, 354);

    Assert.assertEquals("CLEAN-ORPHANS", process.getAction().getName());
  }

  @Test
  public void testBuildCleanOrphansSqlOnlyContainsTable() {
    TableRuntime runtime = Mockito.mock(TableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    PaimonCleanOrphansProcess process = new PaimonCleanOrphansProcess(runtime, engine, 354);
    String sql = process.buildCleanOrphansSql();

    Assert.assertEquals("CALL sys.remove_orphan_files(table => 'default.orders')", sql);
    Assert.assertFalse(sql.contains("older_than"));
    Assert.assertFalse(sql.contains("dry_run"));
    Assert.assertFalse(sql.contains("parallelism"));
    Assert.assertFalse(sql.contains("mode"));
  }

  @Test
  public void testAfterCompleteSuccessUpdatesLastCleanTime() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    PaimonCleanOrphansProcess process = new PaimonCleanOrphansProcess(runtime, engine, 354);
    long triggerTime = Long.parseLong(process.getSummary().get("clean-orphans-trigger-time"));
    process.afterComplete(ProcessStatus.SUCCESS);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<TableRuntimeCleanupState, TableRuntimeCleanupState>> updaterCaptor =
        (ArgumentCaptor) ArgumentCaptor.forClass(Function.class);
    Mockito.verify(runtime)
        .updateState(Mockito.eq(DefaultTableRuntime.CLEANUP_STATE_KEY), updaterCaptor.capture());

    TableRuntimeCleanupState updated =
        updaterCaptor.getValue().apply(new TableRuntimeCleanupState());
    Assert.assertEquals(triggerTime, updated.getLastOrphanFilesCleanTime());
  }

  @Test
  public void testAfterCompleteFailureDoesNotUpdateLastCleanTime() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));

    PaimonCleanOrphansProcess process = new PaimonCleanOrphansProcess(runtime, openedEngine(), 354);
    process.afterComplete(ProcessStatus.FAILED);

    Mockito.verify(runtime, Mockito.never())
        .updateState(Mockito.eq(DefaultTableRuntime.CLEANUP_STATE_KEY), Mockito.any());
  }

  @Test
  public void testTriggerSkipsWithinInterval() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));
    Mockito.when(runtime.getState(DefaultTableRuntime.CLEANUP_STATE_KEY))
        .thenReturn(
            new TableRuntimeCleanupState().setLastOrphanFilesCleanTime(System.currentTimeMillis()));

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    Optional<PaimonCleanOrphansProcess> process =
        PaimonCleanOrphansProcess.trigger(
            runtime, engine, 354, CLEAN_ORPHANS_INTERVAL, STATIC_TABLE_THRESHOLD);

    Assert.assertFalse(process.isPresent());
    Mockito.verify(runtime, Mockito.never()).loadTable();
  }

  @Test
  public void testTriggerSkipsStaticTableAndRecordsCheckTime() {
    long beforeTrigger = System.currentTimeMillis();
    DefaultTableRuntime runtime =
        runtimeWithSnapshot(beforeTrigger - STATIC_TABLE_THRESHOLD.toMillis() - 1, 0L);

    Optional<PaimonCleanOrphansProcess> process = trigger(runtime);

    Assert.assertFalse(process.isPresent());
    Assert.assertTrue(updatedLastCleanTime(runtime) >= beforeTrigger);
  }

  @Test
  public void testTriggerRefreshesStaticTableCheckTime() {
    long beforeTrigger = System.currentTimeMillis();
    DefaultTableRuntime runtime =
        runtimeWithSnapshot(
            beforeTrigger - STATIC_TABLE_THRESHOLD.toMillis() - 1,
            beforeTrigger - CLEAN_ORPHANS_INTERVAL.toMillis() - 1);

    Optional<PaimonCleanOrphansProcess> process = trigger(runtime);

    Assert.assertFalse(process.isPresent());
    Assert.assertTrue(updatedLastCleanTime(runtime) >= beforeTrigger);
  }

  @Test
  public void testTriggerSkipsWhenNoNewSnapshotSinceLastCleanOrStaticCheck() {
    long now = System.currentTimeMillis();
    DefaultTableRuntime runtime =
        runtimeWithSnapshot(
            now - Duration.ofDays(3).toMillis(), now - CLEAN_ORPHANS_INTERVAL.toMillis() - 1);

    Optional<PaimonCleanOrphansProcess> process = trigger(runtime);

    Assert.assertFalse(process.isPresent());
    Mockito.verify(runtime, Mockito.never())
        .updateState(Mockito.eq(DefaultTableRuntime.CLEANUP_STATE_KEY), Mockito.any());
  }

  @Test
  public void testTriggerCreatesProcessForNewNonStaticSnapshot() {
    long now = System.currentTimeMillis();
    DefaultTableRuntime runtime =
        runtimeWithSnapshot(
            now - Duration.ofHours(1).toMillis(), now - CLEAN_ORPHANS_INTERVAL.toMillis() - 1);

    Assert.assertTrue(trigger(runtime).isPresent());
  }

  @Test
  public void testTriggerCreatesProcessForSnapshotCommittedAfterPreviousTrigger() {
    DefaultTableRuntime firstRuntime = runtimeWithSnapshot(System.currentTimeMillis(), 0L);
    PaimonCleanOrphansProcess firstProcess = trigger(firstRuntime).get();
    firstProcess.afterComplete(ProcessStatus.SUCCESS);
    long firstTriggerTime = updatedLastCleanTime(firstRuntime);

    DefaultTableRuntime nextRuntime = runtimeWithSnapshot(firstTriggerTime + 1, firstTriggerTime);
    Optional<PaimonCleanOrphansProcess> nextProcess =
        PaimonCleanOrphansProcess.trigger(
            nextRuntime, openedEngine(), 354, Duration.ZERO, STATIC_TABLE_THRESHOLD);

    Assert.assertTrue(nextProcess.isPresent());
  }

  @Test
  public void testTriggerSkipsNewSnapshotWhenItHasBecomeStatic() {
    long beforeTrigger = System.currentTimeMillis();
    DefaultTableRuntime runtime =
        runtimeWithSnapshot(
            beforeTrigger - Duration.ofDays(7).toMillis(),
            beforeTrigger - Duration.ofDays(10).toMillis());

    Optional<PaimonCleanOrphansProcess> process = trigger(runtime);

    Assert.assertFalse(process.isPresent());
    Assert.assertTrue(updatedLastCleanTime(runtime) >= beforeTrigger);
  }

  @Test
  public void testTriggerFailsWhenNoSnapshotExists() {
    DefaultTableRuntime runtime = runtimeWithSnapshot(null, 0L);

    try {
      trigger(runtime);
      Assert.fail("Expected clean orphans trigger to fail when no snapshot exists");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("no Paimon snapshot exists"));
      Assert.assertTrue(e.getMessage().contains("orders"));
    }
  }

  @Test
  public void testTriggerFailsWhenSnapshotCannotBeRead() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));
    Mockito.when(runtime.getState(DefaultTableRuntime.CLEANUP_STATE_KEY))
        .thenReturn(new TableRuntimeCleanupState());
    AmoroTable<?> amoroTable = Mockito.mock(AmoroTable.class);
    Mockito.when(((AmoroTable) amoroTable).currentSnapshot())
        .thenThrow(new RuntimeException("metadata unavailable"));
    Mockito.doReturn(amoroTable).when(runtime).loadTable();

    try {
      trigger(runtime);
      Assert.fail("Expected clean orphans trigger to fail when snapshot cannot be read");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot read latest Paimon snapshot"));
      Assert.assertTrue(e.getMessage().contains("orders"));
      Assert.assertEquals("metadata unavailable", e.getCause().getMessage());
    }
  }

  private Optional<PaimonCleanOrphansProcess> trigger(DefaultTableRuntime runtime) {
    return PaimonCleanOrphansProcess.trigger(
        runtime, openedEngine(), 354, CLEAN_ORPHANS_INTERVAL, STATIC_TABLE_THRESHOLD);
  }

  private DefaultTableRuntime runtimeWithSnapshot(Long snapshotCommitTime, long lastCleanTime) {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));
    Mockito.when(runtime.getState(DefaultTableRuntime.CLEANUP_STATE_KEY))
        .thenReturn(new TableRuntimeCleanupState().setLastOrphanFilesCleanTime(lastCleanTime));
    AmoroTable<?> amoroTable = Mockito.mock(AmoroTable.class);
    if (snapshotCommitTime != null) {
      TableSnapshot snapshot = Mockito.mock(TableSnapshot.class);
      Mockito.when(snapshot.commitTime()).thenReturn(snapshotCommitTime);
      Mockito.when(((AmoroTable) amoroTable).currentSnapshot()).thenReturn(snapshot);
    }
    Mockito.doReturn(amoroTable).when(runtime).loadTable();
    return runtime;
  }

  private HttpRemoteSparkStandAloneSubmit openedEngine() {
    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));
    return engine;
  }

  private long updatedLastCleanTime(DefaultTableRuntime runtime) {
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<TableRuntimeCleanupState, TableRuntimeCleanupState>> updaterCaptor =
        (ArgumentCaptor) ArgumentCaptor.forClass(Function.class);
    Mockito.verify(runtime)
        .updateState(Mockito.eq(DefaultTableRuntime.CLEANUP_STATE_KEY), updaterCaptor.capture());
    return updaterCaptor
        .getValue()
        .apply(new TableRuntimeCleanupState())
        .getLastOrphanFilesCleanTime();
  }
}
