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
import org.apache.amoro.process.HttpRemoteSparkStandAloneSubmit;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.cleanup.TableRuntimeCleanupState;
import org.apache.paimon.FileStore;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestPaimonExpireSnapshotProcess {

  private static final Pattern OLDER_THAN_PATTERN = Pattern.compile("older_than => '([^']+)'");

  @Test
  public void testParseDurationUsesPaimonTimeUtilsSemantics() {
    Assert.assertEquals(Duration.ofMillis(6), TimeUtils.parseDuration("6"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseDurationRejectsIso8601Format() {
    TimeUtils.parseDuration("PT1H");
  }

  @Test
  public void testTriggerCreatesProcessWhenSnapshotCountExceedsRetainMax() throws IOException {
    Optional<PaimonExpireSnapshotProcess> process = triggerWithSnapshotCount(11, "10");

    Assert.assertTrue(process.isPresent());
  }

  @Test
  public void testTriggerSkipsWhenSnapshotCountEqualsRetainMax() throws IOException {
    Optional<PaimonExpireSnapshotProcess> process = triggerWithSnapshotCount(10, "10");

    Assert.assertFalse(process.isPresent());
  }

  @Test
  public void testTriggerUsesDefaultRetainMaxWhenPropertyIsMissing() throws IOException {
    Optional<PaimonExpireSnapshotProcess> process = triggerWithSnapshotCount(11, null);

    Assert.assertTrue(process.isPresent());
  }

  @Test
  public void testTriggerUsesDefaultRetainMaxWhenPropertyIsInvalid() throws IOException {
    Optional<PaimonExpireSnapshotProcess> process = triggerWithSnapshotCount(10, "invalid");

    Assert.assertFalse(process.isPresent());
  }

  @Test
  public void testTriggerSkipsWhenOriginalTableIsNotFileStoreTable() {
    TableRuntime runtime = runtimeWithCleanupState();
    AmoroTable<?> amoroTable = Mockito.mock(AmoroTable.class);
    Table paimonTable = Mockito.mock(Table.class);
    Mockito.when(((AmoroTable) amoroTable).originalTable()).thenReturn(paimonTable);
    Mockito.doReturn(amoroTable).when(runtime).loadTable();

    Optional<PaimonExpireSnapshotProcess> process =
        PaimonExpireSnapshotProcess.trigger(runtime, openedEngine(), 354, Duration.ZERO);

    Assert.assertFalse(process.isPresent());
  }

  @Test
  public void testTriggerSkipsWhenSnapshotMetadataCannotBeRead() throws IOException {
    TableRuntime runtime = runtimeWithCleanupState();
    AmoroTable<?> amoroTable = Mockito.mock(AmoroTable.class);
    FileStoreTable fileStoreTable = Mockito.mock(FileStoreTable.class);
    FileStore<?> fileStore = Mockito.mock(FileStore.class);
    SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
    Mockito.when(((AmoroTable) amoroTable).originalTable()).thenReturn(fileStoreTable);
    Mockito.doReturn(fileStore).when(fileStoreTable).store();
    Mockito.when(fileStore.snapshotManager()).thenReturn(snapshotManager);
    Mockito.when(snapshotManager.snapshotCount())
        .thenThrow(new IOException("metadata unavailable"));
    Mockito.doReturn(amoroTable).when(runtime).loadTable();

    Optional<PaimonExpireSnapshotProcess> process =
        PaimonExpireSnapshotProcess.trigger(runtime, openedEngine(), 354, Duration.ZERO);

    Assert.assertFalse(process.isPresent());
  }

  @Test
  public void testTriggerDoesNotLoadTableWhenIntervalIsNotReached() {
    TableRuntime runtime = runtimeWithCleanupState();
    TableRuntimeCleanupState cleanupState = new TableRuntimeCleanupState();
    cleanupState.setLastSnapshotsExpiringTime(System.currentTimeMillis());
    Mockito.when(runtime.getState(DefaultTableRuntime.CLEANUP_STATE_KEY)).thenReturn(cleanupState);

    Optional<PaimonExpireSnapshotProcess> process =
        PaimonExpireSnapshotProcess.trigger(runtime, openedEngine(), 354, Duration.ofHours(1));

    Assert.assertFalse(process.isPresent());
    Mockito.verify(runtime, Mockito.never()).loadTable();
  }

  @Test
  public void testGetProcessParametersUseExecuteUser() {
    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    Map<String, String> engineProps = new HashMap<>();
    engineProps.put("execute.user", "sl_real_time_merger");
    engine.open(engineProps);

    TableRuntime runtime = Mockito.mock(TableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));
    Mockito.when(runtime.getTableConfig()).thenReturn(Collections.emptyMap());
    Mockito.when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);

    PaimonExpireSnapshotProcess process = new PaimonExpireSnapshotProcess(runtime, engine, 354);

    Map<String, String> params = process.getProcessParameters();
    Assert.assertEquals("sl_real_time_merger", params.get("curUser"));
    Assert.assertEquals("sl_real_time_merger", params.get("logUser"));
    Assert.assertEquals("AMORO", params.get("sourceTag"));
    Assert.assertEquals(
        "{\"sparkVersion\":\"354\",\"paimon.version\":\"1.3\"}", params.get("conf"));
  }

  @Test
  public void testActionNameUsesPluralExpireSnapshots() {
    TableRuntime runtime = Mockito.mock(TableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    PaimonExpireSnapshotProcess process = new PaimonExpireSnapshotProcess(runtime, engine, 354);

    Assert.assertEquals("EXPIRE-SNAPSHOTS", process.getAction().getName());
  }

  @Test
  public void testBuildExpireSnapshotsSqlUsesDefaultSnapshotOptions() {
    TableRuntime runtime = Mockito.mock(TableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));

    Map<String, String> tableConfig = new HashMap<>();
    tableConfig.put("snapshot.time-retained", "2 h");
    tableConfig.put("snapshot.num-retained.max", "12");
    Mockito.when(runtime.getTableConfig()).thenReturn(tableConfig);

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    PaimonExpireSnapshotProcess process = new PaimonExpireSnapshotProcess(runtime, engine, 354);
    long beforeBuild = System.currentTimeMillis();
    String sql = process.buildExpireSnapshotsSql();
    long afterBuild = System.currentTimeMillis();

    Assert.assertTrue(sql.contains("CALL sys.expire_snapshots"));
    Assert.assertTrue(sql.contains("table => 'default.orders'"));
    Assert.assertTrue(sql.contains("retain_max => 12"));
    Matcher olderThanMatcher = OLDER_THAN_PATTERN.matcher(sql);
    Assert.assertTrue(olderThanMatcher.find());
    long olderThanTimestampMillis =
        DateTimeUtils.parseTimestampData(olderThanMatcher.group(1), 3, TimeZone.getDefault())
            .getMillisecond();
    Assert.assertTrue(olderThanTimestampMillis >= beforeBuild - Duration.ofHours(2).toMillis());
    Assert.assertTrue(olderThanTimestampMillis <= afterBuild - Duration.ofHours(2).toMillis());
    Assert.assertTrue(
        sql.contains(
            "options => 'snapshot.expire.limit=500,snapshot.expire.execution-mode=async,"
                + "snapshot.ignore-empty-commit=true,snapshot.clean-empty-directories=true'"));
    Assert.assertFalse(sql.contains("max_deletes =>"));
  }

  @Test
  public void testBuildExpireSnapshotsSqlUsesTableSnapshotOptions() {
    TableRuntime runtime = Mockito.mock(TableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));

    Map<String, String> tableConfig = new HashMap<>();
    tableConfig.put("snapshot.time-retained", "2 h");
    tableConfig.put("snapshot.num-retained.max", "12");
    tableConfig.put("snapshot.expire.limit", "123");
    tableConfig.put("snapshot.expire.execution-mode", "sync");
    tableConfig.put("snapshot.ignore-empty-commit", "false");
    tableConfig.put("snapshot.clean-empty-directories", "false");
    Mockito.when(runtime.getTableConfig()).thenReturn(tableConfig);

    PaimonExpireSnapshotProcess process =
        new PaimonExpireSnapshotProcess(runtime, openedEngine(), 354);
    String sql = process.buildExpireSnapshotsSql();

    Assert.assertTrue(
        sql.contains(
            "options => 'snapshot.expire.limit=123,snapshot.expire.execution-mode=sync,"
                + "snapshot.ignore-empty-commit=false,snapshot.clean-empty-directories=false'"));
    Assert.assertFalse(sql.contains("max_deletes =>"));
  }

  @Test
  public void testAfterCompleteSuccessUpdatesLastCleanTime() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    PaimonExpireSnapshotProcess process = new PaimonExpireSnapshotProcess(runtime, engine, 354);
    process.afterComplete(ProcessStatus.SUCCESS);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Function<TableRuntimeCleanupState, TableRuntimeCleanupState>> updaterCaptor =
        (ArgumentCaptor) ArgumentCaptor.forClass(Function.class);
    Mockito.verify(runtime)
        .updateState(Mockito.eq(DefaultTableRuntime.CLEANUP_STATE_KEY), updaterCaptor.capture());

    TableRuntimeCleanupState updated =
        updaterCaptor.getValue().apply(new TableRuntimeCleanupState());
    Assert.assertTrue(updated.getLastSnapshotsExpiringTime() > 0);
  }

  @Test
  public void testAfterCompleteFailureDoesNotUpdateLastCleanTime() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    PaimonExpireSnapshotProcess process = new PaimonExpireSnapshotProcess(runtime, engine, 354);
    process.afterComplete(ProcessStatus.FAILED);

    Mockito.verify(runtime, Mockito.never())
        .updateState(Mockito.eq(DefaultTableRuntime.CLEANUP_STATE_KEY), Mockito.any());
  }

  private Optional<PaimonExpireSnapshotProcess> triggerWithSnapshotCount(
      long snapshotCount, String retainMax) throws IOException {
    TableRuntime runtime = runtimeWithCleanupState();
    Map<String, String> tableConfig = new HashMap<>();
    if (retainMax != null) {
      tableConfig.put("snapshot.num-retained.max", retainMax);
    }
    Mockito.when(runtime.getTableConfig()).thenReturn(tableConfig);

    AmoroTable<?> amoroTable = Mockito.mock(AmoroTable.class);
    FileStoreTable fileStoreTable = Mockito.mock(FileStoreTable.class);
    FileStore<?> fileStore = Mockito.mock(FileStore.class);
    SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
    Mockito.when(((AmoroTable) amoroTable).originalTable()).thenReturn(fileStoreTable);
    Mockito.doReturn(fileStore).when(fileStoreTable).store();
    Mockito.when(fileStore.snapshotManager()).thenReturn(snapshotManager);
    Mockito.when(snapshotManager.snapshotCount()).thenReturn(snapshotCount);
    Mockito.doReturn(amoroTable).when(runtime).loadTable();

    return PaimonExpireSnapshotProcess.trigger(runtime, openedEngine(), 354, Duration.ZERO);
  }

  private TableRuntime runtimeWithCleanupState() {
    TableRuntime runtime = Mockito.mock(TableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));
    Mockito.when(runtime.getState(DefaultTableRuntime.CLEANUP_STATE_KEY))
        .thenReturn(new TableRuntimeCleanupState());
    return runtime;
  }

  private HttpRemoteSparkStandAloneSubmit openedEngine() {
    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));
    return engine;
  }
}
