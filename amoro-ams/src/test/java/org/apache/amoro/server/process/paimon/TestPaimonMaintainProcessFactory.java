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

import org.apache.amoro.PaimonActions;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.process.HttpRemoteSparkStandAloneSubmit;
import org.apache.amoro.process.LocalExecutionEngine;
import org.apache.amoro.process.ProcessTriggerStrategy;
import org.apache.amoro.process.TableProcess;
import org.apache.amoro.process.TableProcessStore;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TestPaimonMaintainProcessFactory {

  @Test
  public void testSupportedActionAndTriggerStrategy() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "true");
    properties.put("sync-table-meta.interval", "2 h");
    properties.put("sync-table-meta.trigger-parallelism", "3");
    factory.open(properties);

    Assert.assertTrue(
        factory.supportedActions().getOrDefault(TableFormat.PAIMON, Collections.emptySet()).stream()
            .anyMatch(action -> action.equals(PaimonActions.SYNC_TABLE_META)));

    ProcessTriggerStrategy strategy =
        factory.triggerStrategy(TableFormat.PAIMON, PaimonActions.SYNC_TABLE_META);
    Assert.assertEquals(3, strategy.getTriggerParallelism());
    Assert.assertEquals(2 * 60 * 60 * 1000L, strategy.getTriggerInterval().toMillis());
  }

  @Test
  public void testExpireSnapshotsActionAndTriggerStrategy() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "false");
    properties.put("expire-snapshots.enabled", "true");
    properties.put("expire-snapshots.interval", "24h");
    properties.put("spark-version", "321");
    factory.open(properties);

    Assert.assertTrue(
        factory.supportedActions().getOrDefault(TableFormat.PAIMON, Collections.emptySet()).stream()
            .anyMatch(action -> action.equals(PaimonActions.EXPIRE_SNAPSHOTS)));

    ProcessTriggerStrategy strategy =
        factory.triggerStrategy(TableFormat.PAIMON, PaimonActions.EXPIRE_SNAPSHOTS);
    Assert.assertEquals(Duration.ofHours(24), strategy.getTriggerInterval());
  }

  @Test
  public void testTriggerAndRecoverUseLocalEngine() throws Exception {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    factory.open(Collections.singletonMap("sync-table-meta.enabled", "true"));

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);

    Optional<TableProcess> process = factory.trigger(runtime, PaimonActions.SYNC_TABLE_META);
    Assert.assertTrue(process.isPresent());
    Assert.assertEquals(LocalExecutionEngine.ENGINE_NAME, process.get().getExecutionEngine());
  }

  @Test
  public void testTriggerExpireSnapshotsUseHttpSparkEngine() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "false");
    properties.put("expire-snapshots.enabled", "true");
    properties.put("expire-snapshots.interval", "24h");
    properties.put("spark-version", "321");
    factory.open(properties);

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));
    factory.availableExecuteEngines(Collections.singletonList(engine));

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));
    Mockito.when(runtime.getTableConfig()).thenReturn(Collections.emptyMap());
    Mockito.when(runtime.getLastCleanTime(Mockito.any())).thenReturn(0L);

    Optional<TableProcess> process = factory.trigger(runtime, PaimonActions.EXPIRE_SNAPSHOTS);

    Assert.assertTrue(process.isPresent());
    Assert.assertTrue(process.get() instanceof PaimonExpireSnapshotProcess);
    Assert.assertEquals(
        HttpRemoteSparkStandAloneSubmit.ENGINE_NAME, process.get().getExecutionEngine());
    Assert.assertEquals("321", process.get().getProcessParameters().get("sparkVersion"));
  }

  @Test
  public void testTriggerExpireSnapshotsWithoutHttpSparkEngineReturnsEmpty() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "false");
    properties.put("expire-snapshots.enabled", "true");
    factory.open(properties);
    factory.availableExecuteEngines(Collections.singletonList(new LocalExecutionEngine()));

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);

    Optional<TableProcess> process = factory.trigger(runtime, PaimonActions.EXPIRE_SNAPSHOTS);

    Assert.assertFalse(process.isPresent());
  }

  @Test
  public void testRecoverExpireSnapshotsUseHttpSparkEngine() throws Exception {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "false");
    properties.put("expire-snapshots.enabled", "true");
    properties.put("spark-version", "321");
    factory.open(properties);

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));
    factory.availableExecuteEngines(Collections.singletonList(engine));

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));
    Mockito.when(runtime.getTableConfig()).thenReturn(Collections.emptyMap());
    TableProcessStore store = Mockito.mock(TableProcessStore.class);
    Mockito.when(store.getAction()).thenReturn(PaimonActions.EXPIRE_SNAPSHOTS);

    TableProcess process = factory.recover(runtime, store);

    Assert.assertTrue(process instanceof PaimonExpireSnapshotProcess);
    Assert.assertEquals(HttpRemoteSparkStandAloneSubmit.ENGINE_NAME, process.getExecutionEngine());
    Assert.assertEquals("321", process.getProcessParameters().get("sparkVersion"));
  }

  @Test
  public void testOpenWithEmptyPropertiesUseDefaults() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    factory.open(Collections.emptyMap());

    Set<org.apache.amoro.Action> actions =
        factory.supportedActions().getOrDefault(TableFormat.PAIMON, Collections.emptySet());
    Assert.assertTrue(actions.contains(PaimonActions.SYNC_TABLE_META));
    Assert.assertTrue(actions.contains(PaimonActions.CLEAN_ORPHANS));
    Assert.assertFalse(actions.contains(PaimonActions.EXPIRE_SNAPSHOTS));

    ProcessTriggerStrategy syncStrategy =
        factory.triggerStrategy(TableFormat.PAIMON, PaimonActions.SYNC_TABLE_META);
    Assert.assertEquals(1, syncStrategy.getTriggerParallelism());
    Assert.assertEquals(60 * 60 * 1000L, syncStrategy.getTriggerInterval().toMillis());

    ProcessTriggerStrategy cleanOrphansStrategy =
        factory.triggerStrategy(TableFormat.PAIMON, PaimonActions.CLEAN_ORPHANS);
    Assert.assertEquals(Duration.ofHours(48), cleanOrphansStrategy.getTriggerInterval());
  }

  @Test
  public void testTriggerCleanOrphansUseHttpSparkEngine() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "false");
    properties.put("clean-orphans.enabled", "true");
    properties.put("clean-orphans.interval", "48h");
    properties.put("spark-version", "321");
    factory.open(properties);

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));
    factory.availableExecuteEngines(Collections.singletonList(engine));

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));
    Mockito.when(runtime.getLastCleanTime(Mockito.any())).thenReturn(0L);

    Optional<TableProcess> process = factory.trigger(runtime, PaimonActions.CLEAN_ORPHANS);

    Assert.assertTrue(process.isPresent());
    Assert.assertTrue(process.get() instanceof PaimonCleanOrphansProcess);
    Assert.assertEquals(
        HttpRemoteSparkStandAloneSubmit.ENGINE_NAME, process.get().getExecutionEngine());
    Assert.assertEquals("321", process.get().getProcessParameters().get("sparkVersion"));
  }

  @Test
  public void testTriggerCleanOrphansWithoutHttpSparkEngineReturnsEmpty() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "false");
    properties.put("clean-orphans.enabled", "true");
    factory.open(properties);
    factory.availableExecuteEngines(Collections.singletonList(new LocalExecutionEngine()));

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);

    Optional<TableProcess> process = factory.trigger(runtime, PaimonActions.CLEAN_ORPHANS);

    Assert.assertFalse(process.isPresent());
  }

  @Test
  public void testRecoverCleanOrphansUseHttpSparkEngine() throws Exception {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "false");
    properties.put("clean-orphans.enabled", "true");
    properties.put("spark-version", "321");
    factory.open(properties);

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));
    factory.availableExecuteEngines(Collections.singletonList(engine));

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "db", "tbl", TableFormat.PAIMON));
    TableProcessStore store = Mockito.mock(TableProcessStore.class);
    Mockito.when(store.getAction()).thenReturn(PaimonActions.CLEAN_ORPHANS);

    TableProcess process = factory.recover(runtime, store);

    Assert.assertTrue(process instanceof PaimonCleanOrphansProcess);
    Assert.assertEquals(HttpRemoteSparkStandAloneSubmit.ENGINE_NAME, process.getExecutionEngine());
    Assert.assertEquals("321", process.getProcessParameters().get("sparkVersion"));
  }

  @Test
  public void testRecoverCleanOrphansWithoutHttpSparkEngineFails() throws Exception {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("sync-table-meta.enabled", "false");
    properties.put("clean-orphans.enabled", "true");
    factory.open(properties);

    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    TableProcessStore store = Mockito.mock(TableProcessStore.class);
    Mockito.when(store.getAction()).thenReturn(PaimonActions.CLEAN_ORPHANS);

    try {
      factory.recover(runtime, store);
      Assert.fail("Expected recover clean-orphans to fail without sl-spark-http engine");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("clean orphans"));
      Assert.assertTrue(e.getMessage().contains(HttpRemoteSparkStandAloneSubmit.ENGINE_NAME));
    }
  }

  @Test
  public void testDisableCleanOrphansAction() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    Map<String, String> properties = new HashMap<>();
    properties.put("clean-orphans.enabled", "false");
    factory.open(properties);

    Set<org.apache.amoro.Action> actions =
        factory.supportedActions().getOrDefault(TableFormat.PAIMON, Collections.emptySet());
    Assert.assertFalse(actions.contains(PaimonActions.CLEAN_ORPHANS));
  }

  @Test
  public void testOpenShouldResetPreviousActions() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    factory.open(Collections.emptyMap());

    Map<String, String> disabled = new HashMap<>();
    disabled.put("sync-table-meta.enabled", "false");
    factory.open(disabled);

    Set<org.apache.amoro.Action> actions =
        factory.supportedActions().getOrDefault(TableFormat.PAIMON, Collections.emptySet());
    Assert.assertFalse(actions.contains(PaimonActions.SYNC_TABLE_META));
    Assert.assertTrue(actions.contains(PaimonActions.CLEAN_ORPHANS));
    Assert.assertFalse(actions.contains(PaimonActions.EXPIRE_SNAPSHOTS));
  }

  @Test
  public void testCloseShouldClearActions() {
    PaimonMaintainProcessFactory factory = new PaimonMaintainProcessFactory();
    factory.open(Collections.emptyMap());
    factory.close();

    Set<org.apache.amoro.Action> actions =
        factory.supportedActions().getOrDefault(TableFormat.PAIMON, Collections.emptySet());
    Assert.assertTrue(actions.isEmpty());
  }
}
