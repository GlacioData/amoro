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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.process.HttpRemoteSparkStandAloneSubmit;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.cleanup.CleanupOperation;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TestPaimonCleanOrphansProcess {

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
    process.afterComplete(ProcessStatus.SUCCESS);

    Mockito.verify(runtime)
        .updateLastCleanTime(Mockito.eq(CleanupOperation.ORPHAN_FILES_CLEANING), Mockito.anyLong());
  }

  @Test
  public void testTriggerSkipsWithinInterval() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableIdentifier())
        .thenReturn(ServerTableIdentifier.of("catalog", "default", "orders", TableFormat.PAIMON));
    Mockito.when(runtime.getLastCleanTime(CleanupOperation.ORPHAN_FILES_CLEANING))
        .thenReturn(System.currentTimeMillis());

    HttpRemoteSparkStandAloneSubmit engine = new HttpRemoteSparkStandAloneSubmit();
    engine.open(Collections.singletonMap("execute.user", "amoro"));

    Optional<PaimonCleanOrphansProcess> process =
        PaimonCleanOrphansProcess.trigger(runtime, engine, 354, Duration.ofHours(48));

    Assert.assertFalse(process.isPresent());
  }
}
