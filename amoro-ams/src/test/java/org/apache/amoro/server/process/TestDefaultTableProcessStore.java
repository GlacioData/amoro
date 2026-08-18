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

package org.apache.amoro.server.process;

import org.apache.amoro.Action;
import org.apache.amoro.process.ProcessEvent;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.AMSManagerTestBase;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.utils.SnowflakeIdGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestDefaultTableProcessStore extends AMSManagerTestBase {

  private static final Action TEST_ACTION = Action.register("terminal_summary_test");
  private final Persistency persistency = new Persistency();

  @Test
  public void testCompleteSuccessPersistsSummary() {
    String trackUri = "http://spark.example.com/proxy/application_success/";
    Map<String, String> summary = new HashMap<>();
    summary.put("existingKey", "existingValue");
    summary.put("trackUri", trackUri);

    TableProcessMeta persistedMeta =
        completeAndReload(ProcessStatus.SUCCESS, ProcessEvent.COMPLETE_SUCCESS, "", summary);

    Assert.assertEquals(ProcessStatus.SUCCESS, persistedMeta.getStatus());
    Assert.assertTrue(persistedMeta.getFinishTime() > 0);
    Assert.assertEquals(summary, persistedMeta.getSummary());
  }

  @Test
  public void testCompleteFailedPersistsSummaryAndFailureMessage() {
    String failureMessage = "remote spark failure";
    String trackUri = "https://spark.example.com/proxy/application_failed/";
    Map<String, String> summary = Collections.singletonMap("trackUri", trackUri);

    TableProcessMeta persistedMeta =
        completeAndReload(
            ProcessStatus.FAILED, ProcessEvent.COMPLETE_FAILED, failureMessage, summary);

    Assert.assertEquals(ProcessStatus.FAILED, persistedMeta.getStatus());
    Assert.assertTrue(persistedMeta.getFinishTime() > 0);
    Assert.assertEquals(failureMessage, persistedMeta.getFailMessage());
    Assert.assertEquals(summary, persistedMeta.getSummary());
  }

  private TableProcessMeta completeAndReload(
      ProcessStatus terminalStatus,
      ProcessEvent processEvent,
      String reason,
      Map<String, String> summary) {
    long processId = SnowflakeIdGenerator.INSTANCE.generateId();
    TableProcessMeta meta =
        TableProcessMeta.of(
            processId, processId, TEST_ACTION.getName(), "test-engine", Collections.emptyMap());
    meta.setStatus(ProcessStatus.RUNNING);
    meta.setExternalProcessIdentifier("qid-" + processId);
    persistency.persist(meta);

    DefaultTableProcessStore store = new DefaultTableProcessStore(null, meta, TEST_ACTION);
    boolean transitioned =
        store.tryTransitState(
            terminalStatus,
            processEvent,
            meta.getExternalProcessIdentifier(),
            reason,
            Collections.emptyMap(),
            summary);

    Assert.assertTrue(transitioned);
    return persistency.get(processId);
  }

  private static class Persistency extends PersistentBase {

    private void persist(TableProcessMeta meta) {
      doAs(
          TableProcessMapper.class,
          mapper ->
              mapper.insertProcess(
                  meta.getTableId(),
                  meta.getProcessId(),
                  meta.getExternalProcessIdentifier(),
                  meta.getStatus(),
                  meta.getProcessType(),
                  meta.getProcessStage(),
                  meta.getExecutionEngine(),
                  meta.getRetryNumber(),
                  meta.getCreateTime(),
                  meta.getProcessParameters(),
                  meta.getSummary()));
    }

    private TableProcessMeta get(long processId) {
      return getAs(TableProcessMapper.class, mapper -> mapper.getProcessMeta(processId));
    }
  }
}
