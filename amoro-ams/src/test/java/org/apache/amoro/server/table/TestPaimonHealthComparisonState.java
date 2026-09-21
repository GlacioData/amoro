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

package org.apache.amoro.server.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.formats.paimon.PaimonTable;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthComparisonState;
import org.apache.amoro.formats.paimon.optimizing.health.PaimonHealthComparisonState.Entry;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.mapper.TableMetaMapper;
import org.apache.amoro.server.persistence.mapper.TableProcessMapper;
import org.apache.amoro.server.persistence.mapper.TableRuntimeMapper;
import org.apache.amoro.table.TableRuntimeStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class TestPaimonHealthComparisonState extends PersistentBase {
  private final DerbyPersistence derby = new DerbyPersistence();

  @AfterEach
  void cleanup() {
    derby.after();
  }

  @Test
  void stateRestoresAndSuccessIsRecoveredWithoutCompletionCallback() {
    java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong();
    TableHealthComparisonService service = spy(new TableHealthComparisonService(clock::get));
    ServerTableIdentifier id = ServerTableIdentifier.of("c", "d", "t", TableFormat.PAIMON);
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("default");
    meta.setStatusCode(0);
    doAs(TableMetaMapper.class, mapper -> mapper.insertTable(id));
    meta.setTableId(id.getId());
    doAs(TableRuntimeMapper.class, mapper -> mapper.insertRuntime(meta));
    TableRuntimeStore store =
        new DefaultTableRuntimeStore(
            id,
            meta,
            Collections.singletonList(PaimonHealthComparisonState.KEY),
            Collections.emptyList());
    Entry pending = new Entry();
    pending.setProcessId(100);
    pending.setInputSnapshotId(1);
    pending.setCommitUser("ams-test");
    pending.setCommitIdentifier(77);
    store
        .begin()
        .updateState(
            PaimonHealthComparisonState.KEY,
            state -> {
              state.setPending(pending);
              return state;
            })
        .commit();
    doAs(
        TableProcessMapper.class,
        mapper ->
            mapper.insertProcess(
                id.getId(),
                100,
                "",
                ProcessStatus.SUCCESS,
                "MINOR",
                "committing",
                "AMORO",
                0,
                1000,
                Collections.emptyMap(),
                Collections.emptyMap()));
    // Reconstruct the store after success was persisted, without any completion callback.
    TableRuntimeStore restarted =
        new DefaultTableRuntimeStore(
            id,
            meta,
            Collections.singletonList(PaimonHealthComparisonState.KEY),
            getAs(TableRuntimeMapper.class, TableRuntimeMapper::selectAllStates));
    assertEquals(
        "ams-test",
        restarted.getState(PaimonHealthComparisonState.KEY).getPending().getCommitUser());
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    when(runtime.store()).thenReturn(restarted);
    when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);
    when(runtime.getTableIdentifier()).thenReturn(id);
    PaimonTable table = mock(PaimonTable.class);
    service.refresh(runtime, table);
    assertEquals(
        100, restarted.getState(PaimonHealthComparisonState.KEY).getLatest().getProcessId());
    assertEquals("PREPARING", TableHealthComparisonService.instance().view(runtime).getStatus());
    doAs(
        TableProcessMapper.class,
        mapper ->
            mapper.insertProcess(
                id.getId(),
                101,
                "",
                ProcessStatus.FAILED,
                "MINOR",
                "committing",
                "AMORO",
                0,
                1001,
                Collections.emptyMap(),
                Collections.emptyMap()));
    service.refresh(runtime, table);
    service.refresh(runtime, table);
    verify(service, times(1)).observeLatest(any(), any());
    assertEquals("100", TableHealthComparisonService.instance().view(runtime).getProcessId());
    doAs(
        TableProcessMapper.class,
        mapper ->
            mapper.insertProcess(
                id.getId(),
                102,
                "",
                ProcessStatus.SUCCESS,
                "MINOR",
                "committing",
                "AMORO",
                0,
                1002,
                Collections.emptyMap(),
                Collections.emptyMap()));
    // A persisted completion without a runtime signal is discovered by reconciliation.
    clock.set(java.util.concurrent.TimeUnit.MINUTES.toNanos(5));
    service.refresh(runtime, table);
    assertEquals("102", TableHealthComparisonService.instance().view(runtime).getProcessId());
    assertEquals(
        "MISSING_INPUT", TableHealthComparisonService.instance().view(runtime).getReason());
    verify(service, times(2)).observeLatest(any(), any());
    when(runtime.getLastMinorOptimizingTime()).thenReturn(1L);
    service.refresh(runtime, table);
    verify(service, times(3)).observeLatest(any(), any());
    restarted
        .begin()
        .updateState(
            PaimonHealthComparisonState.KEY,
            state -> {
              state.setVersion(2);
              return state;
            })
        .commit();
    org.junit.jupiter.api.Assertions.assertNull(
        TableHealthComparisonService.instance().view(runtime));
  }

  @Test
  void concurrentInitializationAcceptsOnlyExistingComparisonKey() {
    ServerTableIdentifier id = ServerTableIdentifier.of("c", "d", "t", TableFormat.PAIMON);
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setTableId(id.getId());
    meta.setGroupName("default");
    meta.setStatusCode(0);
    doAs(TableMetaMapper.class, mapper -> mapper.insertTable(id));
    meta.setTableId(id.getId());
    doAs(TableRuntimeMapper.class, mapper -> mapper.insertRuntime(meta));
    new DefaultTableRuntimeStore(
        id,
        meta,
        Collections.singletonList(PaimonHealthComparisonState.KEY),
        Collections.emptyList());
    TableRuntimeStore second =
        new DefaultTableRuntimeStore(
            id,
            meta,
            Collections.singletonList(PaimonHealthComparisonState.KEY),
            Collections.emptyList());
    assertEquals(1, second.getState(PaimonHealthComparisonState.KEY).getVersion());
  }
}
