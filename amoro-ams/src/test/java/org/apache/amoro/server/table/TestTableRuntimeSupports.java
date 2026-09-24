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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.amoro.FormatCatalogFactory;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.formats.paimon.PaimonTableCreationTime;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.optimizing.plan.AbstractOptimizingEvaluator;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.persistence.TableRuntimeState;
import org.apache.amoro.table.FormatPendingInput;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableRuntimeFactory;
import org.apache.amoro.table.TableRuntimeSupport;
import org.apache.amoro.table.descriptor.ServerTableMeta;
import org.apache.amoro.table.health.TableHealthSupport;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class TestTableRuntimeSupports {
  @Test
  void builtInCodecsRestoreExistingJsonAndKeepStateOwnership() {
    for (TableFormat format :
        new TableFormat[] {
          TableFormat.PAIMON, TableFormat.ICEBERG, TableFormat.MIXED_ICEBERG, TableFormat.MIXED_HIVE
        }) {
      TableRuntimeSupport support = TableFormatSupports.instance().runtimeSupport(format).get();
      StateKey<? extends FormatPendingInput> key = support.pendingInputKey();
      assertEquals("pending_input", key.getKey());
      FormatPendingInput input = key.deserialize("{\"dataFileCount\":7,\"dataFileSize\":123}");
      assertEquals(7, input.getDataFileCount());
      assertEquals(123, input.getDataFileSize());
      assertEquals(
          format == TableFormat.PAIMON
              ? PaimonPendingInput.class
              : AbstractOptimizingEvaluator.PendingInput.class,
          input.getClass());
      if (format == TableFormat.PAIMON) {
        assertEquals(PaimonTableCreationTime.KEY, support.requiredStates().get(0));
        PaimonTableCreationTime restored =
            PaimonTableCreationTime.KEY.deserialize(
                "{\"createTime\":1700000000123,\"schemaId\":0,\"source\":\"SCHEMA_TIME\"}");
        assertEquals(1700000000123L, restored.getCreateTime());
      } else {
        assertTrue(support.requiredStates().isEmpty());
        TableRuntime runtime = mock(TableRuntime.class);
        ServerTableMeta detail = new ServerTableMeta();
        detail.setCreateTime(19L);
        support.projectDetail(runtime, detail);
        assertEquals(19L, detail.getCreateTime());
        verifyNoInteractions(runtime);
      }
    }
  }

  @Test
  void newFormatSuppliesCodecAndProjectionWithoutCommonFormatBranchesOrTableLoading() {
    TableFormat format = TableFormat.register("TEST_RUNTIME_CAPABILITY");
    StateKey<GenericPending> pending =
        StateKey.stateKey("pending_input")
            .jsonType(GenericPending.class)
            .defaultValue(new GenericPending());
    StateKey<Long> timestamp =
        StateKey.stateKey("custom_creation_time").longType().defaultValue(0L);
    TableRuntimeSupport support =
        new TableRuntimeSupport() {
          @Override
          public StateKey<GenericPending> pendingInputKey() {
            return pending;
          }

          @Override
          public List<StateKey<?>> requiredStates() {
            return Collections.singletonList(timestamp);
          }

          @Override
          public void projectDetail(TableRuntime runtime, ServerTableMeta target) {
            if (runtime != null) {
              target.setCreateTime(runtime.getState(timestamp));
            }
          }
        };
    FormatCatalogFactory provider = provider(format, support);
    TableFormatSupports registry = new TableFormatSupports(Collections.singletonList(provider));
    DefaultTableRuntimeFactory factory = new DefaultTableRuntimeFactory(registry);
    factory.withTableLoader(
        id -> {
          throw new AssertionError("Table must not be loaded");
        });
    ServerTableIdentifier id = ServerTableIdentifier.of(1L, "c", "d", "t", format);
    TableRuntimeFactory.TableRuntimeCreator creator =
        factory.accept(id, Collections.emptyMap()).get();
    TableRuntimeState savedPending = new TableRuntimeState();
    savedPending.setStateKey("pending_input");
    savedPending.setStateValue("{\"dataFileCount\":9}");
    TableRuntimeState savedTime = new TableRuntimeState();
    savedTime.setStateKey(timestamp.getKey());
    savedTime.setStateValue("12345");
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setStatusCode(700);
    List<StateKey<?>> keys = creator.requiredStateKeys();
    List<TableRuntimeState> restoredStates =
        keys.stream()
            .map(
                key -> {
                  if (key.equals(pending)) {
                    return savedPending;
                  }
                  if (key.equals(timestamp)) {
                    return savedTime;
                  }
                  TableRuntimeState state = new TableRuntimeState();
                  state.setStateKey(key.getKey());
                  state.setStateValue(key.serializeDefault());
                  return state;
                })
            .collect(Collectors.toList());
    DefaultTableRuntimeStore store = new DefaultTableRuntimeStore(id, meta, keys, restoredStates);
    DefaultTableRuntime runtime = (DefaultTableRuntime) creator.create(store);
    assertEquals(GenericPending.class, runtime.getPendingInput().getClass());
    assertEquals(9, runtime.getPendingInput().getDataFileCount());
    ServerTableMeta detail = new ServerTableMeta();
    registry.runtimeSupport(format).get().projectDetail(runtime, detail);
    assertEquals(12345L, detail.getCreateTime());
    verify(provider, never()).create(any(), any(), any(), any());
  }

  @Test
  void missingCapabilityDoesNotGuessAnIcebergCodec() {
    TableFormat format = TableFormat.register("TEST_NO_RUNTIME_CAPABILITY");
    DefaultTableRuntimeFactory factory =
        new DefaultTableRuntimeFactory(new TableFormatSupports(Collections.emptyList()));
    assertFalse(
        factory
            .accept(ServerTableIdentifier.of("c", "d", "t", format), Collections.emptyMap())
            .isPresent());
  }

  @Test
  void duplicateProvidersAndStateKeysFailFast() {
    TableFormat format = TableFormat.register("TEST_DUPLICATE_RUNTIME_CAPABILITY");
    TableRuntimeSupport support = mock(TableRuntimeSupport.class);
    StateKey<GenericPending> pending =
        StateKey.stateKey("pending_input")
            .jsonType(GenericPending.class)
            .defaultValue(new GenericPending());
    doReturn(pending).when(support).pendingInputKey();
    when(support.requiredStates()).thenReturn(Collections.singletonList(pending));
    FormatCatalogFactory provider = provider(format, support);
    assertThrows(
        IllegalStateException.class,
        () -> new TableFormatSupports(Arrays.asList(provider, provider)));
    DefaultTableRuntimeFactory factory =
        new DefaultTableRuntimeFactory(
            new TableFormatSupports(Collections.singletonList(provider)));
    assertThrows(
        IllegalStateException.class,
        () ->
            factory
                .accept(ServerTableIdentifier.of("c", "d", "t", format), Collections.emptyMap())
                .get()
                .requiredStateKeys());
  }

  private FormatCatalogFactory provider(TableFormat format, TableRuntimeSupport support) {
    FormatCatalogFactory provider = mock(FormatCatalogFactory.class);
    when(provider.format()).thenReturn(format);
    when(provider.healthSupport()).thenReturn(TableHealthSupport.NONE);
    when(provider.runtimeSupport()).thenReturn(Optional.of(support));
    return provider;
  }

  /** An independent test format, deliberately unrelated to Iceberg/Paimon pending-input classes. */
  public static class GenericPending implements FormatPendingInput {
    private int dataFileCount;

    public void setDataFileCount(int value) {
      dataFileCount = value;
    }

    @Override
    public int getDataFileCount() {
      return dataFileCount;
    }

    @Override
    public long getDataFileSize() {
      return 0;
    }

    @Override
    public int getTotalFileCount() {
      return dataFileCount;
    }

    @Override
    public long getTotalFileSize() {
      return 0;
    }

    @Override
    public int getHealthScore() {
      return -1;
    }
  }
}
