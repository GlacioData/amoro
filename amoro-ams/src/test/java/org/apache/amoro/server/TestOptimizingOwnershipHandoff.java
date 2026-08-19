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

package org.apache.amoro.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.client.AmsServerInfo;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.exception.BucketAssignStoreException;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.ha.HighAvailabilityContainer;
import org.apache.amoro.server.optimizing.OptimizingQueue;
import org.apache.amoro.server.resource.OptimizerManager;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.RuntimeHandlerChain;
import org.apache.amoro.server.table.TableService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class TestOptimizingOwnershipHandoff {

  private static final String GROUP_NAME = "ownership-handoff";
  private static final String BUCKET_ID = "7";

  private final TableService tableService = mock(TableService.class);
  private final BucketAssignStore assignStore = mock(BucketAssignStore.class);
  private final HighAvailabilityContainer haContainer = mock(HighAvailabilityContainer.class);
  private final CatalogManager catalogManager = mock(CatalogManager.class);
  private final OptimizerManager optimizerManager = mock(OptimizerManager.class);
  private final OptimizingQueue queue = mock(OptimizingQueue.class);
  private final DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
  private final AmoroTable<?> table = mock(AmoroTable.class);
  private final AmsServerInfo currentNode = new AmsServerInfo();

  private DefaultOptimizingService service;
  private RuntimeHandlerChain handler;

  @BeforeEach
  void setUp() throws Exception {
    Configurations configurations = new Configurations();
    configurations.setBoolean(AmoroManagementConf.HA_USE_MASTER_SLAVE_MODE, true);
    service =
        new DefaultOptimizingService(
            configurations,
            catalogManager,
            optimizerManager,
            tableService,
            assignStore,
            haContainer,
            Collections.emptyList());
    handler = service.getTableRuntimeHandler();
    markInitialized(handler);
    queues(service).put(GROUP_NAME, queue);

    ServerTableIdentifier identifier = mock(ServerTableIdentifier.class);
    when(identifier.getId()).thenReturn(1L);
    when(runtime.getTableIdentifier()).thenReturn(identifier);
    when(runtime.getFormat()).thenReturn(TableFormat.PAIMON);
    when(runtime.getGroupName()).thenReturn(GROUP_NAME);
    when(tableService.isCurrentRuntime(runtime)).thenReturn(true);
    when(tableService.getCurrentRuntimeBucketId(runtime)).thenReturn(Optional.of(BUCKET_ID));
    when(haContainer.getOptimizingServiceServerInfo()).thenReturn(currentNode);
  }

  @AfterEach
  void tearDown() {
    service.dispose();
  }

  @Test
  void ownershipLostDetachesWithoutReleasingProcess() throws Exception {
    when(assignStore.getAssignments(currentNode)).thenReturn(Collections.emptyList());

    handler.fireTableRemoved(runtime);

    verify(queue).detachTable(runtime);
    verify(queue, never()).releaseTable(runtime);
  }

  @Test
  void unknownOwnershipDetachesWithoutReleasingProcess() {
    when(tableService.getCurrentRuntimeBucketId(runtime)).thenReturn(Optional.empty());

    handler.fireTableRemoved(runtime);

    verify(queue).detachTable(runtime);
    verify(queue, never()).releaseTable(runtime);
  }

  @Test
  void assignmentReadFailureDetachesWithoutReleasingProcess() throws Exception {
    when(assignStore.getAssignments(currentNode))
        .thenThrow(new BucketAssignStoreException("assignment unavailable"));

    handler.fireTableRemoved(runtime);

    verify(queue).detachTable(runtime);
    verify(queue, never()).releaseTable(runtime);
  }

  @Test
  void ownedRemovalPreservesExistingReleaseSemantics() throws Exception {
    when(assignStore.getAssignments(currentNode)).thenReturn(Collections.singletonList(BUCKET_ID));

    handler.fireTableRemoved(runtime);

    verify(queue).releaseTable(runtime);
    verify(queue, never()).detachTable(runtime);
  }

  @Test
  void ownershipGainedRecoversBeforeScheduling() throws Exception {
    when(assignStore.getAssignments(currentNode)).thenReturn(Collections.singletonList(BUCKET_ID));

    handler.fireTableAdded(table, runtime);

    verify(queue).recoverOwnedTable(runtime);
    verify(queue, never()).refreshTable(runtime);
  }

  @Test
  void unownedOrUnknownAdditionDoesNotRecover() throws Exception {
    when(assignStore.getAssignments(currentNode)).thenReturn(Collections.emptyList());
    handler.fireTableAdded(table, runtime);

    when(tableService.getCurrentRuntimeBucketId(runtime)).thenReturn(Optional.empty());
    handler.fireTableAdded(table, runtime);

    verify(queue, never()).recoverOwnedTable(runtime);
    verify(queue, never()).refreshTable(runtime);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, OptimizingQueue> queues(DefaultOptimizingService service)
      throws Exception {
    Field field = DefaultOptimizingService.class.getDeclaredField("optimizingQueueByGroup");
    field.setAccessible(true);
    return (Map<String, OptimizingQueue>) field.get(service);
  }

  private static void markInitialized(RuntimeHandlerChain handler) throws Exception {
    Field field = RuntimeHandlerChain.class.getDeclaredField("initialized");
    field.setAccessible(true);
    field.setBoolean(handler, true);
  }
}
