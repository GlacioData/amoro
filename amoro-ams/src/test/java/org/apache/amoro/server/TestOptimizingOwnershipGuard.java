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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.client.AmsServerInfo;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.exception.BucketAssignStoreException;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.ha.HighAvailabilityContainer;
import org.apache.amoro.server.optimizing.OptimizingOwnership;
import org.apache.amoro.server.resource.OptimizerManager;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.TableService;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class TestOptimizingOwnershipGuard {

  @Test
  void nonMasterSlaveOwnsCurrentLocalRuntime() {
    TableService tableService = mock(TableService.class);
    DefaultTableRuntime runtime = runtime(1L);
    when(tableService.isCurrentRuntime(runtime)).thenReturn(true);
    BucketAssignStore assignStore = mock(BucketAssignStore.class);

    DefaultOptimizingService service = service(false, tableService, assignStore, null);

    assertEquals(OptimizingOwnership.OWNED, service.currentOwnership(runtime));
    verifyNoInteractions(assignStore);
  }

  @Test
  void detachedRuntimeIsNotOwnedWithoutReadingAssignments() {
    TableService tableService = mock(TableService.class);
    DefaultTableRuntime runtime = runtime(2L);
    when(tableService.isCurrentRuntime(runtime)).thenReturn(false);
    BucketAssignStore assignStore = mock(BucketAssignStore.class);
    HighAvailabilityContainer haContainer = mock(HighAvailabilityContainer.class);

    DefaultOptimizingService service = service(true, tableService, assignStore, haContainer);

    assertEquals(OptimizingOwnership.NOT_OWNED, service.currentOwnership(runtime));
    verifyNoInteractions(assignStore);
    verify(haContainer, never()).getOptimizingServiceServerInfo();
  }

  @Test
  void masterSlaveReadsOnlyCurrentNodeAssignments() throws Exception {
    TableService tableService = mock(TableService.class);
    DefaultTableRuntime runtime = runtime(3L);
    when(tableService.isCurrentRuntime(runtime)).thenReturn(true);
    when(tableService.getCurrentRuntimeBucketId(runtime)).thenReturn(Optional.of("7"));
    BucketAssignStore assignStore = mock(BucketAssignStore.class);
    HighAvailabilityContainer haContainer = mock(HighAvailabilityContainer.class);
    AmsServerInfo currentNode = new AmsServerInfo();
    currentNode.setHost("127.0.0.1");
    currentNode.setThriftBindPort(1261);
    when(haContainer.getOptimizingServiceServerInfo()).thenReturn(currentNode);
    when(assignStore.getAssignments(currentNode)).thenReturn(Collections.singletonList("7"));

    DefaultOptimizingService service = service(true, tableService, assignStore, haContainer);

    assertEquals(OptimizingOwnership.OWNED, service.currentOwnership(runtime));
    verify(assignStore).getAssignments(currentNode);
    verify(assignStore, never()).getAllAssignments();

    when(assignStore.getAssignments(currentNode)).thenReturn(Collections.singletonList("8"));
    assertEquals(OptimizingOwnership.NOT_OWNED, service.currentOwnership(runtime));
  }

  @Test
  void masterSlaveReturnsUnknownWhenOwnershipCannotBeProved() throws Exception {
    TableService tableService = mock(TableService.class);
    DefaultTableRuntime runtime = runtime(4L);
    when(tableService.isCurrentRuntime(runtime)).thenReturn(true);
    when(tableService.getCurrentRuntimeBucketId(runtime)).thenReturn(Optional.empty());
    BucketAssignStore assignStore = mock(BucketAssignStore.class);
    HighAvailabilityContainer haContainer = mock(HighAvailabilityContainer.class);
    DefaultOptimizingService service = service(true, tableService, assignStore, haContainer);

    assertEquals(OptimizingOwnership.UNKNOWN, service.currentOwnership(runtime));

    when(tableService.getCurrentRuntimeBucketId(runtime)).thenReturn(Optional.of("9"));
    assertEquals(OptimizingOwnership.UNKNOWN, service.currentOwnership(runtime));

    AmsServerInfo currentNode = new AmsServerInfo();
    when(haContainer.getOptimizingServiceServerInfo()).thenReturn(currentNode);
    when(assignStore.getAssignments(currentNode))
        .thenThrow(new BucketAssignStoreException("unavailable"));
    assertEquals(OptimizingOwnership.UNKNOWN, service.currentOwnership(runtime));
  }

  @Test
  void unrelatedBucketChangeDoesNotInvalidateOwnedTable() throws Exception {
    TableService tableService = mock(TableService.class);
    DefaultTableRuntime runtime = runtime(5L);
    when(tableService.isCurrentRuntime(runtime)).thenReturn(true);
    when(tableService.getCurrentRuntimeBucketId(runtime)).thenReturn(Optional.of("7"));
    BucketAssignStore assignStore = mock(BucketAssignStore.class);
    HighAvailabilityContainer haContainer = mock(HighAvailabilityContainer.class);
    AmsServerInfo currentNode = new AmsServerInfo();
    when(haContainer.getOptimizingServiceServerInfo()).thenReturn(currentNode);
    when(assignStore.getAssignments(currentNode)).thenReturn(Arrays.asList("7", "8"));
    DefaultOptimizingService service = service(true, tableService, assignStore, haContainer);

    assertEquals(OptimizingOwnership.OWNED, service.currentOwnership(runtime));

    when(assignStore.getAssignments(currentNode)).thenReturn(Arrays.asList("7", "9"));
    assertEquals(OptimizingOwnership.OWNED, service.currentOwnership(runtime));
  }

  private static DefaultTableRuntime runtime(long tableId) {
    DefaultTableRuntime runtime = mock(DefaultTableRuntime.class);
    ServerTableIdentifier identifier = mock(ServerTableIdentifier.class);
    when(identifier.getId()).thenReturn(tableId);
    when(runtime.getTableIdentifier()).thenReturn(identifier);
    return runtime;
  }

  private static DefaultOptimizingService service(
      boolean masterSlave,
      TableService tableService,
      BucketAssignStore assignStore,
      HighAvailabilityContainer haContainer) {
    Configurations configurations = new Configurations();
    configurations.setBoolean(AmoroManagementConf.HA_USE_MASTER_SLAVE_MODE, masterSlave);
    return new DefaultOptimizingService(
        configurations,
        mock(CatalogManager.class),
        mock(OptimizerManager.class),
        tableService,
        assignStore,
        haContainer,
        Collections.emptyList());
  }
}
