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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.catalog.ServerCatalog;
import org.apache.amoro.table.TableRuntimeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class TestDefaultTableServiceCatalogCleanup {

  @Test
  void catalogCreatedAfterExplorerSnapshotKeepsItsNewTableRuntime() throws Exception {
    CatalogManager catalogManager = mock(CatalogManager.class);
    DefaultTableService service = newService(catalogManager);
    AtomicInteger disposals = recordDisposalsAndRemoveRuntime(service);
    ServerTableIdentifier identifier =
        ServerTableIdentifier.of(42L, "new_catalog", "db", "table", TableFormat.ICEBERG);
    TableRuntime runtime = runtimeFor(identifier);
    CatalogMeta newCatalog = new CatalogMeta();
    newCatalog.setCatalogName(identifier.getCatalog());
    AtomicReference<List<CatalogMeta>> committedCatalogs =
        new AtomicReference<>(Collections.emptyList());

    // The initial catalog read has produced an empty snapshot. Before it returns, another
    // request commits a new catalog and table and publishes that table's runtime. Cleanup must
    // use the subsequently committed catalog list, not the old exploration snapshot.
    when(catalogManager.getServerCatalogs())
        .thenAnswer(
            invocation -> {
              List<ServerCatalog> oldSnapshot = Collections.emptyList();
              committedCatalogs.set(Collections.singletonList(newCatalog));
              service.setRuntime(runtime);
              return oldSnapshot;
            });
    when(catalogManager.listCatalogMetas()).thenAnswer(invocation -> committedCatalogs.get());

    try {
      service.exploreTableRuntimes();

      Assertions.assertSame(runtime, service.getRuntime(identifier.getId()));
      Assertions.assertEquals(0, disposals.get(), "a newly created table was disposed");
    } finally {
      service.dispose();
    }
  }

  @Test
  void runtimeOfDroppedCatalogIsStillCleanedUp() throws Exception {
    CatalogManager catalogManager = mock(CatalogManager.class);
    DefaultTableService service = newService(catalogManager);
    AtomicInteger disposals = recordDisposalsAndRemoveRuntime(service);
    ServerTableIdentifier identifier =
        ServerTableIdentifier.of(43L, "dropped_catalog", "db", "table", TableFormat.ICEBERG);
    service.setRuntime(runtimeFor(identifier));
    when(catalogManager.getServerCatalogs()).thenReturn(Collections.emptyList());
    when(catalogManager.listCatalogMetas()).thenReturn(Collections.emptyList());

    try {
      service.exploreTableRuntimes();

      Assertions.assertNull(service.getRuntime(identifier.getId()));
      Assertions.assertEquals(1, disposals.get());
    } finally {
      service.dispose();
    }
  }

  private static DefaultTableService newService(CatalogManager catalogManager) throws Exception {
    DefaultTableService service =
        spy(
            new DefaultTableService(
                new Configurations(), catalogManager, mock(TableRuntimeFactory.class)));
    Field initialized = DefaultTableService.class.getDeclaredField("initialized");
    initialized.setAccessible(true);
    @SuppressWarnings("unchecked")
    CompletableFuture<Boolean> flag = (CompletableFuture<Boolean>) initialized.get(service);
    flag.complete(true);
    return service;
  }

  private static TableRuntime runtimeFor(ServerTableIdentifier identifier) {
    TableRuntime runtime = mock(TableRuntime.class);
    when(runtime.getTableIdentifier()).thenReturn(identifier);
    return runtime;
  }

  private static AtomicInteger recordDisposalsAndRemoveRuntime(DefaultTableService service)
      throws Exception {
    Field runtimeMapField = DefaultTableService.class.getDeclaredField("tableRuntimeMap");
    runtimeMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<Long, TableRuntime> runtimes = (Map<Long, TableRuntime>) runtimeMapField.get(service);
    AtomicInteger disposals = new AtomicInteger();
    // Preserve the observable removal without hitting the database. The production disposeTable
    // also removes the persisted table identifier, making an erroneous call more damaging.
    doAnswer(
            invocation -> {
              ServerTableIdentifier identifier = invocation.getArgument(0);
              runtimes.remove(identifier.getId());
              disposals.incrementAndGet();
              return null;
            })
        .when(service)
        .disposeTable(any(ServerTableIdentifier.class));
    return disposals;
  }
}
