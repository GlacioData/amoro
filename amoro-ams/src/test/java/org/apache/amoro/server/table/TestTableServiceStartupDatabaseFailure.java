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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.amoro.client.AmsServerInfo;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.BucketAssignStore;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.ha.HighAvailabilityContainer;
import org.apache.amoro.server.persistence.PersistenceFailureReporter;
import org.apache.amoro.server.persistence.PersistenceUnavailableDuringStartupException;
import org.apache.amoro.table.TableRuntimeFactory;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class TestTableServiceStartupDatabaseFailure {
  @Test
  void assignmentReadFailureCannotBecomeAnEmptySuccessfulStartup() {
    Configurations config = new Configurations();
    config.set(AmoroManagementConf.HA_USE_MASTER_SLAVE_MODE, true);
    HighAvailabilityContainer ha = mock(HighAvailabilityContainer.class);
    when(ha.getOptimizingServiceServerInfo()).thenReturn(mock(AmsServerInfo.class));
    BucketAssignStore assignments = mock(BucketAssignStore.class);
    IllegalStateException failure =
        new IllegalStateException(new SQLException("connection lost", "08006"));
    when(assignments.getAssignments(any())).thenThrow(failure);
    DefaultTableService service =
        new DefaultTableService(
            config, mock(CatalogManager.class), mock(TableRuntimeFactory.class), ha, assignments);
    try (PersistenceFailureReporter.StartupScope scope =
        PersistenceFailureReporter.startupScope()) {
      PersistenceUnavailableDuringStartupException thrown =
          assertThrows(PersistenceUnavailableDuringStartupException.class, service::initialize);
      assertSame(failure, thrown.getCause());
    } finally {
      service.dispose();
    }
  }
}
