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

import static org.apache.amoro.server.AmoroServiceContainer.HAState.FOLLOWER;
import static org.apache.amoro.server.AmoroServiceContainer.HAState.INITIALIZING;
import static org.apache.amoro.server.AmoroServiceContainer.HAState.LEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.amoro.server.persistence.PersistenceFailureReporter;
import org.apache.amoro.server.persistence.PersistenceUnavailableDuringStartupException;
import org.apache.iceberg.common.DynFields;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class TestAmoroServiceContainer {

  private static final DynFields.UnboundField<Boolean> MASTER_SLAVE_MODE_FIELD =
      DynFields.builder().hiddenImpl(AmoroServiceContainer.class, "IS_MASTER_SLAVE_MODE").build();
  private static final DynFields.UnboundField<AmoroServiceContainer.HAState> HA_STATE_FIELD =
      DynFields.builder().hiddenImpl(AmoroServiceContainer.class, "haState").build();
  private static final DynFields.UnboundField<AmsAssignService> AMS_ASSIGN_SERVICE_FIELD =
      DynFields.builder().hiddenImpl(AmoroServiceContainer.class, "amsAssignService").build();

  private boolean originalMasterSlaveMode;

  @BeforeEach
  public void setUp() {
    originalMasterSlaveMode = MASTER_SLAVE_MODE_FIELD.asStatic().get();
    MASTER_SLAVE_MODE_FIELD.asStatic().set(false);
  }

  @AfterEach
  public void tearDown() {
    MASTER_SLAVE_MODE_FIELD.asStatic().set(originalMasterSlaveMode);
  }

  @Test
  public void testStartBaseServicesTransitionsToFollower() throws Exception {
    AmoroServiceContainer serviceContainer = mockServiceContainer(INITIALIZING);

    assertEquals(INITIALIZING, serviceContainer.getHaState());
    serviceContainer.startBaseServices();

    verify(serviceContainer).startRestServices();
    assertEquals(FOLLOWER, serviceContainer.getHaState());
  }

  @Test
  public void testFailedBaseServiceStartupRemainsInitializing() throws Exception {
    AmoroServiceContainer serviceContainer = mockServiceContainer(INITIALIZING);
    IllegalStateException startupFailure =
        new IllegalStateException("Failed to start REST service");
    doThrow(startupFailure).when(serviceContainer).startRestServices();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, serviceContainer::startBaseServices);

    assertSame(startupFailure, thrown);
    assertEquals(INITIALIZING, serviceContainer.getHaState());
  }

  @Test
  public void testStartBaseServicesDoesNotOverwriteLeaderState() throws Exception {
    AmoroServiceContainer serviceContainer = mockServiceContainer(LEADER);

    serviceContainer.startBaseServices();

    assertEquals(LEADER, serviceContainer.getHaState());
  }

  @Test
  public void testFailedLeaderServiceStartupCleansUp() throws Exception {
    AmoroServiceContainer serviceContainer = mockServiceContainer(INITIALIZING);
    serviceContainer.startBaseServices();
    MASTER_SLAVE_MODE_FIELD.asStatic().set(true);

    AmsAssignService amsAssignService = mock(AmsAssignService.class);
    AMS_ASSIGN_SERVICE_FIELD.bind(serviceContainer).set(amsAssignService);
    IllegalStateException startupFailure =
        new IllegalStateException("Failed to start leader service");
    doThrow(startupFailure).when(amsAssignService).start();
    doCallRealMethod().when(serviceContainer).startLeaderServices();
    doCallRealMethod().when(serviceContainer).stopLeaderServices();

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> {
              try {
                serviceContainer.startLeaderServices();
              } finally {
                serviceContainer.stopLeaderServices();
              }
            });

    assertSame(startupFailure, thrown);
    verify(amsAssignService).stop();
    assertEquals(FOLLOWER, serviceContainer.getHaState());
  }

  @Test
  public void testMasterSlaveStartupSeesSwallowedCleanupFailure() throws Exception {
    MASTER_SLAVE_MODE_FIELD.asStatic().set(true);
    AmoroServiceContainer container = mockServiceContainer(INITIALIZING);
    doAnswer(
            invocation -> {
              PersistenceFailureReporter.record(
                  PersistenceFailureReporter.Phase.POOL_CLEANUP,
                  new SQLException("lost during cleanup", "08006"));
              return null;
            })
        .when(container)
        .initializeOptimizingService();
    assertThrows(PersistenceUnavailableDuringStartupException.class, container::startBaseServices);
    assertEquals(INITIALIZING, container.getHaState());
    PersistenceFailureReporter.throwIfStartupFailed();
  }

  @Test
  public void testActiveStandbyStartupPropagatesFatalDatabaseFailure() throws Exception {
    AmoroServiceContainer container = mockServiceContainer(FOLLOWER);
    doCallRealMethod().when(container).startLeaderServices();
    doThrow(new SQLException("lost during initialization", "08006"))
        .when(container)
        .initializeOptimizingService();
    assertThrows(
        PersistenceUnavailableDuringStartupException.class, container::startLeaderServices);
    verify(container).stopLeaderServices();
    PersistenceFailureReporter.throwIfStartupFailed();
  }

  @Test
  public void testLeaderLoopPreservesFatalStartupFailureAcrossBothCleanupAttempts()
      throws Exception {
    AmoroServiceContainer container = mockServiceContainer(FOLLOWER);
    doCallRealMethod().when(container).runLeaderElectionLoop();
    doCallRealMethod().when(container).startLeaderServices();
    SQLException databaseFailure = new SQLException("connection lost", "08006");
    IllegalStateException startupCleanupFailure =
        new IllegalStateException("startup cleanup failed");
    IllegalStateException loopCleanupFailure = new IllegalStateException("loop cleanup failed");
    doThrow(databaseFailure).when(container).initializeOptimizingService();
    doThrow(startupCleanupFailure).doThrow(loopCleanupFailure).when(container).stopLeaderServices();

    PersistenceUnavailableDuringStartupException thrown =
        assertThrows(
            PersistenceUnavailableDuringStartupException.class, container::runLeaderElectionLoop);
    assertSame(databaseFailure, thrown.getCause());
    assertEquals(2, thrown.getSuppressed().length);
    assertSame(startupCleanupFailure, thrown.getSuppressed()[0]);
    assertSame(loopCleanupFailure, thrown.getSuppressed()[1]);
    verify(container).waitLeaderShip();
    PersistenceFailureReporter.throwIfStartupFailed();
  }

  @Test
  public void testLeaderLoopRetriesOrdinaryFailureAndPropagatesCleanupFailure() throws Exception {
    AmoroServiceContainer container = mockServiceContainer(FOLLOWER);
    doCallRealMethod().when(container).runLeaderElectionLoop();
    doThrow(new IllegalStateException("election failed"))
        .doNothing()
        .when(container)
        .waitLeaderShip();
    IllegalStateException cleanupFailure = new IllegalStateException("loop cleanup failed");
    doNothing().doThrow(cleanupFailure).when(container).stopLeaderServices();

    assertSame(
        cleanupFailure,
        assertThrows(IllegalStateException.class, container::runLeaderElectionLoop));
    verify(container, times(2)).waitLeaderShip();
    verify(container).startLeaderServices();
    verify(container).waitFollowerShip();
    verify(container, times(2)).stopLeaderServices();
  }

  private AmoroServiceContainer mockServiceContainer(AmoroServiceContainer.HAState haState)
      throws Exception {
    AmoroServiceContainer serviceContainer = mock(AmoroServiceContainer.class);
    HA_STATE_FIELD.bind(serviceContainer).set(haState);
    doCallRealMethod().when(serviceContainer).getHaState();
    doCallRealMethod().when(serviceContainer).startBaseServices();
    return serviceContainer;
  }
}
