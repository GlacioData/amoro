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

import org.apache.amoro.PaimonActions;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.process.ActionCoordinator;
import org.apache.amoro.server.table.TableService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;

public class TestActionCoordinatorScheduler {

  @Test
  public void testStartDelayUsesTableSpecificInterval() {
    ActionCoordinator coordinator = Mockito.mock(ActionCoordinator.class);
    TableRuntime tableRuntime = Mockito.mock(TableRuntime.class);
    Mockito.when(coordinator.action()).thenReturn(PaimonActions.EXPIRE_SNAPSHOTS);
    Mockito.when(coordinator.parallelism()).thenReturn(1);
    Mockito.when(coordinator.getNextExecutingTime(tableRuntime))
        .thenReturn(Duration.ofHours(2).toMillis());

    TestableActionCoordinatorScheduler scheduler =
        new TestableActionCoordinatorScheduler(
            coordinator, Mockito.mock(TableService.class), Mockito.mock(ProcessService.class));
    try {
      Assert.assertEquals(
          Duration.ofHours(2).plusSeconds(10).toMillis(), scheduler.startDelayFor(tableRuntime));
    } finally {
      scheduler.dispose();
    }
  }

  private static class TestableActionCoordinatorScheduler extends ActionCoordinatorScheduler {

    private TestableActionCoordinatorScheduler(
        ActionCoordinator coordinator, TableService tableService, ProcessService processService) {
      super(coordinator, tableService, processService);
    }

    private long startDelayFor(TableRuntime tableRuntime) {
      return getStartDelay(tableRuntime);
    }
  }
}
