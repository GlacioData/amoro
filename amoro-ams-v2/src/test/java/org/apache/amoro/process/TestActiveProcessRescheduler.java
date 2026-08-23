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

package org.apache.amoro.process;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.amoro.control.Controller;
import org.apache.amoro.control.ControllerKey;
import org.apache.amoro.control.Scheduler;
import org.apache.amoro.persistence.PersistenceChange;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestActiveProcessRescheduler {

  @Test
  public void listenerLossIsRepairedThroughBoundedStableCursor() {
    ProcessIndexProjection projection = new ProcessIndexProjection();
    for (int index = 0; index < 5; index++) {
      projection.prepare(PersistenceChange.created(active("p" + index))).commit();
    }
    RecordingScheduler scheduler = new RecordingScheduler();
    try (ActiveProcessRescheduler rescheduler =
        new ActiveProcessRescheduler(
            projection, scheduler, name -> new NamedController(name), 2, 1_000L, 30_000L)) {
      assertEquals(2, rescheduler.runOnce());
      assertEquals(2, rescheduler.runOnce());
      assertEquals(1, rescheduler.runOnce());
      assertEquals(0, rescheduler.runOnce(), "tail resets the cursor without a full-cache scan");
      assertEquals(
          2, rescheduler.runOnce(), "the next round wraps to newly/previously active rows");
    }
    assertEquals(List.of("p0", "p1", "p2", "p3", "p4", "p0", "p1"), scheduler.names);
  }

  private static ProcessResource active(String name) {
    return new ProcessResource(
        name,
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("sim", "db", name, name, "simulated"),
            "dummy-maintenance",
            "local",
            "MANUAL",
            "2026-08-24T00:00:00Z",
            "RUN",
            new ProcessResource.RequestIdentity("sha256:key-" + name, "sha256:req-" + name),
            Collections.emptyMap(),
            new ProcessResource.RetryPolicy(3, 2, 30)),
        new ProcessResource.ProcessStatus(
            "PENDING",
            0,
            null,
            Collections.emptyList(),
            null,
            null,
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            null));
  }

  private static final class NamedController implements Controller {
    private final String name;

    private NamedController(String name) {
      this.name = name;
    }

    @Override
    public ControllerKey key() {
      return ControllerKey.of("process", name);
    }

    @Override
    public void invoke() {}
  }

  private static final class RecordingScheduler implements Scheduler {
    private final List<String> names = new ArrayList<>();

    @Override
    public void schedule(Controller controller) {
      names.add(controller.key().resourceId());
    }

    @Override
    public void schedule(Controller controller, Duration nextDelay) {
      schedule(controller);
    }

    @Override
    public void unschedule(ControllerKey key) {}

    @Override
    public void postStart() {}

    @Override
    public void shutdown(Duration timeout) {}
  }
}
