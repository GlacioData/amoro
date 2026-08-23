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

package org.apache.amoro.process.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.amoro.persistence.PersistenceChange;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class TestExecutionHandleReaper {

  @Test
  public void reaperIsTheBoundedUniqueReleaseCaller() {
    RecordingEngine adapter = new RecordingEngine();
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 1_000L);
    ProcessEngineRegistry engines = ProcessEngineRegistry.single("local", dispatcher);
    ExecutionHandleReleaseIndex index = new ExecutionHandleReleaseIndex();
    index
        .prepare(
            PersistenceChange.created(TestExecutionHandleReleaseIndex.terminal("p1", "dummy-1")))
        .commit();

    try (ExecutionHandleReaper reaper =
        new ExecutionHandleReaper(
            index, engines, 1, 1_000L, () -> Instant.parse("2026-08-24T01:01:00Z"))) {
      assertEquals(1, reaper.runOnce(Instant.parse("2026-08-24T01:01:00Z")));
      assertEquals(1, adapter.releases.get());
      assertEquals(0, index.pendingCount());
      assertEquals(0, reaper.runOnce(Instant.parse("2026-08-24T01:02:00Z")));
    } finally {
      dispatcher.close();
    }
  }

  private static final class RecordingEngine extends FakeEngineAdapter {
    private final AtomicInteger releases = new AtomicInteger();

    @Override
    public CompletionStage<Void> release(String externalId) {
      releases.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    }
  }
}
