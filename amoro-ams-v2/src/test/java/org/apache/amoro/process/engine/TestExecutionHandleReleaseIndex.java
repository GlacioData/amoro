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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.persistence.PersistenceChange;
import org.apache.amoro.persistence.PreparedProjectionUpdate;
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class TestExecutionHandleReleaseIndex {

  @Test
  public void publishesOnlyAfterDurableProjectionCommitAndRebuildsAfterRestart() {
    ProcessResource terminal = terminal("p1", "dummy-1");
    ExecutionHandleReleaseIndex index = new ExecutionHandleReleaseIndex();
    PreparedProjectionUpdate prepared = index.prepare(PersistenceChange.created(terminal));

    assertEquals(0, index.pendingCount(), "a DB failure before commit must not publish cleanup");
    prepared.commit();
    assertEquals(1, index.pendingCount());
    assertTrue(index.hasPendingForProcess("p1"));

    List<ExecutionHandleReleaseIndex.ReleaseEntry> claimed =
        index.claimDue(Instant.parse("2026-08-24T01:01:00Z"), 10);
    assertEquals(1, claimed.size());
    index.releaseSucceeded(claimed.get(0));
    assertEquals(0, index.pendingCount());
    assertFalse(index.hasPendingForProcess("p1"));

    index.prepare(PersistenceChange.modified(terminal, terminal)).commit();
    assertEquals(0, index.pendingCount(), "a later write must not re-add a released handle");

    ExecutionHandleReleaseIndex afterRestart = new ExecutionHandleReleaseIndex();
    afterRestart.prepare(PersistenceChange.created(terminal)).commit();
    assertEquals(1, afterRestart.pendingCount(), "restart safely repeats idempotent release");
  }

  @Test
  public void failedReleaseReentersOrderedBackoff() {
    ExecutionHandleReleaseIndex index = new ExecutionHandleReleaseIndex();
    index.prepare(PersistenceChange.created(terminal("p2", "dummy-2"))).commit();
    ExecutionHandleReleaseIndex.ReleaseEntry claimed =
        index.claimDue(Instant.parse("2026-08-24T01:01:00Z"), 1).get(0);

    index.releaseFailed(claimed, Instant.parse("2026-08-24T01:01:00Z"));

    assertEquals(0, index.claimDue(Instant.parse("2026-08-24T01:01:02Z"), 1).size());
    assertEquals(1, index.claimDue(Instant.parse("2026-08-24T01:01:03Z"), 1).size());
  }

  static ProcessResource terminal(String name, String externalId) {
    String finishedAt = "2026-08-24T01:00:00Z";
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            name + ":0:0",
            "sha256:req",
            "ACKNOWLEDGED",
            externalId,
            "2026-08-24T00:59:00Z",
            null,
            "FINAL",
            finishedAt,
            Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    return new ProcessResource(
        name,
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("sim", "db", name, name, "simulated"),
            "dummy-maintenance",
            "local",
            "MANUAL",
            "2026-08-24T00:00:00Z",
            "RUN",
            new ProcessResource.RequestIdentity("sha256:key", "sha256:req"),
            Collections.emptyMap(),
            new ProcessResource.RetryPolicy(3, 2, 1)),
        new ProcessResource.ProcessStatus(
            "SUCCESS",
            0,
            attempt,
            Collections.emptyList(),
            finishedAt,
            null,
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            Collections.emptyList(),
            new ProcessResource.Summary(null, Collections.singletonMap("simulated", true)),
            null,
            finishedAt,
            finishedAt,
            finishedAt));
  }
}
