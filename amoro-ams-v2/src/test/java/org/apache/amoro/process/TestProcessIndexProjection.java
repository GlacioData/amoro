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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.persistence.PersistenceChange;
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Optional;

/** Fail-closed aggregate admission and idempotency projection contracts. */
public class TestProcessIndexProjection {

  @Test
  public void secondActiveSameTableActionFailsAndNamesBothResources() {
    ProcessIndexProjection projection = new ProcessIndexProjection();
    ProcessResource incumbent = resource("p1", "key-1", "PENDING", null);
    projection.prepare(PersistenceChange.created(incumbent)).commit();

    ProcessIndexConflictException conflict =
        assertThrows(
            ProcessIndexConflictException.class,
            () ->
                projection.prepare(
                    PersistenceChange.created(resource("p2", "key-2", "PENDING", null))));

    assertEquals("ACTIVE_PROCESS", conflict.conflictType());
    assertEquals("p1", conflict.incumbentName());
    assertEquals("p2", conflict.contenderName());
    assertEquals(
        Optional.of("p1"), projection.current().activeProcessOf("42", "dummy-maintenance"));
    assertEquals(1, projection.current().resourcesByName().size());
  }

  @Test
  public void terminalReleasesActiveButKeepsIdempotencyUntilDelete() {
    ProcessIndexProjection projection = new ProcessIndexProjection();
    ProcessResource active = resource("p1", "same-key", "PENDING", null);
    projection.prepare(PersistenceChange.created(active)).commit();

    ProcessResource terminal = resource("p1", "same-key", "SUCCESS", "2026-08-22T11:00:00Z");
    projection.prepare(PersistenceChange.modified(active, terminal)).commit();
    assertEquals(Optional.empty(), projection.current().activeProcessOf("42", "dummy-maintenance"));
    assertEquals(
        Optional.of("p1"),
        projection.current().idempotentHolderOf("42", "dummy-maintenance", "sha256:same-key"));

    ProcessIndexConflictException conflict =
        assertThrows(
            ProcessIndexConflictException.class,
            () ->
                projection.prepare(
                    PersistenceChange.created(resource("p2", "same-key", "PENDING", null))));
    assertEquals("IDEMPOTENCY_KEY", conflict.conflictType());

    projection.prepare(PersistenceChange.deleted(terminal)).commit();
    projection
        .prepare(PersistenceChange.created(resource("p2", "same-key", "PENDING", null)))
        .commit();
    assertEquals(
        Optional.of("p2"),
        projection.current().idempotentHolderOf("42", "dummy-maintenance", "sha256:same-key"));
  }

  @Test
  public void deletingOldTerminalDoesNotReleaseNewActiveHolder() {
    ProcessIndexProjection projection = new ProcessIndexProjection();
    ProcessResource first = resource("p1", "key-1", "PENDING", null);
    projection.prepare(PersistenceChange.created(first)).commit();

    ProcessResource firstTerminal = resource("p1", "key-1", "SUCCESS", "2026-08-22T11:00:00Z");
    projection.prepare(PersistenceChange.modified(first, firstTerminal)).commit();
    ProcessResource second = resource("p2", "key-2", "PENDING", null);
    projection.prepare(PersistenceChange.created(second)).commit();

    projection.prepare(PersistenceChange.deleted(firstTerminal)).commit();

    assertEquals(
        Optional.of("p2"), projection.current().activeProcessOf("42", "dummy-maintenance"));
  }

  private static ProcessResource resource(
      String name, String key, String phase, String finishedAt) {
    LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("simulated", true);
    return new ProcessResource(
        name,
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("prod", "db", "table", "42", "simulated"),
            "dummy-maintenance",
            "local",
            "MANUAL",
            "2026-08-22T10:00:00Z",
            "RUN",
            new ProcessResource.RequestIdentity("sha256:" + key, "sha256:request-" + name),
            parameters,
            new ProcessResource.RetryPolicy(3, 2, 30)),
        new ProcessResource.ProcessStatus(
            phase,
            0,
            null,
            null,
            null,
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            null,
            null,
            null,
            null,
            null,
            finishedAt));
  }
}
