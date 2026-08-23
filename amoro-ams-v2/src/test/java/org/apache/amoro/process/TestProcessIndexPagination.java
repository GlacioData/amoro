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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Regression coverage for rank-based Process pagination and overflow-safe page bounds. */
public class TestProcessIndexPagination {

  @Test
  public void rankPagesAreStableCompleteAndFilterable() {
    ProcessIndexSnapshot snapshot = ProcessIndexSnapshot.empty();
    List<String> expected = new ArrayList<>();
    for (int index = 0; index < 123; index++) {
      ProcessResource resource = terminal(index, "dummy-maintenance", "SUCCESS");
      snapshot = snapshot.apply(null, resource);
      expected.add(resource.name());
    }
    ProcessResource otherAction = terminal(200, "dummy-secondary", "SUCCESS");
    snapshot = snapshot.apply(null, otherAction);

    Collections.reverse(expected);
    List<ProcessResource> first = snapshot.list("table-42", "dummy-maintenance", "SUCCESS", 0L, 50);
    List<ProcessResource> second =
        snapshot.list("table-42", "dummy-maintenance", "SUCCESS", 50L, 50);
    List<ProcessResource> third =
        snapshot.list("table-42", "dummy-maintenance", "SUCCESS", 100L, 50);

    List<String> actual = new ArrayList<>();
    first.forEach(resource -> actual.add(resource.name()));
    second.forEach(resource -> actual.add(resource.name()));
    third.forEach(resource -> actual.add(resource.name()));
    assertEquals(List.of(50, 50, 23), List.of(first.size(), second.size(), third.size()));
    assertEquals(expected, actual, "rank pages must preserve newest-first ordering without gaps");
    Set<String> unique = new HashSet<>(actual);
    assertEquals(actual.size(), unique.size(), "adjacent pages must not duplicate a Process");
    assertEquals(123, snapshot.listTotal("table-42", "dummy-maintenance", "SUCCESS"));
    assertEquals(
        List.of(otherAction.name()),
        snapshot.list("table-42", "dummy-secondary", "SUCCESS", 0L, 50).stream()
            .map(ProcessResource::name)
            .toList());
  }

  @Test
  public void oversizedOffsetsAndLimitsAreBoundedBeforeAllocation() {
    ProcessIndexSnapshot snapshot =
        ProcessIndexSnapshot.empty().apply(null, terminal(1, "dummy-maintenance", "SUCCESS"));

    assertTrue(
        snapshot
            .list("table-42", "dummy-maintenance", "SUCCESS", Integer.MAX_VALUE + 1L, 50)
            .isEmpty());
    assertTrue(
        snapshot.list("table-42", "dummy-maintenance", "SUCCESS", Long.MAX_VALUE, 50).isEmpty());
    assertEquals(
        1,
        snapshot.list("table-42", "dummy-maintenance", "SUCCESS", 0L, Integer.MAX_VALUE).size(),
        "an untrusted huge limit must be capped to the remaining rank-view size");
  }

  private static ProcessResource terminal(int sequence, String action, String phase) {
    String name = String.format("process-%03d", sequence);
    String createdAt = Instant.parse("2026-08-22T10:00:00Z").plusSeconds(sequence).toString();
    ProcessResource.ProcessSpec spec =
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("prod", "db", "table", "table-42", "simulated"),
            action,
            "local",
            "MANUAL",
            createdAt,
            "RUN",
            new ProcessResource.RequestIdentity(
                "sha256:idempotency-" + sequence + "-" + action,
                "sha256:request-" + sequence + "-" + action),
            Collections.singletonMap("simulated", true),
            new ProcessResource.RetryPolicy(3, 2, 30));
    ProcessResource.ProcessStatus status =
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
            createdAt);
    return new ProcessResource(name, spec, status);
  }
}
