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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.process.rest.ProcessRestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/** P8: the TTL cleaner — only old final resources leave, in bounded batches. */
@Timeout(60)
public class TestProcessTtlCleaner {

  private DefaultScheduler scheduler;
  private ProcessDomainAssembly assembly;
  private ProcessRestSupport rest;
  private ProcessTtlCleaner cleaner;

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(1, 1000L);
    assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            scheduler,
            128,
            10_000L,
            65536);
    rest = new ProcessRestSupport(assembly);
    cleaner = new ProcessTtlCleaner(assembly);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
  }

  private String createTerminal(String key, String phase) {
    org.apache.amoro.process.ProcessResource created =
        rest.create("prod", "db", "t-" + key, "ttl-" + key, "expire-snapshots", "local", null)
            .resource;
    rest.forceTerminal(created.name(), phase);
    return created.name();
  }

  @Test
  public void deletesOnlyFinalResourcesPastRetention() {
    String old = createTerminal("old", "SUCCESS");
    String fresh = createTerminal("fresh", "SUCCESS");
    String active =
        rest.create("prod", "db", "t-live", "ttl-live", "expire-snapshots", "local", null)
            .resource
            .name(); // PENDING: never eligible

    // force `old` past the retention window by faking the clock 40 days ahead
    int deleted = cleaner.cleanOnce(Instant.now().plus(Duration.ofDays(40)), 30, 100);

    assertEquals(2, deleted, "old and fresh terminals both pass the 30d cutoff at clock+40d");
    assertFalse(assembly.indexProjection().current().find(old).isPresent());
    assertFalse(assembly.indexProjection().current().find(fresh).isPresent());
    assertTrue(
        assembly.indexProjection().current().find(active).isPresent(),
        "the active process is never touched");

    // at the real clock neither is old enough
    String kept = createTerminal("kept", "SUCCESS");
    int none = cleaner.cleanOnce(Instant.now(), 30, 100);
    assertEquals(0, none);
    assertTrue(assembly.indexProjection().current().find(kept).isPresent());
  }

  @Test
  public void batchesAreBounded() {
    for (int i = 0; i < 5; i++) {
      createTerminal("batch-" + i, "SUCCESS");
    }
    int deleted = cleaner.cleanOnce(Instant.now().plus(Duration.ofDays(40)), 30, 2);
    assertEquals(2, deleted, "the batch cap limits one round to two deletes");
    // the remaining terminals still stand; the next round picks them up
    int deletedAgain = cleaner.cleanOnce(Instant.now().plus(Duration.ofDays(40)), 30, 10);
    assertEquals(3, deletedAgain);
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> assembly.indexProjection().current().expiryOrder().isEmpty());
  }
}
