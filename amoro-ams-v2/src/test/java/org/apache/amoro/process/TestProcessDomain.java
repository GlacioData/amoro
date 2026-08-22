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
import org.apache.amoro.persistence.ListenerEventSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** P2: the Process domain assembly over a fake durable store — index views + hook semantics. */
@Timeout(60)
public class TestProcessDomain {

  private DefaultScheduler scheduler;
  private ProcessDomainAssembly assembly;
  private final TestPersistenceBlobStore blob = new TestPersistenceBlobStore();

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(2, 100L);
    scheduler.start();
    ListenerEventSink<ProcessResource> sink = event -> HandoffResult.ACCEPTED;
    assembly = new ProcessDomainAssembly(blob, sink, scheduler, 128, 10_000L, 65536);
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
  }

  private static ProcessResource newResource(String name, String tableId) {
    Map<String, Object> parameters = new LinkedHashMap<String, Object>();
    parameters.put("retainLast", 1);
    return new ProcessResource(
        name,
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("prod", "db", "t", tableId),
            "expire-snapshots",
            "local",
            "MANUAL",
            "2026-08-22T10:00:00Z",
            "RUN",
            new ProcessResource.RequestIdentity("sha256:key-" + name, "sha256:req-" + name),
            parameters,
            new ProcessResource.RetryPolicy(3, 2, 30)),
        new ProcessResource.ProcessStatus(
            "PENDING",
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
            null));
  }

  @Test
  public void admissionAndViewMaintenance() {
    assembly.repository().create(newResource("p1", "42"));
    ProcessIndexSnapshot snapshot = assembly.indexProjection().current();

    assertEquals(Optional.of("p1"), snapshot.activeProcessOf("42", "expire-snapshots"));
    assertEquals(
        Optional.of("p1"), snapshot.idempotentHolderOf("42", "expire-snapshots", "sha256:key-p1"));
    assertTrue(snapshot.find("p1").isPresent());

    // a different table/action combination is independent
    assertEquals(Optional.empty(), snapshot.activeProcessOf("43", "expire-snapshots"));
  }

  @Test
  public void finalReleasesAdmissionAndExpiryTracksIt() {
    assembly.repository().create(newResource("p1", "42"));
    ProcessResource created = assembly.repository().get("p1");
    ProcessResource terminal =
        assembly
            .repository()
            .modify(
                "p1",
                created.resourceVersion(),
                r ->
                    r.withStatus(
                        new ProcessResource.ProcessStatus(
                            "SUCCESS",
                            0,
                            r.status().attempt(),
                            r.status().attemptHistory(),
                            "2026-08-22T10:01:00Z",
                            null,
                            r.status().engineBackoffAttempts(),
                            r.status().conditions(),
                            r.status().summary(),
                            null,
                            "2026-08-22T10:00:06Z",
                            "2026-08-22T10:00:10Z",
                            "2026-08-22T10:01:00Z")));

    assertTrue(ProcessFinality.isFinal(terminal));
    ProcessIndexSnapshot snapshot = assembly.indexProjection().current();
    assertEquals(
        Optional.empty(),
        snapshot.activeProcessOf("42", "expire-snapshots"),
        "final resources release the admission slot");
    assertEquals(1, snapshot.expiryOrder().size());
    assertTrue(snapshot.expiryOrder().get(0).endsWith("|p1"));

    // the same table/action can admit a new process afterwards
    assembly.repository().create(newResource("p2", "42"));
    assertEquals(
        Optional.of("p2"),
        assembly.indexProjection().current().activeProcessOf("42", "expire-snapshots"));
  }

  @Test
  public void deletionHookUnschedulesInLane() throws Exception {
    assembly.repository().create(newResource("p1", "42"));
    // a registered controller would sit in the scheduler; simulate one with a far-future
    // deadline so nothing but the deletion hook can remove it
    final boolean[] invoked = {false};
    scheduler.schedule(
        new org.apache.amoro.control.Controller() {
          @Override
          public org.apache.amoro.control.ControllerKey key() {
            return org.apache.amoro.control.ControllerKey.of("process", "p1");
          }

          @Override
          public void invoke() {
            invoked[0] = true;
            throw org.apache.amoro.control.TerminalState.INSTANCE;
          }
        },
        java.time.Duration.ofSeconds(600));
    assertEquals(1, scheduler.registrySize());

    assembly.persistence().delete("p1").toCompletableFuture().join();
    await().atMost(5, TimeUnit.SECONDS).until(() -> scheduler.registrySize() == 0);
    assertFalse(invoked[0]);
    assertEquals(Optional.empty(), assembly.indexProjection().current().find("p1"));
  }

  @Test
  public void failedRetryableStaysActiveUntilBudgetExhausts() {
    assembly.repository().create(newResource("p1", "42"));
    ProcessResource created = assembly.repository().get("p1");
    ProcessResource failed =
        assembly
            .repository()
            .modify(
                "p1", created.resourceVersion(), r -> r.withStatus(r.status().withPhase("FAILED")));
    assertFalse(ProcessFinality.isFinal(failed), "budget remaining FAILED is retryable, not final");
    assertEquals(
        Optional.of("p1"),
        assembly.indexProjection().current().activeProcessOf("42", "expire-snapshots"));

    // drive retryNumber to the cap -> final, admission released
    ProcessResource atCap = failed;
    for (int v = (int) atCap.resourceVersion() + 1; v <= 5; v++) {
      final long version = v - 1;
      atCap =
          assembly
              .repository()
              .modify(
                  "p1",
                  version,
                  r ->
                      r.withStatus(
                          new ProcessResource.ProcessStatus(
                              "FAILED",
                              3,
                              r.status().attempt(),
                              r.status().attemptHistory(),
                              null,
                              null,
                              r.status().engineBackoffAttempts(),
                              r.status().conditions(),
                              r.status().summary(),
                              "boom",
                              null,
                              null,
                              null)));
    }
    assertTrue(ProcessFinality.isFinal(atCap));
    assertEquals(
        Optional.empty(),
        assembly.indexProjection().current().activeProcessOf("42", "expire-snapshots"));
  }

  /** Minimal fake durable store (name -> YAML bytes). */
  public static final class TestPersistenceBlobStore
      implements org.apache.amoro.persistence.blob.BlobStore {
    final java.util.concurrent.ConcurrentHashMap<String, byte[]> rows =
        new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void insert(String collection, String name, byte[] value) {
      if (rows.putIfAbsent(name, value) != null) {
        throw new org.apache.amoro.persistence.exception.ResourceAlreadyExists("process", name);
      }
    }

    @Override
    public boolean update(String collection, String name, byte[] value) {
      return rows.replace(name, value) != null;
    }

    @Override
    public boolean delete(String collection, String name) {
      return rows.remove(name) != null;
    }

    @Override
    public Optional<byte[]> find(String collection, String name) {
      return Optional.ofNullable(rows.get(name));
    }

    @Override
    public void forEach(String collection, java.util.function.BiConsumer<String, byte[]> action) {
      for (Map.Entry<String, byte[]> entry : rows.entrySet()) {
        action.accept(entry.getKey(), entry.getValue());
      }
    }
  }
}
