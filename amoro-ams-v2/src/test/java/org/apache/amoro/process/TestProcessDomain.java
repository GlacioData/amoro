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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.persistence.ListenerEventSink;
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
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
            new ProcessResource.TableRef("prod", "db", "t", tableId, "simulated"),
            "dummy-maintenance",
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

    assertEquals(Optional.of("p1"), snapshot.activeProcessOf("42", "dummy-maintenance"));
    assertEquals(
        Optional.of("p1"), snapshot.idempotentHolderOf("42", "dummy-maintenance", "sha256:key-p1"));
    assertTrue(snapshot.find("p1").isPresent());

    // a different table/action combination is independent
    assertEquals(Optional.empty(), snapshot.activeProcessOf("43", "dummy-maintenance"));
  }

  @Test
  public void secondActiveResourceIsRejectedBeforeDurableInsert() {
    assembly.repository().create(newResource("p1", "42"));

    ProcessIndexConflictException conflict =
        assertThrows(
            ProcessIndexConflictException.class,
            () -> assembly.repository().create(newResource("p2", "42")));

    assertEquals("ACTIVE_PROCESS", conflict.conflictType());
    assertEquals(1, blob.rows.size());
    assertFalse(blob.rows.containsKey("p2"));
    assertEquals(
        Optional.of("p1"),
        assembly.indexProjection().current().activeProcessOf("42", "dummy-maintenance"));
  }

  @Test
  public void restartFailsClosedWhenDurableRowsContainDuplicateActiveScope() {
    assembly.repository().create(newResource("p1", "42"));
    TestPersistenceBlobStore otherBlob = new TestPersistenceBlobStore();
    ProcessDomainAssembly other =
        new ProcessDomainAssembly(
            otherBlob, event -> HandoffResult.ACCEPTED, scheduler, 128, 10_000L, 65536);
    ProcessDomainAssembly restarted = null;
    try {
      other.repository().create(newResource("p2", "42"));
      blob.rows.put("p2", otherBlob.rows.get("p2"));
      restarted =
          new ProcessDomainAssembly(
              blob, event -> HandoffResult.ACCEPTED, scheduler, 128, 10_000L, 65536);
      ProcessDomainAssembly conflicting = restarted;

      CompletionException failure =
          assertThrows(CompletionException.class, () -> conflicting.persistence().postStart());
      assertTrue(failure.getCause() instanceof ProcessIndexConflictException);
      String message = failure.getCause().getMessage();
      assertTrue(message.contains("p1"));
      assertTrue(message.contains("p2"));
    } finally {
      other.persistence().shutdown(Duration.ofSeconds(5));
      if (restarted != null) {
        restarted.persistence().shutdown(Duration.ofSeconds(5));
      }
    }
  }

  @Test
  public void invalidDomainMutationFailsBeforeDurableWriteAndProjectionPublish() {
    ProcessResource created = assembly.repository().create(newResource("p1", "42"));
    byte[] durableBefore = blob.rows.get("p1").clone();
    ProcessResource.ProcessSpec spec = created.spec();
    ProcessResource.ProcessSpec changedAction =
        new ProcessResource.ProcessSpec(
            spec.table(),
            "dummy-secondary",
            spec.executionEngine(),
            spec.triggerSource(),
            spec.createdAt(),
            spec.desiredState(),
            spec.request(),
            spec.parameters(),
            spec.retryPolicy());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            assembly
                .repository()
                .modify(
                    created.name(),
                    created.resourceVersion(),
                    resource -> resource.withSpec(changedAction)));

    assertArrayEquals(durableBefore, blob.rows.get("p1"));
    assertEquals(created, assembly.repository().get("p1"));
    assertEquals(
        Optional.of("p1"),
        assembly.indexProjection().current().activeProcessOf("42", "dummy-maintenance"));
    assertEquals(
        Optional.empty(),
        assembly.indexProjection().current().activeProcessOf("42", "dummy-secondary"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            assembly
                .repository()
                .create(
                    newResource("p2", "43")
                        .withStatus(newResource("p2", "43").status().withPhase("BROKEN"))));
    assertEquals(1, blob.rows.size(), "invalid create must fail before the durable insert");
  }

  @Test
  public void finalReleasesAdmissionAndExpiryTracksIt() {
    assembly.repository().create(submittedResource("p1", "42", 3));
    ProcessTestFixtures.forceTerminal(assembly, "p1", "SUCCESS");
    ProcessResource terminal = assembly.repository().get("p1");

    assertTrue(ProcessFinality.isFinal(terminal));
    ProcessIndexSnapshot snapshot = assembly.indexProjection().current();
    assertEquals(
        Optional.empty(),
        snapshot.activeProcessOf("42", "dummy-maintenance"),
        "final resources release the admission slot");
    assertEquals(1, snapshot.expiryOrder().size());
    assertTrue(snapshot.expiryOrder().get(0).endsWith("|p1"));

    // the same table/action can admit a new process afterwards
    assembly.repository().create(newResource("p2", "42"));
    assertEquals(
        Optional.of("p2"),
        assembly.indexProjection().current().activeProcessOf("42", "dummy-maintenance"));
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
    assembly.repository().create(submittedResource("p1", "42", 3));
    ProcessResultApplier retryableApplier =
        new ProcessResultApplier(
            assembly.repository(), () -> "2026-08-22T10:01:00Z", 4, 1_000L, 60_000L, 300_000L);
    retryableApplier.applyObservation(
        "p1",
        "p1:0:0",
        "sha256:req-p1",
        "dummy-p1",
        org.apache.amoro.process.engine.EngineTypes.ProcessObservation.known(
            new org.apache.amoro.process.engine.EngineTypes.EngineObservation(
                "FAILED",
                null,
                java.util.Collections.singletonMap("simulated", true),
                new org.apache.amoro.process.engine.EngineTypes.EngineFailure(
                    "DUMMY", "boom", true))),
        null);
    ProcessResource failed = assembly.repository().get("p1");
    assertFalse(ProcessFinality.isFinal(failed), "budget remaining FAILED is retryable, not final");
    assertEquals(
        Optional.of("p1"),
        assembly.indexProjection().current().activeProcessOf("42", "dummy-maintenance"));

    // With maxRetries=0, the same authoritative failure is final and releases admission.
    assembly.repository().create(submittedResource("at-cap", "43", 0));
    retryableApplier.applyObservation(
        "at-cap",
        "at-cap:0:0",
        "sha256:req-at-cap",
        "dummy-at-cap",
        org.apache.amoro.process.engine.EngineTypes.ProcessObservation.known(
            new org.apache.amoro.process.engine.EngineTypes.EngineObservation(
                "FAILED",
                null,
                java.util.Collections.singletonMap("simulated", true),
                new org.apache.amoro.process.engine.EngineTypes.EngineFailure(
                    "DUMMY", "boom", true))),
        null);
    ProcessResource atCap = assembly.repository().get("at-cap");
    assertTrue(ProcessFinality.isFinal(atCap));
    assertEquals(
        Optional.empty(),
        assembly.indexProjection().current().activeProcessOf("43", "dummy-maintenance"));
  }

  private static ProcessResource submittedResource(String name, String tableId, int maxRetries) {
    ProcessResource base = newResource(name, tableId);
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            name + ":0:0",
            "sha256:req-" + name,
            "ACKNOWLEDGED",
            "dummy-" + name,
            "2026-08-22T10:00:05Z",
            null,
            "AUTO",
            null,
            java.util.Collections.emptyList(),
            new ProcessResource.ManualResolutions(null, null));
    ProcessResource.ProcessSpec spec =
        new ProcessResource.ProcessSpec(
            base.spec().table(),
            base.spec().action(),
            base.spec().executionEngine(),
            base.spec().triggerSource(),
            base.spec().createdAt(),
            base.spec().desiredState(),
            base.spec().request(),
            base.spec().parameters(),
            new ProcessResource.RetryPolicy(maxRetries, 2, 30));
    ProcessResource.ProcessStatus status =
        new ProcessResource.ProcessStatus(
            "SUBMITTED",
            0,
            attempt,
            java.util.Collections.emptyList(),
            null,
            null,
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            java.util.Collections.emptyList(),
            null,
            null,
            "2026-08-22T10:00:06Z",
            null,
            null);
    return new ProcessResource(name, spec, status);
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
