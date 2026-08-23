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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.persistence.blob.BlobStore;
import org.apache.amoro.persistence.exception.PersistenceOutcomeUnknownException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** The only Process creation path: replay, active admission, concurrency and unknown outcomes. */
@Timeout(60)
public class TestProcessCreationService {

  private DefaultScheduler scheduler;
  private CountingBlobStore blob;
  private ProcessDomainAssembly assembly;
  private ExecutorService callers;

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(1, 1_000L);
    scheduler.start();
    blob = new CountingBlobStore();
    assembly =
        new ProcessDomainAssembly(
            blob, event -> HandoffResult.ACCEPTED, scheduler, 128, 10_000L, 65_536);
    callers = Executors.newFixedThreadPool(2);
  }

  @AfterEach
  public void tearDown() {
    callers.shutdownNow();
    assembly.persistence().shutdown(Duration.ofSeconds(5));
    scheduler.shutdown(Duration.ofSeconds(5));
  }

  @Test
  public void sameIntentReplaysAndKeyReuseOrActiveConflictFails() {
    ProcessCreationService service = service(new SequenceNames());

    ProcessCreationResult first = service.create(intent("key-1", "request-1"));
    ProcessCreationResult replay = service.create(intent("key-1", "request-1"));
    assertFalse(first.replayed());
    assertTrue(replay.replayed());
    assertEquals(first.resource().name(), replay.resource().name());
    assertEquals(1, blob.inserts.get());

    assertAdmission(
        ProcessAdmissionException.Code.IDEMPOTENCY_KEY_REUSED,
        () -> service.create(intent("key-1", "different-request")));
    assertAdmission(
        ProcessAdmissionException.Code.ACTIVE_PROCESS_EXISTS,
        () -> service.create(intent("key-2", "request-2")));
    assertEquals(1, blob.inserts.get());
  }

  @Test
  public void concurrentDifferentKeysPersistExactlyOneActiveRow() throws Exception {
    ProcessCreationService service = service(new SequenceNames());
    CountDownLatch start = new CountDownLatch(1);
    Future<Object> first = callers.submit(() -> createAfter(start, service, intent("key-1", "r1")));
    Future<Object> second = callers.submit(() -> createAfter(start, service, intent("key-2", "r2")));
    start.countDown();

    Object firstResult = first.get(10, TimeUnit.SECONDS);
    Object secondResult = second.get(10, TimeUnit.SECONDS);
    int successes = countType(firstResult, ProcessCreationResult.class) + countType(secondResult, ProcessCreationResult.class);
    int activeConflicts =
        countAdmission(firstResult, ProcessAdmissionException.Code.ACTIVE_PROCESS_EXISTS)
            + countAdmission(secondResult, ProcessAdmissionException.Code.ACTIVE_PROCESS_EXISTS);
    assertEquals(1, successes);
    assertEquals(1, activeConflicts);
    assertEquals(1, blob.rows.size());
    assertEquals(1, assembly.indexProjection().current().resourcesByName().size());
  }

  @Test
  public void lockTimeoutIsReportedAsAdmissionInProgress() throws Exception {
    CountDownLatch nameEntered = new CountDownLatch(1);
    CountDownLatch releaseName = new CountDownLatch(1);
    ProcessCreationService service =
        new ProcessCreationService(
            assembly,
            fixedClock(),
            () -> {
              nameEntered.countDown();
              await(releaseName);
              return "p1";
            },
            Duration.ofMillis(50));

    Future<ProcessCreationResult> first =
        callers.submit(() -> service.create(intent("key-1", "request-1")));
    assertTrue(nameEntered.await(5, TimeUnit.SECONDS));
    assertAdmission(
        ProcessAdmissionException.Code.ADMISSION_IN_PROGRESS,
        () -> service.create(intent("key-2", "request-2")));

    releaseName.countDown();
    assertEquals("p1", first.get(10, TimeUnit.SECONDS).resource().name());
  }

  @Test
  public void unknownCreateReservesScopeUntilRepairPublishesDurableFact() {
    blob.failNextCommittedInsertOutcome = true;
    ProcessCreationService service =
        new ProcessCreationService(
            assembly, fixedClock(), () -> "p1", Duration.ofSeconds(1));

    assertThrows(
        PersistenceOutcomeUnknownException.class,
        () -> service.create(intent("key-1", "request-1")));
    assertAdmission(
        ProcessAdmissionException.Code.ADMISSION_IN_PROGRESS,
        () -> service.create(intent("key-2", "request-2")));
    assertEquals(1, blob.inserts.get());
    assertEquals(Optional.empty(), assembly.indexProjection().current().find("p1"));

    service.repairUnknown("p1");
    assertEquals(Optional.of("p1"), assembly.indexProjection().current().find("p1").map(ProcessResource::name));
    ProcessCreationResult replay = service.create(intent("key-1", "request-1"));
    assertTrue(replay.replayed());
    assertAdmission(
        ProcessAdmissionException.Code.ACTIVE_PROCESS_EXISTS,
        () -> service.create(intent("key-2", "request-2")));
    assertEquals(1, blob.inserts.get());
  }

  private ProcessCreationService service(java.util.function.Supplier<String> names) {
    return new ProcessCreationService(assembly, fixedClock(), names, Duration.ofSeconds(1));
  }

  private static ProcessCreateIntent intent(String key, String request) {
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("simulated", true);
    return new ProcessCreateIntent(
        new ProcessResource.TableRef("prod", "db", "table", "42"),
        "expire-snapshots",
        "local",
        "MANUAL",
        "sha256:" + key,
        "sha256:" + request,
        parameters);
  }

  private static Clock fixedClock() {
    return Clock.fixed(Instant.parse("2026-08-22T10:00:00Z"), ZoneOffset.UTC);
  }

  private static Object createAfter(
      CountDownLatch start, ProcessCreationService service, ProcessCreateIntent intent) {
    await(start);
    try {
      return service.create(intent);
    } catch (RuntimeException failure) {
      return failure;
    }
  }

  private static int countType(Object value, Class<?> type) {
    return type.isInstance(value) ? 1 : 0;
  }

  private static int countAdmission(Object value, ProcessAdmissionException.Code code) {
    return value instanceof ProcessAdmissionException
            && ((ProcessAdmissionException) value).code() == code
        ? 1
        : 0;
  }

  private static void assertAdmission(
      ProcessAdmissionException.Code code, org.junit.jupiter.api.function.Executable operation) {
    ProcessAdmissionException failure =
        assertThrows(ProcessAdmissionException.class, operation);
    assertEquals(code, failure.code());
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("test latch timed out");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("test interrupted", e);
    }
  }

  private static final class SequenceNames implements java.util.function.Supplier<String> {
    private final AtomicInteger sequence = new AtomicInteger();

    @Override
    public String get() {
      return "p" + sequence.incrementAndGet();
    }
  }

  private static final class CountingBlobStore implements BlobStore {
    private final ConcurrentHashMap<String, byte[]> rows = new ConcurrentHashMap<>();
    private final AtomicInteger inserts = new AtomicInteger();
    private volatile boolean failNextCommittedInsertOutcome;
    private volatile boolean failResolutionRead;

    @Override
    public void insert(String collection, String name, byte[] value) {
      inserts.incrementAndGet();
      if (rows.putIfAbsent(name, value) != null) {
        throw new IllegalStateException("duplicate " + name);
      }
      if (failNextCommittedInsertOutcome) {
        failNextCommittedInsertOutcome = false;
        failResolutionRead = true;
        throw new RuntimeException("connection failed after committed insert");
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
      if (failResolutionRead) {
        failResolutionRead = false;
        throw new RuntimeException("resolution read unavailable");
      }
      return Optional.ofNullable(rows.get(name));
    }

    @Override
    public void forEach(String collection, java.util.function.BiConsumer<String, byte[]> action) {
      rows.forEach(action);
    }
  }
}
