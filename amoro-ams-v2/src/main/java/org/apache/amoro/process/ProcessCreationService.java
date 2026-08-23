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

import org.apache.amoro.persistence.exception.PersistenceOutcomeUnknownException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The sole Process creation transaction for manual, scheduled and future internal entry points. A
 * single-instance admission lease covers one aggregate snapshot read through durable create
 * completion. Unknown outcomes retain an in-memory scope reservation until explicit repair.
 */
public final class ProcessCreationService {

  private static final AtomicLong NAME_SEQUENCE = new AtomicLong();

  private final ProcessDomainAssembly assembly;
  private final Clock clock;
  private final Supplier<String> nameSupplier;
  private final ProcessAdmissionRegistry admissions;
  private final ProcessResource.RetryPolicy retryPolicy;

  public ProcessCreationService(ProcessDomainAssembly assembly) {
    this(assembly, new ProcessResource.RetryPolicy(3, 2, 30));
  }

  public ProcessCreationService(
      ProcessDomainAssembly assembly, ProcessResource.RetryPolicy retryPolicy) {
    this(
        assembly,
        Clock.systemUTC(),
        ProcessCreationService::nextName,
        Duration.ofSeconds(10),
        retryPolicy);
  }

  public ProcessCreationService(
      ProcessDomainAssembly assembly,
      Clock clock,
      Supplier<String> nameSupplier,
      Duration lockTimeout) {
    this(assembly, clock, nameSupplier, lockTimeout, new ProcessResource.RetryPolicy(3, 2, 30));
  }

  public ProcessCreationService(
      ProcessDomainAssembly assembly,
      Clock clock,
      Supplier<String> nameSupplier,
      Duration lockTimeout,
      ProcessResource.RetryPolicy retryPolicy) {
    this.assembly = Objects.requireNonNull(assembly, "assembly");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.nameSupplier = Objects.requireNonNull(nameSupplier, "nameSupplier");
    this.admissions = new ProcessAdmissionRegistry(lockTimeout);
    ProcessResource.RetryPolicy policy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    if (policy.maxRetries() < 0
        || policy.maxRetries() > 3
        || policy.maxSubmissionRetries() < 0
        || policy.maxSubmissionRetries() > 2
        || policy.retryDelaySeconds() < 1
        || policy.retryDelaySeconds() > 86_400) {
      throw new IllegalArgumentException("retryPolicy is outside the server-supported bounds");
    }
    this.retryPolicy =
        new ProcessResource.RetryPolicy(
            policy.maxRetries(), policy.maxSubmissionRetries(), policy.retryDelaySeconds());
  }

  public ProcessCreationResult create(ProcessCreateIntent intent) {
    Objects.requireNonNull(intent, "intent");
    String scope = intent.admissionScope();
    try (ProcessAdmissionRegistry.Lease ignored = admissions.acquire(scope)) {
      Optional<ProcessAdmissionRegistry.Reservation> unresolved = admissions.reservation(scope);
      if (unresolved.isPresent()) {
        ProcessAdmissionRegistry.Reservation reservation = unresolved.get();
        throw new ProcessAdmissionException(
            ProcessAdmissionException.Code.ADMISSION_IN_PROGRESS,
            "create outcome for process "
                + reservation.processName()
                + " is unresolved at "
                + scope);
      }

      ProcessIndexSnapshot snapshot = assembly.indexProjection().current();
      Optional<String> holder =
          snapshot.idempotentHolderOf(
              intent.table().tableId(), intent.action(), intent.idempotencyKeyHash());
      if (holder.isPresent()) {
        ProcessResource original =
            snapshot
                .find(holder.get())
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "aggregate idempotency holder has no resource body: " + holder.get()));
        if (original.spec().request().requestHash().equals(intent.requestHash())) {
          return new ProcessCreationResult(original, true);
        }
        throw new ProcessAdmissionException(
            ProcessAdmissionException.Code.IDEMPOTENCY_KEY_REUSED,
            "the idempotency key was already used for a different Process intent");
      }

      Optional<String> active = snapshot.activeProcessOf(intent.table().tableId(), intent.action());
      if (active.isPresent()) {
        throw new ProcessAdmissionException(
            ProcessAdmissionException.Code.ACTIVE_PROCESS_EXISTS,
            "active process " + active.get() + " already occupies " + scope);
      }

      String name = Objects.requireNonNull(nameSupplier.get(), "generated process name");
      ProcessResource candidate = newResource(name, intent);
      try {
        return new ProcessCreationResult(assembly.repository().create(candidate), false);
      } catch (PersistenceOutcomeUnknownException unknown) {
        admissions.reserve(scope, name, intent.idempotencyKeyHash(), intent.requestHash());
        throw unknown;
      } catch (ProcessIndexConflictException conflict) {
        ProcessAdmissionException.Code code =
            "IDEMPOTENCY_KEY".equals(conflict.conflictType())
                ? ProcessAdmissionException.Code.IDEMPOTENCY_KEY_REUSED
                : ProcessAdmissionException.Code.ACTIVE_PROCESS_EXISTS;
        throw new ProcessAdmissionException(code, conflict.getMessage());
      }
    }
  }

  /** Repairs a previously unknown create and releases its scope reservation only after publish. */
  public void repairUnknown(String processName) {
    String scope =
        admissions
            .scopeForProcess(processName)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "no unknown Process creation reservation for " + processName));
    try (ProcessAdmissionRegistry.Lease ignored = admissions.acquire(scope)) {
      assembly.persistence().repair(processName);
      admissions.clear(scope, processName);
    }
  }

  private ProcessResource newResource(String name, ProcessCreateIntent intent) {
    ProcessResource.ProcessSpec spec =
        new ProcessResource.ProcessSpec(
            intent.table(),
            intent.action(),
            intent.executionEngine(),
            intent.triggerSource(),
            Instant.now(clock).toString(),
            "RUN",
            new ProcessResource.RequestIdentity(intent.idempotencyKeyHash(), intent.requestHash()),
            intent.parameters(),
            retryPolicy);
    ProcessResource.ProcessAttempt attempt =
        new ProcessResource.ProcessAttempt(
            0,
            name + ":0:0",
            ProcessRequestHashes.actionAttempt(name, 0, spec),
            "CREATED",
            null,
            null,
            "AUTO",
            null,
            new ArrayList<ProcessResource.SubmissionSummary>(),
            new ProcessResource.ManualResolutions(null, null));
    ProcessResource.ProcessStatus status =
        new ProcessResource.ProcessStatus(
            "PENDING",
            0,
            attempt,
            null,
            null,
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            null,
            null,
            null,
            null,
            null,
            null);
    return new ProcessResource(name, spec, status);
  }

  private static String nextName() {
    long id = (System.currentTimeMillis() << 20) | (NAME_SEQUENCE.incrementAndGet() & 0xFFFFF);
    return Long.toString(id);
  }
}
