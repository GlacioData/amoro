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

import org.apache.amoro.persistence.exception.PreconditionFailedException;
import org.apache.amoro.persistence.facade.RepositoryFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/** Durable command boundary: read, derive once, then submit one expected-version CAS. */
public final class ProcessCommandService {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessCommandService.class);
  private static final int MAX_CAS_ATTEMPTS = 4;

  private final RepositoryFacade<ProcessResource> repository;
  private final Clock clock;

  public ProcessCommandService(RepositoryFacade<ProcessResource> repository) {
    this(repository, Clock.systemUTC());
  }

  public ProcessCommandService(RepositoryFacade<ProcessResource> repository, Clock clock) {
    this.repository = Objects.requireNonNull(repository, "repository");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public CommandResult resolve(ManualResolutionTransition.Command command, String processName) {
    for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      ProcessResource current = repository.get(processName);
      ManualResolutionTransition.Result transition =
          ManualResolutionTransition.apply(current, command, Instant.now(clock).toString());
      if (transition.replayed()) {
        return new CommandResult(current, true);
      }
      try {
        ProcessResource updated =
            repository.modify(
                processName,
                current.resourceVersion(),
                resource -> resource.withStatus(transition.status()));
        return new CommandResult(updated, false);
      } catch (PreconditionFailedException raced) {
        // Re-read: an identical winner becomes an idempotent replay; an incompatible winner is
        // classified by the pure transition as a stable conflict.
      }
    }
    throw new ProcessCommandException(
        ProcessCommandException.Code.PRECONDITION_FAILED,
        "the Process kept changing concurrently; the resolution was not acknowledged");
  }

  public ProcessResource requestCancel(String processName, String reason) {
    if (reason == null || reason.trim().isEmpty()) {
      throw new ProcessCommandException(
          ProcessCommandException.Code.VALIDATION_FAILED, "cancel reason is required");
    }
    for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      ProcessResource current = repository.get(processName);
      ProcessResource next =
          ToCancelTransition.requestCancel(current, Instant.now(clock).toString());
      if (next == current) {
        return current;
      }
      try {
        ProcessResource updated =
            repository.modify(processName, current.resourceVersion(), ignored -> next);
        LOG.info(
            "Process {} cancel intent accepted; reason={}", processName, sanitizeReason(reason));
        return updated;
      } catch (PreconditionFailedException raced) {
        // Re-read and re-derive so a concurrent observation cannot silently discard cancellation.
      }
    }
    throw new ProcessCommandException(
        ProcessCommandException.Code.PRECONDITION_FAILED,
        "the Process kept changing concurrently; cancellation was not acknowledged");
  }

  private static String sanitizeReason(String reason) {
    String compact = reason.replace('\n', ' ').replace('\r', ' ').trim();
    return compact.length() <= 256 ? compact : compact.substring(0, 256);
  }

  public static final class CommandResult {
    private final ProcessResource resource;
    private final boolean replayed;

    private CommandResult(ProcessResource resource, boolean replayed) {
      this.resource = resource;
      this.replayed = replayed;
    }

    public ProcessResource resource() {
      return resource;
    }

    public boolean replayed() {
      return replayed;
    }
  }
}
