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

import org.apache.amoro.process.rest.ProcessActionCatalog;
import org.apache.amoro.process.rest.ProcessRestSupport;

import java.time.Instant;

/** Test-only durable state staging for API and lifecycle tests. */
public final class ProcessTestFixtures {

  private ProcessTestFixtures() {}

  /** Explicit simulated REST fixture; production/default constructors intentionally expose none. */
  public static ProcessRestSupport simulatedRestSupport(ProcessDomainAssembly assembly) {
    return simulatedRestSupport(assembly, new ProcessCreationService(assembly));
  }

  public static ProcessRestSupport simulatedRestSupport(
      ProcessDomainAssembly assembly, ProcessCreationService creationService) {
    return new ProcessRestSupport(
        assembly,
        new ProcessRestSupport.TableCatalogPort() {
          @Override
          public ProcessRestSupport.TableIdentity resolve(
              String catalog, String database, String table) {
            return "ghost-table".equals(table) || "ghost".equals(database)
                ? null
                : new ProcessRestSupport.TableIdentity(
                    Integer.toUnsignedString(java.util.Objects.hash(catalog, database, table), 16),
                    "simulated");
          }
        },
        creationService,
        ProcessActionCatalog.simulatedRoutingFixtures());
  }

  public static void forceSubmissionUnresolved(ProcessDomainAssembly assembly, String processName) {
    forceUnresolved(assembly, processName, ProcessConditions.SUBMISSION_UNRESOLVED);
  }

  public static void forceExecutionUnresolved(ProcessDomainAssembly assembly, String processName) {
    ProcessResource current = assembly.repository().get(processName);
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    String now = Instant.now().toString();
    ProcessResource.ProcessAttempt acknowledged =
        new ProcessResource.ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            "ACKNOWLEDGED",
            "dummy-execution-" + processName,
            attempt.dispatchedAt() == null ? now : attempt.dispatchedAt(),
            null,
            attempt.retryDisposition(),
            null,
            attempt.submissionHistory(),
            attempt.manualResolutions());
    ProcessResource.ProcessStatus staged =
        new ProcessResource.ProcessStatus(
            "SUBMITTED",
            status.retryNumber(),
            acknowledged,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            now,
            status.engineBackoffAttempts(),
            ProcessConditions.set(
                ProcessConditions.remove(
                    status.conditions(),
                    ProcessConditions.SUBMISSION_UNRESOLVED,
                    ProcessConditions.EXECUTION_UNRESOLVED),
                ProcessConditions.EXECUTION_UNRESOLVED,
                "TestFixture",
                "simulated unresolved execution identity",
                now,
                null),
            status.summary(),
            null,
            now,
            status.startedAt(),
            null);
    assembly
        .repository()
        .modify(processName, current.resourceVersion(), resource -> resource.withStatus(staged));
  }

  private static void forceUnresolved(
      ProcessDomainAssembly assembly, String processName, String conditionType) {
    ProcessResource current = assembly.repository().get(processName);
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    String now = Instant.now().toString();
    ProcessResource.ProcessAttempt dispatching =
        new ProcessResource.ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            "DISPATCHING",
            null,
            attempt.dispatchedAt() == null ? now : attempt.dispatchedAt(),
            null,
            attempt.retryDisposition(),
            null,
            attempt.submissionHistory(),
            attempt.manualResolutions());
    ProcessResource.ProcessStatus staged =
        new ProcessResource.ProcessStatus(
            status.phase(),
            status.retryNumber(),
            dispatching,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            now,
            status.engineBackoffAttempts(),
            ProcessConditions.set(
                ProcessConditions.remove(
                    status.conditions(),
                    ProcessConditions.SUBMISSION_UNRESOLVED,
                    ProcessConditions.EXECUTION_UNRESOLVED),
                conditionType,
                "TestFixture",
                "simulated unresolved identity",
                now,
                null),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            null);
    assembly
        .repository()
        .modify(processName, current.resourceVersion(), resource -> resource.withStatus(staged));
  }

  public static void forceTerminal(
      ProcessDomainAssembly assembly, String processName, String phase) {
    ProcessResource current = assembly.repository().get(processName);
    ProcessResource.ProcessStatus status = current.status();
    ProcessResource.ProcessAttempt attempt = status.attempt();
    String now = Instant.now().toString();
    ProcessResource.ProcessAttempt closed =
        new ProcessResource.ProcessAttempt(
            attempt.dispatchGeneration(),
            attempt.submissionKey(),
            attempt.requestHash(),
            attempt.submitState(),
            attempt.externalId(),
            attempt.dispatchedAt(),
            attempt.lastError(),
            "FINAL",
            now,
            attempt.submissionHistory(),
            attempt.manualResolutions());
    ProcessResource.ProcessStatus terminal =
        new ProcessResource.ProcessStatus(
            phase,
            status.retryNumber(),
            closed,
            status.attemptHistory(),
            status.lastObservedAt(),
            status.lastCancelAttemptAt(),
            null,
            new ProcessResource.EngineBackoff(0, 0, 0, 0),
            status.conditions(),
            status.summary(),
            null,
            status.submittedAt(),
            status.startedAt(),
            now);
    assembly
        .repository()
        .modify(processName, current.resourceVersion(), resource -> resource.withStatus(terminal));
  }
}
