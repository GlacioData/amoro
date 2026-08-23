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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * The Process TTL cleaner (process spec §9.1): deletes final resources whose finishedAt is older
 * than the retention, in bounded batches, straight from the expiry index order — never a full-cache
 * scan and never TRUNCATE. Only final resources with a stamped finishedAt ever enter the index, so
 * active, retryable-FAILED and unresolved resources can never be deleted.
 */
public final class ProcessTtlCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessTtlCleaner.class);

  private final ProcessDomainAssembly assembly;
  private final org.apache.amoro.process.engine.ExecutionHandleRegistry handleRegistry;
  private final org.apache.amoro.process.engine.ExecutionHandleReleaseIndex releaseIndex;
  private ProcessIndexSnapshot.ExpiryEntry cursor;

  public ProcessTtlCleaner(ProcessDomainAssembly assembly) {
    this(assembly, new org.apache.amoro.process.engine.ExecutionHandleRegistry());
  }

  public ProcessTtlCleaner(
      ProcessDomainAssembly assembly,
      org.apache.amoro.process.engine.ExecutionHandleRegistry handleRegistry) {
    this.assembly = assembly;
    this.handleRegistry = handleRegistry;
    this.releaseIndex = assembly.releaseIndex();
  }

  /** One bounded cleaning round; returns the number of durable deletes issued. */
  public int cleanOnce(Instant now, int retentionDays, int batchSize) {
    if (retentionDays < 7 || batchSize <= 0 || batchSize > 1000) {
      throw new IllegalArgumentException(
          "retentionDays must be >= 7 and batchSize must be in [1, 1000]");
    }
    Instant cutoff = now.minusSeconds((long) retentionDays * 24L * 3600L);
    ProcessIndexSnapshot snapshot = assembly.indexProjection().current();
    List<ProcessIndexSnapshot.ExpiryEntry> expiryOrder = snapshot.expiryAfter(cursor, batchSize);
    if (expiryOrder.isEmpty()) {
      cursor = null;
      return 0;
    }
    int deleted = 0;
    boolean blocked = false;
    for (ProcessIndexSnapshot.ExpiryEntry entry : expiryOrder) {
      if (Instant.parse(entry.finishedAt()).isAfter(cutoff)) {
        cursor = null;
        blocked = true;
        break;
      }
      ProcessResource resource;
      try {
        resource = assembly.repository().get(entry.name());
      } catch (org.apache.amoro.persistence.exception.ResourceDoesNotExist deletedAlready) {
        cursor = entry;
        continue;
      }
      if (resource.resourceVersion() != entry.resourceVersion()
          || !ProcessFinality.isFinal(resource)
          || !entry.finishedAt().equals(resource.status().finishedAt())
          || Instant.parse(resource.status().finishedAt()).isAfter(cutoff)) {
        cursor = entry;
        continue;
      }
      if (handleRegistry.hasPendingHandle(entry.name())
          || releaseIndex.hasPendingForProcess(entry.name())) {
        // spec §9.1: a row may not disappear while its local engine handle is pending
        // release — otherwise the handle leaks forever with no durable trace
        blocked = true;
        break;
      }
      try {
        assembly
            .persistence()
            .delete(entry.name(), entry.resourceVersion())
            .toCompletableFuture()
            .join();
        deleted++;
        cursor = entry;
        LOG.info("TTL deleted final process {} (finishedAt {}).", entry.name(), entry.finishedAt());
      } catch (org.apache.amoro.persistence.exception.PreconditionFailedException raced) {
        blocked = true;
        break; // inclusive retry: do not advance the cursor past this candidate
      } catch (RuntimeException durableFailure) {
        LOG.warn("TTL delete of {} failed this round.", entry.name(), durableFailure);
        blocked = true;
        break;
      }
    }
    if (!blocked && expiryOrder.size() < batchSize) {
      cursor = null;
    }
    return deleted;
  }
}
