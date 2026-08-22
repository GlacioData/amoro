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

  public ProcessTtlCleaner(ProcessDomainAssembly assembly) {
    this.assembly = assembly;
  }

  /** One bounded cleaning round; returns the number of durable deletes issued. */
  public int cleanOnce(Instant now, int retentionDays, int batchSize) {
    String cutoff = now.minusSeconds((long) retentionDays * 24L * 3600L).toString();
    List<String> expiryOrder = assembly.indexProjection().current().expiryOrder();
    int deleted = 0;
    for (String entry : expiryOrder) {
      if (deleted >= batchSize) {
        break;
      }
      // entries are "finishedAt|name", lexicographic = chronological for RFC 3339
      String finishedAt = entry.substring(0, entry.indexOf('|'));
      String name = entry.substring(entry.indexOf('|') + 1);
      if (finishedAt.compareTo(cutoff) > 0) {
        break; // ordered: everything after this is younger than the cutoff
      }
      ProcessResource resource = assembly.repository().get(name);
      if (!ProcessFinality.isFinal(resource) || resource.status().finishedAt() == null) {
        continue; // raced back to life or never stamped: skip, the index will drop it
      }
      try {
        assembly
            .persistence()
            .delete(name, resource.resourceVersion())
            .toCompletableFuture()
            .join();
        deleted++;
        LOG.info("TTL deleted final process {} (finishedAt {}).", name, finishedAt);
      } catch (org.apache.amoro.persistence.exception.PreconditionFailedException raced) {
        // version moved under us: next round re-reads the fresh state
      } catch (RuntimeException durableFailure) {
        LOG.warn("TTL delete of {} failed this round.", name, durableFailure);
        break; // stop the batch; later rounds retry
      }
    }
    return deleted;
  }
}
