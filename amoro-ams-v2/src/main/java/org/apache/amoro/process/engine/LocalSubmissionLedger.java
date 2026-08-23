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

package org.apache.amoro.process.engine;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Process-local accepted-submission ledger. It intentionally makes no restart durability claim. */
final class LocalSubmissionLedger {

  private final Map<String, Entry> bySubmissionKey = new ConcurrentHashMap<>();

  Optional<Entry> find(String submissionKey) {
    return Optional.ofNullable(bySubmissionKey.get(submissionKey));
  }

  void record(String submissionKey, String requestHash, String externalId) {
    Entry prior = bySubmissionKey.putIfAbsent(submissionKey, new Entry(requestHash, externalId));
    if (prior != null) {
      throw new IllegalStateException("submission was concurrently recorded: " + submissionKey);
    }
  }

  void removeExternalId(String externalId) {
    bySubmissionKey.entrySet().removeIf(entry -> entry.getValue().externalId.equals(externalId));
  }

  int size() {
    return bySubmissionKey.size();
  }

  static final class Entry {
    private final String requestHash;
    private final String externalId;

    private Entry(String requestHash, String externalId) {
      this.requestHash = Objects.requireNonNull(requestHash, "requestHash");
      this.externalId = Objects.requireNonNull(externalId, "externalId");
    }

    String requestHash() {
      return requestHash;
    }

    String externalId() {
      return externalId;
    }
  }
}
