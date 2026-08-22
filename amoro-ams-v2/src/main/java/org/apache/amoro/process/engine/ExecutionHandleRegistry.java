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

import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which processes still own unreleased engine execution handles (process spec §6.1/§9.1):
 * the reconciler registers a handle when a terminal observation becomes durable and the release
 * succeeded only after the engine confirms; the TTL cleaner consults the gate before deleting a row
 * so a Process can never disappear while its local handle is still pending cleanup.
 */
public final class ExecutionHandleRegistry {

  private final ConcurrentHashMap<String, String> pendingByProcess =
      new ConcurrentHashMap<String, String>();

  /** Registers (or refreshes) the handle a process currently owns. */
  public void track(String processName, String externalId) {
    pendingByProcess.put(processName, externalId);
  }

  /** Marks the process's handle released; idempotent. */
  public void release(String processName) {
    pendingByProcess.remove(processName);
  }

  /** True while the process still owns an unreleased handle. */
  public boolean hasPendingHandle(String processName) {
    return pendingByProcess.containsKey(processName);
  }

  public int pendingCount() {
    return pendingByProcess.size();
  }
}
