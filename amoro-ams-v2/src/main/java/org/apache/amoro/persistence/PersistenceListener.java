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

package org.apache.amoro.persistence;

/**
 * Domain reactions to durable resource changes (framework spec §6). Callbacks run asynchronously on
 * the listener dispatcher — never on the mutation lane and never on the caller's thread.
 *
 * <p>Contract: implementations must be idempotent and level-triggered. Crashes and repairs may
 * replay or drop events; a dropped handoff is compensated by the resource domain's repair sweep,
 * not by re-running the mutation. Listener failures never roll back or fail the durable write.
 */
public interface PersistenceListener<R extends ControlledResource> {

  void afterCreated(R resource);

  void afterModified(R resource);

  void afterDeleted(R resource);

  /**
   * Replay of one existing resource during {@link PersistenceService#postStart()}; this is where
   * listeners (re-)register the resource with the scheduler.
   */
  void postStart(R existingResource);
}
