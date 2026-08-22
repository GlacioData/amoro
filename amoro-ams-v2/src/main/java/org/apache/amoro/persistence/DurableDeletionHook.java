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
 * Synchronous in-lane cleanup after a durable delete (framework spec §5.1). Runs on the mutation
 * lane after the DB commit, the cache publish and the projection commit, but before the delete
 * stage completes and before the next mutation of the same name is dequeued — so a same-name create
 * can never overtake the cleanup, and a late unschedule can never kill a recreated entry.
 *
 * <p>Constraints: non-blocking, no I/O, no waiting on futures, no recursive persistence calls —
 * only O(1) in-process cleanup or queue/index handoff (the Process domain uses it for the key-only
 * scheduler unschedule). A hook failure fails the delete stage with {@link
 * org.apache.amoro.persistence.exception.PostCommitCleanupException}; the name is fenced (a staged
 * deleted snapshot is retained) and same-name creates are rejected until repair retries the hook
 * successfully. The fence is process-local: if the process crashes before running the hook, the new
 * process has no old scheduler entry and the postStart DB replay comes up empty for that name,
 * which safely clears the window without cross-process fencing.
 */
public interface DurableDeletionHook<R extends ControlledResource> {

  void afterDurableDelete(R deletedResource);
}
