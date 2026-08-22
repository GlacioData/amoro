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

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Fully asynchronous persistence surface for one resource domain (framework spec §3). A success
 * completion means the database change is durable — never "merely enqueued". The database is the
 * source of truth; the in-memory state is a rebuildable read projection.
 *
 * <p>Write semantics: the caller's thread only enqueues a logical {@link MutationCommand}; the
 * domain's single mutation lane performs read-latest → detached copy → precondition check → apply
 * updateFn → assign resourceVersion → serialize → projection prepare → DB write → publish
 * cache/projection → deletion hook → listener handoff → complete the stage.
 */
public interface PersistenceService<R extends ControlledResource> {

  /**
   * Durable create. The argument must carry {@code resourceVersion == 0} — arguments with any other
   * version are rejected before enqueueing with {@code IllegalArgumentException}; the returned
   * resource carries {@code 1}.
   *
   * @throws org.apache.amoro.persistence.exception.ResourceAlreadyExists completed exceptionally
   *     when the name already exists in the domain
   */
  CompletionStage<R> create(R resource);

  /**
   * Unconditional atomic read-apply-write inside the mutation lane. Reserved for framework and
   * domain bookkeeping that legitimately needs lane-atomic accumulation (e.g. counters); resource
   * state machines must not bypass optimistic concurrency with this overload.
   */
  CompletionStage<R> modify(String id, Function<R, R> updateFn);

  /**
   * Optimistic-concurrency modify: the command applies only when the current version equals {@code
   * expectedResourceVersion}; on success the resourceVersion is exactly {@code +1} (see {@link
   * ControlledResource#resourceVersion()}).
   *
   * @throws org.apache.amoro.persistence.exception.PreconditionFailedException completed
   *     exceptionally on version mismatch; never auto-retried — the caller re-reads and retries at
   *     its own level
   */
  CompletionStage<R> modify(String id, long expectedResourceVersion, Function<R, R> updateFn);

  /** Read from the rebuildable in-memory projection; returns a detached copy. */
  CompletionStage<R> get(String id);

  /**
   * Durable delete. After the DB commit, the registered {@link DurableDeletionHook} runs in the
   * same lane before this stage completes and before the next mutation of the same name is
   * dequeued.
   */
  CompletionStage<R> delete(String id);

  /** Version-CAS delete with the same guarantees as {@link #delete(String)}. */
  CompletionStage<R> delete(String id, long expectedResourceVersion);

  /** Select from the in-memory projection; predicates receive detached copies. */
  CompletionStage<List<R>> select(Selector<R> selector);

  void addListener(PersistenceListener<R> listener);

  /**
   * Loads the durable state of the whole domain (DB cursor), rebuilds the cache and projections,
   * replays lazy serde upgrades, then hands a POST_START envelope per existing resource to the
   * listener sink. This is the restart replay entry — the scheduler has no replay of its own.
   */
  void postStart();
}
