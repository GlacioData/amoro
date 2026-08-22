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
 * Two-phase domain index maintenance (framework spec §5.1). {@link #prepare} runs on the mutation
 * lane before the DB write and must perform every computation and allocation that can fail — it
 * must be pure, non-blocking and free of I/O. {@link PreparedProjectionUpdate#commit} runs on the
 * same lane after the DB write succeeded and may only switch a pre-built immutable snapshot or
 * apply a key-count-bounded update declared in the domain contract; it must not throw and must not
 * traverse or allocate proportionally to the resource population.
 *
 * <p>The same-lane ordering of cache publish and projection commit gives write order only; it does
 * not make two independent containers atomically visible across threads. Domains whose readers must
 * see resource bodies and indexes consistently must aggregate the canonical read map and those
 * indexes into a single immutable snapshot and read it once (or provide an equivalent read
 * barrier).
 */
public interface DurableStateProjection<R extends ControlledResource> {

  /**
   * @throws RuntimeException any preparation failure aborts the command before the DB write: no
   *     side effects, memory/version/listeners unchanged
   */
  PreparedProjectionUpdate prepare(PersistenceChange<R> change);
}
