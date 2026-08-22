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
 * Marker contract for framework-controlled resources. Implementations must be deeply immutable:
 * final fields, defensive copies on construction, unmodifiable collections, no mutators — for the
 * implementing class and every nested value it reaches.
 *
 * <p>Deep immutability cannot be proven statically for generic code, so the framework still defends
 * itself with serde round-trip {@code detachedCopy} isolation on every boundary: create arguments
 * at enqueue time, the input and output of update functions, get/select returns, listener
 * envelopes. Callers can therefore never alias the canonical cache snapshot.
 */
public interface ControlledResource {

  /** Globally unique resource id inside its persistence domain; immutable after creation. */
  String name();

  /** Resource kind, lowercase (maps to the {@code collection} column); immutable. */
  String collection();

  /**
   * Optimistic-concurrency version. Must be {@code 0} on a create argument; the framework assigns
   * {@code 1} on the first durable insert and increments by exactly 1 on every successful modify.
   * Callers never choose values.
   */
  long resourceVersion();
}
