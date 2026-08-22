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

package org.apache.amoro.serde;

/**
 * Serialization contract of one domain format (framework spec §5.4). Every write path produces the
 * latest schema version; reads walk the converter chain to the latest version when needed.
 */
public interface ResourceSerde<R> {

  /**
   * Serializes the resource as the latest-version document of this serde's format.
   *
   * @throws org.apache.amoro.persistence.exception.PersistenceException when encoding fails, the
   *     resource's apiVersion does not equal the latest version (fail-fast: never silently relabel
   *     an unconverted old shape as latest), or the document exceeds the size limit
   */
  byte[] serialize(R resource);

  /** Version-aware deserialization; old versions are upgraded and flagged for write-back. */
  DeserializedResource<R> deserialize(byte[] bytes);

  /**
   * Alias isolation for untrusted resource models: a serialize/deserialize round-trip sharing no
   * mutable state with the input. Resources carrying an old apiVersion are upgraded silently and
   * the modification flag is discarded — callers needing the flag use {@link #deserialize} on
   * {@link #serialize} output themselves.
   */
  R detachedCopy(R resource);
}
