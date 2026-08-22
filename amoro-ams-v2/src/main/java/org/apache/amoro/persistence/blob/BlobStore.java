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

package org.apache.amoro.persistence.blob;

import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * L7: the durable KV surface consumed by the mutation lane. Exactly five operations (framework spec
 * §5.3): INSERT, UPDATE, DELETE, point read, full-collection scan. Values are the encoded resource
 * bytes (Base64 of the JSON/YAML document happens inside the MyBatis implementation, not at this
 * interface).
 *
 * <p>All methods are invoked from the domain's single mutation-lane actor thread; implementors need
 * not be thread-safe for concurrent writers, but must tolerate callers from other threads for point
 * reads during outcome-unknown resolution.
 */
public interface BlobStore {

  /**
   * @throws org.apache.amoro.persistence.exception.ResourceAlreadyExists when the name already
   *     exists (translated from the DB duplicate-key error)
   */
  void insert(String collection, String name, byte[] value);

  /** @return false when the name does not exist (nothing updated) */
  boolean update(String collection, String name, byte[] value);

  /** @return false when the name did not exist */
  boolean delete(String collection, String name);

  /**
   * Fresh point read on a usable connection. The mutation lane never reads through this method —
   * lane reads come from the in-memory canonical snapshot; this exists for outcome-unknown
   * resolution (previous/candidate comparison) and repair reloads.
   */
  Optional<byte[]> find(String collection, String name);

  /**
   * Full-collection scan for startup load and repair reloads; action receives (name, bytes). The
   * {@code last} update-timestamp column is maintained internally by the MyBatis implementation on
   * every write; it is not part of this interface.
   */
  void forEach(String collection, BiConsumer<String, byte[]> action);
}
