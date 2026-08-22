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

/** Result of a versioned deserialization; see {@link VersionAwareJacksonSerde#deserialize}. */
public final class DeserializedResource<R> {

  private final R resource;
  private final boolean modifiedDuringDeserialization;

  private DeserializedResource(R resource, boolean modifiedDuringDeserialization) {
    this.resource = resource;
    this.modifiedDuringDeserialization = modifiedDuringDeserialization;
  }

  public static <R> DeserializedResource<R> current(R resource) {
    return new DeserializedResource<R>(resource, false);
  }

  public static <R> DeserializedResource<R> upgraded(R resource) {
    return new DeserializedResource<R>(resource, true);
  }

  public R resource() {
    return resource;
  }

  /**
   * True when a converter chain ran during this deserialization. Callers loading durable state at
   * startup must write the upgraded resource back to the database (lazy migration — no downtime
   * migration scripts).
   */
  public boolean modifiedDuringDeserialization() {
    return modifiedDuringDeserialization;
  }
}
