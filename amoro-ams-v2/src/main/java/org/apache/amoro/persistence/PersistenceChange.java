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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.util.Objects;

/**
 * The before/after pair a {@link DurableStateProjection} prepares from. Both resources are detached
 * copies, never canonical cache references. CREATE: previous=null, current=new; MODIFY:
 * previous=old, current=new; DELETE: previous=old, current=null.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class PersistenceChange<R extends ControlledResource> {

  public enum Type {
    CREATE,
    MODIFY,
    DELETE
  }

  @NonNull private final Type type;
  private final R previous;
  private final R current;

  public static <R extends ControlledResource> PersistenceChange<R> created(R current) {
    return new PersistenceChange<R>(Type.CREATE, null, Objects.requireNonNull(current));
  }

  public static <R extends ControlledResource> PersistenceChange<R> modified(
      R previous, R current) {
    return new PersistenceChange<R>(
        Type.MODIFY, Objects.requireNonNull(previous), Objects.requireNonNull(current));
  }

  public static <R extends ControlledResource> PersistenceChange<R> deleted(R previous) {
    return new PersistenceChange<R>(Type.DELETE, Objects.requireNonNull(previous), null);
  }

  @Override
  public String toString() {
    return "PersistenceChange{"
        + type
        + ", name="
        + (current != null ? current.name() : previous.name())
        + '}';
  }
}
