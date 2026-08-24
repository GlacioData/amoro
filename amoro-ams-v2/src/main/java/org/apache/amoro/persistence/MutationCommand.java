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
import java.util.function.Function;

/**
 * The deferred logical command enqueued by {@link PersistenceService} calls and executed inside the
 * domain's mutation lane (framework spec §5.1). Callers' threads never precompute a write candidate
 * — that is why this type carries an update function (and the create argument) but no result
 * resource: two concurrent modifies must both apply on the lane's latest committed value, never on
 * a shared stale snapshot.
 *
 * <p>Internal framework plumbing between L5 and L6; not part of the domain-facing surface.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class MutationCommand<R extends ControlledResource> {

  public enum Type {
    CREATE,
    MODIFY,
    DELETE
  }

  @NonNull private final Type type;
  @NonNull private final String name;
  private final Long expectedResourceVersion; // null = unconditional (lane-atomic ops only)
  private final Function<R, R> updateFn; // null for CREATE/DELETE
  private final R createResource; // detached copy; non-null only for CREATE

  public static <R extends ControlledResource> MutationCommand<R> create(R detachedResource) {
    return new MutationCommand<R>(
        Type.CREATE, detachedResource.name(), null, null, Objects.requireNonNull(detachedResource));
  }

  public static <R extends ControlledResource> MutationCommand<R> modify(
      String name, Function<R, R> updateFn) {
    return new MutationCommand<R>(Type.MODIFY, name, null, Objects.requireNonNull(updateFn), null);
  }

  public static <R extends ControlledResource> MutationCommand<R> modify(
      String name, long expectedResourceVersion, Function<R, R> updateFn) {
    return new MutationCommand<R>(
        Type.MODIFY, name, expectedResourceVersion, Objects.requireNonNull(updateFn), null);
  }

  public static <R extends ControlledResource> MutationCommand<R> delete(String name) {
    return new MutationCommand<R>(Type.DELETE, name, null, null, null);
  }

  public static <R extends ControlledResource> MutationCommand<R> delete(
      String name, long expectedResourceVersion) {
    return new MutationCommand<R>(Type.DELETE, name, expectedResourceVersion, null, null);
  }
}
