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

import lombok.Getter;
import lombok.NonNull;

/**
 * Immutable, per-listener event envelope handed from the mutation lane to the async dispatcher
 * (framework spec §6). Fixed identity: {@code (listenerIdentity, domain, name, resourceVersion,
 * eventType)} plus a detached snapshot of the resource — never a canonical cache reference.
 */
@Getter
public final class ListenerEnvelope<R extends ControlledResource> {

  public enum EventType {
    AFTER_CREATED,
    AFTER_MODIFIED,
    AFTER_DELETED,
    POST_START
  }

  @NonNull private final String listenerIdentity;
  @NonNull private final String domain;
  @NonNull private final String name;
  private final long resourceVersion;
  @NonNull private final EventType eventType;
  @NonNull private final R detachedResource;
  private final PersistenceListener<R> listener;

  public ListenerEnvelope(
      String listenerIdentity,
      String domain,
      String name,
      long resourceVersion,
      EventType eventType,
      R detachedResource) {
    this(listenerIdentity, domain, name, resourceVersion, eventType, detachedResource, null);
  }

  public ListenerEnvelope(
      @NonNull String listenerIdentity,
      @NonNull String domain,
      @NonNull String name,
      long resourceVersion,
      @NonNull EventType eventType,
      @NonNull R detachedResource,
      PersistenceListener<R> listener) {
    this.listenerIdentity = listenerIdentity;
    this.domain = domain;
    this.name = name;
    this.resourceVersion = resourceVersion;
    this.eventType = eventType;
    this.detachedResource = detachedResource;
    this.listener = listener;
  }

  @Override
  public String toString() {
    return "ListenerEnvelope{"
        + "listener='"
        + listenerIdentity
        + "', domain='"
        + domain
        + "', name='"
        + name
        + "', version="
        + resourceVersion
        + ", type="
        + eventType
        + '}';
  }
}
