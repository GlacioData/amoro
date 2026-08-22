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

import java.util.Objects;

/**
 * Immutable, per-listener event envelope handed from the mutation lane to the async dispatcher
 * (framework spec §6). Fixed identity: {@code (listenerIdentity, domain, name, resourceVersion,
 * eventType)} plus a detached snapshot of the resource — never a canonical cache reference.
 */
public final class ListenerEnvelope<R extends ControlledResource> {

  public enum EventType {
    AFTER_CREATED,
    AFTER_MODIFIED,
    AFTER_DELETED,
    POST_START
  }

  private final String listenerIdentity;
  private final String domain;
  private final String name;
  private final long resourceVersion;
  private final EventType eventType;
  private final R detachedResource;

  public ListenerEnvelope(
      String listenerIdentity,
      String domain,
      String name,
      long resourceVersion,
      EventType eventType,
      R detachedResource) {
    this.listenerIdentity = Objects.requireNonNull(listenerIdentity, "listenerIdentity");
    this.domain = Objects.requireNonNull(domain, "domain");
    this.name = Objects.requireNonNull(name, "name");
    this.resourceVersion = resourceVersion;
    this.eventType = Objects.requireNonNull(eventType, "eventType");
    this.detachedResource = Objects.requireNonNull(detachedResource, "detachedResource");
  }

  public String listenerIdentity() {
    return listenerIdentity;
  }

  public String domain() {
    return domain;
  }

  public String name() {
    return name;
  }

  public long resourceVersion() {
    return resourceVersion;
  }

  public EventType eventType() {
    return eventType;
  }

  /** Detached snapshot; mutating it cannot affect the canonical cache. */
  public R detachedResource() {
    return detachedResource;
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
