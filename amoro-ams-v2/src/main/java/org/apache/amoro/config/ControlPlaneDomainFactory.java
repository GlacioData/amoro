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

package org.apache.amoro.config;

import org.apache.amoro.persistence.ControlledResource;
import org.apache.amoro.persistence.InMemoryPersistence;
import org.apache.amoro.persistence.ListenerDispatcher;
import org.apache.amoro.persistence.PersistenceDomain;
import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.persistence.Repository;
import org.apache.amoro.persistence.blob.MyBatisBlobStore;
import org.apache.amoro.persistence.blob.ResourceBlobMapper;
import org.apache.amoro.persistence.facade.RepositoryFacade;
import org.apache.amoro.serde.ResourceSerde;
import org.apache.amoro.serde.SerdeRegistry;
import org.apache.amoro.serde.VersionAwareJacksonSerde;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Assembles persistence domains from the shared building blocks (framework spec §7). Every domain —
 * the default {@code amoro_resource} JSON domain as well as later domains like the Process domain
 * on {@code amoro_process} (YAML) — binds its own table, serde format, mailbox budget and mutation
 * lane through this factory.
 */
public final class ControlPlaneDomainFactory {

  private final ResourceBlobMapper mapper;
  private final AmoroControlProperties properties;
  private final ListenerDispatcher<ControlledResource> dispatcher;

  public ControlPlaneDomainFactory(
      ResourceBlobMapper mapper,
      AmoroControlProperties properties,
      ListenerDispatcher<ControlledResource> dispatcher) {
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.properties = Objects.requireNonNull(properties, "properties");
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
  }

  public <R extends ControlledResource> InMemoryPersistence<R> createPersistence(
      PersistenceDomain domain,
      Class<R> resourceClass,
      String resourceCollection,
      java.util.List<org.apache.amoro.serde.VersionedResourceConverter> converters) {
    return createPersistence(
        domain,
        resourceClass,
        resourceCollection,
        converters,
        new ArrayList<org.apache.amoro.persistence.DurableStateProjection<R>>(),
        resource -> {});
  }

  /** Full assembly with domain projections and the durable-deletion hook. */
  public <R extends ControlledResource> InMemoryPersistence<R> createPersistence(
      PersistenceDomain domain,
      Class<R> resourceClass,
      String resourceCollection,
      java.util.List<org.apache.amoro.serde.VersionedResourceConverter> converters,
      java.util.List<org.apache.amoro.persistence.DurableStateProjection<R>> projections,
      org.apache.amoro.persistence.DurableDeletionHook<R> deletionHook) {
    SerdeRegistry registry =
        new SerdeRegistry(latestVersionOf(resourceClass, converters), converters);
    ResourceSerde<R> serde =
        new VersionAwareJacksonSerde<R>(
            resourceClass,
            registry,
            domain.serdeFormat(),
            properties.getStorage().getMaxResourceBytes());
    MyBatisBlobStore blobStore = new MyBatisBlobStore(domain, mapper);
    // one dispatcher serves every domain: envelopes carry their own typed listener reference,
    // so the cross-domain variance is safe by construction and bridged here once
    @SuppressWarnings("unchecked")
    org.apache.amoro.persistence.ListenerEventSink<R> sink =
        (org.apache.amoro.persistence.ListenerEventSink<R>)
            (org.apache.amoro.persistence.ListenerEventSink<?>) dispatcher;
    return new InMemoryPersistence<R>(
        domain,
        resourceCollection,
        serde,
        blobStore,
        properties.getActor().getQueueCapacity(),
        sink,
        projections,
        deletionHook);
  }

  public <R extends ControlledResource> Repository<R> createRepository(
      InMemoryPersistence<R> persistence) {
    return new RepositoryFacade<R>(persistence, properties.getRepository().getTimeoutMs());
  }

  private static String latestVersionOf(
      Class<?> resourceClass,
      java.util.List<org.apache.amoro.serde.VersionedResourceConverter> converters) {
    if (converters.isEmpty()) {
      return "v1"; // latest-only registry: the resource type starts at v1 with no history
    }
    // the latest version is the chain's sink: no converter leaves it (order-independent)
    java.util.Set<String> sources = new java.util.HashSet<String>();
    for (org.apache.amoro.serde.VersionedResourceConverter converter : converters) {
      sources.add(converter.fromVersion().trim());
    }
    for (org.apache.amoro.serde.VersionedResourceConverter converter : converters) {
      String target = converter.toVersion().trim();
      if (!sources.contains(target)) {
        return target;
      }
    }
    throw new IllegalArgumentException(
        "converter list of " + resourceClass.getSimpleName() + " has no terminal version");
  }

  static SerdeFormat defaultFormat() {
    return SerdeFormat.JSON;
  }
}
