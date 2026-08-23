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

package org.apache.amoro.process;

import org.apache.amoro.control.ControllerKey;
import org.apache.amoro.control.Scheduler;
import org.apache.amoro.persistence.InMemoryPersistence;
import org.apache.amoro.persistence.ListenerEventSink;
import org.apache.amoro.persistence.PersistenceDomain;
import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.persistence.blob.BlobStore;
import org.apache.amoro.persistence.facade.RepositoryFacade;
import org.apache.amoro.serde.ResourceSerde;
import org.apache.amoro.serde.SerdeRegistry;
import org.apache.amoro.serde.VersionAwareJacksonSerde;
import org.apache.amoro.serde.VersionedResourceConverter;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembles the Process domain on the framework (process spec §4.1): the {@code amoro_process}
 * table, Base64(YAML) serde, the aggregate index projection and the deletion hook that unschedules
 * the controller in the same mutation lane as the durable delete.
 */
public final class ProcessDomainAssembly {

  public static final PersistenceDomain DOMAIN =
      new PersistenceDomain("process", "amoro_process_v2", SerdeFormat.YAML);

  private final InMemoryPersistence<ProcessResource> persistence;
  private final RepositoryFacade<ProcessResource> repository;
  private final ProcessIndexProjection indexProjection;
  private final org.apache.amoro.process.engine.ExecutionHandleRegistry handleRegistry;

  public ProcessDomainAssembly(
      BlobStore blobStore,
      ListenerEventSink<ProcessResource> eventSink,
      Scheduler scheduler,
      int mailboxCapacity,
      long repositoryTimeoutMillis,
      int maxResourceBytes) {
    this(
        blobStore,
        eventSink,
        scheduler,
        mailboxCapacity,
        repositoryTimeoutMillis,
        maxResourceBytes,
        new org.apache.amoro.process.engine.ExecutionHandleRegistry());
  }

  public ProcessDomainAssembly(
      BlobStore blobStore,
      ListenerEventSink<ProcessResource> eventSink,
      Scheduler scheduler,
      int mailboxCapacity,
      long repositoryTimeoutMillis,
      int maxResourceBytes,
      org.apache.amoro.process.engine.ExecutionHandleRegistry handleRegistry) {
    this.indexProjection = new ProcessIndexProjection();
    this.handleRegistry = handleRegistry;
    ResourceSerde<ProcessResource> serde =
        new VersionAwareJacksonSerde<ProcessResource>(
            ProcessResource.class,
            new SerdeRegistry(
                ProcessResource.API_VERSION, new ArrayList<VersionedResourceConverter>()),
            SerdeFormat.YAML,
            maxResourceBytes);
    this.persistence =
        new InMemoryPersistence<ProcessResource>(
            DOMAIN,
            ProcessResource.COLLECTION,
            serde,
            blobStore,
            mailboxCapacity,
            eventSink,
            singleProjection(),
            deleted -> scheduler.unschedule(ControllerKey.of("process", deleted.name())));
    this.repository = new RepositoryFacade<ProcessResource>(persistence, repositoryTimeoutMillis);
  }

  /** Shared execution-handle registry: reconcilers track, the TTL cleaner gates on it. */
  public org.apache.amoro.process.engine.ExecutionHandleRegistry handleRegistry() {
    return handleRegistry;
  }

  private List<org.apache.amoro.persistence.DurableStateProjection<ProcessResource>>
      singleProjection() {
    List<org.apache.amoro.persistence.DurableStateProjection<ProcessResource>> projections =
        new ArrayList<org.apache.amoro.persistence.DurableStateProjection<ProcessResource>>();
    projections.add(indexProjection);
    return projections;
  }

  public InMemoryPersistence<ProcessResource> persistence() {
    return persistence;
  }

  public RepositoryFacade<ProcessResource> repository() {
    return repository;
  }

  public ProcessIndexProjection indexProjection() {
    return indexProjection;
  }
}
