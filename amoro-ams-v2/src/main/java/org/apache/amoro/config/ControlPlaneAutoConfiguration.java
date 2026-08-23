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

import jakarta.annotation.PostConstruct;
import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.ControlledResource;
import org.apache.amoro.persistence.InMemoryPersistence;
import org.apache.amoro.persistence.ListenerDispatcher;
import org.apache.amoro.persistence.blob.ResourceBlobMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Spring assembly of the control-plane framework (framework spec §7). Wires the shared building
 * blocks — scheduler, listener dispatcher, blob mapper, schema initializer, domain factory — and
 * the {@link ControlPlaneLifecycle} shutdown ordering. Concrete domains materialize through the
 * {@link ControlPlaneDomainFactory}: the default {@code amoro_resource} JSON domain and later the
 * Process domain on {@code amoro_process} (YAML). Converters register per resource type as Spring
 * beans; the classpath scan replaces the reference implementation's Reflections (fidelity ledger
 * #6) and conflict/chain checks run at registry construction.
 */
@Configuration
@EnableConfigurationProperties(AmoroControlProperties.class)
public class ControlPlaneAutoConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(ControlPlaneAutoConfiguration.class);

  private final AmoroControlProperties properties;

  public ControlPlaneAutoConfiguration(AmoroControlProperties properties) {
    this.properties = properties;
  }

  @PostConstruct
  public void validate() {
    properties.validate(); // fail-fast on illegal amoro.control.* values
  }

  @Bean
  public DefaultScheduler controlPlaneScheduler() {
    return DefaultScheduler.create(
        properties.getScheduler().getWorkers(), properties.getScheduler().getDelayMs());
  }

  @Bean
  public org.mybatis.spring.mapper.MapperFactoryBean<ResourceBlobMapper> resourceBlobMapper(
      org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory) {
    // MapperFactoryBean registers the interface into the session factory's configuration;
    // a plain getMapper() would fail because the starter's factory does not know it yet
    org.mybatis.spring.mapper.MapperFactoryBean<ResourceBlobMapper> mapper =
        new org.mybatis.spring.mapper.MapperFactoryBean<>(ResourceBlobMapper.class);
    mapper.setSqlSessionFactory(sqlSessionFactory);
    return mapper;
  }

  @Bean(initMethod = "initialize")
  public ControlPlaneSchemaInitializer controlPlaneSchemaInitializer(DataSource dataSource) {
    return new ControlPlaneSchemaInitializer(dataSource);
  }

  @Bean
  public ListenerDispatcher<ControlledResource> controlPlaneListenerDispatcher() {
    return ListenerDispatcher.start(
        "control-plane",
        properties.getListener().getWorkers(),
        properties.getListener().getQueueCapacity(),
        properties.getListener().getMaxRetries(),
        properties.getListener().getRetryDelayMs());
  }

  @Bean
  public ControlPlaneDomainFactory controlPlaneDomainFactory(
      ResourceBlobMapper mapper, ListenerDispatcher<ControlledResource> dispatcher) {
    return new ControlPlaneDomainFactory(mapper, properties, dispatcher);
  }

  /** The Process domain on its dedicated {@code amoro_process} table (Base64(YAML)). */
  @Bean
  public org.apache.amoro.process.ProcessDomainAssembly processDomainAssembly(
      ResourceBlobMapper mapper,
      ListenerDispatcher<ControlledResource> dispatcher,
      DefaultScheduler scheduler,
      ControlPlaneSchemaInitializer schemaInitializer) {
    org.apache.amoro.persistence.blob.MyBatisBlobStore processBlobStore =
        new org.apache.amoro.persistence.blob.MyBatisBlobStore(
            org.apache.amoro.process.ProcessDomainAssembly.DOMAIN, mapper);
    // one dispatcher serves every domain: envelopes carry their typed listener reference,
    // so the cross-domain variance is bridged here (same rationale as the domain factory)
    @SuppressWarnings("unchecked")
    org.apache.amoro.persistence.ListenerEventSink<org.apache.amoro.process.ProcessResource>
        processSink =
            (org.apache.amoro.persistence.ListenerEventSink<
                    org.apache.amoro.process.ProcessResource>)
                (org.apache.amoro.persistence.ListenerEventSink<?>) dispatcher;
    org.apache.amoro.process.ProcessDomainAssembly assembly =
        new org.apache.amoro.process.ProcessDomainAssembly(
            processBlobStore,
            processSink,
            scheduler,
            properties.getActor().getQueueCapacity(),
            properties.getRepository().getTimeoutMs(),
            properties.getStorage().getMaxResourceBytes());
    // NOTE: postStart is intentionally NOT called here — the scheduling listener must be
    // registered first so the replay's POST_START events reach it (see processSchedulingListener)
    return assembly;
  }

  @Bean
  public org.apache.amoro.process.ProcessCreationService processCreationService(
      org.apache.amoro.process.ProcessDomainAssembly assembly) {
    return new org.apache.amoro.process.ProcessCreationService(assembly);
  }

  @Bean
  public org.apache.amoro.process.rest.ProcessRestSupport processRestSupport(
      org.apache.amoro.process.ProcessDomainAssembly assembly,
      org.apache.amoro.process.ProcessCreationService creationService) {
    return new org.apache.amoro.process.rest.ProcessRestSupport(
        assembly, creationService);
  }

  // ------------------------------------------------------------------ process runtime (engines +
  // scheduling)

  /**
   * The local execution engine: a bounded action pool. Pool sizing reuses the scheduler worker
   * budget and the actor mailbox capacity — dedicated {@code amoro.process.*} keys can be added
   * when the tuning needs diverge.
   */
  @Bean(destroyMethod = "shutdown")
  public org.apache.amoro.process.engine.LocalEngineAdapter localEngineAdapter() throws Exception {
    return new org.apache.amoro.process.engine.LocalEngineAdapter(
        Math.max(2, properties.getScheduler().getWorkers() / 2),
        properties.getActor().getQueueCapacity(),
        org.apache.amoro.process.engine.LocalEngineAdapter.simulatedAction());
  }

  /**
   * Engine registry keyed by {@code spec.executionEngine}. "local" is deployed out of the box; a
   * remote-Spark engine plugs in by registering another {@link
   * org.apache.amoro.process.engine.ProcessEnginePort} here — the reconciler looks the engine up
   * per process and waits (Step.WAIT) for engines that are not deployed.
   */
  @Bean
  public org.apache.amoro.process.engine.ProcessEngineRegistry processEngineRegistry(
      org.apache.amoro.process.engine.LocalEngineAdapter localEngineAdapter) {
    return org.apache.amoro.process.engine.ProcessEngineRegistry.builder()
        .registerPort("local", localEngineAdapter, properties.getRepository().getTimeoutMs())
        .build();
  }

  /**
   * The scheduling bridge: durable create/modify/replay events register a {@link
   * org.apache.amoro.process.ProcessReconciler} for that process on the shared scheduler — this is
   * what makes a REST-created process actually run without any manual wiring.
   */
  @Bean
  public org.apache.amoro.persistence.PersistenceListener<org.apache.amoro.process.ProcessResource>
      processSchedulingListener(
          org.apache.amoro.process.ProcessDomainAssembly assembly,
          org.apache.amoro.process.engine.ProcessEngineRegistry engines,
          DefaultScheduler scheduler) {
    org.apache.amoro.persistence.PersistenceListener<org.apache.amoro.process.ProcessResource>
        listener =
            new org.apache.amoro.persistence.PersistenceListener<
                org.apache.amoro.process.ProcessResource>() {
              @Override
              public void afterCreated(org.apache.amoro.process.ProcessResource resource) {
                schedule(resource);
              }

              @Override
              public void afterModified(org.apache.amoro.process.ProcessResource resource) {
                schedule(resource);
              }

              @Override
              public void afterDeleted(org.apache.amoro.process.ProcessResource resource) {
                // the deletion hook already unscheduled the key in the mutation lane
              }

              @Override
              public void postStart(org.apache.amoro.process.ProcessResource existing) {
                schedule(existing);
              }

              private void schedule(org.apache.amoro.process.ProcessResource resource) {
                scheduler.schedule(
                    new org.apache.amoro.process.ProcessReconciler(
                        resource.name(),
                        assembly.repository(),
                        engines,
                        scheduler,
                        org.apache.amoro.process.ProcessReconciler.Clock.systemUtc(),
                        1_000L,
                        assembly.handleRegistry()));
              }
            };
    assembly.persistence().addListener(listener);
    // restart replay (spec §8.7) happens HERE, after the listener is registered: the read
    // model rebuilds from the durable rows and every live process is re-scheduled
    assembly.persistence().postStart();
    return listener;
  }

  @Bean
  public ControlPlaneLifecycle controlPlaneLifecycle(
      DefaultScheduler scheduler,
      ListenerDispatcher<ControlledResource> dispatcher,
      org.apache.amoro.process.ProcessDomainAssembly processDomain,
      org.springframework.context.ApplicationContext context) {
    List<InMemoryPersistence<?>> domains = new ArrayList<InMemoryPersistence<?>>();
    for (InMemoryPersistence<?> untyped :
        context.getBeansOfType(InMemoryPersistence.class).values()) {
      domains.add(untyped);
    }
    domains.add(processDomain.persistence());
    ControlPlaneLifecycle lifecycle =
        ControlPlaneLifecycle.from(
            scheduler, dispatcher, domains, properties.getLifecycle().getShutdownTimeoutMs());
    return lifecycle;
  }
}
