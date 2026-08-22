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
    // restart replay (spec §8.7): rebuild the read model from the durable rows, then the
    // POST_START events re-schedule every live controller
    assembly.persistence().postStart();
    return assembly;
  }

  @Bean
  public org.apache.amoro.process.rest.ProcessRestSupport processRestSupport(
      org.apache.amoro.process.ProcessDomainAssembly assembly) {
    return new org.apache.amoro.process.rest.ProcessRestSupport(assembly);
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
