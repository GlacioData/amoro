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
import org.apache.amoro.resources.ProcessResource;
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
@EnableConfigurationProperties({AmoroControlProperties.class, AmoroProcessProperties.class})
public class ControlPlaneAutoConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(ControlPlaneAutoConfiguration.class);

  private final AmoroControlProperties properties;
  private final AmoroProcessProperties processProperties;

  public ControlPlaneAutoConfiguration(
      AmoroControlProperties properties, AmoroProcessProperties processProperties) {
    this.properties = properties;
    this.processProperties = processProperties;
  }

  @PostConstruct
  public void validate() {
    properties.validate(); // fail-fast on illegal amoro.control.* values
    processProperties.validate();
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
    org.apache.amoro.persistence.ListenerEventSink<ProcessResource> processSink =
        (org.apache.amoro.persistence.ListenerEventSink<ProcessResource>)
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
    AmoroProcessProperties.Creation creation = processProperties.getCreation();
    return new org.apache.amoro.process.ProcessCreationService(
        assembly,
        new ProcessResource.RetryPolicy(
            creation.getMaxRetries(),
            creation.getMaxSubmissionRetries(),
            creation.getRetryDelaySeconds()));
  }

  @Bean
  public org.apache.amoro.process.rest.ProcessRestSupport processRestSupport(
      org.apache.amoro.process.ProcessDomainAssembly assembly,
      org.apache.amoro.process.ProcessCreationService creationService,
      org.apache.amoro.process.rest.ProcessRestSupport.TableCatalogPort tableCatalog,
      org.apache.amoro.process.rest.ProcessActionCatalog actionCatalog) {
    return new org.apache.amoro.process.rest.ProcessRestSupport(
        assembly, tableCatalog, creationService, actionCatalog);
  }

  // ------------------------------------------------------------------ process runtime (engines +
  // scheduling)

  @Bean
  public org.apache.amoro.process.ProcessResultPersistenceRetryer
      processResultPersistenceRetryer() {
    AmoroProcessProperties.ResultPersistence result = processProperties.getResultPersistence();
    return new org.apache.amoro.process.ProcessResultPersistenceRetryer(
        result.getMaxPending(), result.getBatchSize(), result.getRetryIntervalMs());
  }

  @Bean
  public org.apache.amoro.process.engine.ProviderMode processProviderMode() {
    return processProperties.getSimulation().isEnabled()
        ? org.apache.amoro.process.engine.ProviderMode.SIMULATED
        : org.apache.amoro.process.engine.ProviderMode.REAL;
  }

  @Bean
  public org.apache.amoro.process.engine.local.LocalActionRegistry localActionRegistry(
      org.apache.amoro.process.engine.ProviderMode mode) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    return org.apache.amoro.process.engine.local.LocalActionRegistry.fromFactories(
        org.apache.amoro.process.engine.ProcessPluginLoader.loadLocalActionFactories(loader), mode);
  }

  @Bean
  public org.apache.amoro.process.engine.ProcessEngineRegistry processEngineRegistry(
      org.apache.amoro.process.engine.ProviderMode mode,
      org.apache.amoro.process.engine.local.LocalActionRegistry localActions) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    AmoroProcessProperties.Simulation simulation = processProperties.getSimulation();
    return org.apache.amoro.process.engine.ProcessEngineRegistry.fromFactories(
        org.apache.amoro.process.engine.ProcessPluginLoader.loadEngineFactories(loader),
        mode,
        new org.apache.amoro.process.engine.ProcessEngineFactory.Context(
            "spring",
            simulation.getWorkerThreads(),
            simulation.getQueueCapacity(),
            localActions,
            processProperties.getLocal().getTerminalResultRetentionDays()),
        processProperties.getEngine().getCommandTimeoutMs());
  }

  @Bean
  public org.apache.amoro.process.trigger.ProcessActionRegistry processActionRegistry(
      org.apache.amoro.process.engine.ProviderMode mode) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    return org.apache.amoro.process.trigger.ProcessActionRegistry.fromFactories(
        org.apache.amoro.process.engine.ProcessPluginLoader.loadActionFactories(loader),
        mode,
        new org.apache.amoro.process.trigger.ProcessActionPluginFactory.Context("spring"));
  }

  @Bean
  public org.apache.amoro.process.rest.ProcessActionCatalog processActionCatalog(
      org.apache.amoro.process.engine.ProcessEngineRegistry engines,
      org.apache.amoro.process.trigger.ProcessActionRegistry actions) {
    return org.apache.amoro.process.rest.ProcessActionCatalog.from(engines, actions);
  }

  @Bean
  public org.apache.amoro.process.ProcessSubmissionBuilder processSubmissionBuilder(
      org.apache.amoro.process.rest.ProcessActionCatalog actions) {
    return resource ->
        actions.buildSubmission(
            resource.spec(), java.util.Collections.singletonMap("processName", resource.name()));
  }

  @Bean
  public org.apache.amoro.process.rest.ProcessRestSupport.TableCatalogPort processTableCatalog() {
    if (!processProperties.getSimulation().isEnabled()) {
      return emptyProcessTableCatalog();
    }
    return new org.apache.amoro.process.rest.ProcessRestSupport.TableCatalogPort() {
      @Override
      public org.apache.amoro.process.rest.ProcessRestSupport.TableIdentity resolve(
          String catalog, String database, String table) {
        if (!org.apache.amoro.process.trigger.simulated.SimulatedProcessFixture.matches(
            catalog, database, table)) {
          return null;
        }
        return new org.apache.amoro.process.rest.ProcessRestSupport.TableIdentity(
            org.apache.amoro.process.trigger.simulated.SimulatedProcessFixture.TABLE_ID,
            org.apache.amoro.process.trigger.simulated.SimulatedProcessFixture.TABLE_FORMAT);
      }
    };
  }

  private static org.apache.amoro.process.rest.ProcessRestSupport.TableCatalogPort
      emptyProcessTableCatalog() {
    return new org.apache.amoro.process.rest.ProcessRestSupport.TableCatalogPort() {
      @Override
      public org.apache.amoro.process.rest.ProcessRestSupport.TableIdentity resolve(
          String catalog, String database, String table) {
        return null;
      }
    };
  }

  @Bean
  public org.apache.amoro.process.trigger.ManagedTablePort processManagedTables() {
    java.util.List<org.apache.amoro.process.trigger.ManagedTablePort.TableSnapshot> tables =
        new java.util.ArrayList<>();
    if (processProperties.getSimulation().isEnabled()) {
      tables.add(
          org.apache.amoro.process.trigger.simulated.SimulatedProcessFixture.tableSnapshot());
    }
    return new org.apache.amoro.process.trigger.SimulatedManagedTablePort(tables);
  }

  /**
   * The scheduling bridge: durable create/modify/replay events register a {@link
   * org.apache.amoro.process.ProcessReconciler} for that process on the shared scheduler — this is
   * what makes a REST-created process actually run without any manual wiring.
   */
  @Bean
  public org.apache.amoro.persistence.PersistenceListener<ProcessResource>
      processSchedulingListener(
          org.apache.amoro.process.ProcessDomainAssembly assembly,
          org.apache.amoro.process.engine.ProcessEngineRegistry engines,
          DefaultScheduler scheduler,
          org.apache.amoro.process.ProcessResultPersistenceRetryer resultRetryer,
          org.apache.amoro.process.ProcessSubmissionBuilder submissionBuilder) {
    org.apache.amoro.persistence.PersistenceListener<ProcessResource> listener =
        new org.apache.amoro.persistence.PersistenceListener<ProcessResource>() {
          @Override
          public void afterCreated(ProcessResource resource) {
            schedule(resource);
          }

          @Override
          public void afterModified(ProcessResource resource) {
            schedule(resource);
          }

          @Override
          public void afterDeleted(ProcessResource resource) {
            // the deletion hook already unscheduled the key in the mutation lane
          }

          @Override
          public void postStart(ProcessResource existing) {
            schedule(existing);
          }

          private void schedule(ProcessResource resource) {
            scheduler.schedule(
                new org.apache.amoro.process.ProcessReconciler(
                    resource.name(),
                    assembly.repository(),
                    engines,
                    scheduler,
                    org.apache.amoro.process.ProcessReconciler.Clock.systemUtc(),
                    processProperties.getReconcile().getPollIntervalMs(),
                    processProperties.getReconcile().getSubmissionUnresolvedIntervalMs(),
                    processProperties.getReconcile().getCancelRetryIntervalMs(),
                    processProperties.getReconcile().getCommandInFlightDelayMs(),
                    processProperties.getReconcile().getExecutionUnresolvedReminderIntervalMs(),
                    resultRetryer,
                    submissionBuilder));
          }
        };
    assembly.persistence().addListener(listener);
    // restart replay (spec §8.7) happens HERE, after the listener is registered: the read
    // model rebuilds from the durable rows and every live process is re-scheduled
    assembly.persistence().postStart();
    return listener;
  }

  @Bean
  public org.apache.amoro.process.ActiveProcessRescheduler activeProcessRescheduler(
      org.apache.amoro.process.ProcessDomainAssembly assembly,
      org.apache.amoro.process.engine.ProcessEngineRegistry engines,
      DefaultScheduler scheduler,
      org.apache.amoro.process.ProcessResultPersistenceRetryer resultRetryer,
      org.apache.amoro.process.ProcessSubmissionBuilder submissionBuilder,
      org.apache.amoro.persistence.PersistenceListener<ProcessResource> processSchedulingListener) {
    AmoroProcessProperties.Reconcile reconcile = processProperties.getReconcile();
    AmoroProcessProperties.Rescheduler rescheduler = processProperties.getRescheduler();
    return new org.apache.amoro.process.ActiveProcessRescheduler(
        assembly.indexProjection(),
        scheduler,
        name ->
            new org.apache.amoro.process.ProcessReconciler(
                name,
                assembly.repository(),
                engines,
                scheduler,
                org.apache.amoro.process.ProcessReconciler.Clock.systemUtc(),
                reconcile.getPollIntervalMs(),
                reconcile.getSubmissionUnresolvedIntervalMs(),
                reconcile.getCancelRetryIntervalMs(),
                reconcile.getCommandInFlightDelayMs(),
                reconcile.getExecutionUnresolvedReminderIntervalMs(),
                resultRetryer,
                submissionBuilder),
        rescheduler.getBatchSize(),
        rescheduler.getMaxRuntimeMs(),
        rescheduler.getIntervalMs());
  }

  @Bean
  public org.apache.amoro.process.engine.ExecutionHandleReaper executionHandleReaper(
      org.apache.amoro.process.ProcessDomainAssembly assembly,
      org.apache.amoro.process.engine.ProcessEngineRegistry engines,
      org.apache.amoro.persistence.PersistenceListener<ProcessResource> processSchedulingListener) {
    AmoroProcessProperties.ExecutionReaper reaper = processProperties.getExecutionReaper();
    return new org.apache.amoro.process.engine.ExecutionHandleReaper(
        assembly.releaseIndex(), engines, reaper.getBatchSize(), reaper.getIntervalMs());
  }

  @Bean
  public org.apache.amoro.process.ProcessTtlRuntime processTtlRuntime(
      org.apache.amoro.process.ProcessDomainAssembly assembly,
      org.apache.amoro.persistence.PersistenceListener<ProcessResource> processSchedulingListener) {
    AmoroProcessProperties.Ttl ttl = processProperties.getTtl();
    return new org.apache.amoro.process.ProcessTtlRuntime(
        new org.apache.amoro.process.ProcessTtlCleaner(assembly, assembly.handleRegistry()),
        ttl.getRetentionDays(),
        ttl.getBatchSize(),
        ttl.getIntervalMs());
  }

  @Bean
  public org.apache.amoro.process.trigger.ProcessTriggerCoordinator processTriggerCoordinator(
      org.apache.amoro.process.ProcessCreationService creationService,
      org.apache.amoro.process.trigger.ManagedTablePort tables,
      org.apache.amoro.process.trigger.ProcessActionRegistry actions,
      org.apache.amoro.persistence.PersistenceListener<ProcessResource> processSchedulingListener) {
    AmoroProcessProperties.Trigger trigger = processProperties.getTrigger();
    return new org.apache.amoro.process.trigger.ProcessTriggerCoordinator(
        creationService, tables, actions, trigger.getIntervalMs(), trigger.getBatchSize());
  }

  @Bean
  public ControlPlaneLifecycle controlPlaneLifecycle(
      DefaultScheduler scheduler,
      ListenerDispatcher<ControlledResource> dispatcher,
      org.apache.amoro.process.ProcessDomainAssembly processDomain,
      org.apache.amoro.process.trigger.ProcessTriggerCoordinator trigger,
      org.apache.amoro.process.ActiveProcessRescheduler rescheduler,
      org.apache.amoro.process.engine.ExecutionHandleReaper reaper,
      org.apache.amoro.process.ProcessTtlRuntime ttl,
      org.apache.amoro.process.engine.ProcessEngineRegistry engines,
      org.apache.amoro.process.ProcessResultPersistenceRetryer resultRetryer,
      org.springframework.context.ApplicationContext context) {
    List<InMemoryPersistence<?>> domains = new ArrayList<InMemoryPersistence<?>>();
    for (InMemoryPersistence<?> untyped :
        context.getBeansOfType(InMemoryPersistence.class).values()) {
      domains.add(untyped);
    }
    domains.add(processDomain.persistence());
    java.time.Duration timeout =
        java.time.Duration.ofMillis(properties.getLifecycle().getShutdownTimeoutMs());
    long timeoutMillis = timeout.toMillis();
    return new ControlPlaneLifecycle(
        scheduler::start,
        () -> {
          rescheduler.start();
          reaper.start();
          ttl.start();
          trigger.start();
        },
        () -> {
          trigger.shutdown(timeoutMillis);
          rescheduler.shutdown(timeoutMillis);
          reaper.shutdown(timeoutMillis);
          ttl.shutdown(timeoutMillis);
        },
        () -> scheduler.shutdown(timeout),
        () -> {
          engines.shutdown(timeoutMillis);
          resultRetryer.shutdown(timeoutMillis);
        },
        () -> dispatcher.shutdown(timeout),
        () -> {
          for (InMemoryPersistence<?> persistence : domains) {
            persistence.shutdown(timeout);
          }
        });
  }
}
