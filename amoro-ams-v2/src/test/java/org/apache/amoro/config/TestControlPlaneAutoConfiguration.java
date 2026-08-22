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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.persistence.InMemoryPersistence;
import org.apache.amoro.persistence.ListenerDispatcher;
import org.apache.amoro.persistence.blob.ResourceBlobMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * T10 assembly tests: property defaults, fail-fast validation, bean wiring over an embedded Derby
 * datasource, and the observable shutdown ordering.
 */
@Timeout(60)
public class TestControlPlaneAutoConfiguration {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withPropertyValues(
              "spring.datasource.url=jdbc:derby:memory:amoroV2T10;create=true",
              "spring.datasource.driver-class-name=org.apache.derby.iapi.jdbc.AutoloadedDriver")
          .withUserConfiguration(TestConfig.class);

  @Configuration
  @EnableConfigurationProperties(AmoroControlProperties.class)
  static class TestConfig {
    @Bean
    AmoroControlPropertiesValidator validator(AmoroControlProperties properties) {
      return new AmoroControlPropertiesValidator(properties);
    }
  }

  /** Mirrors the auto-configuration's @PostConstruct fail-fast without the full context. */
  static class AmoroControlPropertiesValidator {
    AmoroControlPropertiesValidator(AmoroControlProperties properties) {
      properties.validate();
    }
  }

  @Test
  public void propertyDefaultsMatchTheSpecTable() {
    AmoroControlProperties properties = new AmoroControlProperties();
    assertEquals(10, properties.getScheduler().getWorkers());
    assertEquals(3000L, properties.getScheduler().getDelayMs());
    assertEquals(65536, properties.getStorage().getMaxResourceBytes());
    assertEquals(1024, properties.getActor().getQueueCapacity());
    assertEquals(4, properties.getListener().getWorkers());
    assertEquals(1024, properties.getListener().getQueueCapacity());
    assertEquals(3, properties.getListener().getMaxRetries());
    assertEquals(1000L, properties.getListener().getRetryDelayMs());
    assertEquals(10000L, properties.getRepository().getTimeoutMs());
    assertEquals(10000L, properties.getLifecycle().getShutdownTimeoutMs());
  }

  @Test
  public void illegalValuesFailContextStartup() {
    runner
        .withPropertyValues("amoro.control.scheduler.workers=0")
        .run(
            context ->
                assertTrue(context.getStartupFailure() != null, "workers=0 must fail the context"));
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          AmoroControlProperties bad = new AmoroControlProperties();
          bad.getListener().setMaxRetries(-1);
          bad.validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          AmoroControlProperties bad = new AmoroControlProperties();
          bad.getRepository().setTimeoutMs(0);
          bad.validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          AmoroControlProperties bad = new AmoroControlProperties();
          bad.getLifecycle().setShutdownTimeoutMs(-5);
          bad.validate();
        });
  }

  @Test
  public void schemaInitializationIsIdempotentAcrossContextBoots() {
    for (String pass : new String[] {"first", "second"}) {
      runner
          .withUserConfiguration(FullConfig.class)
          .run(
              context -> {
                assertTrue(
                    context.getStartupFailure() == null,
                    pass + " boot failed: " + context.getStartupFailure());
                // a second initialize over the same database must be a no-op (metadata guard)
                context.getBean(ControlPlaneSchemaInitializer.class).initialize();
              });
    }
  }

  @Test
  public void shutdownOrderIsSchedulerThenDispatcherThenLanes() {
    List<String> order = new ArrayList<String>();
    ControlPlaneLifecycle lifecycle =
        new ControlPlaneLifecycle(
            () -> order.add("scheduler"), () -> order.add("dispatcher"), () -> order.add("lanes"));
    lifecycle.start();
    assertTrue(lifecycle.isRunning());
    lifecycle.stop();
    assertEquals(3, order.size());
    assertEquals("scheduler", order.get(0), "scheduler stops first");
    assertEquals("dispatcher", order.get(1), "dispatcher drains second");
    assertEquals("lanes", order.get(2), "mutation lanes drain last");
    assertTrue(!lifecycle.isRunning());
  }

  @Test
  public void fullAssemblyWiresCoreBeansTogether() {
    runner
        .withUserConfiguration(FullConfig.class)
        .run(
            context -> {
              if (context.getStartupFailure() != null) {
                throw new AssertionError("assembly failed", context.getStartupFailure());
              }
              assertTrue(context.getBean(DefaultScheduler.class) != null);
              assertTrue(context.getBean(ListenerDispatcher.class) != null);
              assertTrue(context.getBean(ControlPlaneDomainFactory.class) != null);
              assertTrue(context.getBean(ControlPlaneLifecycle.class) != null);
              assertTrue(context.getBean(ControlPlaneSchemaInitializer.class) != null);
              assertTrue(context.getBean(DefaultScheduler.class).registrySize() >= 0);
            });
  }

  /** Full wiring incl. the mybatis mapper over the test datasource. */
  @Configuration
  @EnableConfigurationProperties(AmoroControlProperties.class)
  static class FullConfig {
    @Bean
    javax.sql.DataSource dataSource() {
      org.springframework.jdbc.datasource.DriverManagerDataSource dataSource =
          new org.springframework.jdbc.datasource.DriverManagerDataSource();
      dataSource.setDriverClassName("org.apache.derby.iapi.jdbc.AutoloadedDriver");
      dataSource.setUrl("jdbc:derby:memory:amoroV2T10Full;create=true");
      return dataSource;
    }

    @Bean
    DefaultScheduler scheduler() {
      return DefaultScheduler.create(2, 1000L);
    }

    @Bean
    ListenerDispatcher<org.apache.amoro.persistence.ControlledResource> dispatcher() {
      return ListenerDispatcher.start("t10", 1, 16, 1, 5L);
    }

    @Bean
    ControlPlaneDomainFactory factory(
        org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory) {
      org.mybatis.spring.mapper.MapperFactoryBean<ResourceBlobMapper> mapperFactory =
          new org.mybatis.spring.mapper.MapperFactoryBean<>(ResourceBlobMapper.class);
      mapperFactory.setSqlSessionFactory(sqlSessionFactory);
      ResourceBlobMapper mapper;
      try {
        mapper = mapperFactory.getObject();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      return new ControlPlaneDomainFactory(mapper, new AmoroControlProperties(), dispatcher());
    }

    @Bean
    ControlPlaneSchemaInitializer initializer(javax.sql.DataSource dataSource) {
      return new ControlPlaneSchemaInitializer(dataSource);
    }

    @Bean
    ControlPlaneLifecycle lifecycle(
        DefaultScheduler scheduler,
        ListenerDispatcher<org.apache.amoro.persistence.ControlledResource> dispatcher) {
      return ControlPlaneLifecycle.from(
          scheduler, dispatcher, List.<InMemoryPersistence<?>>of(), 1000L);
    }

    @Bean
    org.apache.ibatis.session.SqlSessionFactory sqlSessionFactory(javax.sql.DataSource dataSource) {
      org.apache.ibatis.mapping.Environment environment =
          new org.apache.ibatis.mapping.Environment(
              "t10", new org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory(), dataSource);
      org.apache.ibatis.session.Configuration configuration =
          new org.apache.ibatis.session.Configuration(environment);
      configuration.addMapper(ResourceBlobMapper.class);
      return new org.apache.ibatis.session.SqlSessionFactoryBuilder().build(configuration);
    }
  }

  @Test
  public void converterRegistryValidationIsEager() {
    // the registry constructor itself fails on duplicates/broken chains — wired through the
    // factory this makes context startup fail instead of surfacing at first use
    org.apache.amoro.serde.VersionedResourceConverter v1ToV2 =
        new org.apache.amoro.serde.VersionedResourceConverter() {
          @Override
          public String fromVersion() {
            return "v1";
          }

          @Override
          public String toVersion() {
            return "v2";
          }

          @Override
          public com.fasterxml.jackson.databind.JsonNode upgrade(
              com.fasterxml.jackson.databind.JsonNode thisVersion) {
            return thisVersion;
          }
        };
    // v2 links nowhere but the latest is v3: broken chain fails construction eagerly
    assertThrows(
        IllegalArgumentException.class,
        () -> new org.apache.amoro.serde.SerdeRegistry("v3", List.of(v1ToV2)));
  }

  @Test
  public void propertiesBindFromConfiguration() {
    runner
        .withPropertyValues(
            "amoro.control.scheduler.workers=3",
            "amoro.control.listener.max-retries=0",
            "amoro.control.repository.timeout-ms=2500")
        .run(
            context -> {
              AmoroControlProperties properties = context.getBean(AmoroControlProperties.class);
              assertEquals(3, properties.getScheduler().getWorkers());
              assertEquals(0, properties.getListener().getMaxRetries());
              assertEquals(2500L, properties.getRepository().getTimeoutMs());
            });
  }
}
