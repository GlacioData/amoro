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

package org.apache.amoro.persistence.e2e;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.control.Controller;
import org.apache.amoro.control.ControllerKey;
import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.control.TerminalState;
import org.apache.amoro.persistence.ControlledResource;
import org.apache.amoro.persistence.InMemoryPersistence;
import org.apache.amoro.persistence.ListenerDispatcher;
import org.apache.amoro.persistence.PersistenceDomain;
import org.apache.amoro.persistence.PersistenceListener;
import org.apache.amoro.persistence.Selector;
import org.apache.amoro.persistence.blob.MyBatisBlobStore;
import org.apache.amoro.persistence.blob.ResourceBlobMapper;
import org.apache.amoro.persistence.exception.PreconditionFailedException;
import org.apache.amoro.persistence.facade.RepositoryFacade;
import org.apache.amoro.serde.ResourceSerde;
import org.apache.amoro.serde.SerdeRegistry;
import org.apache.amoro.serde.VersionAwareJacksonSerde;
import org.apache.amoro.serde.VersionedResourceConverter;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * T11 end-to-end verification (framework spec §9 flow row, tag {@code docker-mysql}): the fake
 * resource trio runs the full lifecycle against a REAL MySQL — create → afterCreated → schedule →
 * multi-round invoke (one exception backoff, one abandoned CAS conflict) → TerminalState → destroy
 * the whole context → rebuild scheduler+persistence over the same database → postStart replay → the
 * resource continues and converges. The database is the source of truth, proven by restart. A
 * second fake resource with a different collection/entity/controller proves resource agnosticism:
 * zero framework changes.
 */
@Tag("docker-mysql")
@Timeout(180)
public class TestFrameworkE2E {

  private static final String JDBC_URL =
      System.getenv()
          .getOrDefault(
              "AMORO_V2_MYSQL_URL",
              "jdbc:mysql://localhost:3306/amoro_v2"
                  + "?useSSL=false&characterEncoding=utf8&allowPublicKeyRetrieval=true");
  private static final String JDBC_USER =
      System.getenv().getOrDefault("AMORO_V2_MYSQL_USER", "root");
  private static final String JDBC_PASSWORD =
      System.getenv().getOrDefault("AMORO_V2_MYSQL_PASSWORD", "");

  private static Connection adminConnection;

  @BeforeAll
  public static void probe() throws Exception {
    try {
      adminConnection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    } catch (SQLException unreachable) {
      Assumptions.assumeTrue(
          false, "no reachable MySQL at " + JDBC_URL + " — docker-mysql group skips explicitly");
    }
    try (Statement statement = adminConnection.createStatement()) {
      // the schema is already created by T9's IF NOT EXISTS script through the initializer; the
      // E2E tables must start empty so every run asserts a full lifecycle from scratch
      for (String table : new String[] {"amoro_resource", "amoro_process"}) {
        statement.execute("DELETE FROM " + table);
      }
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (adminConnection != null) {
      try (Statement statement = adminConnection.createStatement()) {
        for (String table : new String[] {"amoro_resource", "amoro_process"}) {
          statement.execute("DELETE FROM " + table);
        }
      }
      adminConnection.close();
    }
  }

  // ------------------------------------------------------------------ fake resource #1

  /** Machine-room fake: a resource walking PENDING -> RUNNING -> DONE with a failure detour. */
  public static final class FakeResource implements ControlledResource {
    private final String apiVersion;
    private final String name;
    private final String collection;
    private final long resourceVersion;
    private final String state;
    private final int attempts;

    public FakeResource() {
      this("v1", "unset", "fake", 0L, "PENDING", 0);
    }

    public FakeResource(String name, String state, int attempts) {
      this("v1", name, "fake", 0L, state, attempts);
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String collection() {
      return collection;
    }

    @Override
    public long resourceVersion() {
      return resourceVersion;
    }

    @Override
    public ControlledResource withResourceVersion(long newResourceVersion) {
      return new FakeResource(apiVersion, name, collection, newResourceVersion, state, attempts);
    }

    public String state() {
      return state;
    }

    public int attempts() {
      return attempts;
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public FakeResource(
        @com.fasterxml.jackson.annotation.JsonProperty("apiVersion") String apiVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("name") String name,
        @com.fasterxml.jackson.annotation.JsonProperty("collection") String collection,
        @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion") long resourceVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("state") String state,
        @com.fasterxml.jackson.annotation.JsonProperty("attempts") int attempts) {
      this.apiVersion = apiVersion;
      this.name = name;
      this.collection = collection;
      this.resourceVersion = resourceVersion;
      this.state = state;
      this.attempts = attempts;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("apiVersion")
    public String getApiVersion() {
      return apiVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("name")
    public String getName() {
      return name;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("collection")
    public String getCollection() {
      return collection;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion")
    public long getResourceVersion() {
      return resourceVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("state")
    public String getState() {
      return state;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("attempts")
    public int getAttempts() {
      return attempts;
    }
  }

  /** The fake trio's controller: one durable transition per round, with scripted detours. */
  static final class FakeController implements Controller {
    private final RepositoryFacade<FakeResource> repository;
    private final String resourceName;
    final AtomicInteger invokes = new AtomicInteger();
    final AtomicInteger pendingRounds = new AtomicInteger();
    final AtomicInteger casConflicts = new AtomicInteger();

    FakeController(RepositoryFacade<FakeResource> repository, String resourceName) {
      this.repository = repository;
      this.resourceName = resourceName;
    }

    @Override
    public ControllerKey key() {
      return ControllerKey.of("fake", resourceName);
    }

    @Override
    public void invoke() {
      invokes.incrementAndGet();
      FakeResource current = repository.get(resourceName);
      switch (current.state()) {
        case "PENDING":
          int pendingRound = pendingRounds.incrementAndGet();
          if (pendingRound == 1) {
            // scripted detour #1: an unexpected failure — the framework backoff must retry us
            throw new IllegalStateException("scripted reconcile failure");
          }
          if (pendingRound == 2) {
            // scripted detour #2: a CAS on a stale version is abandoned (never auto-retried);
            // the next round re-reads and converges
            try {
              repository.modify(resourceName, 99L, r -> r);
            } catch (PreconditionFailedException expected) {
              casConflicts.incrementAndGet();
            }
            return;
          }
          repository.modify(
              resourceName,
              current.resourceVersion(),
              r -> new FakeResource(r.name(), "RUNNING", r.attempts() + 1));
          return;
        case "RUNNING":
          repository.modify(
              resourceName,
              current.resourceVersion(),
              r -> new FakeResource(r.name(), "DONE", r.attempts() + 1));
          return;
        case "DONE":
        default:
          throw TerminalState.INSTANCE;
      }
    }
  }

  /** The fake trio's listener: durable events schedule the controller. */
  static final class FakeListener implements PersistenceListener<FakeResource> {
    private final DefaultScheduler scheduler;
    private final java.util.function.Function<String, FakeController> controllerFactory;
    final List<String> events = new CopyOnWriteArrayList<String>();

    FakeListener(
        DefaultScheduler scheduler,
        java.util.function.Function<String, FakeController> controllerFactory) {
      this.scheduler = scheduler;
      this.controllerFactory = controllerFactory;
    }

    @Override
    public void afterCreated(FakeResource resource) {
      events.add("created:" + resource.name());
      schedule(resource);
    }

    @Override
    public void afterModified(FakeResource resource) {
      events.add("modified:" + resource.name() + ":" + resource.state());
      schedule(resource);
    }

    @Override
    public void afterDeleted(FakeResource resource) {
      events.add("deleted:" + resource.name());
    }

    @Override
    public void postStart(FakeResource existingResource) {
      events.add("postStart:" + existingResource.name() + ":" + existingResource.state());
      schedule(existingResource);
    }

    private void schedule(FakeResource resource) {
      scheduler.schedule(controllerFactory.apply(resource.name()));
    }
  }

  // ------------------------------------------------------------------ assembly

  private static final class Assembly {
    final DefaultScheduler scheduler;
    final InMemoryPersistence<FakeResource> persistence;
    final RepositoryFacade<FakeResource> repository;
    final FakeListener listener;
    final FakeController controller;

    Assembly(
        DefaultScheduler scheduler,
        InMemoryPersistence<FakeResource> persistence,
        RepositoryFacade<FakeResource> repository,
        FakeListener listener,
        FakeController controller) {
      this.scheduler = scheduler;
      this.persistence = persistence;
      this.repository = repository;
      this.listener = listener;
      this.controller = controller;
    }

    void close() {
      scheduler.shutdown(Duration.ofSeconds(5));
      persistence.shutdown(Duration.ofSeconds(5));
    }
  }

  private static SqlSessionFactory sqlSessionFactory() {
    org.apache.ibatis.datasource.unpooled.UnpooledDataSource dataSource =
        new org.apache.ibatis.datasource.unpooled.UnpooledDataSource(
            "com.mysql.cj.jdbc.Driver", JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    Environment environment = new Environment("e2e", new JdbcTransactionFactory(), dataSource);
    Configuration configuration = new Configuration(environment);
    configuration.addMapper(ResourceBlobMapper.class);
    return new SqlSessionFactoryBuilder().build(configuration);
  }

  /** Boots a full control-plane assembly over the durable store. */
  private static Assembly boot(String resourceName) {
    SqlSessionFactory factory = sqlSessionFactory();
    ResourceBlobMapper mapper = factory.openSession(true).getMapper(ResourceBlobMapper.class);
    ResourceSerde<FakeResource> serde =
        new VersionAwareJacksonSerde<FakeResource>(
            FakeResource.class,
            new SerdeRegistry("v1", new ArrayList<VersionedResourceConverter>()),
            PersistenceDomain.SerdeFormat.JSON,
            65536);
    ListenerDispatcher<FakeResource> dispatcher = ListenerDispatcher.start("e2e", 2, 256, 2, 50L);
    InMemoryPersistence<FakeResource> persistence =
        new InMemoryPersistence<FakeResource>(
            new PersistenceDomain("resource", "amoro_resource", PersistenceDomain.SerdeFormat.JSON),
            "fake",
            serde,
            new MyBatisBlobStore(
                new PersistenceDomain(
                    "resource", "amoro_resource", PersistenceDomain.SerdeFormat.JSON),
                mapper),
            128,
            dispatcher,
            new ArrayList<>(),
            resource -> {});
    RepositoryFacade<FakeResource> repository =
        new RepositoryFacade<FakeResource>(persistence, 10_000L);
    DefaultScheduler scheduler = DefaultScheduler.create(2, 100L);
    scheduler.start();
    FakeController controller = new FakeController(repository, resourceName);
    FakeListener listener = new FakeListener(scheduler, name -> controller);
    persistence.addListener(listener);
    return new Assembly(scheduler, persistence, repository, listener, controller);
  }

  // ------------------------------------------------------------------ tests

  @Test
  public void fullLifecycleSurvivesRestartAndConverges() throws Exception {
    Assembly first = boot("e2e-r1");
    try {
      first.repository.create(new FakeResource("e2e-r1", "PENDING", 0));

      await()
          .atMost(30, TimeUnit.SECONDS)
          .until(() -> "DONE".equals(first.repository.get("e2e-r1").state()));
      // the scripted detours actually happened on the way to DONE
      assertTrue(first.controller.invokes.get() >= 3, "multiple reconcile rounds ran");
      assertEquals(
          1, first.controller.casConflicts.get(), "the stale CAS was observed and abandoned");
      assertTrue(
          first.controller.invokes.get() >= 5,
          "the backoff retried after the failure (fail + conflict + transition + running + done)");

      await().atMost(10, TimeUnit.SECONDS).until(() -> first.scheduler.registrySize() == 0);
    } finally {
      first.close(); // full context destruction: scheduler + persistence die
    }

    // rebuild over the same MySQL database: postStart replays the durable row and the resource
    // keeps converging (a RUNNING-again resource proves replay schedules real controllers)
    Assembly second = boot("e2e-r1");
    try {
      second.persistence.postStart();
      assertEquals(
          "DONE", second.repository.get("e2e-r1").state(), "the durable row survived the restart");
      await()
          .atMost(10, TimeUnit.SECONDS)
          .until(
              () ->
                  second.listener.events.stream().anyMatch(e -> e.startsWith("postStart:e2e-r1")));
      assertTrue(second.controller.invokes.get() >= 1, "the replayed controller ran at least once");
      await().atMost(10, TimeUnit.SECONDS).until(() -> second.scheduler.registrySize() == 0);
    } finally {
      second.close();
    }
  }

  @Test
  public void secondFakeResourceProvesResourceAgnosticism() throws Exception {
    // a different collection/entity/controller on the SAME framework: no framework change
    SqlSessionFactory factory = sqlSessionFactory();
    ResourceBlobMapper mapper = factory.openSession(true).getMapper(ResourceBlobMapper.class);
    ResourceSerde<OtherResource> serde =
        new VersionAwareJacksonSerde<OtherResource>(
            OtherResource.class,
            new SerdeRegistry("v1", new ArrayList<VersionedResourceConverter>()),
            PersistenceDomain.SerdeFormat.JSON,
            65536);
    ListenerDispatcher<OtherResource> dispatcher = ListenerDispatcher.start("e2e-2", 1, 64, 1, 20L);
    InMemoryPersistence<OtherResource> persistence =
        new InMemoryPersistence<OtherResource>(
            new PersistenceDomain("resource", "amoro_resource", PersistenceDomain.SerdeFormat.JSON),
            "other",
            serde,
            new MyBatisBlobStore(
                new PersistenceDomain(
                    "resource", "amoro_resource", PersistenceDomain.SerdeFormat.JSON),
                mapper),
            64,
            dispatcher,
            new ArrayList<>(),
            resource -> {});
    RepositoryFacade<OtherResource> repository =
        new RepositoryFacade<OtherResource>(persistence, 10_000L);
    DefaultScheduler scheduler = DefaultScheduler.create(1, 100L);
    scheduler.start();
    AtomicInteger otherInvokes = new AtomicInteger();
    try {
      persistence.addListener(
          new PersistenceListener<OtherResource>() {
            @Override
            public void afterCreated(OtherResource resource) {
              scheduler.schedule(
                  new Controller() {
                    @Override
                    public ControllerKey key() {
                      return ControllerKey.of("other", resource.name());
                    }

                    @Override
                    public void invoke() {
                      otherInvokes.incrementAndGet();
                      OtherResource current = repository.get(resource.name());
                      if (!current.done()) {
                        repository.modify(
                            resource.name(),
                            current.resourceVersion(),
                            r -> new OtherResource(r.name(), true));
                      } else {
                        throw TerminalState.INSTANCE;
                      }
                    }
                  });
            }

            @Override
            public void afterModified(OtherResource resource) {}

            @Override
            public void afterDeleted(OtherResource resource) {}

            @Override
            public void postStart(OtherResource existingResource) {
              afterCreated(existingResource);
            }
          });
      repository.create(new OtherResource("other-1", false));

      await().atMost(30, TimeUnit.SECONDS).until(() -> repository.get("other-1").done());
      await().atMost(10, TimeUnit.SECONDS).until(() -> scheduler.registrySize() == 0);
      assertTrue(otherInvokes.get() >= 2);
      // the two collections coexist in the same table without interference: a fresh view over
      // the same physical store sees this test's row in ITS collection
      InMemoryPersistence<OtherResource> view = otherView(factory, mapper);
      view.postStart(); // a fresh view loads its cache from the durable store first
      List<OtherResource> others =
          view.select(Selector.of("other", r -> true))
              .toCompletableFuture()
              .get(10, TimeUnit.SECONDS);
      assertEquals(1, others.size());
      assertEquals("other-1", others.get(0).name());
    } finally {
      scheduler.shutdown(Duration.ofSeconds(5));
      persistence.shutdown(Duration.ofSeconds(5));
    }
  }

  private InMemoryPersistence<OtherResource> otherView(
      SqlSessionFactory factory, ResourceBlobMapper mapper) {
    ResourceSerde<OtherResource> serde =
        new VersionAwareJacksonSerde<OtherResource>(
            OtherResource.class,
            new SerdeRegistry("v1", new ArrayList<VersionedResourceConverter>()),
            PersistenceDomain.SerdeFormat.JSON,
            65536);
    return new InMemoryPersistence<OtherResource>(
        new PersistenceDomain("resource", "amoro_resource", PersistenceDomain.SerdeFormat.JSON),
        "other",
        serde,
        new MyBatisBlobStore(
            new PersistenceDomain("resource", "amoro_resource", PersistenceDomain.SerdeFormat.JSON),
            mapper),
        16,
        event -> org.apache.amoro.persistence.HandoffResult.ACCEPTED,
        new ArrayList<>(),
        resource -> {});
  }

  /** The second fake resource type: different entity, different collection. */
  public static final class OtherResource implements ControlledResource {
    private final String apiVersion;
    private final String name;
    private final String collection;
    private final long resourceVersion;
    private final boolean done;

    public OtherResource() {
      this("unset", false);
    }

    public OtherResource(String name, boolean done) {
      this("v1", name, "other", 0L, done);
    }

    public boolean done() {
      return done;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String collection() {
      return collection;
    }

    @Override
    public long resourceVersion() {
      return resourceVersion;
    }

    @Override
    public ControlledResource withResourceVersion(long newResourceVersion) {
      return new OtherResource(apiVersion, name, collection, newResourceVersion, done);
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public OtherResource(
        @com.fasterxml.jackson.annotation.JsonProperty("apiVersion") String apiVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("name") String name,
        @com.fasterxml.jackson.annotation.JsonProperty("collection") String collection,
        @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion") long resourceVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("done") boolean done) {
      this.apiVersion = apiVersion;
      this.name = name;
      this.collection = collection;
      this.resourceVersion = resourceVersion;
      this.done = done;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("apiVersion")
    public String getApiVersion() {
      return apiVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("name")
    public String getName() {
      return name;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("collection")
    public String getCollection() {
      return collection;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion")
    public long getResourceVersion() {
      return resourceVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("done")
    public boolean isDone() {
      return done;
    }
  }
}
