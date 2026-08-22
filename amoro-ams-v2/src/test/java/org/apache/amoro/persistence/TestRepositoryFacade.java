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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.persistence.exception.PersistenceException;
import org.apache.amoro.persistence.exception.PreconditionFailedException;
import org.apache.amoro.persistence.exception.ResourceAlreadyExists;
import org.apache.amoro.persistence.exception.ResourceDoesNotExist;
import org.apache.amoro.persistence.facade.NamespacedPersistenceServiceFacade;
import org.apache.amoro.persistence.facade.RepositoryFacade;
import org.apache.amoro.serde.ResourceSerde;
import org.apache.amoro.serde.SerdeRegistry;
import org.apache.amoro.serde.VersionAwareJacksonSerde;
import org.apache.amoro.serde.VersionedResourceConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@Timeout(30)
public class TestRepositoryFacade {

  private static final class SlowFakeService
      implements PersistenceService<TestPersistenceListener.Res> {
    final AtomicReference<Duration> createDelay = new AtomicReference<>(Duration.ZERO);

    @Override
    public CompletionStage<TestPersistenceListener.Res> create(
        TestPersistenceListener.Res resource) {
      return delayedFuture(resource);
    }

    private CompletableFuture<TestPersistenceListener.Res> delayedFuture(
        TestPersistenceListener.Res resource) {
      CompletableFuture<TestPersistenceListener.Res> future = new CompletableFuture<>();
      Thread worker =
          new Thread(
              () -> {
                try {
                  Duration delay = createDelay.get();
                  if (!delay.isZero()) {
                    Thread.sleep(delay.toMillis());
                  }
                  future.complete(resource);
                } catch (InterruptedException e) {
                  future.completeExceptionally(e);
                }
              },
              "slow-fake");
      worker.setDaemon(true);
      worker.start();
      return future;
    }

    @Override
    public CompletionStage<TestPersistenceListener.Res> modify(
        String id, Function<TestPersistenceListener.Res, TestPersistenceListener.Res> updateFn) {
      return CompletableFuture.failedFuture(new ResourceDoesNotExist("facade", id));
    }

    @Override
    public CompletionStage<TestPersistenceListener.Res> modify(
        String id,
        long expectedResourceVersion,
        Function<TestPersistenceListener.Res, TestPersistenceListener.Res> updateFn) {
      if ("x".equals(id)) {
        return CompletableFuture.failedFuture(new ResourceDoesNotExist("facade", id));
      }
      return CompletableFuture.failedFuture(
          new PreconditionFailedException("facade", id, expectedResourceVersion, 99L));
    }

    @Override
    public CompletionStage<TestPersistenceListener.Res> get(String id) {
      return CompletableFuture.completedFuture(new TestPersistenceListener.Res(id, 4L));
    }

    @Override
    public CompletionStage<TestPersistenceListener.Res> delete(String id) {
      return CompletableFuture.failedFuture(new ResourceAlreadyExists("facade", id));
    }

    @Override
    public CompletionStage<TestPersistenceListener.Res> delete(
        String id, long expectedResourceVersion) {
      return CompletableFuture.completedFuture(new TestPersistenceListener.Res(id, 4L));
    }

    @Override
    public CompletionStage<List<TestPersistenceListener.Res>> select(
        Selector<TestPersistenceListener.Res> selector) {
      List<TestPersistenceListener.Res> list =
          Collections.singletonList(new TestPersistenceListener.Res("one", 1L));
      return CompletableFuture.completedFuture(list);
    }

    @Override
    public void addListener(PersistenceListener<TestPersistenceListener.Res> listener) {}

    @Override
    public void postStart() {}
  }

  private SlowFakeService service;
  private RepositoryFacade<TestPersistenceListener.Res> facade;

  @BeforeEach
  public void setUp() {
    service = new SlowFakeService();
    facade = new RepositoryFacade<TestPersistenceListener.Res>(service, 500L);
  }

  @Test
  public void normalOperationsReturnValues() {
    TestPersistenceListener.Res created = facade.create(new TestPersistenceListener.Res("r1", 0L));
    assertEquals("r1", created.name());

    assertEquals(4L, facade.get("r1").resourceVersion());
    assertEquals(1, facade.select(Selector.of("res", r -> true)).size());
    assertEquals(4L, facade.delete("r1", 4L).resourceVersion());
  }

  @Test
  public void underlyingFailuresSurfaceUnchanged() {
    assertThrows(ResourceDoesNotExist.class, () -> facade.modify("x", 1L, r -> r));
    assertThrows(PreconditionFailedException.class, () -> facade.modify("y", 1L, r -> r));
  }

  @Test
  public void slowStageTimesOutWithConfigurableBound() {
    service.createDelay.set(Duration.ofSeconds(5));
    long start = System.nanoTime();
    PersistenceException thrown =
        assertThrows(
            PersistenceException.class,
            () -> facade.create(new TestPersistenceListener.Res("slow", 0L)));
    long elapsedMillis =
        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(elapsedMillis < 2000L, "facade timeout must kick in early");
    assertTrue(thrown.getMessage().contains("500"), "message carries the bound");
  }

  @Test
  public void timeoutBoundIsConfigurable() {
    RepositoryFacade<TestPersistenceListener.Res> oneSecond =
        new RepositoryFacade<TestPersistenceListener.Res>(service, 1000L);
    service.createDelay.set(Duration.ofMillis(600));
    assertEquals("ok", oneSecond.create(new TestPersistenceListener.Res("ok", 0L)).name());
  }

  @Test
  public void namespacedFacadePassesThroughSingleNamespace() throws Exception {
    // L3 single-namespace pass-through: every call reaches the backing service unchanged
    org.apache.amoro.persistence.blob.BlobStore blob =
        new TestPersistenceListener.FakeBlobStoreForListener();
    ResourceSerde<TestPersistenceListener.Res> serde =
        new VersionAwareJacksonSerde<TestPersistenceListener.Res>(
            TestPersistenceListener.Res.class,
            new SerdeRegistry("v1", new ArrayList<VersionedResourceConverter>()),
            SerdeFormat.JSON,
            65536);
    InMemoryPersistence<TestPersistenceListener.Res> persistence =
        new InMemoryPersistence<TestPersistenceListener.Res>(
            new PersistenceDomain("ns", "amoro_resource", SerdeFormat.JSON),
            "res",
            serde,
            blob,
            64,
            event -> HandoffResult.ACCEPTED,
            Collections.emptyList(),
            r -> {});
    NamespacedPersistenceServiceFacade<TestPersistenceListener.Res> namespaced =
        new NamespacedPersistenceServiceFacade<TestPersistenceListener.Res>(persistence);
    try {
      namespaced.create(new TestPersistenceListener.Res("n1", 0L)).toCompletableFuture().join();
      assertEquals(
          1L,
          namespaced
              .get("n1")
              .toCompletableFuture()
              .get(5, java.util.concurrent.TimeUnit.SECONDS)
              .resourceVersion());
      assertEquals(
          2L,
          namespaced
              .modify("n1", 1L, r -> r)
              .toCompletableFuture()
              .get(5, java.util.concurrent.TimeUnit.SECONDS)
              .resourceVersion());
    } finally {
      persistence.shutdown(Duration.ofSeconds(5));
    }
  }
}
