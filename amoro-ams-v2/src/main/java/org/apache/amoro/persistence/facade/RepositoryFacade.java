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

package org.apache.amoro.persistence.facade;

import org.apache.amoro.persistence.ControlledResource;
import org.apache.amoro.persistence.PersistenceService;
import org.apache.amoro.persistence.Repository;
import org.apache.amoro.persistence.Selector;
import org.apache.amoro.persistence.exception.PersistenceException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * L2: the synchronous domain-facing facade over one {@link PersistenceService} (framework spec
 * §2/§7). Domain code should not juggle asynchronous stages; the facade awaits each stage with a
 * configurable bounded timeout ({@code amoro.control.repository.timeout-ms}, default 10s) and
 * surfaces the underlying failure unchanged. Only version-CAS mutations are exposed (Spec §5.2).
 *
 * <p>A timeout does NOT mean the write failed to commit — the underlying stage keeps running
 * durable-first on the mutation lane and may still commit afterwards. Level-triggered callers
 * converge by re-reading; never blind-retry a mutation after a timeout.
 */
public final class RepositoryFacade<R extends ControlledResource> implements Repository<R> {

  private final PersistenceService<R> service;
  private final long timeoutMillis;

  public RepositoryFacade(PersistenceService<R> service, long timeoutMillis) {
    this.service = Objects.requireNonNull(service, "service");
    if (timeoutMillis <= 0) {
      throw new IllegalArgumentException("timeoutMillis must be > 0, got " + timeoutMillis);
    }
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public R create(R resource) {
    return await(service.create(resource));
  }

  @Override
  public R get(String name) {
    return await(service.get(name));
  }

  @Override
  public R modify(String name, long expectedResourceVersion, Function<R, R> updateFn) {
    return await(service.modify(name, expectedResourceVersion, updateFn));
  }

  @Override
  public List<R> select(Selector<R> selector) {
    return await(service.select(selector));
  }

  @Override
  public R delete(String name, long expectedResourceVersion) {
    return await(service.delete(name, expectedResourceVersion));
  }

  private <T> T await(CompletionStage<T> stage) {
    try {
      return stage.toCompletableFuture().get(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new PersistenceException(
          "repository operation did not complete within " + timeoutMillis + "ms", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PersistenceException("interrupted while awaiting the repository stage", e);
    } catch (java.util.concurrent.ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new PersistenceException("repository stage failed", e.getCause());
    }
  }
}
