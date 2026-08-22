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
import org.apache.amoro.persistence.PersistenceListener;
import org.apache.amoro.persistence.PersistenceService;
import org.apache.amoro.persistence.Selector;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * L3: the namespace cross-cutting facade (framework spec §2). The current deployment model is a
 * single namespace, so this facade passes every call straight through to the backing {@link
 * PersistenceService} — fidelity ledger #7: the shape exists so a future multi-namespace deployment
 * can add resolution here without touching domain code. Extend the interface when a second
 * namespace actually arrives, not before.
 */
public final class NamespacedPersistenceServiceFacade<R extends ControlledResource>
    implements PersistenceService<R> {

  private final PersistenceService<R> delegate;

  public NamespacedPersistenceServiceFacade(PersistenceService<R> delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public CompletionStage<R> create(R resource) {
    return delegate.create(resource);
  }

  @Override
  public CompletionStage<R> modify(String id, Function<R, R> updateFn) {
    return delegate.modify(id, updateFn);
  }

  @Override
  public CompletionStage<R> modify(
      String id, long expectedResourceVersion, Function<R, R> updateFn) {
    return delegate.modify(id, expectedResourceVersion, updateFn);
  }

  @Override
  public CompletionStage<R> get(String id) {
    return delegate.get(id);
  }

  @Override
  public CompletionStage<R> delete(String id) {
    return delegate.delete(id);
  }

  @Override
  public CompletionStage<R> delete(String id, long expectedResourceVersion) {
    return delegate.delete(id, expectedResourceVersion);
  }

  @Override
  public CompletionStage<List<R>> select(Selector<R> selector) {
    return delegate.select(selector);
  }

  @Override
  public void addListener(PersistenceListener<R> listener) {
    delegate.addListener(listener);
  }

  @Override
  public void postStart() {
    delegate.postStart();
  }
}
