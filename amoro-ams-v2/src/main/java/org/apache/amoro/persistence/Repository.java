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

import java.util.List;
import java.util.function.Function;

/**
 * L1: the synchronous, domain-facing surface over one {@link PersistenceService} (framework spec
 * §2). The L2 facade adapts the asynchronous stages with a bounded timeout; values crossing this
 * boundary are detached copies.
 *
 * <p>Deliberately exposes only version-CAS mutations: resource state machines must never bypass
 * optimistic concurrency, so the unconditional modify overload of {@link PersistenceService} is not
 * reachable from here.
 */
public interface Repository<R extends ControlledResource> {

  R create(R resource);

  R get(String name);

  R modify(String name, long expectedResourceVersion, Function<R, R> updateFn);

  List<R> select(Selector<R> selector);

  R delete(String name, long expectedResourceVersion);
}
