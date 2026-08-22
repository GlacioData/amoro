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

import java.util.Objects;
import java.util.function.Predicate;

/**
 * In-memory selection: candidates are enumerated from the read projection without touching the
 * database; the predicate receives detached copies only.
 */
public final class Selector<R extends ControlledResource> {

  private final String collection;
  private final Predicate<R> predicate;

  private Selector(String collection, Predicate<R> predicate) {
    this.collection = collection; // nullable: match every collection
    this.predicate = Objects.requireNonNull(predicate, "predicate");
  }

  public static <R extends ControlledResource> Selector<R> of(
      String collection, Predicate<R> predicate) {
    return new Selector<R>(collection, predicate);
  }

  public static <R extends ControlledResource> Selector<R> anyCollection(Predicate<R> predicate) {
    return new Selector<R>(null, predicate);
  }

  /** Resource kind filter; null means every collection. */
  public String collection() {
    return collection;
  }

  public boolean test(R candidate) {
    return predicate.test(candidate);
  }
}
