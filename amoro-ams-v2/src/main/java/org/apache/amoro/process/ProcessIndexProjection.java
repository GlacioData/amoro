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

package org.apache.amoro.process;

import org.apache.amoro.persistence.DurableStateProjection;
import org.apache.amoro.persistence.PersistenceChange;
import org.apache.amoro.persistence.PreparedProjectionUpdate;
import org.apache.amoro.resources.ProcessResource;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Two-phase index maintenance for the Process domain (framework spec §5.1 + process spec §8.7):
 * prepare builds the next immutable {@link ProcessIndexSnapshot} on the mutation lane before the DB
 * write (pure, no I/O); commit swaps the single {@code AtomicReference} after the durable success —
 * an O(1) visibility flip. Readers take the reference once and read both resource bodies and index
 * views from that one snapshot.
 */
public final class ProcessIndexProjection implements DurableStateProjection<ProcessResource> {

  private final AtomicReference<ProcessIndexSnapshot> snapshot =
      new AtomicReference<>(ProcessIndexSnapshot.empty());

  public ProcessIndexSnapshot current() {
    return snapshot.get();
  }

  @Override
  public PreparedProjectionUpdate prepare(PersistenceChange<ProcessResource> change) {
    ProcessIndexSnapshot base = snapshot.get();
    ProcessResource previous = change.previous();
    ProcessResource current = change.current();
    ProcessIndexSnapshot next = base.apply(previous, current);
    return new Swap(snapshot, base, next);
  }

  /** Prepared update carrying the exact base and next snapshots (identity CAS). */
  private static final class Swap implements PreparedProjectionUpdate {
    private final AtomicReference<ProcessIndexSnapshot> reference;
    private final ProcessIndexSnapshot base;
    private final ProcessIndexSnapshot next;

    Swap(
        AtomicReference<ProcessIndexSnapshot> reference,
        ProcessIndexSnapshot base,
        ProcessIndexSnapshot next) {
      this.reference = reference;
      this.base = base;
      this.next = next;
    }

    @Override
    public void commit() {
      reference.compareAndSet(base, next);
    }
  }
}
