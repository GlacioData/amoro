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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/** Bounded striped admission mutexes plus fail-closed reservations for unknown create outcomes. */
final class ProcessAdmissionRegistry {

  private static final int STRIPES = 256;

  private final ReentrantLock[] locks = new ReentrantLock[STRIPES];
  private final long lockTimeoutMillis;
  private final ConcurrentHashMap<String, Reservation> reservationsByScope =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, String> scopeByProcessName = new ConcurrentHashMap<>();

  ProcessAdmissionRegistry(Duration lockTimeout) {
    if (lockTimeout == null || lockTimeout.isZero() || lockTimeout.isNegative()) {
      throw new IllegalArgumentException("lockTimeout must be positive");
    }
    this.lockTimeoutMillis = lockTimeout.toMillis();
    if (lockTimeoutMillis <= 0L) {
      throw new IllegalArgumentException("lockTimeout must be at least one millisecond");
    }
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantLock();
    }
  }

  Lease acquire(String scope) {
    ReentrantLock lock = locks[(scope.hashCode() & Integer.MAX_VALUE) % locks.length];
    boolean acquired;
    try {
      acquired = lock.tryLock(lockTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw inProgress(scope, "interrupted while waiting for admission");
    }
    if (!acquired) {
      throw inProgress(scope, "admission lock is held by another create");
    }
    return new Lease(lock);
  }

  Optional<Reservation> reservation(String scope) {
    return Optional.ofNullable(reservationsByScope.get(scope));
  }

  void reserve(String scope, String processName, String keyHash, String requestHash) {
    Reservation candidate = new Reservation(processName, keyHash, requestHash);
    Reservation existing = reservationsByScope.putIfAbsent(scope, candidate);
    if (existing != null && !existing.processName.equals(processName)) {
      throw inProgress(scope, "another unknown create already reserves the scope");
    }
    scopeByProcessName.put(processName, scope);
  }

  Optional<String> scopeForProcess(String processName) {
    return Optional.ofNullable(scopeByProcessName.get(processName));
  }

  void clear(String scope, String processName) {
    reservationsByScope.computeIfPresent(
        scope, (ignored, reservation) -> reservation.processName.equals(processName) ? null : reservation);
    scopeByProcessName.remove(processName, scope);
  }

  private static ProcessAdmissionException inProgress(String scope, String detail) {
    return new ProcessAdmissionException(
        ProcessAdmissionException.Code.ADMISSION_IN_PROGRESS,
        "process admission is unresolved for " + scope + ": " + detail);
  }

  static final class Reservation {
    private final String processName;
    private final String keyHash;
    private final String requestHash;

    Reservation(String processName, String keyHash, String requestHash) {
      this.processName = processName;
      this.keyHash = keyHash;
      this.requestHash = requestHash;
    }

    String processName() {
      return processName;
    }

    String keyHash() {
      return keyHash;
    }

    String requestHash() {
      return requestHash;
    }
  }

  static final class Lease implements AutoCloseable {
    private final ReentrantLock lock;
    private boolean closed;

    Lease(ReentrantLock lock) {
      this.lock = lock;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        lock.unlock();
      }
    }
  }
}
