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

package org.apache.amoro.control;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Production {@link SchedulerWaitStrategy} on a {@link ReentrantLock} condition. The version is
 * re-checked inside the lock before parking, so a signal raised between a caller's peek and its
 * await is never lost.
 */
public final class ConditionWaitStrategy implements SchedulerWaitStrategy {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition changed = lock.newCondition();
  private long version; // guarded by lock

  @Override
  public long signalVersion() {
    lock.lock();
    try {
      return version;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void awaitChange(long observedVersion) throws InterruptedException {
    lock.lock();
    try {
      while (version == observedVersion) {
        changed.await();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void awaitChange(long observedVersion, Duration maximumWait) throws InterruptedException {
    lock.lock();
    try {
      long remainingNanos = maximumWait.toNanos();
      while (version == observedVersion && remainingNanos > 0L) {
        remainingNanos = changed.awaitNanos(remainingNanos);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void signal() {
    lock.lock();
    try {
      version++;
      changed.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
