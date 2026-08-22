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

/**
 * The single-flight registry entry for one {@link ControllerKey}: the generation identity that
 * isolates a removed entry from a recreated one under the same key (framework spec §4.6). All state
 * transitions and the reschedule-deadline merge happen inside {@code synchronized (entry)} so no
 * two updaters can race a remove/reinsert.
 */
final class ScheduledEntry {

  enum State {
    QUEUED,
    CLAIMED,
    TERMINATED
  }

  final ControllerKey key;
  final ScheduledController wrapper;

  /** Guarded by this entry's monitor. */
  State state;

  /**
   * Earliest deadline requested by schedule() callers while the wrapper was in flight; the worker
   * consumes it after the invocation returns and merges it into the next deadline (earliest wins).
   * Null when no request is pending. Guarded by this entry's monitor.
   */
  Long rescheduleRequestedMillis;

  ScheduledEntry(ControllerKey key, ScheduledController wrapper) {
    this.key = key;
    this.wrapper = wrapper;
    this.state = State.QUEUED;
  }
}
