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

/**
 * The fixed port between the durable mutation lane and asynchronous listener execution (T4
 * contract; T5 tests it with a fake sink, T6 provides the bounded dispatcher implementation).
 *
 * <p>handoff only transfers the envelope; it never executes the listener callback. The caller
 * (mutation lane) treats {@link HandoffResult#ACCEPTED} as "delivered to the async world" and
 * {@link HandoffResult#DROPPED} as "queued out — record a metric/alert and rely on the resource
 * domain repair sweep"; either way the mutation stage already succeeded durably.
 */
public interface ListenerEventSink<R extends ControlledResource> {

  HandoffResult handoff(ListenerEnvelope<R> event);
}
