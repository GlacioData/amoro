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

package org.apache.amoro.persistence.exception;

/**
 * The DB commit outcome could not be determined (framework spec §5.1): a connection failure may
 * have happened after the commit, and a fresh point read returned neither the previous state, nor
 * the candidate state, nor a readable answer. The framework fences {@code (domain, name)} — all
 * later mutations of that key fail fast until repair reloads the durable state and lifts the fence.
 * Fenced-key counts must surface in health metrics and alerts.
 *
 * <p>Do not treat this as an ordinary failure and retry blindly: writing on a stale in-memory
 * snapshot after an unknown commit is exactly what the fence prevents.
 */
public class PersistenceOutcomeUnknownException extends PersistenceException {

  public PersistenceOutcomeUnknownException(String domain, String name, Throwable cause) {
    super(
        "durable commit outcome unknown for " + domain + "/" + name + "; key fenced until repair",
        cause);
  }

  public PersistenceOutcomeUnknownException(String domain, String name) {
    this(domain, name, null);
  }
}
