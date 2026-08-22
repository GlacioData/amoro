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
 * Optimistic-concurrency failure: the current resourceVersion differs from the expected version of
 * a modify/delete command. The command had no effect and the framework never auto-retries —
 * level-triggered callers re-read the latest state and converge on their next round.
 */
public class PreconditionFailedException extends PersistenceException {

  public PreconditionFailedException(
      String domain, String name, long expectedVersion, long actualVersion) {
    super(
        "precondition failed for "
            + domain
            + "/"
            + name
            + ": expected resourceVersion "
            + expectedVersion
            + " but current is "
            + actualVersion);
  }
}
