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

package org.apache.amoro.process.engine.local;

import java.util.Arrays;
import java.util.Objects;

/** Format-neutral frozen command passed to a future native Local Action implementation. */
public final class LocalActionCommand {
  private final String action;
  private final String submissionKey;
  private final String requestHash;
  private final byte[] payload;

  public LocalActionCommand(String submissionKey, String requestHash, byte[] payload) {
    this("legacy-action", submissionKey, requestHash, payload);
  }

  public LocalActionCommand(
      String action, String submissionKey, String requestHash, byte[] payload) {
    this.action = Objects.requireNonNull(action, "action");
    this.submissionKey = Objects.requireNonNull(submissionKey, "submissionKey");
    this.requestHash = Objects.requireNonNull(requestHash, "requestHash");
    this.payload = Arrays.copyOf(Objects.requireNonNull(payload, "payload"), payload.length);
  }

  public String action() {
    return action;
  }

  public String submissionKey() {
    return submissionKey;
  }

  public String requestHash() {
    return requestHash;
  }

  public byte[] payload() {
    return Arrays.copyOf(payload, payload.length);
  }
}
