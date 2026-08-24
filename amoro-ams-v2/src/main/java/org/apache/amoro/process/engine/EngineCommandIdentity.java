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

package org.apache.amoro.process.engine;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/** Strongly typed single-flight identities shared by related engine operations. */
public interface EngineCommandIdentity {

  /** submit and resolveSubmission share this exact identity. */
  @RequiredArgsConstructor
  @EqualsAndHashCode
  final class SubmissionIdentity implements EngineCommandIdentity {
    @NonNull private final String processName;
    @NonNull private final String submissionKey;
    @NonNull private final String requestHash;

    @Override
    public String toString() {
      return "submission(" + processName + "," + submissionKey + ")";
    }
  }

  /** observe and cancel share this exact identity. */
  @RequiredArgsConstructor
  @EqualsAndHashCode
  final class ExecutionIdentity implements EngineCommandIdentity {
    @NonNull private final String processName;
    @NonNull private final String externalId;

    @Override
    public String toString() {
      return "execution(" + processName + "," + externalId + ")";
    }
  }

  /** Cleanup identity is engine-scoped because external ids need not be globally unique. */
  @RequiredArgsConstructor
  @EqualsAndHashCode
  final class ReleaseIdentity implements EngineCommandIdentity {
    @NonNull private final String executionEngine;
    @NonNull private final String externalId;

    @Override
    public String toString() {
      return "release(" + executionEngine + "," + externalId + ")";
    }
  }
}
