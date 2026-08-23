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

import java.util.Objects;

/** Strongly typed single-flight identities shared by related engine operations. */
public interface EngineCommandIdentity {

  /** submit and resolveSubmission share this exact identity. */
  final class SubmissionIdentity implements EngineCommandIdentity {
    private final String processName;
    private final String submissionKey;
    private final String requestHash;

    public SubmissionIdentity(String processName, String submissionKey, String requestHash) {
      this.processName = Objects.requireNonNull(processName, "processName");
      this.submissionKey = Objects.requireNonNull(submissionKey, "submissionKey");
      this.requestHash = Objects.requireNonNull(requestHash, "requestHash");
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof SubmissionIdentity)) {
        return false;
      }
      SubmissionIdentity that = (SubmissionIdentity) other;
      return processName.equals(that.processName)
          && submissionKey.equals(that.submissionKey)
          && requestHash.equals(that.requestHash);
    }

    @Override
    public int hashCode() {
      return Objects.hash(processName, submissionKey, requestHash);
    }

    @Override
    public String toString() {
      return "submission(" + processName + "," + submissionKey + ")";
    }
  }

  /** observe and cancel share this exact identity. */
  final class ExecutionIdentity implements EngineCommandIdentity {
    private final String processName;
    private final String externalId;

    public ExecutionIdentity(String processName, String externalId) {
      this.processName = Objects.requireNonNull(processName, "processName");
      this.externalId = Objects.requireNonNull(externalId, "externalId");
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ExecutionIdentity)) {
        return false;
      }
      ExecutionIdentity that = (ExecutionIdentity) other;
      return processName.equals(that.processName) && externalId.equals(that.externalId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(processName, externalId);
    }

    @Override
    public String toString() {
      return "execution(" + processName + "," + externalId + ")";
    }
  }

  /** Cleanup identity is engine-scoped because external ids need not be globally unique. */
  final class ReleaseIdentity implements EngineCommandIdentity {
    private final String executionEngine;
    private final String externalId;

    public ReleaseIdentity(String executionEngine, String externalId) {
      this.executionEngine = Objects.requireNonNull(executionEngine, "executionEngine");
      this.externalId = Objects.requireNonNull(externalId, "externalId");
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ReleaseIdentity)) {
        return false;
      }
      ReleaseIdentity that = (ReleaseIdentity) other;
      return executionEngine.equals(that.executionEngine) && externalId.equals(that.externalId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(executionEngine, externalId);
    }

    @Override
    public String toString() {
      return "release(" + executionEngine + "," + externalId + ")";
    }
  }
}
