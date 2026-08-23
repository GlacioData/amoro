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

/** A durable Process row would violate one of the aggregate admission index invariants. */
public final class ProcessIndexConflictException extends IllegalStateException {

  private final String conflictType;
  private final String scope;
  private final String incumbentName;
  private final String contenderName;

  ProcessIndexConflictException(
      String conflictType, String scope, String incumbentName, String contenderName) {
    super(
        "process index conflict "
            + conflictType
            + " at "
            + scope
            + ": incumbent="
            + incumbentName
            + ", contender="
            + contenderName);
    this.conflictType = conflictType;
    this.scope = scope;
    this.incumbentName = incumbentName;
    this.contenderName = contenderName;
  }

  public String conflictType() {
    return conflictType;
  }

  public String scope() {
    return scope;
  }

  public String incumbentName() {
    return incumbentName;
  }

  public String contenderName() {
    return contenderName;
  }
}
