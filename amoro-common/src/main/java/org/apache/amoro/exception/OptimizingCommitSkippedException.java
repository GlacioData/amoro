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

package org.apache.amoro.exception;

/** A recognized commit skip which must not advance successful optimization watermarks. */
public class OptimizingCommitSkippedException extends OptimizingCommitException {
  private static final long serialVersionUID = 1L;

  public enum Reason {
    STALE_FULL
  }

  private final Reason reason;
  private final boolean publicationUncertain;

  public OptimizingCommitSkippedException(Reason reason, String message) {
    this(reason, message, false);
  }

  public OptimizingCommitSkippedException(
      Reason reason, String message, boolean publicationUncertain) {
    super(message, false);
    this.publicationUncertain = publicationUncertain;
    this.reason = java.util.Objects.requireNonNull(reason, "skip reason");
  }

  /**
   * If publication was attempted but remains uncertain, callers must not release the owner as an
   * ordinary skip.
   */
  public boolean isPublicationUncertain() {
    return publicationUncertain;
  }

  public Reason getReason() {
    return reason;
  }
}
