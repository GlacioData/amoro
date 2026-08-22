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
 * A {@link org.apache.amoro.persistence.DurableDeletionHook} failed after the delete was already
 * durable. The DB row is gone — that fact stands — but the in-process cleanup (e.g. the key-only
 * scheduler unschedule) did not run. The name is fenced with a staged copy of the deleted snapshot;
 * same-name creates are rejected until repair retries the hook in the mutation lane and succeeds,
 * after which the fence clears. Callers must not blindly recreate the resource.
 */
public class PostCommitCleanupException extends PersistenceException {

  public PostCommitCleanupException(String domain, String name, Throwable cause) {
    super(
        "post-commit cleanup failed for delete of "
            + domain
            + "/"
            + name
            + "; name fenced until repair",
        cause);
  }
}
