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

package org.apache.amoro.server.dashboard.controller;

import io.javalin.http.Context;
import org.apache.amoro.server.dashboard.response.ErrorResponse;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.table.descriptor.SnapshotReadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;

/** Endpoint-local mapping; do not change unrelated dashboard error contracts. */
final class SnapshotRequestErrors {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRequestErrors.class);

  private SnapshotRequestErrors() {}

  static void respond(Context ctx, Supplier<?> request) {
    try {
      ctx.json(OkResponse.of(request.get()));
    } catch (RuntimeException failure) {
      SnapshotReadException typed = find(failure);
      int status = 500;
      String message = "Failed to read snapshot metadata";
      if (typed != null) {
        message = typed.getMessage();
        switch (typed.getReason()) {
          case INVALID_REQUEST:
            status = 400;
            break;
          case NOT_FOUND:
            status = 404;
            break;
          case BUSY:
            status = 429;
            break;
          case UNAVAILABLE:
            status = 503;
            break;
          case TIMEOUT:
            status = 504;
            break;
          default:
            status = 500;
        }
      } else if (failure instanceof org.apache.amoro.table.tag.TagOperationException) {
        org.apache.amoro.table.tag.TagOperationException tag =
            (org.apache.amoro.table.tag.TagOperationException) failure;
        status = tag.getStatus();
        message = tag.getMessage();
      } else if (failure instanceof IllegalArgumentException) {
        status = 400;
        message = "Invalid snapshot request parameters";
      }
      if (status == 500) {
        LOG.warn("Snapshot metadata request failed", failure);
      }
      if ("TAG".equals(ctx.queryParam("refType"))) {
        ctx.attribute(ErrorResponse.HTTP_ERROR_HANDLED, true);
      }
      ctx.status(status).json(new ErrorResponse(status, message, null));
    }
  }

  static SnapshotReadException find(Throwable failure) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    while (failure != null && seen.add(failure)) {
      if (failure instanceof SnapshotReadException) {
        return (SnapshotReadException) failure;
      }
      failure = failure.getCause();
    }
    return null;
  }

  static Integer integer(Context ctx, String key) {
    String value = ctx.queryParam(key);
    return value == null ? null : Integer.valueOf(value);
  }
}
