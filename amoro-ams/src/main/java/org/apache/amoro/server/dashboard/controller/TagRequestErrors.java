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

import io.javalin.http.BadRequestResponse;
import io.javalin.http.Context;
import org.apache.amoro.NoSuchTableException;
import org.apache.amoro.server.dashboard.TableTagService;
import org.apache.amoro.server.dashboard.response.ErrorResponse;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.cfg.CoercionAction;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.databind.type.LogicalType;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.tag.TagOperationException;
import org.apache.amoro.table.tag.TagScope;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;

/** Endpoint-local error mapping preserves all other dashboard contracts. */
final class TagRequestErrors {
  private TagRequestErrors() {}

  static void respond(Context context, Supplier<?> request) {
    try {
      context.json(OkResponse.of(request.get()));
    } catch (RuntimeException failure) {
      TagOperationException typed = null;
      Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
      for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
        if (cause instanceof TagOperationException) {
          typed = (TagOperationException) cause;
          break;
        }
      }
      if (typed == null) {
        if (failure instanceof IllegalArgumentException || failure instanceof BadRequestResponse) {
          typed =
              new TagOperationException(
                  400, "INVALID_ARGUMENT", "NOT_APPLIED", "Invalid Tag request");
        } else if (failure instanceof NoSuchTableException) {
          typed =
              new TagOperationException(
                  404, "TABLE_NOT_FOUND", "NOT_APPLIED", "Table no longer exists");
        } else {
          typed =
              new TagOperationException(
                  500, "OUTCOME_UNKNOWN", "UNKNOWN", "Tag request failed; refresh before retrying");
        }
      }
      ErrorResponse error = new ErrorResponse(typed.getStatus(), typed.getMessage(), null);
      error.setReason(typed.getReason());
      error.setOutcome(typed.getOutcome());
      context.attribute(ErrorResponse.HTTP_ERROR_HANDLED, true);
      context.status(typed.getStatus()).json(error);
    }
  }

  static <T> T body(Context context, Class<T> type) {
    try {
      ObjectMapper mapper =
          new ObjectMapper()
              .enable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES)
              .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
              .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS);
      for (CoercionInputShape shape :
          new CoercionInputShape[] {
            CoercionInputShape.Integer, CoercionInputShape.Float, CoercionInputShape.Boolean
          }) {
        mapper.coercionConfigFor(LogicalType.Textual).setCoercion(shape, CoercionAction.Fail);
      }
      return mapper.readValue(context.body(), type);
    } catch (IOException invalid) {
      throw TagOperationException.invalid("Invalid Tag request body");
    }
  }

  static String match(Context context) {
    String header = context.header("If-Match");
    if (header == null || header.isEmpty()) {
      return null;
    }
    if (!header.matches("\"[^\"\\x00-\\x20\\x7f]+\"")) {
      throw TagOperationException.invalid(
          "If-Match must contain one quoted strong observation token");
    }
    return header.substring(1, header.length() - 1);
  }

  static TagScope scope(Context context) {
    TagScope scope = new TagScope();
    if (context.queryParam("scopeKind") != null) {
      scope.setKind(context.queryParam("scopeKind"));
    }
    scope.setBranch(context.queryParam("branch"));
    TableTagService.scope(scope);
    return scope;
  }

  static TableIdentifier table(Context context) {
    return TableIdentifier.of(
        context.pathParam("catalog"), context.pathParam("db"), context.pathParam("table"));
  }
}
