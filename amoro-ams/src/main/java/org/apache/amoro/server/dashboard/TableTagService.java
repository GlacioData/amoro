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

package org.apache.amoro.server.dashboard;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.table.TableIdentifier;
import org.apache.amoro.table.tag.CopyTagRequest;
import org.apache.amoro.table.tag.CreateTagRequest;
import org.apache.amoro.table.tag.DeleteTagRequest;
import org.apache.amoro.table.tag.TableTagInfo;
import org.apache.amoro.table.tag.TableTagOperations;
import org.apache.amoro.table.tag.TagDeleteResult;
import org.apache.amoro.table.tag.TagOperationException;
import org.apache.amoro.table.tag.TagRetentionRequest;
import org.apache.amoro.table.tag.TagScope;
import org.apache.amoro.table.tag.UpdateTagRetentionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.function.Function;

/**
 * Format-neutral routing and admission. Authentication/authorization remains in DashboardServer.
 */
public final class TableTagService {
  private static final Logger LOG = LoggerFactory.getLogger(TableTagService.class);
  private final CatalogManager catalogs;
  private final Semaphore admission = new Semaphore(4);

  public TableTagService(CatalogManager catalogs) {
    this.catalogs = catalogs;
  }

  public TableTagInfo create(TableIdentifier table, CreateTagRequest request) {
    if (request == null) {
      throw TagOperationException.invalid("Request is required");
    }
    text(request.getName(), 255);
    scope(request.getScope());
    if (request.getTarget() == null) {
      throw TagOperationException.invalid("Target version is required");
    }
    text(request.getTarget().getId(), 512);
    retention(request.getRetention());
    return execute(table, "CREATE", ops -> ops.createTag(request));
  }

  public TableTagInfo copy(TableIdentifier table, CopyTagRequest request) {
    if (request == null) {
      throw TagOperationException.invalid("Request is required");
    }
    text(request.getSourceName(), 255);
    text(request.getNewName(), 255);
    scope(request.getScope());
    retention(request.getRetention());
    return execute(table, "COPY", ops -> ops.copyTag(request));
  }

  public TagDeleteResult delete(TableIdentifier table, DeleteTagRequest request) {
    if (request == null) {
      throw TagOperationException.invalid("Request is required");
    }
    text(request.getName(), 255);
    scope(request.getScope());
    return execute(table, "DELETE", ops -> ops.deleteTag(request));
  }

  public TableTagInfo updateRetention(TableIdentifier table, UpdateTagRetentionRequest request) {
    if (request == null) {
      throw TagOperationException.invalid("Request is required");
    }
    text(request.getName(), 255);
    scope(request.getScope());
    retention(request.getRetention());
    text(request.getExpectedRetentionModel(), 128);
    text(request.getExpectedUpdateClock(), 128);
    return execute(table, "UPDATE_RETENTION", ops -> ops.updateTagRetention(request));
  }

  public static void text(String value, int maximum) {
    if (value == null
        || value.trim().isEmpty()
        || value.length() > maximum
        || value.chars().anyMatch(Character::isISOControl)) {
      throw TagOperationException.invalid("Invalid Tag request string");
    }
  }

  public static void scope(TagScope scope) {
    if (scope == null || scope.getKind() == null) {
      throw TagOperationException.invalid("Scope is required");
    }
    switch (scope.getKind()) {
      case "DEFAULT":
      case "TABLE":
        if (scope.getBranch() != null) {
          throw TagOperationException.invalid("This scope does not accept a branch");
        }
        break;
      case "BRANCH":
        text(scope.getBranch(), 255);
        break;
      default:
        throw TagOperationException.invalid("Invalid Tag scope kind");
    }
  }

  private static void retention(TagRetentionRequest request) {
    if (request == null || request.getMode() == null) {
      throw TagOperationException.invalid("Retention is required");
    }
    if ("AGE_LIMIT".equals(request.getMode())) {
      if (request.getMaxAgeMillis() == null
          || request.getMaxAgeMillis() < 1
          || request.getMaxAgeMillis() > 9007199254740991L) {
        throw TagOperationException.invalid("Invalid retention age");
      }
    } else if (!("NATIVE_DEFAULT".equals(request.getMode())
            || "NO_AGE_LIMIT".equals(request.getMode()))
        || request.getMaxAgeMillis() != null) {
      throw TagOperationException.invalid("Invalid retention mode or fields");
    }
  }

  private <T> T execute(
      TableIdentifier table, String action, Function<TableTagOperations, T> operation) {
    if (!admission.tryAcquire()) {
      throw new TagOperationException(429, "BUSY", "NOT_APPLIED", "Tag operation capacity is busy");
    }
    try {
      AmoroTable<?> loaded =
          catalogs
              .getServerCatalog(table.getCatalog())
              .loadTable(table.getDatabase(), table.getTableName());
      TableTagOperations ops =
          loaded.tagOperations().orElseThrow(() -> TagOperationException.unsupported(action));
      if (!ops.capabilities().getActions().contains(action)) {
        throw TagOperationException.unsupported(action);
      }
      T result = operation.apply(ops);
      LOG.info("Tag {} completed for {}", action, table);
      return result;
    } catch (TagOperationException failure) {
      LOG.warn(
          "Tag {} failed for {}: reason={}, outcome={}",
          action,
          table,
          failure.getReason(),
          failure.getOutcome());
      throw failure;
    } finally {
      admission.release();
    }
  }
}
