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

package org.apache.amoro.table;

import org.apache.amoro.TableRuntime;
import org.apache.amoro.table.descriptor.ServerTableMeta;

import java.util.Collections;
import java.util.List;

/**
 * Format-owned persisted runtime state and its read-only dashboard projection.
 *
 * <p>Registered by FormatCatalogFactory, independently of enabled process/optimizer plugins.
 * Implementations must be thread-safe and must not load tables, read files or query persistence
 * when declaring keys or projecting details. State names and JSON shapes are durable contracts.
 */
public interface TableRuntimeSupport {
  /**
   * The concrete pending-input codec owned by this format, never inferred by the common runtime.
   */
  StateKey<? extends FormatPendingInput> pendingInputKey();

  /**
   * Additional state required even when the format's optimizer/maintenance actions are disabled.
   */
  default List<StateKey<?>> requiredStates() {
    return Collections.emptyList();
  }

  /**
   * Project already-restored in-memory state only; null runtime must leave the detail unchanged.
   */
  default void projectDetail(TableRuntime runtime, ServerTableMeta target) {}
}
