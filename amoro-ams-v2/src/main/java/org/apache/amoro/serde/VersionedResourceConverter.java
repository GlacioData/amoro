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

package org.apache.amoro.serde;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * One adjacent upgrade link in a resource's version chain (framework spec §5.4): a converter takes
 * the document tree at {@link #fromVersion()} and returns the equivalent tree at {@link
 * #toVersion()} — which must be the next version towards the latest. Converters operate on
 * format-neutral trees, so the same chain serves the JSON and YAML domain formats.
 *
 * <p>Evolution discipline (strict tier, interview-confirmed): converters may only add fields with
 * defaults, move/rename fields, or add enum values. Field semantics must never change, retired
 * names must never be reused, and fields are deprecated, never physically removed from the stored
 * history. Converters and per-version golden fixtures are kept forever — live rows may span
 * versions across a rolling upgrade, and dropping a link breaks startup loading.
 */
public interface VersionedResourceConverter {

  /** Version of the input document, e.g. {@code "v1"}. Must differ from {@link #toVersion()}. */
  String fromVersion();

  /** Version of the produced document; the link's successor towards the latest version. */
  String toVersion();

  /**
   * @param thisVersion the input tree at {@link #fromVersion()}; must not be mutated in place
   * @return the equivalent tree at {@link #toVersion()}, with the {@code apiVersion} field set
   */
  JsonNode upgrade(JsonNode thisVersion);
}
