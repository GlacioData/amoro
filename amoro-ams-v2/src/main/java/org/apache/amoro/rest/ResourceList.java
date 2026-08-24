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

package org.apache.amoro.rest;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * K8s-style list envelope for REST collection endpoints: {@code apiVersion/kind/metadata/items}.
 * Unlike the appmanager reference (whose metadata block is empty), pagination rides inside {@link
 * ResourceListMetadata}: one index snapshot serves both the page and the total.
 */
@Getter
@Builder
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "items"})
public final class ResourceList<T> {

  private final String apiVersion;
  private final String kind;
  private final ResourceListMetadata metadata;
  private final List<T> items;

  /** Pagination metadata: total comes from the same snapshot that produced the items. */
  @Getter
  @Builder
  @JsonPropertyOrder({"total", "page", "pageSize"})
  public static final class ResourceListMetadata {
    private final int total;
    private final int page;
    private final int pageSize;
  }
}
