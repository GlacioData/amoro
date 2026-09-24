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

package org.apache.amoro.table.descriptor;

import java.util.Objects;

/** Format-owned model identity; codes remain open to future format versions and native models. */
public final class TableModel {
  public static final String FORMAT_VERSION = "FORMAT_VERSION";
  public static final String BUCKET_MODE = "BUCKET_MODE";

  private final String kind;
  private final String code;

  public TableModel(String kind, String code) {
    this.kind = Objects.requireNonNull(kind, "kind");
    this.code = Objects.requireNonNull(code, "code");
    if (kind.trim().isEmpty() || code.trim().isEmpty()) {
      throw new IllegalArgumentException("Table model kind and code must not be empty");
    }
  }

  public String getKind() {
    return kind;
  }

  public String getCode() {
    return code;
  }
}
