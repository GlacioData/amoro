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

package org.apache.amoro.maintainer.input;

import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.maintainer.api.BaseMaintainerInput;
import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;

public class DanglingDeleteFilesInput extends BaseMaintainerInput {

  private final CatalogMeta catalogMeta;
  private final boolean danglingFileCleanEnabled;
  private final long lastDanglingFileCleanTime;

  public DanglingDeleteFilesInput(
      String database,
      CatalogMeta catalogMeta,
      boolean danglingFileCleanEnabled,
      long lastDanglingFileCleanTime) {
    super(database);
    this.catalogMeta = catalogMeta;
    this.danglingFileCleanEnabled = danglingFileCleanEnabled;
    this.lastDanglingFileCleanTime = lastDanglingFileCleanTime;
  }

  public boolean isDanglingFileCleanEnabled() {
    return danglingFileCleanEnabled;
  }

  public long getLastDanglingFileCleanTime() {
    return lastDanglingFileCleanTime;
  }

  public CatalogMeta getCatalogMeta() {
    return catalogMeta;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("danglingFileCleanEnabled", danglingFileCleanEnabled)
        .add("lastDanglingFileCleanTime", lastDanglingFileCleanTime)
        .addValue(super.toString())
        .toString();
  }
}
