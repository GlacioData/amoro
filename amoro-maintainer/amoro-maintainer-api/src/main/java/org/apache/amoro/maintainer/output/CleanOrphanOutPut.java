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

package org.apache.amoro.maintainer.output;

import org.apache.amoro.maintainer.api.BaseMaintainerOutput;
import org.apache.amoro.maintainer.api.MaintainerType;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;

public class CleanOrphanOutPut extends BaseMaintainerOutput {

  private Integer expectedFileCount = 0;
  private Integer DeletedFileCount = 0;

  public CleanOrphanOutPut(
      String catalog,
      String database,
      String table,
      String type,
      Long lastTime,
      Integer expectedFileCount,
      Integer deletedFileCount) {
    super(catalog, database, table, type, lastTime);
    this.expectedFileCount = expectedFileCount;
    this.DeletedFileCount = deletedFileCount;
  }

  public Integer getExpectedFileCount() {
    return expectedFileCount;
  }

  public Integer getDeletedFileCount() {
    return DeletedFileCount;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("expectedFileCount", expectedFileCount)
        .append("DeletedFileCount", DeletedFileCount)
        .toString();
  }

  @Override
  public Map<String, String> summary() {
    Map<String, String> summary = super.summary();
    summary.put("type", MaintainerType.CLEAN_ORPHAN_FILES.name());
    summary.put("expectedFileCount", expectedFileCount.toString());
    summary.put("DeletedFileCount", expectedFileCount.toString());
    return summary;
  }
}
