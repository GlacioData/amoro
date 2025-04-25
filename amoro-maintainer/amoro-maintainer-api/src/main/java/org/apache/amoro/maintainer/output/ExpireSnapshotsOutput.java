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

public class ExpireSnapshotsOutput extends BaseMaintainerOutput {

  private final Long deleteFilesTotal;

  public ExpireSnapshotsOutput(
      String catalog,
      String database,
      String table,
      String type,
      Long lastTime,
      Long deleteFilesTotal) {
    super(catalog, database, table, type, lastTime);
    this.deleteFilesTotal = deleteFilesTotal;
  }

  public Long getDeleteFilesTotal() {
    return deleteFilesTotal;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("deleteFilesTotal", deleteFilesTotal).toString();
  }

  @Override
  public Map<String, String> summary() {
    Map<String, String> summary = super.summary();
    summary.put("type", MaintainerType.EXPIRE_SNAPSHOTS.name());
    summary.put("deleteFilesTotal", deleteFilesTotal.toString());
    return summary;
  }
}
