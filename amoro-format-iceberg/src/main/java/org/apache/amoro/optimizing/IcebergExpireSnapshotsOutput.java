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

package org.apache.amoro.optimizing;

import org.apache.amoro.maintainer.output.ExpireSnapshotsOutput;
import org.apache.iceberg.actions.ExpireSnapshots;

import java.util.Map;

public class IcebergExpireSnapshotsOutput extends ExpireSnapshotsOutput {

  private final ExpireSnapshots.Result result;

  public IcebergExpireSnapshotsOutput(
      String catalog,
      String database,
      String table,
      String type,
      Long lastTime,
      Long deleteFilesTotal,
      ExpireSnapshots.Result result) {
    super(catalog, database, table, type, lastTime, deleteFilesTotal);
    this.result = result;
  }

  @Override
  public Map<String, String> summary() {
    Map<String, String> summary = super.summary();
    summary.put("deletedDataFilesCount", String.valueOf(result.deletedDataFilesCount()));
    summary.put("deletedManifestsCount", String.valueOf(result.deletedManifestsCount()));
    summary.put("deletedManifestListsCount", String.valueOf(result.deletedManifestListsCount()));
    summary.put(
        "deletedEqualityDeleteFilesCount",
        String.valueOf(result.deletedEqualityDeleteFilesCount()));
    summary.put(
        "deletedPositionDeleteFilesCount",
        String.valueOf(result.deletedPositionDeleteFilesCount()));
    summary.put(
        "deletedStatisticsFilesCount", String.valueOf(result.deletedStatisticsFilesCount()));
    return summary;
  }
}
