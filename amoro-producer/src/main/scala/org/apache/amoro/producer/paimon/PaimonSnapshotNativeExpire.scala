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

package org.apache.amoro.producer.paimon

import org.apache.paimon.options.ExpireConfig
import org.apache.paimon.table.{ExpireSnapshots, FileStoreTable}
import org.apache.paimon.utils.ParameterUtils

private[paimon] object PaimonSnapshotNativeExpire {

  def create(table: FileStoreTable, policy: PaimonExpireSnapshotsConfig): ExpireSnapshots = {
    // Match the procedure's table.copy and four snapshot overrides, including changelog defaults.
    // https://github.com/apache/paimon/blob/release-1.3.2/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExpireSnapshotsProcedure.java
    // https://github.com/apache/paimon/blob/release-1.3.2/paimon-common/src/main/java/org/apache/paimon/utils/ProcedureUtils.java
    val options = ParameterUtils.parseCommaSeparatedKeyValues(policy.procedureOptions)
    val configured = table.copy(options)
    configured.newExpireSnapshots().config(
      ExpireConfig.builder()
        .snapshotRetainMin(policy.retainMin)
        .snapshotRetainMax(policy.retainMax)
        .snapshotMaxDeletes(policy.maxDeletes.toInt)
        .snapshotTimeRetain(policy.timeRetained.get)
        .build())
  }
}
