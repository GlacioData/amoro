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

import java.time.Duration

import scala.collection.JavaConverters._

import org.apache.paimon.CoreOptions
import org.apache.paimon.utils.ParameterUtils

private[paimon] object PaimonSnapshotRetention {
  private val UnlimitedRetention = Duration.ofMillis(Long.MaxValue)

  private[paimon] def resolveConfig(
      requested: PaimonExpireSnapshotsConfig,
      properties: Map[String, String],
      supportsOptions: Boolean = true): PaimonExpireSnapshotsConfig = {
    val procedureOptions = ParameterUtils.parseCommaSeparatedKeyValues(requested.procedureOptions)
      .asScala.toMap
    val tableOptions = new CoreOptions(properties.asJava)
    val retainMin = if (properties.contains(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key()))
      tableOptions.snapshotNumRetainMin()
    else requested.retainMin
    val retainMax = if (properties.contains(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key()))
      tableOptions.snapshotNumRetainMax()
    else requested.retainMax
    val maxDeletes = if (properties.contains(CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key()))
      tableOptions.snapshotExpireLimit().toLong
    else requested.maxDeletes
    val timeOptions = procedureOptions ++ properties
    // Unlike 1.3, the 0.9 procedure has unlimited time retention unless older_than is supplied.
    // https://github.com/apache/paimon/blob/release-0.9.0/paimon-common/src/main/java/org/apache/paimon/options/ExpireConfig.java
    val timeRetained =
      if (!supportsOptions && !timeOptions.contains(CoreOptions.SNAPSHOT_TIME_RETAINED.key()))
        UnlimitedRetention
      else new CoreOptions(timeOptions.asJava).snapshotTimeRetain()
    require(retainMin >= 1, "snapshot 最少保留数量必须大于等于 1")
    require(retainMax >= retainMin, s"snapshot 最大保留数量 $retainMax 不能小于最少保留数量 $retainMin")
    require(
      maxDeletes > 0 && maxDeletes <= Int.MaxValue,
      "snapshot 单轮删除数量必须在 1 到 Integer.MAX_VALUE 之间")
    require(!timeRetained.isNegative, "snapshot 保留时间不能小于 0")

    // Explicit CALL arguments win inside Paimon, so resolve table precedence before building SQL.
    // Also pin the options used by table.copy to the same values as the CALL arguments.
    // https://github.com/apache/paimon/blob/release-1.3.2/paimon-common/src/main/java/org/apache/paimon/utils/ProcedureUtils.java
    val effectiveOptions = procedureOptions ++ Map(
      CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key() -> retainMin.toString,
      CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key() -> retainMax.toString,
      CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key() -> maxDeletes.toString,
      CoreOptions.SNAPSHOT_TIME_RETAINED.key() -> s"${timeRetained.toMillis} ms")
    requested.copy(
      retainMin = retainMin,
      retainMax = retainMax,
      maxDeletes = maxDeletes,
      procedureOptions = effectiveOptions.toSeq.sortBy(_._1).map { case (key, value) =>
        s"$key=$value"
      }.mkString(","),
      timeRetained = Some(timeRetained))
  }

}
