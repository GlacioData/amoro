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

import scala.util.control.NonFatal

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.Table
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.amoro.producer.spi.ProducerRuntimeValidationException

private[paimon] object PaimonOrphanFilesRuntime {
  val StartupOverrides: Map[String, String] = Map(
    "spark.scheduler.mode" -> "FAIR",
    "spark.speculation" -> "false",
    "spark.task.maxFailures" -> "1")

  def supported: Boolean = {
    try org.apache.spark.SPARK_VERSION == "3.5.4" &&
        PaimonSnapshotRuntime.versionOf(classOf[Table], "paimon-core") == "1.3.2" &&
        PaimonSnapshotRuntime.versionOf(
          classOf[org.apache.paimon.options.ExpireConfig],
          "paimon-api") == "1.3.2" &&
        PaimonSnapshotRuntime.versionOf(classOf[SparkTable], "paimon-spark-common_2.12") == "1.3.2"
    catch {
      case NonFatal(_) => false
      case _: LinkageError => false
    }
  }

  def validate(spark: SparkSession, concurrency: Int): Unit = {
    check(
      supported && spark.version == "3.5.4",
      "Concurrent orphan cleaning requires Spark 3.5.4 / Paimon 1.3.2; legacy execution is unchanged when --tableConcurrency is omitted")
    val conf = spark.sparkContext.getConf
    check(
      !conf.getBoolean("spark.speculation", false),
      "Orphan cleaning requires spark.speculation=false; restart the application")
    check(
      conf.getInt("spark.task.maxFailures", 4) == 1,
      "Orphan cleaning requires spark.task.maxFailures=1; restart the application")
    validateSubmissionConf(conf)
    check(
      concurrency == 1 || spark.sparkContext.getSchedulingMode.toString == "FAIR",
      "Concurrent orphan cleaning requires spark.scheduler.mode=FAIR; restart the application")
  }

  private[paimon] def validateSubmissionConf(conf: SparkConf): Unit = {
    // YARN fixes the attempt limit before the Driver starts. Never overwrite it in the builder:
    // doing so would only hide an unsafe submission configuration from this check.
    check(
      conf.get("spark.master", "") != "yarn" ||
        conf.getOption("spark.yarn.maxAppAttempts").contains("1"),
      "Orphan cleaning on YARN requires --conf spark.yarn.maxAppAttempts=1 in spark-submit before the JAR path; resubmit the application")
  }

  private def check(condition: Boolean, message: String): Unit =
    if (!condition) throw new ProducerRuntimeValidationException(message)
}
