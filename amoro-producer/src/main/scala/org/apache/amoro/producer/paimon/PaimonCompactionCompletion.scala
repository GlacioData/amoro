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

import java.time.Instant

import scala.collection.JavaConverters._

import org.apache.paimon.utils.JsonSerdeUtil
import org.slf4j.LoggerFactory

/** Final outcome of actual compact attempts. SQL and errors are redacted for observation. */
final case class PaimonCompactionCompletion(
    catalog: String,
    database: String,
    table: String,
    sql: String,
    success: Boolean,
    startTime: Instant,
    endTime: Instant,
    durationMs: Double,
    attempts: Int,
    effectiveVersion: String,
    error: Option[String],
    scope: PaimonCompactionScope)

object PaimonCompactionCompletion {
  private val logger = LoggerFactory.getLogger(classOf[PaimonCompactionCompletion])
  private val sensitiveKey =
    "(?i)secret|password|passwd|token|credential|access[._-]?key|private[._-]?key|account[._-]?key".r
  private val redacted = "[REDACTED]"

  def log(completion: PaimonCompactionCompletion): Unit = {
    val fields = Map[String, AnyRef](
      "catalog" -> completion.catalog,
      "database" -> completion.database,
      "table" -> completion.table,
      "sql" -> completion.sql,
      "success" -> Boolean.box(completion.success),
      "startTime" -> completion.startTime.toString,
      "endTime" -> completion.endTime.toString,
      "durationMs" -> Double.box(completion.durationMs),
      "attempts" -> Int.box(completion.attempts),
      "effectiveVersion" -> completion.effectiveVersion,
      "error" -> completion.error.orNull) ++ completion.scope.fields
    logger.info("Compaction completed: {}", JsonSerdeUtil.toFlatJson(fields.asJava))
  }

  private[paimon] def redactOptions(
      config: PaimonCompactConfig): (PaimonCompactConfig, Seq[String]) = {
    val entries = config.procedureOptions.split(",", -1).toSeq
    val sensitive = entries.flatMap { entry =>
      val parts = entry.split("=", 2)
      if (parts.length == 2 && sensitiveKey.findFirstIn(parts(0)).isDefined) {
        Some(entry -> parts(1).trim)
      } else None
    }.toMap
    val displayOptions = entries.map { entry =>
      if (sensitive.contains(entry)) entry.take(entry.indexOf('=') + 1) + redacted else entry
    }.mkString(",")
    (config.copy(procedureOptions = displayOptions), sensitive.values.filter(_.nonEmpty).toSeq)
  }

  private[paimon] def redactError(message: String, secrets: Seq[String]): String = {
    secrets.flatMap { secret =>
      Seq(secret, PaimonCompactAction.procedureString(secret).drop(1).dropRight(1))
    }.distinct.sortBy(value => -value.length)
      .foldLeft(message)((text, secret) => text.replace(secret, redacted))
  }
}
