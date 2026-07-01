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

package org.apache.amoro.producer.core

import java.util.Locale

private[producer] object TableActionJsonLog {

  private val EventName = "lakehouse_producer_table_action"
  private val DeletedSnapshotsTotalKey = "deletedSnapshotsTotal"

  def render(
      common: CommonProducerConfig,
      table: TargetTable,
      result: ActionResult,
      measuredCostMs: Double): String = {
    val fields =
      Seq(
        jsonField("event", jsonString(EventName)),
        jsonField("format", jsonString(common.format)),
        jsonField("catalog", jsonString(common.catalogName)),
        jsonField("database", jsonString(table.database)),
        jsonField("table", jsonString(table.table)),
        jsonField("target", jsonString(table.raw)),
        jsonField("action", jsonString(common.action)),
        jsonField("status", jsonString(statusName(result.status))),
        jsonField("costMs", jsonNumber(measuredCostMs)),
        jsonField("deletedSnapshotsTotal", deletedSnapshotsTotal(result.metrics)),
        jsonField("metrics", jsonObject(result.metrics)),
        jsonField("message", result.message.map(jsonStringOrNull).getOrElse("null")))
    fields.mkString("{", ",", "}")
  }

  private def statusName(status: ActionStatus): String = {
    status match {
      case ActionStatus.Success => "success"
      case ActionStatus.Failed => "failed"
      case ActionStatus.Skipped => "skipped"
    }
  }

  private def deletedSnapshotsTotal(metrics: Map[String, String]): String = {
    Option(metrics)
      .flatMap(metricsMap => metricsMap.get(DeletedSnapshotsTotalKey))
      .flatMap(toLong)
      .map(_.toString)
      .getOrElse("null")
  }

  private def toLong(value: String): Option[Long] = {
    Option(value).flatMap { nonNullValue =>
      try {
        Some(nonNullValue.trim.toLong)
      } catch {
        case _: NumberFormatException => None
      }
    }
  }

  private def jsonObject(values: Map[String, String]): String = {
    Option(values).getOrElse(Map.empty).toSeq
      .filter(_._1 != null)
      .sortBy(_._1)
      .map {
        case (key, value) => s"${jsonString(key)}:${jsonStringOrNull(value)}"
      }
      .mkString("{", ",", "}")
  }

  private def jsonField(name: String, value: String): String = {
    s"${jsonString(name)}:$value"
  }

  private def jsonString(value: String): String = {
    val builder = new StringBuilder
    builder.append('"')
    value.foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
      case '\b' => builder.append("\\b")
      case '\f' => builder.append("\\f")
      case '\n' => builder.append("\\n")
      case '\r' => builder.append("\\r")
      case '\t' => builder.append("\\t")
      case char if char < ' ' =>
        builder.append("\\u")
        builder.append(f"${char.toInt}%04x")
      case char => builder.append(char)
    }
    builder.append('"')
    builder.toString()
  }

  private def jsonStringOrNull(value: String): String = {
    if (value == null) {
      "null"
    } else {
      jsonString(value)
    }
  }

  private def jsonNumber(value: Double): String = {
    if (value.isNaN || value.isInfinity) {
      "null"
    } else {
      java.lang.Double.toString(value).toLowerCase(Locale.ROOT)
    }
  }
}
