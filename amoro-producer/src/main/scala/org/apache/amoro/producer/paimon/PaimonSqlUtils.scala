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

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.Row

private[paimon] object PaimonSqlUtils {

  def quoteIdent(name: String): String = {
    "`" + name.replace("`", "``") + "`"
  }

  def sqlString(value: String): String = {
    "'" + value.replace("'", "''") + "'"
  }

  def fieldOpt(row: Row, normalizedNames: Set[String]): Option[Any] = {
    val schemaOpt =
      try {
        Option(row.schema)
      } catch {
        case NonFatal(_) => None
      }

    schemaOpt.flatMap { schema =>
      schema.fields.zipWithIndex
        .find { case (field, _) => normalizedNames.contains(normalizeFieldName(field.name)) }
        .flatMap { case (_, index) => valueAt(row, index) }
    }
  }

  def valueAt(row: Row, index: Int): Option[Any] = {
    if (index >= 0 && index < row.size && !row.isNullAt(index)) {
      Some(row.get(index))
    } else {
      None
    }
  }

  def normalizeFieldName(name: String): String = {
    name.toLowerCase(Locale.ROOT).replace("_", "")
  }
}
