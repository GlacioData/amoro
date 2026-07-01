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

import org.apache.spark.sql.{Row, SparkSession}

import org.apache.amoro.producer.core.{CommonProducerConfig, TargetTable}
import org.apache.amoro.producer.paimon.PaimonSqlUtils.{fieldOpt, quoteIdent, valueAt}
import org.apache.amoro.producer.spi.{LakeFormatAdapter, ProducerAction}

class PaimonFormatAdapter(runner: PaimonSqlRunner = SparkPaimonSqlRunner)
  extends LakeFormatAdapter {

  override def format: String = "paimon"

  override def actions: Seq[ProducerAction] = {
    Seq(
      new PaimonCompactAction(runner),
      new PaimonCompactManifestAction(runner),
      new PaimonExpireSnapshotsAction(runner),
      new PaimonRemoveOrphanFilesAction(runner))
  }

  override def listTables(
      spark: SparkSession,
      config: CommonProducerConfig,
      database: String): Seq[TargetTable] = {
    val showTablesSql =
      s"SHOW TABLES IN ${quoteIdent(config.catalogName)}.${quoteIdent(database)}"

    runner.run(spark, showTablesSql).flatMap { row =>
      tableName(row).filter(_.nonEmpty).filter(_ => !isTemporary(row)).map { table =>
        TargetTable(s"$database.$table", database, table)
      }
    }
  }

  private def tableName(row: Row): Option[String] = {
    fieldOpt(row, Set("tablename", "table"))
      .orElse(valueAt(row, 1))
      .filter(_ != null)
      .map(_.toString.trim)
  }

  private def isTemporary(row: Row): Boolean = {
    fieldOpt(row, Set("istemporary", "temporary"))
      .orElse(valueAt(row, 2))
      .exists(toBoolean)
  }

  private def toBoolean(value: Any): Boolean = {
    value match {
      case null => false
      case b: java.lang.Boolean => b.booleanValue()
      case s: String => s.trim.equalsIgnoreCase("true") || s.trim == "1"
      case i: java.lang.Integer => i == 1
      case l: java.lang.Long => l == 1L
      case other => other.toString.trim.equalsIgnoreCase("true")
    }
  }
}

object PaimonFormatAdapter extends PaimonFormatAdapter(SparkPaimonSqlRunner)
