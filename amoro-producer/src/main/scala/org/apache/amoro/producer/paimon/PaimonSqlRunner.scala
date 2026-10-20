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

import scala.collection.JavaConverters._

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalog.ProcedureCatalog
import org.apache.paimon.table.FileStoreTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.amoro.producer.core.TargetTable

final case class PaimonTableMetadata(
    primaryKeys: Seq[String],
    partitionKeys: Seq[String],
    schema: StructType,
    properties: Map[String, String])

trait PaimonSqlRunner {

  def run(spark: SparkSession, sqlText: String): Seq[Row]

  def expireSnapshotsSupportsOptions(spark: SparkSession, catalogName: String): Boolean = {
    val previousSession = SparkSession.getActiveSession
    SparkSession.setActiveSession(spark)
    try {
      SQLConf.withExistingConf(spark.sessionState.conf) {
        // Inspect the runtime procedure, since Producer supports both Paimon 0.9 and 1.3.
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/catalog/ProcedureCatalog.java
        spark.sessionState.catalogManager.catalog(catalogName) match {
          case catalog: ProcedureCatalog =>
            catalog.loadProcedure(Identifier.of(Array("sys"), "expire_snapshots"))
              .parameters().exists(_.name() == "options")
          case _ =>
            throw new IllegalArgumentException(s"Catalog $catalogName cannot load procedures")
        }
      }
    } finally {
      previousSession match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

  def readExplicitProperties(
      spark: SparkSession,
      catalogName: String,
      table: TargetTable): Map[String, String] = {
    // OptionsTable reads schema options, without filling native defaults.
    // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/table/system/OptionsTable.java
    import PaimonSqlUtils.quoteIdent
    val optionsTable = s"${quoteIdent(catalogName)}.${quoteIdent(table.database)}." +
      quoteIdent(table.table + "$options")
    val pairs = run(spark, s"SELECT `key`, `value` FROM $optionsTable").map { row =>
      require(row.size == 2 && !row.isNullAt(0) && !row.isNullAt(1), "Invalid Paimon options row")
      row.getString(0) -> row.getString(1)
    }
    require(pairs.map(_._1).distinct.size == pairs.size, "Duplicate Paimon option keys")
    pairs.toMap
  }

  def loadFileStoreTable(
      spark: SparkSession,
      catalogName: String,
      table: TargetTable): FileStoreTable = {
    withSparkTable(spark, catalogName, table) { loaded =>
      loaded.getTable match {
        case fileTable: FileStoreTable => fileTable
        case _ =>
          throw new IllegalArgumentException("Snapshot expiration requires a FileStoreTable")
      }
    }
  }

  def loadTableMetadata(
      spark: SparkSession,
      catalogName: String,
      table: TargetTable): PaimonTableMetadata = {
    withSparkTable(spark, catalogName, table) { loaded =>
      val paimonTable = loaded.getTable
      PaimonTableMetadata(
        paimonTable.primaryKeys().asScala.toVector,
        paimonTable.partitionKeys().asScala.toVector,
        loaded.schema,
        loaded.properties().asScala.toMap)
    }
  }

  private def withSparkTable[T](
      spark: SparkSession,
      catalogName: String,
      table: TargetTable)(body: SparkTable => T): T = {
    val previousSession = SparkSession.getActiveSession
    SparkSession.setActiveSession(spark)
    try {
      SQLConf.withExistingConf(spark.sessionState.conf) {
        spark.sessionState.catalogManager.catalog(catalogName) match {
          case catalog: TableCatalog =>
            catalog.loadTable(Identifier.of(Array(table.database), table.table)) match {
              case loaded: SparkTable => body(loaded)
              case other => throw new IllegalArgumentException(
                  s"Expected a Paimon SparkTable, got ${other.getClass.getName}")
            }
          case _ =>
            throw new IllegalArgumentException(s"Catalog $catalogName cannot load tables")
        }
      }
    } finally {
      previousSession match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

}

object SparkPaimonSqlRunner extends PaimonSqlRunner {

  override def run(spark: SparkSession, sqlText: String): Seq[Row] = {
    spark.sql(sqlText).collect().toSeq
  }
}
