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

import org.apache.spark.sql.SparkSession

import org.apache.amoro.producer.spi.LakeFormatAdapter

object TableResolver {

  def validateTargetOptions(common: CommonProducerConfig): Unit = {
    (common.databaseName, common.tableName) match {
      case (Some(database), None) =>
        requireNonEmpty(database, "--databaseName")
        common.tableNameRegex.foreach(TableFilter.compileTableNameRegex)

      case (Some(database), Some(table)) =>
        rejectRegexWithTableName(common.tableNameRegex)
        requireNonEmpty(database, "--databaseName")
        requireSimpleTableName(table)

      case (None, Some(table)) =>
        rejectRegexWithTableName(common.tableNameRegex)
        requireQualifiedTableName(table)

      case (None, None) =>
        throw new IllegalArgumentException(
          "--databaseName or qualified --tableName must be specified")
    }
  }

  def resolveTargets(
      spark: SparkSession,
      adapter: LakeFormatAdapter,
      common: CommonProducerConfig): Seq[TargetTable] = {
    validateTargetOptions(common)
    (common.databaseName, common.tableName) match {
      case (Some(database), None) =>
        val pattern = common.tableNameRegex.map(TableFilter.compileTableNameRegex)
        val tables = adapter.listTables(spark, common, requireNonEmpty(database, "--databaseName"))
        TableFilter.filterByCompiledRegex(tables, pattern)

      case (Some(database), Some(table)) =>
        rejectRegexWithTableName(common.tableNameRegex)
        val databaseName = requireNonEmpty(database, "--databaseName")
        val tableName = requireSimpleTableName(table)
        Seq(TargetTable(s"$databaseName.$tableName", databaseName, tableName))

      case (None, Some(table)) =>
        rejectRegexWithTableName(common.tableNameRegex)
        val (databaseName, tableName) = requireQualifiedTableName(table)
        Seq(TargetTable(s"$databaseName.$tableName", databaseName, tableName))

      case (None, None) =>
        throw new IllegalArgumentException(
          "--databaseName or qualified --tableName must be specified")
    }
  }

  private def rejectRegexWithTableName(regex: Option[String]): Unit = {
    if (regex.isDefined) {
      throw new IllegalArgumentException(
        "--tableNameRegex can only be used with --databaseName without --tableName")
    }
  }

  private def requireSimpleTableName(table: String): String = {
    val tableName = requireNonEmpty(table, "--tableName")
    if (tableName.contains(".")) {
      throw new IllegalArgumentException(
        "--tableName must be a simple table name when --databaseName is specified")
    }
    tableName
  }

  private def requireQualifiedTableName(table: String): (String, String) = {
    val tableName = requireNonEmpty(table, "--tableName")
    val parts = tableName.split("\\.", -1)
    if (parts.length != 2 || parts.exists(_.trim.isEmpty)) {
      throw new IllegalArgumentException(
        "--tableName must be qualified as database.table when --databaseName is not specified")
    }
    (parts(0).trim, parts(1).trim)
  }

  private def requireNonEmpty(value: String, name: String): String = {
    val trimmed = Option(value).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException(s"$name must not be empty")
    }
    trimmed
  }
}
