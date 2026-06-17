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

package org.apache.amoro.producer

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.{Row, SparkSession}

object PaimonCompactManifest {

  private val DefaultCatalogName = "paimon"
  private val DefaultRetryTimes = 3
  private val DefaultContinueOnTableFailure = true

  final private case class AppConfig(
      catalogName: String,
      databaseName: Option[String],
      tableName: Option[String],
      retryTimes: Int,
      continueOnTableFailure: Boolean,
      procedureOptions: String)

  final private case class TargetTable(raw: String, database: String, table: String)

  final private case class CompactResult(result: Boolean, costMs: Double)

  def main(args: Array[String]): Unit = {
    val config =
      try {
        val parsed = parseArgs(args)
        validateConfig(parsed)
        parsed
      } catch {
        case e: IllegalArgumentException =>
          printUsage()
          println(s"参数错误: ${e.getMessage}")
          sys.exit(2)
      }

    val spark = SparkSession.builder().getOrCreate()
    try {
      val targets = resolveTargetTables(spark, config)
      val modeDesc =
        if (targets.size > 1 || config.tableName.isEmpty) {
          "整库 Manifest 合并"
        } else {
          "单表 Manifest 合并"
        }

      if (targets.nonEmpty) {
        if (targets.size > 1 || config.tableName.isEmpty) {
          val databaseName = config.databaseName.getOrElse(targets.head.database)
          println(
            s"${modeDesc}启动，catalog=${config.catalogName}, 数据库 $databaseName 共发现 ${targets.size} 张表：")
          targets.foreach(t => println(s"  - ${t.raw}"))
        } else {
          println(s"${modeDesc}启动，catalog=${config.catalogName}, 目标表: ${targets.head.raw}")
        }
      }

      var successTables = 0
      var failedTables = 0
      val allStartNs = System.nanoTime()

      targets.foreach { table =>
        try {
          compactOneTable(spark, config, table)
          successTables += 1
        } catch {
          case NonFatal(t) =>
            failedTables += 1
            println(s"[表 ${table.raw} Manifest 合并失败] ${t.getClass.getSimpleName}: ${t.getMessage}")
            if (!config.continueOnTableFailure) {
              throw t
            }
        }
      }

      val allCostMs = (System.nanoTime() - allStartNs) / 1e6
      println(
        f"[Manifest 合并汇总] catalog=${config.catalogName}, 模式=$modeDesc, " +
          f"successTables=$successTables, failedTables=$failedTables, " +
          f"总耗时=${allCostMs}%.3f ms, 表数=${targets.size}")
    } finally {
      spark.stop()
    }
  }

  private def resolveTargetTables(spark: SparkSession, config: AppConfig): Seq[TargetTable] = {
    (config.databaseName, config.tableName) match {
      case (Some(database), Some(table)) =>
        Seq(makeTarget(
          requireNonEmpty(database, "--databaseName"),
          requireNonEmpty(table, "--tableName")))

      case (Some(database), None) =>
        listTables(spark, config.catalogName, requireNonEmpty(database, "--databaseName"))

      case (None, Some(table)) =>
        val trimTable = requireNonEmpty(table, "--tableName")
        val parts = trimTable.split("\\.", -1)
        if (parts.length != 2 || parts.exists(_.trim.isEmpty)) {
          throw new IllegalArgumentException(
            "只传入 --tableName 时，必须使用 db.table 格式，例如 --tableName db.table")
        }
        Seq(makeTarget(parts(0).trim, parts(1).trim))

      case (None, None) =>
        throw new IllegalArgumentException("参数不足：请至少提供 --databaseName 或 --tableName")
    }
  }

  private def listTables(
      spark: SparkSession,
      catalogName: String,
      database: String): Seq[TargetTable] = {
    val showTablesSql = s"SHOW TABLES IN ${quoteIdent(catalogName)}.${quoteIdent(database)}"
    println(s"listing tables: $showTablesSql")

    val rows = spark.sql(showTablesSql).collect()
    rows.flatMap { row =>
      val tableNameOpt = fieldOpt(row, Set("tablename", "table"))
        .orElse(valueAt(row, 1))
        .filter(_ != null)
        .map(_.toString.trim)

      val isTemporary = fieldOpt(row, Set("istemporary", "temporary")).exists {
        case b: java.lang.Boolean => b.booleanValue()
        case s: String => s.equalsIgnoreCase("true") || s == "1"
        case i: java.lang.Integer => i == 1
        case l: java.lang.Long => l == 1L
        case _ => false
      }

      tableNameOpt
        .filter(name => name.nonEmpty && !isTemporary)
        .map(name => makeTarget(database, name))
    }.toSeq
  }

  private def compactOneTable(
      spark: SparkSession,
      config: AppConfig,
      table: TargetTable): CompactResult = {
    println(s"========== 表 ${table.raw} 开始 Manifest 合并 ==========")
    val startNs = System.nanoTime()
    val result = runOnceWithRetry(spark, config, table)
    val costMs = (System.nanoTime() - startNs) / 1e6

    println(
      f"[表 ${table.raw} Manifest 合并结束] " +
        f"耗时=${costMs}%.3f ms, result=$result")

    CompactResult(result, costMs)
  }

  private def runOnceWithRetry(
      spark: SparkSession,
      config: AppConfig,
      table: TargetTable): Boolean = {
    var attempt = 1
    var lastError: Throwable = null

    while (attempt <= config.retryTimes) {
      try {
        println(s"[表 ${table.raw}] 第 $attempt 次执行 compact_manifest")
        return runCompactManifest(spark, config, table)
      } catch {
        case NonFatal(t) =>
          lastError = t
          println(
            s"[表 ${table.raw}] 第 $attempt 次执行失败: ${t.getClass.getSimpleName}: ${t.getMessage}")
          attempt += 1
      }
    }

    throw lastError
  }

  private def runCompactManifest(
      spark: SparkSession,
      config: AppConfig,
      table: TargetTable): Boolean = {
    val sqlText = buildCompactManifestSql(config, table)
    println(s"executing compact_manifest, table = ${table.raw}")
    println(sqlText)

    val resultRows = spark.sql(sqlText).collect()
    val result = resultRows.headOption
      .flatMap(row => fieldOpt(row, Set("result")).orElse(valueAt(row, 0)))
      .exists(toBoolean)

    println(s"SUCCESS table = ${table.raw}, result = $result")
    result
  }

  private def buildCompactManifestSql(config: AppConfig, table: TargetTable): String = {
    val optionsClause =
      if (config.procedureOptions.trim.isEmpty) {
        ""
      } else {
        s",\n  options => ${sqlString(config.procedureOptions)}"
      }

    s"""
       |CALL ${quoteIdent(config.catalogName)}.sys.compact_manifest(
       |  table => ${sqlString(table.raw)}$optionsClause
       |)
       |""".stripMargin
  }

  private def makeTarget(database: String, table: String): TargetTable = {
    TargetTable(s"${database}.${table}", database, table)
  }

  private def validateConfig(config: AppConfig): Unit = {
    requireNonEmpty(config.catalogName, "--catalogName")

    (config.databaseName, config.tableName) match {
      case (Some(database), Some(table)) =>
        requireNonEmpty(database, "--databaseName")
        val trimTable = requireNonEmpty(table, "--tableName")
        if (trimTable.contains(".")) {
          throw new IllegalArgumentException(
            "当 --databaseName 与 --tableName 同时提供时，--tableName 仅允许传入表名，不允许使用 db.table")
        }

      case (Some(database), None) =>
        requireNonEmpty(database, "--databaseName")

      case (None, Some(table)) =>
        val trimTable = requireNonEmpty(table, "--tableName")
        val parts = trimTable.split("\\.", -1)
        if (parts.length != 2 || parts.exists(_.trim.isEmpty)) {
          throw new IllegalArgumentException(
            "只传入 --tableName 时，必须使用 db.table 格式，例如 --tableName db.table")
        }

      case (None, None) =>
        throw new IllegalArgumentException("参数不足：请至少提供 --databaseName 或 --tableName")
    }

    if (config.retryTimes <= 0) {
      throw new IllegalArgumentException("--retryTimes 必须大于 0")
    }
  }

  private def parseArgs(args: Array[String]): AppConfig = {
    var catalogName = DefaultCatalogName
    var databaseName: Option[String] = None
    var tableName: Option[String] = None
    var retryTimes = DefaultRetryTimes
    var continueOnTableFailure = DefaultContinueOnTableFailure
    var procedureOptions = ""

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--help" | "-h" =>
          printUsage()
          sys.exit(0)

        case "--catalogName" =>
          catalogName = readValue(args, i, "--catalogName")
          i += 1

        case "--databaseName" =>
          databaseName = Some(readValue(args, i, "--databaseName"))
          i += 1

        case "--tableName" =>
          tableName = Some(readValue(args, i, "--tableName"))
          i += 1

        case "--retryTimes" =>
          retryTimes = parseInt(readValue(args, i, "--retryTimes"), "--retryTimes")
          i += 1

        case "--continueOnTableFailure" =>
          continueOnTableFailure =
            parseBoolean(readValue(args, i, "--continueOnTableFailure"), "--continueOnTableFailure")
          i += 1

        case "--procedureOptions" =>
          procedureOptions = readValue(args, i, "--procedureOptions")
          i += 1

        case other =>
          throw new IllegalArgumentException(s"未知参数: $other")
      }
      i += 1
    }

    AppConfig(
      catalogName,
      databaseName,
      tableName,
      retryTimes,
      continueOnTableFailure,
      procedureOptions)
  }

  private def readValue(args: Array[String], index: Int, argName: String): String = {
    if (index + 1 >= args.length || args(index + 1).startsWith("--")) {
      throw new IllegalArgumentException(s"$argName 缺少参数值")
    }
    args(index + 1)
  }

  private def parseInt(value: String, argName: String): Int = {
    try {
      value.toInt
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$argName 必须是整数")
    }
  }

  private def parseBoolean(value: String, argName: String): Boolean = {
    value.trim.toLowerCase(Locale.ROOT) match {
      case "true" => true
      case "false" => false
      case _ => throw new IllegalArgumentException(s"$argName 必须是 true 或 false")
    }
  }

  private def requireNonEmpty(value: String, argName: String): String = {
    val trimmed = value.trim
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException(s"$argName 的值不能为空")
    }
    trimmed
  }

  private def quoteIdent(name: String): String = {
    "`" + name.replace("`", "``") + "`"
  }

  private def sqlString(value: String): String = {
    "'" + value.replace("'", "''") + "'"
  }

  private def fieldOpt(row: Row, normalizedNames: Set[String]): Option[Any] = {
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

  private def valueAt(row: Row, index: Int): Option[Any] = {
    if (index >= 0 && index < row.size && !row.isNullAt(index)) {
      Some(row.get(index))
    } else {
      None
    }
  }

  private def normalizeFieldName(name: String): String = {
    name.toLowerCase(Locale.ROOT).replace("_", "")
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

  private def printUsage(): Unit = {
    println(
      s"""
         |Usage:
         |  spark-submit --class org.apache.amoro.producer.PaimonCompactManifest <jar> [options]
         |
         |Required target options:
         |  --databaseName <db>                 合并指定库下全部非临时表 Manifest
         |  --databaseName <db> --tableName <t> 只合并指定库下的单表 Manifest
         |  --tableName <db.table>              只合并全限定单表 Manifest
         |
         |Optional options:
         |  --catalogName <catalog>             Paimon catalog 名称，默认: $DefaultCatalogName
         |  --procedureOptions <options>        compact_manifest options，默认不传
         |  --retryTimes <num>                  单表失败最大执行次数，默认: $DefaultRetryTimes
         |  --continueOnTableFailure <true|false>
         |                                      单表失败后是否继续后续表，默认: $DefaultContinueOnTableFailure
         |  --help, -h                          打印帮助信息
         |
         |Examples:
         |  --databaseName sl_oki_test
         |  --databaseName sl_oki_test --tableName t_order
         |  --tableName sl_oki_test.t_order
         |""".stripMargin)
  }
}
