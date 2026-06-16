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

object PaimonExpireSnapshots {

  private val DefaultRetainMax = 20
  private val DefaultRetainMin = 10
  private val DefaultMaxDeletes = 550L
  private val DefaultRetryTimes = 3
  private val DefaultContinueOnTableFailure = true
  private val DefaultProcedureOptions =
    "file-operation.thread-num=32,snapshot.expire.execution-mode=sync"

  final private case class AppConfig(
      databaseName: Option[String],
      tableName: Option[String],
      retainMax: Int,
      retainMin: Int,
      maxDeletes: Long,
      retryTimes: Int,
      continueOnTableFailure: Boolean,
      procedureOptions: String)

  final private case class TargetTable(raw: String, database: String, table: String)

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

      val (targetsByMode, modeDesc) =
        if (targets.size > 1 || config.tableName.isEmpty) {
          val databaseName = config.databaseName.getOrElse("unknown")
          (Some(databaseName), "整库清理")
        } else {
          (None, "单表清理")
        }

      if (targets.nonEmpty) {
        if (targetsByMode.nonEmpty) {
          println(s"${modeDesc}启动，数据库 ${targetsByMode.get} 共发现 ${targets.size} 张表：")
          targets.foreach(t => println(s"  - ${t.raw}"))
        } else {
          println(s"${modeDesc}启动，目标表: ${targets.head.raw}")
        }
      }

      var successTables = 0
      var failedTables = 0
      val allStartNs = System.nanoTime()

      targets.foreach { table =>
        try {
          expireOneTable(spark, config, table)
          successTables += 1
        } catch {
          case NonFatal(t) =>
            failedTables += 1
            println(s"[表 ${table.raw} 清理失败] ${t.getClass.getSimpleName}: ${t.getMessage}")
            if (!config.continueOnTableFailure) {
              throw t
            }
        }
      }

      val allCostMs = (System.nanoTime() - allStartNs) / 1e6
      println(
        f"[清理汇总] 模式=$modeDesc, successTables=$successTables, failedTables=$failedTables, " +
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
        listTables(spark, requireNonEmpty(database, "--databaseName"))

      case (None, Some(table)) =>
        val trimTable = requireNonEmpty(table, "--tableName")
        val parts = trimTable.split("\\.", -1)
        if (parts.length != 2 || parts.exists(_.trim.isEmpty)) {
          throw new IllegalArgumentException(
            "只传入 --tableName 时，必须使用 db.table 格式，例如 --tableName db.table")
        }
        if (parts.length == 2) {
          Seq(makeTarget(parts(0).trim, parts(1).trim))
        } else {
          throw new IllegalArgumentException(
            "只传入 --tableName 时，必须使用 db.table 格式，例如 --tableName db.table")
        }

      case (None, None) =>
        throw new IllegalArgumentException("参数不足：请至少提供 --databaseName 或 --tableName")
    }
  }

  private def requireNonEmpty(value: String, argName: String): String = {
    val trimmed = value.trim
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException(s"$argName 的值不能为空")
    }
    trimmed
  }

  private def validateConfig(config: AppConfig): Unit = {
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

    if (config.retainMax < 0) {
      throw new IllegalArgumentException("--retainMax 不能小于 0")
    }
    if (config.retainMin < 0) {
      throw new IllegalArgumentException("--retainMin 不能小于 0")
    }
    if (config.maxDeletes < 0) {
      throw new IllegalArgumentException("--maxDeletes 不能小于 0")
    }
    if (config.retryTimes <= 0) {
      throw new IllegalArgumentException("--retryTimes 必须大于 0")
    }
  }

  private def makeTarget(database: String, table: String): TargetTable = {
    TargetTable(s"${database}.${table}", database, table)
  }

  private def listTables(spark: SparkSession, database: String): Seq[TargetTable] = {
    val df = spark.sql(s"SHOW TABLES IN ${quoteIdent(database)}")
    val rows = df.collect()
    val tables = rows.flatMap { row =>
      val tableNameOpt = fieldOpt(row, Set("tablename", "table_name", "table"))
        .orElse(if (row.size > 1) {
          Option(row.get(1))
        } else {
          None
        })
        .filter(_ != null)
        .map(_.toString)

      val isTemporary = fieldOpt(row, Set("istemporary", "is_temporary")).exists {
        case b: java.lang.Boolean => b.booleanValue()
        case s: String => s.equalsIgnoreCase("true") || s == "1"
        case i: java.lang.Integer => i == 1
        case l: java.lang.Long => l == 1L
        case _ => false
      }

      tableNameOpt
        .filter(name => name != null && name.trim.nonEmpty && !isTemporary)
        .map(name => makeTarget(database, name))
    }
    tables.toSeq
  }

  private def expireOneTable(spark: SparkSession, config: AppConfig, table: TargetTable): Unit = {
    var round = 1
    var executedRounds = 0
    var totalCostMs = 0.0
    var maxCostMs = 0.0
    var minCostMs = Double.PositiveInfinity
    var shouldContinue = true

    println(s"========== 表 ${table.raw} 开始清理 ==========")
    while (shouldContinue) {
      println(s"========== 表 ${table.raw} 开始第 ${round} 轮过期快照清理 ==========")
      val roundStartNs = System.nanoTime()

      val deletedCount = runOnceWithRetry(spark, config, table.raw)
      val roundCostMs = (System.nanoTime() - roundStartNs) / 1e6
      executedRounds += 1
      totalCostMs += roundCostMs
      maxCostMs = math.max(maxCostMs, roundCostMs)
      minCostMs = math.min(minCostMs, roundCostMs)

      val avgCostMs = totalCostMs / executedRounds
      shouldContinue = deletedCount >= config.maxDeletes

      println(
        f"[表 ${table.raw} 第 ${round} 轮统计] " +
          f"本轮耗时=${roundCostMs}%.3f ms, " +
          f"平均耗时=${avgCostMs}%.3f ms, " +
          f"最大耗时=${maxCostMs}%.3f ms, " +
          f"最小耗时=${minCostMs}%.3f ms, " +
          f"deletedCount=$deletedCount, continue=$shouldContinue")
      round += 1
    }

    println(
      f"[表 ${table.raw} 清理结束] " +
        f"总轮次=$executedRounds, " +
        f"平均耗时=${if (executedRounds == 0) 0.0 else totalCostMs / executedRounds}%.3f ms, " +
        f"最大耗时=${if (executedRounds == 0) 0.0 else maxCostMs}%.3f ms, " +
        f"最小耗时=${if (executedRounds == 0) 0.0 else minCostMs}%.3f ms")
  }

  private def runOnceWithRetry(
      spark: SparkSession,
      config: AppConfig,
      tableName: String): Long = {
    var attempt = 1
    var lastError: Throwable = null

    while (attempt <= config.retryTimes) {
      try {
        val df = spark.sql(buildExpireSql(tableName, config))
        val rows = df.collect()

        if (rows.isEmpty) {
          throw new IllegalStateException(s"expire_snapshots 返回空结果，table=$tableName")
        }

        val deletedCount = parseDeletedCount(rows(0))
        println(
          s"[表 $tableName 当前轮尝试 ${attempt}/${config.retryTimes}] " +
            s"成功，schema=${df.schema.simpleString}, " +
            s"row=${rows(0).mkString("[", ", ", "]")}, deletedCount=$deletedCount")
        return deletedCount
      } catch {
        case NonFatal(t) =>
          lastError = t
          println(
            s"[表 $tableName 当前轮尝试 ${attempt}/${config.retryTimes}] 失败: " +
              s"${t.getClass.getSimpleName}: ${t.getMessage}")
          if (attempt == config.retryTimes) {
            throw new RuntimeException(
              s"表 $tableName expire_snapshots 连续失败 ${config.retryTimes} 次",
              lastError)
          }
          attempt += 1
      }
    }
    throw new RuntimeException("不应到达此分支")
  }

  private def buildExpireSql(tableName: String, config: AppConfig): String = {
    val escapedTableName = escapeSqlString(tableName)
    val escapedOptions = escapeSqlString(config.procedureOptions)
    val optionsSql =
      if (config.procedureOptions.trim.isEmpty) "" else s", options => '$escapedOptions'"
    s"""
       |CALL sys.expire_snapshots(
       |  table => '$escapedTableName',
       |  retain_max => ${config.retainMax},
       |  retain_min => ${config.retainMin},
       |  max_deletes => ${config.maxDeletes}
       |$optionsSql
       |)
       |""".stripMargin
  }

  private def quoteIdent(s: String): String = "`" + s.replace("`", "``") + "`"

  private def escapeSqlString(value: String): String = value.replace("'", "''")

  private def parseDeletedCount(row: Row): Long = {
    val preferredNames = Set(
      "deleted_count",
      "deletedcount",
      "deleted_snapshots",
      "deleted_snapshots_count",
      "deletedsnapshotcount",
      "result")

    val byName =
      try {
        val schema = row.schema
        schema.fieldNames.zipWithIndex.collectFirst {
          case (name, idx) if preferredNames.contains(name.toLowerCase(Locale.ROOT)) =>
            row.get(idx)
        }.flatMap(toLongOpt)
      } catch {
        case NonFatal(_) => None
      }

    byName
      .orElse(row.toSeq.flatMap(toLongOpt).headOption)
      .getOrElse {
        throw new IllegalStateException(
          s"无法从返回结果解析删除数量，row=${row.mkString("[", ", ", "]")}")
      }
  }

  private def toLongOpt(v: Any): Option[Long] = v match {
    case null => None
    case n: java.lang.Number => Some(n.longValue())
    case s: String if s.trim.matches("-?\\d+") => Some(s.trim.toLong)
    case _ => None
  }

  private def fieldOpt(row: Row, names: Set[String]): Option[Any] = {
    try {
      val schema = row.schema
      schema.fieldNames.zipWithIndex.collectFirst {
        case (name, idx) if names.contains(name.toLowerCase(Locale.ROOT)) => row.get(idx)
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  private def parseArgs(args: Array[String]): AppConfig = {
    var databaseName: Option[String] = None
    var tableName: Option[String] = None
    var retainMax = DefaultRetainMax
    var retainMin = DefaultRetainMin
    var maxDeletes = DefaultMaxDeletes
    var retryTimes = DefaultRetryTimes
    var continueOnTableFailure = DefaultContinueOnTableFailure
    var procedureOptions = DefaultProcedureOptions

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--help" | "-h" =>
          printUsage()
          sys.exit(0)
        case "--databaseName" =>
          i += 1
          if (i >= args.length) {
            throw new IllegalArgumentException(s"--databaseName 需要值")
          }
          databaseName = Some(args(i))
        case "--tableName" =>
          i += 1
          if (i >= args.length) {
            throw new IllegalArgumentException(s"--tableName 需要值")
          }
          tableName = Some(args(i))
        case "--retainMax" =>
          i += 1
          if (i >= args.length) {
            throw new IllegalArgumentException("--retainMax 需要值")
          }
          retainMax = parseIntArg("--retainMax", args(i))
        case "--retainMin" =>
          i += 1
          if (i >= args.length) {
            throw new IllegalArgumentException("--retainMin 需要值")
          }
          retainMin = parseIntArg("--retainMin", args(i))
        case "--maxDeletes" =>
          i += 1
          if (i >= args.length) {
            throw new IllegalArgumentException("--maxDeletes 需要值")
          }
          maxDeletes = parseLongArg("--maxDeletes", args(i))
        case "--retryTimes" =>
          i += 1
          if (i >= args.length) {
            throw new IllegalArgumentException("--retryTimes 需要值")
          }
          retryTimes = parseIntArg("--retryTimes", args(i))
        case "--continueOnTableFailure" =>
          i += 1
          if (i >= args.length) {
            throw new IllegalArgumentException("--continueOnTableFailure 需要 true/false")
          }
          continueOnTableFailure = parseBooleanArg("--continueOnTableFailure", args(i))
        case "--procedureOptions" =>
          i += 1
          if (i >= args.length) {
            throw new IllegalArgumentException("--procedureOptions 需要值")
          }
          procedureOptions = args(i)
        case value if value.startsWith("--") =>
          throw new IllegalArgumentException(s"未知参数: $value")
        case value =>
          throw new IllegalArgumentException(s"参数格式错误: $value")
      }
      i += 1
    }

    AppConfig(
      databaseName,
      tableName,
      retainMax,
      retainMin,
      maxDeletes,
      retryTimes,
      continueOnTableFailure,
      procedureOptions)
  }

  private def parseIntArg(argName: String, value: String): Int = {
    try {
      value.toInt
    } catch {
      case NonFatal(_) => throw new IllegalArgumentException(s"$argName 必须是整数，当前: $value")
    }
  }

  private def parseLongArg(argName: String, value: String): Long = {
    try {
      value.toLong
    } catch {
      case NonFatal(_) => throw new IllegalArgumentException(s"$argName 必须是整数，当前: $value")
    }
  }

  private def parseBooleanArg(argName: String, value: String): Boolean = {
    value.toLowerCase(Locale.ROOT) match {
      case "true" | "1" | "yes" => true
      case "false" | "0" | "no" => false
      case _ =>
        throw new IllegalArgumentException(
          s"$argName 仅支持 true/false/1/0/yes/no，当前: $value")
    }
  }

  private def printUsage(): Unit = {
    println(
      """
        |Usage:
        |  spark-submit --class org.apache.amoro.producer.PaimonExpireSnapshots <jar> [options]
        |
        |Options:
        |  --databaseName <database>    指定数据库名（与 --tableName 不同时提供则扫描全库）
        |  --tableName <table>          指定表名（与 --databaseName 同时使用时只清理该表；仅单独使用时需 db.table）
        |  --retainMax <int>            保留快照数上限，默认 20
        |  --retainMin <int>            保留快照下限，默认 10
        |  --maxDeletes <long>          单次最多删除快照数，默认 550
        |  --retryTimes <int>           SQL 重试次数，默认 3
        |  --continueOnTableFailure <bool> 出错是否继续，默认 true
        |  --procedureOptions <string>   expire_snapshots options 字符串，默认 file-operation.thread-num=32,snapshot.expire.execution-mode=sync
        |  --help, -h                   打印帮助
        |""".stripMargin)
  }
}
