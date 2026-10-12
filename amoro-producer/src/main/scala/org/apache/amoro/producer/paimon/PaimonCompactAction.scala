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

import java.time.{Instant, LocalDate}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.paimon.utils.{JsonSerdeUtil, ParameterUtils, StringUtils}
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import org.apache.amoro.producer.core.{ActionOption, ActionResult, ActionStatus, ActionTaskResult, CommonProducerConfig, ProducerContext, ProducerParsedOptions, RetryMode, TargetTable}
import org.apache.amoro.producer.paimon.PaimonSqlUtils.quoteIdent
import org.apache.amoro.producer.spi.{ActionConfig, ProducerAction}

final case class PaimonCompactConfig(
    compactStrategy: String,
    procedureOptions: String,
    version: String,
    partitionIdleTime: String,
    where: Option[String],
    compactStrategyExplicitlySet: Boolean = false) extends ActionConfig

class PaimonCompactAction(
    runner: PaimonSqlRunner = SparkPaimonSqlRunner,
    onCompactionCompleted: PaimonCompactionCompletion => Unit = PaimonCompactionCompletion.log)
  extends ProducerAction {

  private val log = LoggerFactory.getLogger(classOf[PaimonCompactAction])

  override def name: String = "compact"
  override def retryMode: RetryMode = RetryMode.ActionManagedRetry

  override def options: Seq[ActionOption] = Seq(
    ActionOption("startBucket", description = "Removed: official Paimon does not support buckets"),
    ActionOption("step", description = "Removed: official Paimon does not support buckets"),
    ActionOption(
      "compactStrategy",
      description =
        "Paimon 1.3 full or minor; default minor for unpartitioned primary-key tables, full otherwise"),
    ActionOption("procedureOptions", description = "Paimon 1.3 compact procedure options"),
    ActionOption("version", description = "Table execution version: 0.9 or 1.3 (default 0.9)"),
    ActionOption("partitionIdleTime", description = "Paimon 0.9 partition idle time"),
    ActionOption(
      "where",
      description = "Partition predicate adapted to each table; mutually exclusive with --between"),
    ActionOption(
      "between",
      description =
        "Date yyyy-MM-dd: generate preset_dt BETWEEN previous day and date; exclusive with --where"))

  override def parse(commandLine: ProducerParsedOptions): ActionConfig = {
    require(
      !(commandLine.hasOption("where") && commandLine.hasOption("between")),
      "--where and --between are mutually exclusive; provide only one")
    Seq("startBucket", "step").foreach { option =>
      require(
        !commandLine.hasOption(option),
        s"--$option is no longer supported: official Paimon compact has no buckets argument")
    }
    PaimonCompactConfig(
      compactStrategy =
        commandLine.getOptionValue("compactStrategy", "full").trim.toLowerCase(Locale.ROOT),
      procedureOptions =
        commandLine.getOptionValue("procedureOptions", "target-file-size=256m").trim,
      version = commandLine.getOptionValue("version", "0.9").trim,
      partitionIdleTime = commandLine.getOptionValue("partitionIdleTime", "1h").trim,
      where = if (commandLine.hasOption("between")) {
        Some(PaimonCompactAction.betweenPredicate(commandLine.getOptionValue("between")))
      } else Option(commandLine.getOptionValue("where")),
      compactStrategyExplicitlySet = commandLine.hasOption("compactStrategy"))
  }

  override def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit = {
    val config = asCompactConfig(actionConfig)
    require(common.retryTimes > 0, "--retryTimes must be greater than 0")
    require(Set("0.9", "1.3").contains(config.version), "--version must be 0.9 or 1.3")
    config.where.foreach(value => requireNonBlank(value, "--where"))
  }

  override def execute(
      context: ProducerContext,
      table: TargetTable,
      actionConfig: ActionConfig): ActionResult = {
    validate(context.common, actionConfig)
    val config = asCompactConfig(actionConfig)
    val (displayConfig, secrets) = PaimonCompactionCompletion.redactOptions(config)
    val startNs = System.nanoTime()
    var metrics = Map("table" -> table.raw, "requestedVersion" -> config.version)

    def result(task: ActionTaskResult): ActionResult = ActionResult(
      task.status,
      Seq(task),
      task.message,
      metrics,
      (System.nanoTime() - startNs) / 1e6)
    def failed(taskName: String, error: Throwable, attempts: Int): ActionResult = result(
      ActionTaskResult(
        taskName,
        ActionStatus.Failed,
        Some(PaimonCompactionCompletion.redactError(errorMessage(error), secrets)),
        Map("attempts" -> attempts.toString)))

    val (readResult, readAttempts) = withRetry(context.common.retryTimes) {
      readProperties(context, table)
    }
    metrics += "metadataAttempts" -> readAttempts.toString
    val properties = readResult match {
      case Right(value) => value
      case Left(error) => return failed("read-options", error, readAttempts)
    }
    val managed =
      enabled(properties, "self-optimizing.enabled") && enabled(properties, "write-only")
    val effectiveVersion = if (managed) "1.3" else "0.9"
    if (config.version != effectiveVersion) {
      val reason = if (managed) {
        "table enables both self-optimizing.enabled and write-only and requires --version 1.3"
      } else {
        "table does not enable both self-optimizing.enabled and write-only"
      }
      log.warn(
        s"compact skipped: catalog=${context.common.catalogName}, database=${table.database}, " +
          s"table=${table.table}, requestedVersion=${config.version}, reason=$reason")
      return result(ActionTaskResult("compact", ActionStatus.Skipped, Some(reason), Map.empty))
    }
    metrics += "effectiveVersion" -> effectiveVersion

    // Spark applies session/table-scoped options when loading the target. The $options
    // system table exposes only persistent options and cannot prove range safety.
    // https://github.com/apache/paimon/blob/release-1.3.2/paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/SparkTable.scala
    val (loaded, metadataAttempts) = withRetry(context.common.retryTimes) {
      runner.loadTableMetadata(context.spark, context.common.catalogName, table)
    }
    metrics += "effectiveOptionsAttempts" -> metadataAttempts.toString
    val metadata = loaded match {
      case Right(value) => value
      case Left(error) => return failed("read-table-metadata", error, metadataAttempts)
    }
    metrics ++= Map(
      "primaryKeys" -> JsonSerdeUtil.toFlatJson(metadata.primaryKeys.asJava),
      "partitionKeys" -> JsonSerdeUtil.toFlatJson(metadata.partitionKeys.asJava))

    val (sql, scope, effectiveStrategy) =
      try {
        validateSqlArguments(context, config, effectiveVersion)
        val resolved = PaimonCompactionScope.resolve(context.spark, metadata, config.where)
        val observed = resolved.redacted(secrets)
        metrics ++= observed.metrics
        val scopeLog = JsonSerdeUtil.toFlatJson((observed.fields ++ Map[String, AnyRef](
          "catalog" -> context.common.catalogName,
          "database" -> table.database,
          "table" -> table.table)).asJava)
        if (resolved.removesWhere) {
          log.warn(
            "Compaction scope adjusted; the requested where no longer limits this table: {}",
            scopeLog)
        } else {
          log.info("Compaction scope resolved: {}", scopeLog)
        }
        val scopedConfig = config.copy(where = resolved.effectiveWhere)
        validateExecution(scopedConfig, metadata.properties, effectiveVersion)
        val useMinorDefault = effectiveVersion == "1.3" &&
          !config.compactStrategyExplicitlySet && metadata.primaryKeys.nonEmpty &&
          metadata.partitionKeys.isEmpty
        val strategy = if (useMinorDefault) "minor" else config.compactStrategy
        val effectiveConfig = scopedConfig.copy(compactStrategy = strategy)
        if (effectiveVersion == "1.3") {
          metrics ++= Map(
            "requestedCompactStrategy" -> (if (config.compactStrategyExplicitlySet)
                                             config.compactStrategy
                                           else "default"),
            "effectiveCompactStrategy" -> strategy)
        }
        if (useMinorDefault) {
          log.info(
            s"Compaction defaults to minor for an unpartitioned primary-key table: " +
              s"catalog=${context.common.catalogName}, database=${table.database}, table=${table.table}")
        }
        (
          PaimonCompactAction.buildCompactSql(
            context.common.catalogName,
            effectiveConfig,
            table,
            effectiveVersion),
          observed,
          strategy)
      } catch {
        case NonFatal(error) => return failed("validate", error, 0)
      }
    val compactionStartTime = Instant.now()
    val compactionStartNs = System.nanoTime()
    val descriptionStrategy = if (effectiveVersion == "1.3") effectiveStrategy else "native"
    val (execution, attempts) = withCompactionJobDescription(
      context,
      table,
      descriptionStrategy,
      scope.effectiveWhere) {
      withRetry(context.common.retryTimes) {
        requireSuccessfulResult(runner.run(context.spark, sql))
      }
    }
    val compactionEndTime = Instant.now()
    val compactionDurationMs = (System.nanoTime() - compactionStartNs) / 1e6
    val actionResult = execution match {
      case Left(error) => failed("compact", error, attempts)
      case Right(_) => result(ActionTaskResult(
          "compact",
          ActionStatus.Success,
          None,
          Map("attempts" -> attempts.toString, "result" -> "true")))
    }
    // A completion callback is an observation of the final result, never part of a retry.
    try {
      val displaySql = PaimonCompactAction.buildCompactSql(
        context.common.catalogName,
        displayConfig.copy(compactStrategy = effectiveStrategy, where = scope.effectiveWhere),
        table,
        effectiveVersion)
      onCompactionCompleted(PaimonCompactionCompletion(
        catalog = context.common.catalogName,
        database = table.database,
        table = table.table,
        sql = displaySql,
        success = actionResult.status == ActionStatus.Success,
        startTime = compactionStartTime,
        endTime = compactionEndTime,
        durationMs = compactionDurationMs,
        attempts = attempts,
        effectiveVersion = effectiveVersion,
        error = actionResult.message,
        scope = scope))
    } catch {
      case NonFatal(error) =>
        log.warn(
          "Compaction completion callback failed; compaction result is unchanged: {}",
          error.getClass.getName)
    }
    actionResult
  }

  private def withCompactionJobDescription[T](
      context: ProducerContext,
      table: TargetTable,
      strategy: String,
      where: Option[String])(body: => T): T = {
    val sparkContext = Option(context.spark).map(_.sparkContext)
    val previous = sparkContext.map(_.getLocalProperty("spark.job.description"))
    sparkContext.foreach(_.setJobDescription(
      s"Compact table: ${context.common.catalogName}.${table.database}.${table.table} " +
        s"| strategy=$strategy | where=${where.getOrElse("ALL")}"))
    try body
    finally sparkContext.foreach(_.setJobDescription(previous.orNull))
  }

  private def readProperties(context: ProducerContext, table: TargetTable): Map[String, String] = {
    requireLiteralArguments(context, Seq(context.common.catalogName, table.database, table.table))
    val optionsTable = s"${quoteIdent(context.common.catalogName)}.${quoteIdent(table.database)}." +
      quoteIdent(table.table + "$options")
    val rows = runner.run(context.spark, s"SELECT `key`, `value` FROM $optionsTable")
    val pairs = rows.map { row =>
      require(
        row.size == 2 && !row.isNullAt(0) && !row.isNullAt(1),
        "Invalid Paimon options row: expected non-null key and value")
      row.getString(0) -> row.getString(1)
    }
    require(pairs.map(_._1).distinct.size == pairs.size, "Duplicate Paimon option keys")
    pairs.toMap
  }

  private def validateSqlArguments(
      context: ProducerContext,
      config: PaimonCompactConfig,
      effectiveVersion: String): Unit = {
    // CALL arguments are parsed by Spark. Legacy escapedStringLiterals changes their values.
    // https://spark.apache.org/docs/3.5.6/sql-ref-literals.html#string-literal
    require(
      context.spark == null ||
        !context.spark.conf.get("spark.sql.parser.escapedStringLiterals", "false").toBoolean,
      "compact requires spark.sql.parser.escapedStringLiterals=false to preserve SQL arguments")

    requireLiteralArguments(
      context,
      config.where.toSeq ++ (if (effectiveVersion == "1.3") {
                               Seq(config.compactStrategy, config.procedureOptions)
                             } else {
                               Seq(config.partitionIdleTime)
                             }))

  }

  private def validateExecution(
      config: PaimonCompactConfig,
      properties: Map[String, String],
      effectiveVersion: String): Unit = {
    val dynamicOptions = if (effectiveVersion == "1.3") {
      require(
        Set("full", "minor").contains(config.compactStrategy),
        "--compactStrategy must be full or minor")
      // Use the same parser as Paimon's Procedure, including duplicate-key precedence.
      // https://github.com/apache/paimon/blob/release-1.3.2/paimon-common/src/main/java/org/apache/paimon/utils/ParameterUtils.java
      val options =
        ParameterUtils.parseCommaSeparatedKeyValues(config.procedureOptions).asScala.toMap
      options.get("write-only").foreach { value =>
        require(value.equalsIgnoreCase("false"), "--procedureOptions must not enable write-only")
      }
      options
    } else {
      requireNonBlank(config.partitionIdleTime, "--partitionIdleTime")
      Map.empty[String, String]
    }
    val effectiveOptions = properties ++ dynamicOptions
    // 1.3.2's incremental unaware-bucket branch does not forward the partition predicate.
    // https://github.com/apache/paimon/blob/release-1.3.2/paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/CompactProcedure.java
    if (config.where.isDefined && enabled(effectiveOptions, "clustering.incremental")) {
      val bucket = effectiveOptions.getOrElse("bucket", "-1").trim.toInt
      require(
        bucket != -1,
        "--where is unsupported for unaware-bucket incremental clustering in Paimon 1.3.2")
    }
  }

  private def enabled(properties: Map[String, String], key: String): Boolean =
    properties.get(key).exists(_.trim.equalsIgnoreCase("true"))

  // Use the Procedure's Unicode whitespace semantics, preserving the original value.
  private def requireNonBlank(value: String, option: String): Unit = {
    require(
      !StringUtils.isNullOrWhitespaceOnly(value) && value.trim.nonEmpty,
      s"$option must not be empty or whitespace-only")
  }

  // Paimon's Spark parser substitutes variables before parsing CALL arguments. Escaping a
  // SQL literal cannot prevent substitution from changing the predicate or dynamic options.
  private def requireLiteralArguments(context: ProducerContext, values: Seq[String]): Unit = {
    val substitutionEnabled = context.spark == null ||
      context.spark.conf.get("spark.sql.variable.substitute", "true").toBoolean
    require(
      !substitutionEnabled || !values.exists(_.contains("${")),
      "compact requires spark.sql.variable.substitute=false when arguments contain ${...}")
  }

  private def requireSuccessfulResult(rows: Seq[Row]): Unit = {
    require(
      rows.size == 1 && rows.head.size == 1 && !rows.head.isNullAt(0),
      "Invalid compact result: expected one non-null boolean result")
    rows.head.get(0) match {
      case value: java.lang.Boolean if value.booleanValue() =>
      case value: java.lang.Boolean =>
        throw new IllegalStateException("Paimon compact returned false")
      case _ => throw new IllegalStateException("Invalid compact result: expected boolean")
    }
  }

  private def withRetry[A](maxAttempts: Int)(operation: => A): (Either[Throwable, A], Int) = {
    var attempt = 0
    var lastError: Throwable = null
    while (attempt < maxAttempts) {
      attempt += 1
      try {
        return (Right(operation), attempt)
      } catch {
        case NonFatal(error) => lastError = error
      }
    }
    (Left(lastError), attempt)
  }

  private def asCompactConfig(actionConfig: ActionConfig): PaimonCompactConfig =
    actionConfig match {
      case config: PaimonCompactConfig => config
      case other => throw new IllegalArgumentException(
          s"Action compact requires PaimonCompactConfig, but got ${Option(other).map(
              _.getClass.getName).getOrElse("null")}")
    }

  private def errorMessage(error: Throwable): String =
    s"${error.getClass.getSimpleName}: ${Option(error.getMessage).getOrElse("")}"
}

object PaimonCompactAction {
  private[paimon] def betweenPredicate(input: String): String = {
    val value = Option(input).getOrElse("").trim
    require(
      value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"),
      "--between must be a date in yyyy-MM-dd format")
    val end =
      try {
        LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE)
      } catch {
        case error: DateTimeParseException =>
          throw new IllegalArgumentException(
            "--between must be a valid calendar date in yyyy-MM-dd format",
            error)
      }
    val start = end.minusDays(1)
    require(
      end.getYear >= 1 && start.getYear >= 1,
      "--between and its previous day must have years from 0001 to 9999")
    s"preset_dt BETWEEN '${start.format(DateTimeFormatter.ISO_LOCAL_DATE)}' AND '${end.format(DateTimeFormatter.ISO_LOCAL_DATE)}'"
  }

  private[paimon] def buildCompactSql(
      catalogName: String,
      config: PaimonCompactConfig,
      table: TargetTable,
      effectiveVersion: String): String = {
    val arguments = Seq("table" -> table.raw) ++
      (if (effectiveVersion == "1.3") {
         Seq("compact_strategy" -> config.compactStrategy) ++
           Option(config.procedureOptions).filter(_.nonEmpty).map("options" -> _)
       } else {
         Seq("partition_idle_time" -> config.partitionIdleTime)
       }) ++ config.where.map("where" -> _)
    arguments.map { case (name, value) => s"  $name => ${procedureString(value)}" }
      .mkString(s"CALL ${quoteIdent(catalogName)}.sys.compact(\n", ",\n", "\n)")
  }

  // Spark concatenates adjacent quoted literals; doubling quotes loses embedded quotes.
  // https://spark.apache.org/docs/3.5.6/sql-ref-literals.html#string-literal
  private[paimon] def procedureString(value: String): String =
    "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
}
