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

final case class CliPeek(format: Option[String], action: Option[String], help: Boolean)

object ProducerCliParser {

  private val DefaultCatalogName = "spark_catalog"
  private val DefaultRetryTimes = 3
  private val DefaultContinueOnTableFailure = true

  def peek(args: Array[String]): CliPeek = {
    var format: Option[String] = None
    var action: Option[String] = None
    var help = false
    var index = 0

    while (index < args.length) {
      args(index) match {
        case longOption if longOption.startsWith("--format=") =>
          format = Some(longOption.substring("--format=".length))
        case "--format" if index + 1 < args.length =>
          format = Some(args(index + 1))
          index += 1
        case longOption if longOption.startsWith("--action=") =>
          action = Some(longOption.substring("--action=".length))
        case "--action" if index + 1 < args.length =>
          action = Some(args(index + 1))
          index += 1
        case "--help" | "-h" =>
          help = true
        case _ =>
      }
      index += 1
    }

    CliPeek(format.map(_.trim).filter(_.nonEmpty), action.map(_.trim).filter(_.nonEmpty), help)
  }

  def commonOptions: Seq[ActionOption] = {
    Seq(
      ActionOption("format", description = "Lakehouse table format"),
      ActionOption("action", description = "Producer action name"),
      ActionOption("catalogName", description = "Spark catalog name"),
      ActionOption("databaseName", description = "Target database name"),
      ActionOption("tableName", description = "Target table name"),
      ActionOption("tableNameRegex", description = "Target table name regex"),
      ActionOption("retryTimes", description = "Retry times"),
      ActionOption("continueOnTableFailure", description = "Continue when one table fails"),
      ActionOption("help", hasValue = false, description = "Print help", shortName = Some("h")))
  }

  def mergeOptions(actionOptions: Seq[ActionOption]): Seq[ActionOption] = {
    val common = commonOptions
    val commonLongOptions = common.map(_.name).toSet
    val commonShortOptions = common.flatMap(_.shortName).toSet
    actionOptions.foreach { option =>
      Option(option.name)
        .filter(commonLongOptions.contains)
        .foreach { longOpt =>
          throw new IllegalArgumentException(s"Duplicate CLI long option: --$longOpt")
        }
      option.shortName
        .filter(commonShortOptions.contains)
        .foreach { shortOpt =>
          throw new IllegalArgumentException(s"Duplicate CLI short option: -$shortOpt")
        }
    }
    common ++ actionOptions
  }

  def parseMerged(actionOptions: Seq[ActionOption], args: Array[String]): ProducerParsedOptions = {
    val options = mergeOptions(actionOptions)
    val byLongName = options.map(option => option.name -> option).toMap
    val byShortName = options.flatMap(option => option.shortName.map(_ -> option)).toMap
    def isRegisteredOptionToken(token: String): Boolean = {
      if (token.startsWith("--")) {
        byLongName.contains(token.stripPrefix("--"))
      } else if (token.startsWith("-") && token.length > 1) {
        byShortName.contains(token.stripPrefix("-"))
      } else {
        false
      }
    }
    var values = Map.empty[String, String]
    var flags = Set.empty[String]
    var index = 0

    while (index < args.length) {
      val token = args(index)
      val longOptionWithValue =
        if (token.startsWith("--")) {
          val equalsIndex = token.indexOf('=')
          if (equalsIndex > 2) {
            Some(token.substring(2, equalsIndex) -> token.substring(equalsIndex + 1))
          } else {
            None
          }
        } else {
          None
        }
      val option =
        if (longOptionWithValue.nonEmpty) {
          byLongName.getOrElse(
            longOptionWithValue.get._1,
            throw new ProducerCliParseException(
              s"Unrecognized option: --${longOptionWithValue.get._1}"))
        } else if (token.startsWith("--")) {
          byLongName.getOrElse(
            token.stripPrefix("--"),
            throw new ProducerCliParseException(s"Unrecognized option: $token"))
        } else if (token.startsWith("-") && token.length > 1) {
          byShortName.getOrElse(
            token.stripPrefix("-"),
            throw new ProducerCliParseException(s"Unrecognized option: $token"))
        } else {
          throw new ProducerCliParseException(s"Unexpected argument: $token")
        }

      if (option.hasValue) {
        val value =
          longOptionWithValue.map(_._2).getOrElse {
            if (index + 1 >= args.length) {
              throw new ProducerCliParseException(s"Missing argument for option: --${option.name}")
            }
            val nextToken = args(index + 1)
            if (isRegisteredOptionToken(nextToken)) {
              throw new ProducerCliParseException(s"Missing argument for option: --${option.name}")
            }
            index += 1
            nextToken
          }
        values += option.name -> value
      } else {
        if (longOptionWithValue.nonEmpty) {
          throw new ProducerCliParseException(s"Option does not accept argument: --${option.name}")
        }
        flags += option.name
      }
      index += 1
    }

    ProducerParsedOptions(values, flags)
  }

  def parseCommon(commandLine: ProducerParsedOptions): CommonProducerConfig = {
    val config =
      CommonProducerConfig(
        format = stringValue(commandLine.getOptionValue("format")),
        action = stringValue(commandLine.getOptionValue("action")),
        catalogName = stringValue(commandLine.getOptionValue("catalogName", DefaultCatalogName)),
        databaseName = optionValue(commandLine, "databaseName"),
        tableName = optionValue(commandLine, "tableName"),
        tableNameRegex = optionValue(commandLine, "tableNameRegex"),
        retryTimes =
          parseInt(
            commandLine.getOptionValue("retryTimes", DefaultRetryTimes.toString),
            "--retryTimes"),
        continueOnTableFailure =
          parseBoolean(
            commandLine.getOptionValue(
              "continueOnTableFailure",
              DefaultContinueOnTableFailure.toString),
            "--continueOnTableFailure"))

    validateCommon(config)
    config
  }

  def validateCommon(config: CommonProducerConfig): Unit = {
    requireNonEmpty(config.format, "--format")
    requireNonEmpty(config.action, "--action")
    requireNonEmpty(config.catalogName, "--catalogName")
    if (config.retryTimes <= 0) {
      throw new IllegalArgumentException("--retryTimes 必须大于 0")
    }
  }

  def renderUsage(options: Seq[ActionOption], availableFormats: Seq[String]): String = {
    val lines =
      Seq("usage: LakehouseProducer") ++
        mergeOptions(options).map { option =>
          val value = if (option.hasValue) " <arg>" else ""
          val names =
            option.shortName
              .map(shortName => s"-$shortName, --${option.name}$value")
              .getOrElse(s"    --${option.name}$value")
          f"  $names%-40s ${option.description}"
        } ++
        Seq(s"Available formats: ${availableFormats.mkString(", ")}")
    lines.mkString(System.lineSeparator())
  }

  private def optionValue(commandLine: ProducerParsedOptions, name: String): Option[String] = {
    Option(commandLine.getOptionValue(name)).map(_.trim).filter(_.nonEmpty)
  }

  private def stringValue(value: String): String = {
    if (value == null) null else value.trim
  }

  private def requireNonEmpty(value: String, optionName: String): Unit = {
    if (value == null || value.trim.isEmpty) {
      throw new IllegalArgumentException(s"$optionName 不能为空")
    }
  }

  private def parseInt(value: String, optionName: String): Int = {
    try {
      value.toInt
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$optionName 必须是整数: $value")
    }
  }

  private def parseBoolean(value: String, optionName: String): Boolean = {
    value.trim.toLowerCase(Locale.ROOT) match {
      case "true" => true
      case "false" => false
      case _ => throw new IllegalArgumentException(s"$optionName 必须是 true/false: $value")
    }
  }
}
