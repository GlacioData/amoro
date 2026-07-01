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

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class ProducerCliParserTest {

  @Test
  def peekReadsFormatAndActionWithoutBeingDisturbedByActionOptions(): Unit = {
    val peek =
      ProducerCliParser.peek(
        Array(
          "--startBucket",
          "10",
          "--format",
          "paimon",
          "--endBucket",
          "20",
          "--action",
          "compact"))

    assertEquals(Some("paimon"), peek.format)
    assertEquals(Some("compact"), peek.action)
    assertEquals(false, peek.help)
  }

  @Test
  def peekTrimsFormatAndActionValues(): Unit = {
    val peek =
      ProducerCliParser.peek(Array("--format", "  paimon  ", "--action", " compact "))

    assertEquals(Some("paimon"), peek.format)
    assertEquals(Some("compact"), peek.action)
  }

  @Test
  def peekAcceptsLongOptionsWithEquals(): Unit = {
    val peek = ProducerCliParser.peek(Array("--format=paimon", "--action=compact"))

    assertEquals(Some("paimon"), peek.format)
    assertEquals(Some("compact"), peek.action)
  }

  @Test
  def peekFiltersBlankFormatAndActionValues(): Unit = {
    val peek = ProducerCliParser.peek(Array("--format", "   ", "--action", ""))

    assertEquals(None, peek.format)
    assertEquals(None, peek.action)
  }

  @Test
  def parseCommonUsesDefaults(): Unit = {
    val commandLine =
      ProducerCliParser.parseMerged(
        Seq.empty,
        Array("--format", "paimon", "--action", "compact"))

    val config = ProducerCliParser.parseCommon(commandLine)

    assertEquals("paimon", config.format)
    assertEquals("compact", config.action)
    assertEquals("spark_catalog", config.catalogName)
    assertEquals(None, config.databaseName)
    assertEquals(None, config.tableName)
    assertEquals(None, config.tableNameRegex)
    assertEquals(3, config.retryTimes)
    assertEquals(true, config.continueOnTableFailure)
  }

  @Test
  def parseCommonTrimsStringValues(): Unit = {
    val commandLine =
      ProducerCliParser.parseMerged(
        Seq.empty,
        Array(
          "--format",
          " paimon ",
          "--action",
          " compact ",
          "--catalogName",
          " paimon ",
          "--databaseName",
          " db ",
          "--tableName",
          " orders ",
          "--tableNameRegex",
          " order "))

    val config = ProducerCliParser.parseCommon(commandLine)

    assertEquals("paimon", config.format)
    assertEquals("compact", config.action)
    assertEquals("paimon", config.catalogName)
    assertEquals(Some("db"), config.databaseName)
    assertEquals(Some("orders"), config.tableName)
    assertEquals(Some("order"), config.tableNameRegex)
  }

  @Test
  def parseCommonAcceptsTrimmedBooleanValue(): Unit = {
    val commandLine =
      ProducerCliParser.parseMerged(
        Seq.empty,
        Array(
          "--format",
          "paimon",
          "--action",
          "compact",
          "--continueOnTableFailure",
          " TRUE "))

    val config = ProducerCliParser.parseCommon(commandLine)

    assertEquals(true, config.continueOnTableFailure)
  }

  @Test
  def parseCommonRejectsZeroRetryTimes(): Unit = {
    val commandLine =
      ProducerCliParser.parseMerged(
        Seq.empty,
        Array("--format", "paimon", "--action", "compact", "--retryTimes", "0"))

    assertThrows(
      classOf[IllegalArgumentException],
      () => ProducerCliParser.parseCommon(commandLine))
  }

  @Test
  def parseCommonRejectsIllegalBooleanValue(): Unit = {
    val commandLine =
      ProducerCliParser.parseMerged(
        Seq.empty,
        Array(
          "--format",
          "paimon",
          "--action",
          "compact",
          "--continueOnTableFailure",
          "maybe"))

    assertThrows(
      classOf[IllegalArgumentException],
      () => ProducerCliParser.parseCommon(commandLine))
  }

  @Test
  def parseMergedAcceptsRegisteredActionOption(): Unit = {
    val actionOptions = Seq(ActionOption("startBucket", description = "start bucket"))

    val commandLine =
      ProducerCliParser.parseMerged(
        actionOptions,
        Array("--format", "paimon", "--action", "compact", "--startBucket", "10"))

    assertEquals("10", commandLine.getOptionValue("startBucket"))
  }

  @Test
  def parseMergedAcceptsLongOptionsWithEquals(): Unit = {
    val actionOptions = Seq(ActionOption("startBucket", description = "start bucket"))

    val commandLine =
      ProducerCliParser.parseMerged(
        actionOptions,
        Array("--format=paimon", "--action=compact", "--startBucket=-1"))

    assertEquals("paimon", commandLine.getOptionValue("format"))
    assertEquals("compact", commandLine.getOptionValue("action"))
    assertEquals("-1", commandLine.getOptionValue("startBucket"))
  }

  @Test
  def parseMergedAcceptsValueStartingWithDashWhenItIsNotARegisteredOption(): Unit = {
    val actionOptions = Seq(ActionOption("startBucket", description = "start bucket"))

    val commandLine =
      ProducerCliParser.parseMerged(
        actionOptions,
        Array("--format", "paimon", "--action", "compact", "--startBucket", "-1"))

    assertEquals("-1", commandLine.getOptionValue("startBucket"))
  }

  @Test
  def mergeOptionsRejectsActionOptionWithCommonLongOpt(): Unit = {
    val actionOptions = Seq(ActionOption("format", description = "bad"))

    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () => ProducerCliParser.mergeOptions(actionOptions))

    assertTrue(exception.getMessage.contains("format"))
  }

  @Test
  def mergeOptionsRejectsActionOptionWithCommonShortOpt(): Unit = {
    val actionOptions = Seq(ActionOption("customHelp", hasValue = false, shortName = Some("h")))

    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () => ProducerCliParser.mergeOptions(actionOptions))

    assertTrue(exception.getMessage.contains("h"))
  }

  @Test
  def parseMergedRejectsUnregisteredOption(): Unit = {
    assertThrows(
      classOf[ProducerCliParseException],
      () =>
        ProducerCliParser.parseMerged(
          Seq.empty,
          Array("--format", "paimon", "--action", "compact", "--badOption", "value")))
  }

  @Test
  def peekRecognizesHelp(): Unit = {
    val peek = ProducerCliParser.peek(Array("--help"))

    assertEquals(None, peek.format)
    assertEquals(None, peek.action)
    assertEquals(true, peek.help)
  }
}
