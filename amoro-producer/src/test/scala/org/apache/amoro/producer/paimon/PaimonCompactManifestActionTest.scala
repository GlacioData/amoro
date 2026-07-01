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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core.{ActionStatus, CommonProducerConfig, ProducerCliParser, ProducerContext, RetryMode, TargetTable}
import org.apache.amoro.producer.spi.{ActionConfig, LakeFormatAdapter, ProducerAction}

class PaimonCompactManifestActionTest {

  @Test
  def parseUsesDefaultsAndTrimsProcedureOptions(): Unit = {
    val action = new PaimonCompactManifestAction()

    assertEquals("compact-manifest", action.name)
    assertEquals(RetryMode.FrameworkTableRetry, action.retryMode)
    assertEquals("", parse(action).procedureOptions)
    assertEquals(
      "k=''v''",
      parse(action, Array("--procedureOptions", " k=''v'' ")).procedureOptions)
  }

  @Test
  def buildSqlIncludesEscapedOptionsWhenProcedureOptionsIsNonEmpty(): Unit = {
    val sql =
      PaimonCompactManifestAction.buildCompactManifestSql(
        catalogName = "pa`imon",
        config = PaimonCompactManifestConfig("k='v'"),
        table = table)

    assertTrue(sql.contains("CALL `pa``imon`.sys.compact_manifest("))
    assertTrue(sql.contains("table => 'db.t1'"))
    assertTrue(sql.contains("options => 'k=''v'''"))
  }

  @Test
  def buildSqlOmitsOptionsWhenProcedureOptionsIsEmpty(): Unit = {
    val sql =
      PaimonCompactManifestAction.buildCompactManifestSql(
        catalogName = "paimon",
        config = PaimonCompactManifestConfig(""),
        table = table)

    assertTrue(sql.contains("CALL `paimon`.sys.compact_manifest("))
    assertTrue(sql.contains("table => 'db.t1'"))
    assertFalse(sql.contains("options =>"))
  }

  @Test
  def validateRejectsWrongActionConfigType(): Unit = {
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () => new PaimonCompactManifestAction().validate(commonConfig(), WrongConfig))

    assertTrue(exception.getMessage.contains("compact-manifest"))
    assertTrue(exception.getMessage.contains("PaimonCompactManifestConfig"))
  }

  @Test
  def executeUsesRunnerOnceAndParsesBooleanResultField(): Unit = {
    val config = PaimonCompactManifestConfig("k='v'")
    val runner =
      new FakeRunner(
        Seq(
          new GenericRowWithSchema(
            Array[Any](java.lang.Boolean.TRUE),
            StructType(Seq(StructField("result", BooleanType))))))
    val result =
      new PaimonCompactManifestAction(runner)
        .execute(context(), table, config)

    assertEquals(1, runner.sqls.size)
    assertTrue(runner.sqls.head.contains("CALL `paimon`.sys.compact_manifest("))
    assertTrue(runner.sqls.head.contains("table => 'db.t1'"))
    assertTrue(runner.sqls.head.contains("options => 'k=''v'''"))
    assertEquals(ActionStatus.Success, result.status)
    assertEquals("true", result.metrics("result"))
    assertEquals(Seq(ActionStatus.Success), result.tasks.map(_.status))
    assertEquals(Seq("compact_manifest"), result.tasks.map(_.name))
    assertTrue(result.costMs >= 0.0)
  }

  @Test
  def executeParsesStringAndFirstColumnBooleanResults(): Unit = {
    val stringRunner = new FakeRunner(Seq(Row("1")))
    val falseRunner = new FakeRunner(Seq(Row(false)))

    val stringResult =
      new PaimonCompactManifestAction(stringRunner)
        .execute(context(), table, PaimonCompactManifestConfig(""))
    val falseResult =
      new PaimonCompactManifestAction(falseRunner)
        .execute(context(), table, PaimonCompactManifestConfig(""))

    assertEquals("true", stringResult.metrics("result"))
    assertEquals("false", falseResult.metrics("result"))
  }

  private def parse(
      action: PaimonCompactManifestAction,
      args: Array[String] = Array.empty): PaimonCompactManifestConfig = {
    val commandLine = ProducerCliParser.parseMerged(action.options, args)
    action.parse(commandLine).asInstanceOf[PaimonCompactManifestConfig]
  }

  private def context(): ProducerContext = {
    ProducerContext(null.asInstanceOf[SparkSession], commonConfig(), DummyAdapter)
  }

  private def commonConfig(): CommonProducerConfig = {
    CommonProducerConfig(
      format = "paimon",
      action = "compact-manifest",
      catalogName = "paimon",
      databaseName = Some("db"),
      tableName = Some("t1"),
      tableNameRegex = None,
      retryTimes = 3,
      continueOnTableFailure = true)
  }

  private val table = TargetTable("db.t1", "db", "t1")

  private object WrongConfig extends ActionConfig

  private object DummyAdapter extends LakeFormatAdapter {
    override def format: String = "paimon"

    override def actions: Seq[ProducerAction] = Seq.empty

    override def listTables(
        spark: SparkSession,
        config: CommonProducerConfig,
        database: String): Seq[TargetTable] = Seq.empty
  }

  private class FakeRunner(rows: Seq[Row]) extends PaimonSqlRunner {
    var sqls: Seq[String] = Seq.empty

    override def run(spark: SparkSession, sqlText: String): Seq[Row] = {
      sqls :+= sqlText
      rows
    }
  }
}
