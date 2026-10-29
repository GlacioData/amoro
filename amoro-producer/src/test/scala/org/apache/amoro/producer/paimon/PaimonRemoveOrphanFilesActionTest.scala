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
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core.{ActionStatus, CommonProducerConfig, ProducerCliParser, ProducerContext, RetryMode, TargetTable}
import org.apache.amoro.producer.spi.{ActionConfig, LakeFormatAdapter, ProducerAction}

class PaimonRemoveOrphanFilesActionTest {

  @Test def startupOverridesPreserveTheYarnSubmissionRetryPolicy(): Unit = {
    val action = new PaimonRemoveOrphanFilesAction()
    Seq(None, Some("2"), Some("1")).foreach { attempts =>
      val conf = new org.apache.spark.SparkConf(false).setMaster("yarn")
      attempts.foreach(value => conf.set("spark.yarn.maxAppAttempts", value))
      action.sparkConfOverrides(PaimonRemoveOrphanFilesConfig(2, "distributed"))
        .foreach { case (key, value) => conf.set(key, value) }
      assertEquals(attempts, conf.getOption("spark.yarn.maxAppAttempts"))
    }
  }

  @Test def acceptsTableConcurrencyOption(): Unit = {
    val config = parse(new PaimonRemoveOrphanFilesAction(), Array("--tableConcurrency", "2"))
    assertEquals(2, config.tableConcurrency)
    assertTrue(config.tableConcurrencyExplicit)
  }

  @Test def rejectsNonPositiveConcurrencyAndLocalModeBeforeCreatingSpark(): Unit = {
    val action = new PaimonRemoveOrphanFilesAction()
    Seq("0", "-1").foreach(value =>
      assertThrows(
        classOf[IllegalArgumentException],
        () =>
          action.validate(commonConfig(), parse(action, Array("--tableConcurrency", value)))))
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        action.validate(
          commonConfig(),
          parse(action, Array("--tableConcurrency", "2", "--mode", "local"))))
  }

  @Test def legacyAndLocalExecutionRetainOriginalStartupAndRetryBehavior(): Unit = {
    val action = new PaimonRemoveOrphanFilesAction()
    val local = PaimonRemoveOrphanFilesConfig(2, "local")
    assertTrue(action.sparkConfOverrides(local).isEmpty)
    assertTrue(action.batchExecutor(context(), local).isEmpty)
    assertEquals(RetryMode.FrameworkTableRetry, action.retryMode)
    if (!PaimonOrphanFilesRuntime.supported) {
      val config = PaimonRemoveOrphanFilesConfig(10, "distributed")
      assertTrue(action.sparkConfOverrides(config).isEmpty)
      assertTrue(action.batchExecutor(context(), config).isEmpty)
      assertThrows(
        classOf[org.apache.amoro.producer.spi.ProducerRuntimeValidationException],
        () =>
          action.validateRuntime(
            context(),
            config.copy(tableConcurrency = 2, tableConcurrencyExplicit = true)))
    } else {
      assertEquals(
        "FAIR",
        action.sparkConfOverrides(PaimonRemoveOrphanFilesConfig(10, "distributed"))(
          "spark.scheduler.mode"))
    }
  }

  @Test def strictResultsRejectEmptyMetricsWhileLegacyResultsRemainUnchanged(): Unit = {
    val action = new PaimonRemoveOrphanFilesAction(new FakeRunner(Seq.empty))
    val config = PaimonRemoveOrphanFilesConfig(2, "distributed")
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.executeOnce(context(), table, config, strict = true))
    assertEquals("0", action.execute(context(), table, config).metrics("deletedFileCount"))
  }

  @Test def originalLocalPathStillUsesFrameworkTableRetries(): Unit = {
    var calls = 0
    val runner = new PaimonSqlRunner {
      override def run(spark: SparkSession, sql: String): Seq[Row] = {
        calls += 1
        if (calls < 3) throw new java.io.IOException("retry legacy call")
        Seq(Row(1L, 10L))
      }
    }
    val action = new PaimonRemoveOrphanFilesAction(runner)
    val result = org.apache.amoro.producer.core.ProducerApp.executeOneTableWithRetry(
      context(),
      action,
      PaimonRemoveOrphanFilesConfig(2, "local"),
      table)
    assertEquals(3, calls)
    assertEquals(ActionStatus.Success, result.status)
  }

  @Test
  def parseUsesDefaultsAndTrimsMode(): Unit = {
    val action = new PaimonRemoveOrphanFilesAction()

    assertEquals("remove-orphan-files", action.name)
    assertEquals(RetryMode.FrameworkTableRetry, action.retryMode)
    assertEquals(PaimonRemoveOrphanFilesConfig(10, "distributed"), parse(action))
    assertEquals(
      PaimonRemoveOrphanFilesConfig(2, "local"),
      parse(action, Array("--parallelism", " 2 ", "--mode", " local ")))
  }

  @Test
  def validateRejectsInvalidArguments(): Unit = {
    val action = new PaimonRemoveOrphanFilesAction()
    val common = commonConfig()

    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonRemoveOrphanFilesConfig(0, "distributed")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonRemoveOrphanFilesConfig(-1, "distributed")))
    assertThrows(
      classOf[IllegalArgumentException],
      () => action.validate(common, PaimonRemoveOrphanFilesConfig(10, " ")))
  }

  @Test
  def validateRejectsWrongActionConfigType(): Unit = {
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () => new PaimonRemoveOrphanFilesAction().validate(commonConfig(), WrongConfig))

    assertTrue(exception.getMessage.contains("remove-orphan-files"))
    assertTrue(exception.getMessage.contains("PaimonRemoveOrphanFilesConfig"))
  }

  @Test
  def buildSqlIncludesCatalogTableParallelismAndEscapedMode(): Unit = {
    val sql =
      PaimonRemoveOrphanFilesAction.buildRemoveOrphanFilesSql(
        catalogName = "pa`imon",
        config = PaimonRemoveOrphanFilesConfig(10, "dis'tributed"),
        table = table)

    assertTrue(sql.contains("CALL `pa``imon`.sys.remove_orphan_files("))
    assertTrue(sql.contains("table => 'db.t1'"))
    assertTrue(sql.contains("parallelism => 10"))
    assertTrue(sql.contains("mode => 'dis''tributed'"))
  }

  @Test
  def buildSqlUsesDefaultParallelismAndMode(): Unit = {
    val sql =
      PaimonRemoveOrphanFilesAction.buildRemoveOrphanFilesSql(
        catalogName = "paimon",
        config = PaimonRemoveOrphanFilesConfig(10, "distributed"),
        table = table)

    assertTrue(sql.contains("CALL `paimon`.sys.remove_orphan_files("))
    assertTrue(sql.contains("table => 'db.t1'"))
    assertTrue(sql.contains("parallelism => 10"))
    assertTrue(sql.contains("mode => 'distributed'"))
  }

  @Test
  def executeAggregatesMetricsFromAllReturnedRows(): Unit = {
    val config = PaimonRemoveOrphanFilesConfig(5, "local")
    val runner =
      new FakeRunner(
        Seq(
          new GenericRowWithSchema(
            Array[Any](1L, 10L),
            StructType(
              Seq(
                StructField("deleted_file_count", LongType),
                StructField("deleted_file_total_len_in_bytes", LongType)))),
          Row("2", "20")))
    val result =
      new PaimonRemoveOrphanFilesAction(runner)
        .execute(context(), table, config)

    assertEquals(1, runner.sqls.size)
    assertTrue(runner.sqls.head.contains("CALL `paimon`.sys.remove_orphan_files("))
    assertTrue(runner.sqls.head.contains("table => 'db.t1'"))
    assertTrue(runner.sqls.head.contains("parallelism => 5"))
    assertTrue(runner.sqls.head.contains("mode => 'local'"))
    assertEquals(ActionStatus.Success, result.status)
    assertEquals("3", result.metrics("deletedFileCount"))
    assertEquals("30", result.metrics("deletedFileTotalLenInBytes"))
    assertEquals(Seq(ActionStatus.Success), result.tasks.map(_.status))
    assertEquals(Seq("remove_orphan_files"), result.tasks.map(_.name))
    assertTrue(result.costMs >= 0.0)
  }

  private def parse(
      action: PaimonRemoveOrphanFilesAction,
      args: Array[String] = Array.empty): PaimonRemoveOrphanFilesConfig = {
    val commandLine = ProducerCliParser.parseMerged(action.options, args)
    action.parse(commandLine).asInstanceOf[PaimonRemoveOrphanFilesConfig]
  }

  private def context(): ProducerContext = {
    ProducerContext(null.asInstanceOf[SparkSession], commonConfig(), DummyAdapter)
  }

  private def commonConfig(): CommonProducerConfig = {
    CommonProducerConfig(
      format = "paimon",
      action = "remove-orphan-files",
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
