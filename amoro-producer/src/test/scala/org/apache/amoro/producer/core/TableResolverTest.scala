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

import java.util.regex.PatternSyntaxException

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.spi.{LakeFormatAdapter, ProducerAction}

class TableResolverTest {

  @Test
  def resolveDatabaseOnlyListsTables(): Unit = {
    val adapter = new FakeAdapter(Seq(TargetTable("db.orders", "db", "orders")))

    val targets = TableResolver.resolveTargets(null, adapter, common(databaseName = Some("db")))

    assertEquals(Seq(TargetTable("db.orders", "db", "orders")), targets)
    assertEquals(Seq("db"), adapter.listedDatabases)
  }

  @Test
  def resolveDatabaseWithSimpleTableReturnsSingleTarget(): Unit = {
    val adapter = new FakeAdapter(Seq.empty)

    val targets =
      TableResolver.resolveTargets(
        null,
        adapter,
        common(databaseName = Some("db"), tableName = Some("orders")))

    assertEquals(Seq(TargetTable("db.orders", "db", "orders")), targets)
    assertEquals(Seq.empty, adapter.listedDatabases)
  }

  @Test
  def resolveQualifiedTableOnlyReturnsSingleTarget(): Unit = {
    val adapter = new FakeAdapter(Seq.empty)

    val targets =
      TableResolver.resolveTargets(null, adapter, common(tableName = Some("db.orders")))

    assertEquals(Seq(TargetTable("db.orders", "db", "orders")), targets)
    assertEquals(Seq.empty, adapter.listedDatabases)
  }

  @Test
  def resolveRejectsSimpleTableOnly(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        TableResolver.resolveTargets(
          null,
          new FakeAdapter(Seq.empty),
          common(tableName = Some("orders"))))
  }

  @Test
  def resolveRejectsDatabaseWithQualifiedTable(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        TableResolver.resolveTargets(
          null,
          new FakeAdapter(Seq.empty),
          common(databaseName = Some("db"), tableName = Some("db.orders"))))
  }

  @Test
  def resolveRejectsRegexWithTableName(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        TableResolver.resolveTargets(
          null,
          new FakeAdapter(Seq.empty),
          common(
            databaseName = Some("db"),
            tableName = Some("orders"),
            tableNameRegex = Some("ord"))))
  }

  @Test
  def resolveFiltersDatabaseOnlyWithRegex(): Unit = {
    val adapter =
      new FakeAdapter(
        Seq(
          TargetTable("db.orders", "db", "orders"),
          TargetTable("db.customers", "db", "customers"),
          TargetTable("db.order_items", "db", "order_items")))

    val targets =
      TableResolver.resolveTargets(
        null,
        adapter,
        common(databaseName = Some("db"), tableNameRegex = Some("order")))

    assertEquals(
      Seq(
        TargetTable("db.orders", "db", "orders"),
        TargetTable("db.order_items", "db", "order_items")),
      targets)
    assertEquals(Seq("db"), adapter.listedDatabases)
  }

  @Test
  def resolveRejectsEmptyRegex(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        TableResolver.resolveTargets(
          null,
          new FakeAdapter(Seq.empty),
          common(databaseName = Some("db"), tableNameRegex = Some("  "))))
  }

  @Test
  def resolveRejectsInvalidRegexBeforeListingTables(): Unit = {
    val exception =
      assertThrows(
        classOf[IllegalArgumentException],
        () =>
          TableResolver.resolveTargets(
            null,
            new FailingListAdapter,
            common(databaseName = Some("db"), tableNameRegex = Some("["))))

    assertEquals(classOf[PatternSyntaxException], exception.getCause.getClass)
  }

  @Test
  def resolveRejectsMissingDatabaseAndTable(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => TableResolver.resolveTargets(null, new FakeAdapter(Seq.empty), common()))
  }

  private def common(
      databaseName: Option[String] = None,
      tableName: Option[String] = None,
      tableNameRegex: Option[String] = None): CommonProducerConfig = {
    CommonProducerConfig(
      format = "paimon",
      action = "compact",
      catalogName = "spark_catalog",
      databaseName = databaseName,
      tableName = tableName,
      tableNameRegex = tableNameRegex,
      retryTimes = 3,
      continueOnTableFailure = true)
  }

  final private class FakeAdapter(tables: Seq[TargetTable]) extends LakeFormatAdapter {
    var listedDatabases: Seq[String] = Seq.empty

    override def format: String = "paimon"

    override def actions: Seq[ProducerAction] = Seq.empty

    override def listTables(
        spark: SparkSession,
        config: CommonProducerConfig,
        database: String): Seq[TargetTable] = {
      listedDatabases = listedDatabases :+ database
      tables
    }
  }

  final private class FailingListAdapter extends LakeFormatAdapter {
    override def format: String = "paimon"

    override def actions: Seq[ProducerAction] = Seq.empty

    override def listTables(
        spark: SparkSession,
        config: CommonProducerConfig,
        database: String): Seq[TargetTable] = {
      throw new AssertionError("listTables must not be called before regex validation")
    }
  }
}
