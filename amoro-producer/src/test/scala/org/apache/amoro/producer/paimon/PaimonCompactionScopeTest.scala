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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class PaimonCompactionScopeTest {
  private val parser = new SparkSqlParser

  private def resolve(
      where: Option[String],
      partitions: Seq[String],
      keys: Seq[String] = Seq.empty): PaimonCompactionScope = {
    val schema = new StructType().add("id", "int").add("preset_dt", "string")
      .add("preset_pt", "string").add("preset_hr", "string")
    PaimonCompactionScope.resolve(
      null,
      PaimonTableMetadata(keys, partitions, schema, Map.empty),
      where)
  }

  @Test
  def usesRealKeysAndPartitionsInsteadOfColumnNames(): Unit = {
    val where = Some("preset_dt = '2026-10-08'")
    assertEquals("retained", resolve(where, Seq("preset_dt")).decision)
    assertEquals("removed-primary-key", resolve(where, Seq("preset_dt"), Seq("id")).decision)
    assertEquals("removed-primary-key", resolve(where, Seq.empty, Seq("id")).decision)
    // A regular schema column is not a partition column.
    assertEquals("removed-partition-mismatch", resolve(where, Seq.empty).decision)
    assertEquals("removed-partition-mismatch", resolve(where, Seq("preset_hr")).decision)
    assertEquals("not-provided", resolve(None, Seq("preset_pt"), Seq("id")).decision)
  }

  @Test
  def preservesMatchingPredicatesExactlyAndDropsTheWholeMismatchingExpression(): Unit = {
    val original = "  preset_dt >= '2026-10-01' AND preset_hr < '09'  "
    assertEquals(
      Some(original),
      resolve(Some(original), Seq("preset_dt", "preset_hr")).effectiveWhere)
    assertEquals(
      Some("preset_hr = '08'"),
      resolve(Some("preset_hr = '08'"), Seq("preset_hr")).effectiveWhere)
    assertEquals(None, resolve(Some(original), Seq("preset_dt")).effectiveWhere)
    assertEquals(None, resolve(Some(original), Seq("preset_pt")).effectiveWhere)
    assertEquals(None, resolve(Some("preset_dt = 'x' OR id = 1"), Seq("preset_pt")).effectiveWhere)
  }

  @Test
  def rewritesOnlyAttributeNodesAndRoundTripsLiteralValues(): Unit = {
    val predicate = "(`preset_dt` = 'preset_dt' OR preset_dt >= '2026-10-08') AND preset_hr = '08'"
    val scope = resolve(Some(predicate), Seq("preset_pt", "preset_hr"))
    assertEquals("replaced-preset-dt", scope.decision)
    val before = parser.parseExpression(predicate)
    val after = parser.parseExpression(scope.effectiveWhere.get)
    assertEquals(
      before.collect { case literal: Literal => literal },
      after.collect { case literal: Literal => literal })
    assertEquals(
      Seq("preset_pt", "preset_pt", "preset_hr"),
      after.collect { case attribute: UnresolvedAttribute => attribute.name })
    assertEquals(
      before.transformUp {
        case attribute: UnresolvedAttribute if attribute.name == "preset_dt" =>
          UnresolvedAttribute(Seq("preset_pt"))
      },
      after)
    Seq("中文 preset_dt", "quote' slash\\ path", "line\nvalue").foreach { value =>
      val input = "preset_dt = " + PaimonCompactAction.procedureString(value)
      val rewritten = resolve(Some(input), Seq("preset_pt")).effectiveWhere.get
      assertEquals(
        parser.parseExpression(input).collect { case literal: Literal => literal },
        parser.parseExpression(rewritten).collect { case literal: Literal => literal })
    }
  }

  @Test
  def followsSparkCaseSensitivity(): Unit = {
    Seq(false, true).foreach { sensitive =>
      val conf = new SQLConf
      conf.setConfString("spark.sql.caseSensitive", sensitive.toString)
      SQLConf.withExistingConf(conf) {
        val result = resolve(Some("PRESET_DT = 'x'"), Seq("preset_pt"))
        assertEquals(
          if (sensitive) "removed-partition-mismatch" else "replaced-preset-dt",
          result.decision)
      }
    }
  }

  @Test
  def rejectsMalformedAndOpaqueReferencesEvenForPrimaryKeyTables(): Unit = {
    for {
      keys <- Seq(Seq.empty[String], Seq("id"))
      predicate <- Seq(
        "preset_dt =",
        "preset_dt IN (SELECT id FROM t)",
        "db.preset_dt = 'x'",
        "exists(array(1), x -> x = 1)",
        "preset_dt = *")
    } {
      assertThrows(
        classOf[Exception],
        () => { resolve(Some(predicate), Seq("preset_pt"), keys); () },
        predicate)
    }
  }
}
