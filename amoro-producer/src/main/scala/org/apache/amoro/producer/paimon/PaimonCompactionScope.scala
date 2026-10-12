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

import scala.collection.JavaConverters._

import org.apache.paimon.utils.JsonSerdeUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{Star, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, SubqueryExpression}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

final case class PaimonCompactionScope(
    primaryKeys: Seq[String],
    partitionKeys: Seq[String],
    requestedWhere: Option[String],
    effectiveWhere: Option[String],
    decision: String,
    reason: String) {

  def removesWhere: Boolean = requestedWhere.isDefined && effectiveWhere.isEmpty

  private[paimon] def fields: Map[String, AnyRef] = Map(
    "primaryKeys" -> primaryKeys.asJava,
    "partitionKeys" -> partitionKeys.asJava,
    "requestedWhere" -> requestedWhere.orNull,
    "effectiveWhere" -> effectiveWhere.orNull,
    "whereDecision" -> decision,
    "whereReason" -> reason)

  private[paimon] def metrics: Map[String, String] = Map(
    "primaryKeys" -> JsonSerdeUtil.toFlatJson(primaryKeys.asJava),
    "partitionKeys" -> JsonSerdeUtil.toFlatJson(partitionKeys.asJava),
    "requestedWhere" -> requestedWhere.getOrElse(""),
    "effectiveWhere" -> effectiveWhere.getOrElse(""),
    "whereDecision" -> decision,
    "whereReason" -> reason)

  private[paimon] def redacted(secrets: Seq[String]): PaimonCompactionScope = copy(
    requestedWhere = requestedWhere.map(PaimonCompactionCompletion.redactError(_, secrets)),
    effectiveWhere = effectiveWhere.map(PaimonCompactionCompletion.redactError(_, secrets)))
}

private[paimon] object PaimonCompactionScope {
  def resolve(
      spark: SparkSession,
      metadata: PaimonTableMetadata,
      requested: Option[String]): PaimonCompactionScope = {
    def outcome(where: Option[String], decision: String, reason: String) =
      PaimonCompactionScope(
        metadata.primaryKeys,
        metadata.partitionKeys,
        requested,
        where,
        decision,
        reason)

    if (requested.isEmpty) {
      return outcome(None, "not-provided", "No where predicate was provided")
    }
    val conf = Option(spark).map(_.sessionState.conf).getOrElse(SQLConf.get)
    SQLConf.withExistingConf(conf) {
      val parser = Option(spark).map(_.sessionState.sqlParser).getOrElse(new SparkSqlParser)
      // Parse even for primary-key tables: malformed input must never broaden execution.
      val expression = parser.parseExpression(requested.get)
      require(
        expression.find {
          case _: SubqueryExpression | _: LambdaFunction | _: Star => true
          case attribute: UnresolvedAttribute => attribute.nameParts.size != 1
          case _ => false
        }.isEmpty,
        "--where contains column references that cannot be safely adapted")
      val resolver = conf.resolver
      def columns(expr: Expression): Seq[String] = expr.collect {
        case attribute: UnresolvedAttribute => attribute.nameParts.head
      }
      def isPartition(name: String): Boolean = metadata.partitionKeys.exists(resolver(_, name))
      def fits(expr: Expression): Boolean =
        metadata.partitionKeys.nonEmpty && columns(expr).forall(isPartition)

      if (metadata.primaryKeys.nonEmpty) {
        outcome(
          None,
          "removed-primary-key",
          "Primary-key tables compact without the requested where")
      } else if (fits(expression)) {
        outcome(requested, "retained", "All referenced columns are partition columns")
      } else {
        val presetPt = metadata.partitionKeys.find(resolver(_, "preset_pt"))
        val rewritten = expression.transformUp {
          case attribute: UnresolvedAttribute
              if presetPt.isDefined && resolver(attribute.nameParts.head, "preset_dt") &&
                !isPartition(attribute.nameParts.head) =>
            UnresolvedAttribute(Seq(presetPt.get))
        }
        if (rewritten != expression && fits(rewritten)) {
          outcome(
            Some(rewritten.sql),
            "replaced-preset-dt",
            "Replaced preset_dt with preset_pt; predicate values and operators are unchanged")
        } else {
          outcome(
            None,
            "removed-partition-mismatch",
            "Table is unpartitioned or the predicate still references non-partition columns")
        }
      }
    }
  }
}
