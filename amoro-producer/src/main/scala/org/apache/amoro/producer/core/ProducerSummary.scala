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

final case class FailedTableSummary(table: TargetTable, message: String)

final case class ProducerSummary(
    tableCount: Int,
    successTables: Seq[TargetTable],
    failedTables: Seq[TargetTable],
    skippedTables: Seq[TargetTable],
    failedTableSummaries: Seq[FailedTableSummary],
    taskSuccessCount: Int,
    taskFailedCount: Int)

object ProducerSummary {

  def fromResults(results: Seq[(TargetTable, ActionResult)]): ProducerSummary = {
    val taskResults = results.flatMap(_._2.tasks)
    ProducerSummary(
      tableCount = results.size,
      successTables = tablesWithStatus(results, ActionStatus.Success),
      failedTables = tablesWithStatus(results, ActionStatus.Failed),
      skippedTables = tablesWithStatus(results, ActionStatus.Skipped),
      failedTableSummaries = failedTableSummaries(results),
      taskSuccessCount = taskResults.count(_.status == ActionStatus.Success),
      taskFailedCount = taskResults.count(_.status == ActionStatus.Failed))
  }

  private def tablesWithStatus(
      results: Seq[(TargetTable, ActionResult)],
      status: ActionStatus): Seq[TargetTable] = {
    results.collect {
      case (table, result) if result.status == status => table
    }
  }

  private def failedTableSummaries(
      results: Seq[(TargetTable, ActionResult)]): Seq[FailedTableSummary] = {
    results.collect {
      case (table, result) if result.status == ActionStatus.Failed =>
        FailedTableSummary(table, failureMessage(result))
    }
  }

  private def failureMessage(result: ActionResult): String = {
    result.message
      .orElse(
        result.tasks
          .find(_.status == ActionStatus.Failed)
          .flatMap(_.message))
      .map(normalizeMessage)
      .filter(_.nonEmpty)
      .getOrElse("action failed without message")
  }

  private def normalizeMessage(message: String): String = {
    message.replaceAll("\\s+", " ").trim
  }
}
