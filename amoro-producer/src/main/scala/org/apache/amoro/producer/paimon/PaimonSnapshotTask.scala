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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext}

import org.apache.amoro.producer.core.{ActionResult, ActionStatus, ActionTaskResult, ActionTaskTotals}

private[paimon] object PaimonSnapshotTask {
  private val DetailLimit = 100

  def runPartition(input: Iterator[PaimonSnapshotWork]): Iterator[ActionResult] = {
    require(input.hasNext, "Snapshot partition must contain one table")
    val work = input.next()
    require(!input.hasNext, "Snapshot partition must contain exactly one table")
    val context = TaskContext.get()
    require(context != null, "Snapshot cleanup must execute in a Spark Task")
    // Both counters start at zero, independently of the globally increasing taskAttemptId.
    // https://github.com/apache/spark/blob/v3.5.4/core/src/main/scala/org/apache/spark/TaskContext.scala
    val metrics = Map(
      "stageId" -> context.stageId().toString,
      "stageAttemptNumber" -> context.stageAttemptNumber().toString,
      "partitionId" -> context.partitionId().toString,
      "taskAttemptId" -> context.taskAttemptId().toString,
      "taskAttemptNumber" -> context.attemptNumber().toString,
      "executorId" -> SparkEnv.get.executorId,
      "applicationId" -> SparkEnv.get.conf.get("spark.app.id", ""))
    val result = execute(
      work,
      context.attemptNumber(),
      context.stageAttemptNumber(),
      metrics,
      () => context.isInterrupted() || Thread.currentThread().isInterrupted,
      () => {
        PaimonSnapshotRuntime.validate(org.apache.spark.SPARK_VERSION, SparkEnv.get.conf)
        work.validate()
        val expire = PaimonSnapshotNativeExpire.create(work.table, work.policy)
        () => expire.expire()
      })
    Iterator(result)
  }

  private[paimon] def execute(
      work: PaimonSnapshotWork,
      taskAttempt: Int,
      stageAttempt: Int,
      taskMetrics: Map[String, String],
      interrupted: () => Boolean,
      createExpire: () => (() => Int)): ActionResult = {
    val start = System.nanoTime()
    val startedAt = System.currentTimeMillis()
    val base = baseMetrics(work) ++ taskMetrics + ("startedAt" -> startedAt.toString)
    if (taskAttempt != 0 || stageAttempt != 0) {
      return failure(base, "replay", "Non-first Spark attempt rejected", unknown = true)
    }
    var phase = "initialization"
    var enteredNative = false
    var nativeInProgress = false
    var completed = 0L
    var deleted = 0L
    val details = mutable.Queue.empty[ActionTaskResult]
    def result(status: ActionStatus, message: Option[String], complete: Boolean): ActionResult = {
      val elapsed = (System.nanoTime() - start) / 1e6
      val counts = if (completed > 0 || complete) Map(
        "deletedSnapshotsTotal" -> deleted.toString,
        "totalDeletedCount" -> deleted.toString)
      else Map.empty[String, String]
      ActionResult(
        status,
        details.toVector,
        message,
        base ++ counts ++ Map(
          "failurePhase" -> (if (status == ActionStatus.Failed) phase else ""),
          "rounds" -> completed.toString,
          "countsComplete" -> complete.toString,
          "completionState" -> (if (complete && status == ActionStatus.Success) "COMPLETED"
                                else if (complete) "FAILED"
                                else "UNKNOWN"),
          "detailsTruncated" -> (completed > details.size).toString,
          "executionMs" -> elapsed.toString,
          "finishedAt" -> System.currentTimeMillis().toString),
        elapsed,
        Some(ActionTaskTotals(
          completed,
          if (status == ActionStatus.Failed && nativeInProgress) 1 else 0,
          complete)))
    }
    try {
      if (interrupted())
        return failure(base, "cancelled", "Task interrupted before cleanup", unknown = false)
      val expire = createExpire()
      var more = true
      while (more) {
        phase = "before-round"
        if (interrupted()) throw new InterruptedException("Snapshot cleanup interrupted")
        val roundStart = System.nanoTime()
        phase = "native"
        enteredNative = true
        nativeInProgress = true
        val count = expire()
        require(count >= 0, "Native expiration returned a negative count")
        nativeInProgress = false
        completed += 1
        deleted += count
        val roundMetrics = Map(
          "round" -> completed.toString,
          "deletedCount" -> count.toString,
          "costMs" -> ((System.nanoTime() - roundStart) / 1e6).toString,
          "attempts" -> "1")
        details.enqueue(ActionTaskResult(
          s"round-$completed",
          ActionStatus.Success,
          None,
          roundMetrics))
        if (details.size > DetailLimit) details.dequeue()
        PaimonSnapshotObservability.logRound(base ++ roundMetrics)
        more = count >= work.policy.maxDeletes
      }
      result(ActionStatus.Success, None, complete = true)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        result(
          ActionStatus.Failed,
          Some("Snapshot cleanup interrupted; no replay"),
          complete = !enteredNative)
      case NonFatal(error) =>
        // Do not expose FileIO options, SQL, URLs or credentials from a nested exception message.
        result(
          ActionStatus.Failed,
          Some(s"Snapshot cleanup failed (${error.getClass.getSimpleName}); no replay"),
          complete = !enteredNative)
      case _: LinkageError =>
        result(
          ActionStatus.Failed,
          Some("Snapshot cleanup linkage failure; no replay"),
          complete = !enteredNative)
    }
  }

  def baseMetrics(work: PaimonSnapshotWork): Map[String, String] = Map(
    "executionMode" -> "executor",
    "effectiveVersion" -> PaimonSnapshotRuntime.PaimonVersion,
    "runId" -> work.runId,
    "workId" -> work.workId,
    "table" -> work.target.raw,
    "physicalTableFingerprint" -> PaimonSnapshotWork.digest(work.physicalKey),
    "policyFingerprint" -> work.policyFingerprint,
    "retainMin" -> work.policy.retainMin.toString,
    "retainMax" -> work.policy.retainMax.toString,
    "maxDeletes" -> work.policy.maxDeletes.toString,
    "timeRetainedMs" -> work.policy.timeRetained.map(_.toMillis.toString).getOrElse(
      "")) ++ work.policySources

  def failure(
      metrics: Map[String, String],
      phase: String,
      message: String,
      unknown: Boolean): ActionResult = ActionResult(
    ActionStatus.Failed,
    Seq.empty,
    Some(message),
    metrics ++ Map(
      "failurePhase" -> phase,
      "completionState" -> (if (unknown) "UNKNOWN" else "FAILED"),
      "countsComplete" -> (!unknown).toString),
    0.0,
    Some(ActionTaskTotals(0, 0, complete = !unknown)))
}
