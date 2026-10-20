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

import org.apache.spark.FutureAction

import org.apache.amoro.producer.core.ActionResult

/** In-memory lifecycle only. Cancellation and registration of a cleanup Job are atomic. */
final private[paimon] class PaimonSnapshotSubmission extends AutoCloseable {
  private val actions = mutable.Map.empty[String, FutureAction[Seq[ActionResult]]]
  private val cancelled = mutable.Set.empty[String]
  private var closed = false

  def submit(id: String)(start: => FutureAction[Seq[ActionResult]])
      : FutureAction[Seq[ActionResult]] = synchronized {
    if (closed || cancelled(id) || Thread.currentThread().isInterrupted)
      throw new InterruptedException()
    require(!actions.contains(id), "Cleanup already submitted")
    // collectAsync submits one Job and returns its cancellable handle; waiting happens outside
    // this monitor. A cancel cannot slip between Spark submission and handle registration.
    // https://github.com/apache/spark/blob/v3.5.4/core/src/main/scala/org/apache/spark/rdd/AsyncRDDActions.scala
    val action = start
    actions.put(id, action)
    action
  }

  def completed(id: String): Unit = synchronized { actions.remove(id); () }

  def cancel(id: String): Unit = synchronized {
    cancelled.add(id)
    PaimonSnapshotCancellation.run(actions.get(id).toSeq.map(action => () => action.cancel()))
  }

  override def close(): Unit = synchronized {
    closed = true
    PaimonSnapshotCancellation.run(actions.values.toSeq.map(action => () => action.cancel()))
    actions.clear()
    cancelled.clear()
  }
}

/** Keep cancellation failures from skipping other handles, groups or resource cleanup. */
private[paimon] object PaimonSnapshotCancellation {
  def run(requests: Seq[() => Unit]): Unit = {
    var interrupted = Thread.interrupted()
    try requests.foreach { request =>
        // A nested cancellation may have restored its caller's interrupt flag.
        interrupted = Thread.interrupted() || interrupted
        try request()
        catch {
          case _: InterruptedException =>
            interrupted = true
            Thread.interrupted()
            // Retry only the cancellation request, never the cleanup action.
            try request()
            catch {
              case _: InterruptedException => Thread.interrupted()
              case NonFatal(_) => ()
            }
          case NonFatal(_) => ()
        }
      }
    finally {
      interrupted = Thread.interrupted() || interrupted
      if (interrupted) Thread.currentThread().interrupt()
    }
  }
}
