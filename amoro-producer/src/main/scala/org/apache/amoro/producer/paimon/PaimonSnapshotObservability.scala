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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskStart}
import org.slf4j.LoggerFactory

/** Driver event times only; never subtract an Executor clock from a Driver clock. */
final private[paimon] class PaimonSnapshotObservability extends SparkListener {
  private val jobs = new ConcurrentHashMap[String, Map[String, String]]()
  private val stages = new ConcurrentHashMap[Int, String]()

  def track(group: String): Unit = jobs.put(group, Map.empty)

  override def onJobStart(event: SparkListenerJobStart): Unit = synchronized {
    val group = Option(event.properties).flatMap(p => Option(p.getProperty("spark.jobGroup.id")))
    group.filter(jobs.containsKey).foreach { key =>
      jobs.put(key, Map("jobId" -> event.jobId.toString, "jobSubmittedAt" -> event.time.toString))
      event.stageIds.foreach(stage => stages.put(stage, key))
    }
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = synchronized {
    Option(stages.get(event.stageId)).foreach { key =>
      Option(jobs.get(key)).filterNot(_.contains("queueWaitMs")).foreach { current =>
        val waitMs = event.taskInfo.launchTime - current("jobSubmittedAt").toLong
        jobs.put(key, current + ("queueWaitMs" -> math.max(0L, waitMs).toString))
      }
    }
  }

  def take(group: String): Map[String, String] = synchronized {
    val result = Option(jobs.remove(group)).getOrElse(Map.empty)
    stages.entrySet().asScala.filter(_.getValue == group).foreach(e => stages.remove(e.getKey))
    result
  }
}

private[paimon] object PaimonSnapshotObservability {
  private val mapper = new ObjectMapper()
  private val log = LoggerFactory.getLogger("org.apache.amoro.producer.snapshot.round")

  def logRound(metrics: Map[String, String]): Unit = {
    log.info(mapper.writeValueAsString((metrics + ("event" -> "producer_snapshot_round")).asJava))
  }
}
