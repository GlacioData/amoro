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

import java.util.Properties

import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListenerTaskStart, StageInfo, TaskInfo}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

class PaimonSnapshotObservabilityTest {
  @Test def correlatesConcurrentJobsAndDropsLateEventsAfterCompletion(): Unit = {
    val observer = new PaimonSnapshotObservability()
    observer.track("a")
    observer.track("b")
    observer.onJobStart(start("a", 10, 100L))
    observer.onJobStart(start("b", 11, 200L))
    observer.onTaskStart(task(11, 225L))
    observer.onTaskStart(task(10, 110L))
    observer.onTaskStart(task(10, 115L))
    assertEquals(
      Map("jobId" -> "10", "jobSubmittedAt" -> "100", "queueWaitMs" -> "10"),
      observer.take("a"))
    assertEquals("25", observer.take("b")("queueWaitMs"))
    observer.onTaskStart(task(10, 120L))
    observer.onJobStart(start("a", 10, 100L))
    assertTrue(observer.take("a").isEmpty)
  }

  @Test def missingEventsRemainAbsentAndReadonlyJobsAreIgnored(): Unit = {
    val observer = new PaimonSnapshotObservability()
    observer.track("cleanup")
    observer.onJobStart(start("preparation", 1, 10L))
    observer.onTaskStart(task(1, 20L))
    assertTrue(observer.take("cleanup").isEmpty)
    assertTrue(observer.take("preparation").isEmpty)
  }

  private def start(group: String, id: Int, time: Long): SparkListenerJobStart = {
    val properties = new Properties()
    properties.setProperty("spark.jobGroup.id", group)
    val stage = mock(classOf[StageInfo])
    when(stage.stageId).thenReturn(id)
    SparkListenerJobStart(id, time, Seq(stage), properties)
  }
  private def task(stage: Int, time: Long): SparkListenerTaskStart = {
    val info = mock(classOf[TaskInfo])
    when(info.launchTime).thenReturn(time)
    SparkListenerTaskStart(stage, 0, info)
  }
}
