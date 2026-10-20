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

import java.net.URI

import org.apache.paimon.table.Table
import org.apache.spark.SparkConf
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.spi.ProducerRuntimeValidationException

class PaimonSnapshotRuntimeTest {
  private def conf: SparkConf = new SparkConf(false).set("spark.task.maxFailures", "1")

  @Test def requiresExactSparkAndSafeSchedulerSettings(): Unit = {
    assertThrows(
      classOf[ProducerRuntimeValidationException],
      () => PaimonSnapshotRuntime.validate("3.2.1", conf))
    assertThrows(
      classOf[ProducerRuntimeValidationException],
      () =>
        PaimonSnapshotRuntime.validate("3.5.4", conf.set("spark.speculation", "true")))
    assertThrows(
      classOf[ProducerRuntimeValidationException],
      () =>
        PaimonSnapshotRuntime.validate("3.5.4", new SparkConf(false)))
    assertThrows(
      classOf[ProducerRuntimeValidationException],
      () =>
        PaimonSnapshotRuntime.validate("3.5.4", conf.set("spark.task.maxFailures", "2")))
  }

  @Test def verifiesActualLoadedArtifactsAndRejectsLegacyCombination(): Unit = {
    val actual = PaimonSnapshotRuntime.versionOf(classOf[Table], "paimon-core")
    if (actual == "1.3.2") {
      PaimonSnapshotRuntime.validate("3.5.4", conf)
    } else {
      assertEquals("0.9.0", actual)
      assertThrows(
        classOf[ProducerRuntimeValidationException],
        () => PaimonSnapshotRuntime.validate("3.5.4", conf))
    }
    assertThrows(
      classOf[IllegalStateException],
      () =>
        PaimonSnapshotRuntime.versionOf(classOf[Table], "missing-component"))
  }

  @Test def normalizesOnlyPhysicalUriIdentity(): Unit = {
    assertEquals(
      PaimonSnapshotWork.normalizeLocation(new URI("hdfs://NN:8020/a/../table/")),
      PaimonSnapshotWork.normalizeLocation(new URI("hdfs://nn:8020/table")))
    assertNotEquals(
      PaimonSnapshotWork.normalizeLocation(new URI("hdfs://nn/table")),
      PaimonSnapshotWork.normalizeLocation(new URI("hdfs://other/table")))
    assertNotEquals(
      PaimonSnapshotWork.normalizeLocation(new URI("s3://bucket/Table")),
      PaimonSnapshotWork.normalizeLocation(new URI("s3://bucket/table")))
    Seq(
      "/relative",
      "hdfs://user:password@nn/table",
      "s3://bucket/table?key=secret",
      "s3://bucket/table#fragment")
      .foreach(uri =>
        assertThrows(
          classOf[IllegalArgumentException],
          () =>
            PaimonSnapshotWork.normalizeLocation(new URI(uri))))
  }
}
