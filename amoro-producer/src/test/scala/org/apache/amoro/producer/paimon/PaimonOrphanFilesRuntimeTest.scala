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

import org.apache.spark.SparkConf
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.spi.ProducerRuntimeValidationException

class PaimonOrphanFilesRuntimeTest {
  @Test def yarnRejectsMissingOrUnsafeSubmissionPolicyWithAnActionableMessage(): Unit = {
    Seq("client", "cluster").foreach { mode =>
      Seq(None, Some("2"), Some("0"), Some("invalid")).foreach { attempts =>
        val conf = new SparkConf(false).setMaster("yarn").set("spark.submit.deployMode", mode)
        attempts.foreach(value => conf.set("spark.yarn.maxAppAttempts", value))
        val error = assertThrows(
          classOf[ProducerRuntimeValidationException],
          () => PaimonOrphanFilesRuntime.validateSubmissionConf(conf))
        assertTrue(error.getMessage.contains("--conf spark.yarn.maxAppAttempts=1"))
        assertTrue(error.getMessage.contains("before the JAR path"))
        assertEquals(attempts, conf.getOption("spark.yarn.maxAppAttempts"))
      }
    }
  }

  @Test def yarnAcceptsAnExplicitSingleAttemptAtSubmission(): Unit = {
    Seq("client", "cluster").foreach { mode =>
      val conf = new SparkConf(false).setMaster("yarn").set("spark.submit.deployMode", mode)
        .set("spark.yarn.maxAppAttempts", "1")
      PaimonOrphanFilesRuntime.validateSubmissionConf(conf)
      assertEquals("1", conf.get("spark.yarn.maxAppAttempts"))
    }
  }

  @Test def nonYarnRuntimesDoNotRequireTheYarnSubmissionOption(): Unit = {
    Seq("local[2]", "spark://localhost:7077").foreach { master =>
      val conf = new SparkConf(false).setMaster(master)
      PaimonOrphanFilesRuntime.validateSubmissionConf(conf)
      assertFalse(conf.contains("spark.yarn.maxAppAttempts"))
    }
  }
}
