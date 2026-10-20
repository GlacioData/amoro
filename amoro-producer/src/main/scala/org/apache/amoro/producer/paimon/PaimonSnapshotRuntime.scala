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

import java.io.File
import java.util.Properties
import java.util.jar.JarFile

import scala.util.control.NonFatal

import org.apache.paimon.options.ExpireConfig
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.{ExpireSnapshots, Table}
import org.apache.spark.SparkConf

import org.apache.amoro.producer.spi.ProducerRuntimeValidationException

private[paimon] object PaimonSnapshotRuntime {
  val SparkVersion = "3.5.4"
  val PaimonVersion = "1.3.2"

  def validate(sparkVersion: String, conf: SparkConf): Unit = {
    check(
      sparkVersion == SparkVersion,
      s"Executor expiration requires Spark $SparkVersion, found $sparkVersion")
    // Retries still require the TaskContext gate; this setting alone is not an exactly-once guarantee.
    // https://github.com/apache/spark/blob/v3.5.4/docs/configuration.md
    check(
      !conf.getBoolean("spark.speculation", false),
      "Executor expiration requires spark.speculation=false")
    check(
      conf.getInt("spark.task.maxFailures", 4) == 1,
      "Executor expiration requires spark.task.maxFailures=1")
    try {
      val versions = Seq(
        versionOf(classOf[Table], "paimon-core"),
        versionOf(classOf[ExpireConfig], "paimon-api"),
        versionOf(classOf[SparkTable], "paimon-spark-common_2.12"))
      check(
        versions.forall(_ == PaimonVersion),
        s"Executor expiration requires Paimon $PaimonVersion; loaded core/api/spark=${versions.mkString("/")}")
      // Check linkage before deletion, including when a deployment supplies conflicting artifacts.
      classOf[Table].getMethod("newExpireSnapshots")
      classOf[ExpireSnapshots].getMethod("config", classOf[ExpireConfig])
    } catch {
      case error: ProducerRuntimeValidationException => throw error
      case NonFatal(_) => throw new ProducerRuntimeValidationException(
          "Cannot verify loaded Paimon artifacts for Executor expiration")
      case _: LinkageError => throw new ProducerRuntimeValidationException(
          "Conflicting Paimon artifacts for Executor expiration")
    }
  }

  // Read metadata from the same JAR that defined the class, not a random classpath resource.
  // Java CodeSource API: https://docs.oracle.com/javase/8/docs/api/java/security/CodeSource.html#getLocation--
  private[paimon] def versionOf(clazz: Class[_], artifact: String): String = {
    val source = Option(clazz.getProtectionDomain).flatMap(d => Option(d.getCodeSource))
      .map(_.getLocation).getOrElse(throw new IllegalStateException("Missing class source"))
    require(source.getProtocol == "file", "Unsupported class source")
    val jar = new JarFile(new File(source.toURI))
    try {
      val name = s"META-INF/maven/org.apache.paimon/$artifact/pom.properties"
      val entry = Option(jar.getJarEntry(name)).getOrElse(
        throw new IllegalStateException("Missing Paimon version metadata"))
      val input = jar.getInputStream(entry)
      val properties = new Properties()
      try properties.load(input)
      finally input.close()
      require(
        properties.getProperty("artifactId") == artifact &&
          properties.getProperty("groupId") == "org.apache.paimon",
        "Inconsistent Paimon metadata")
      Option(properties.getProperty("version")).getOrElse(
        throw new IllegalStateException("Missing Paimon version"))
    } finally jar.close()
  }

  private def check(condition: Boolean, message: => String): Unit = {
    if (!condition) throw new ProducerRuntimeValidationException(message)
  }
}
