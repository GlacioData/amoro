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
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale

import org.apache.paimon.table.FileStoreTable

import org.apache.amoro.producer.core.TargetTable

final private[paimon] case class PaimonSnapshotWork(
    runId: String,
    workId: String,
    target: TargetTable,
    table: FileStoreTable,
    policy: PaimonExpireSnapshotsConfig,
    physicalKey: String,
    fileIOClass: String,
    policyFingerprint: String,
    policySources: Map[String, String]) extends Serializable {

  def validate(): Unit = {
    require(table != null && table.fileIO() != null, "Missing Paimon table/FileIO")
    require(PaimonSnapshotWork.physicalKey(table) == physicalKey, "Table identity changed")
    require(table.fileIO().getClass.getName == fileIOClass, "FileIO identity changed")
    require(PaimonSnapshotWork.fingerprint(policy) == policyFingerprint, "Retention policy changed")
    require(
      policy.timeRetained.isDefined && policy.retainMin >= 1 &&
        policy.retainMax >= policy.retainMin && policy.maxDeletes > 0 &&
        policy.maxDeletes <= Int.MaxValue && !policy.timeRetained.get.isNegative,
      "Invalid resolved retention policy")
  }

  // Never expand the FileIO/options through the generated case-class toString.
  override def toString: String = s"PaimonSnapshotWork($runId,$workId)"
}

private[paimon] object PaimonSnapshotWork {
  def physicalKey(table: FileStoreTable): String = normalizeLocation(table.location().toUri)

  def normalizeLocation(uri: URI): String = {
    require(
      uri.getScheme != null && uri.getQuery == null && uri.getFragment == null &&
        uri.getUserInfo == null,
      "Table location must be an absolute URI without credentials/query")
    val scheme = uri.getScheme.toLowerCase(Locale.ROOT)
    val authority = Option(uri.getAuthority).filter(_.nonEmpty).map(_.toLowerCase(Locale.ROOT))
    val path = Option(uri.normalize().getPath).getOrElse("")
    require(path.startsWith("/"), "Table location must have an absolute path")
    new URI(scheme, authority.orNull, if (path == "/") path else path.stripSuffix("/"), null, null)
      .toString
  }

  def fingerprint(policy: PaimonExpireSnapshotsConfig): String = digest(
    s"${policy.retainMin}|${policy.retainMax}|${policy.maxDeletes}|${policy.timeRetained}|${policy.procedureOptions}")

  def digest(value: String): String = MessageDigest.getInstance("SHA-256")
    .digest(value.getBytes(StandardCharsets.UTF_8)).map(b => f"${b & 0xFF}%02x").mkString
}
