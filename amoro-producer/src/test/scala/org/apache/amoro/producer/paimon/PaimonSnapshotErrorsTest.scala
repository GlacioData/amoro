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

import java.io.IOException

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import org.apache.amoro.producer.core._

class PaimonSnapshotErrorsTest {
  @Test def redactsEntireQuotedValuesWithEscapedQuotesAndBackslashes(): Unit = {
    Seq(
      "{\"password\":\"prefix\\\"LEAKED-SUFFIX\"}",
      "token='prefix\\'TOKEN-SUFFIX' next=visible",
      "password=\"prefix\\\\\\\"BACKSLASH-SUFFIX\" next=visible")
      .foreach { input =>
        val message = PaimonSnapshotErrors.format("failed", new IOException(input))
        assertTrue(message.contains("[REDACTED]"), message)
        Seq("prefix", "LEAKED-SUFFIX", "TOKEN-SUFFIX", "BACKSLASH-SUFFIX")
          .foreach(value => assertFalse(message.contains(value), message))
        if (input.contains("next=visible")) assertTrue(message.contains("next=visible"), message)
      }
  }

  @Test def unterminatedQuotedSecretsAreRedactedToTheEndOfTheMessage(): Unit = {
    val input = "password=\"prefix\\\"LEAKED-SUFFIX trailing-value"
    val message = PaimonSnapshotErrors.format("failed", new IOException(input))
    Seq("prefix", "LEAKED-SUFFIX", "trailing-value")
      .foreach(value => assertFalse(message.contains(value), message))
    assertTrue(message.contains("[REDACTED]"), message)
  }

  @Test def redactionMarkersInsideAnInputValueCannotExposeItsSuffix(): Unit = {
    val message = PaimonSnapshotErrors.format(
      "failed",
      new IOException("password=[REDACTED]LEAKED-SUFFIX next=visible"))
    assertFalse(message.contains("LEAKED-SUFFIX"), message)
    assertTrue(message.contains("next=visible"), message)
    assertFalse(message.contains("[REDACTED]]"), message)
  }

  @Test def redactsJsonEscapedKnownValuesWithoutSensitiveKeysAndStillProducesValidJson(): Unit = {
    val secret = "prefix\"SECRET-SUFFIX\\line\nnext"
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val encoded = mapper.writeValueAsString(secret)
    val message = PaimonSnapshotErrors.format("failed", new IOException(encoded), Seq(secret))
    assertFalse(message.contains("SECRET-SUFFIX"), message)
    assertFalse(message.contains("prefix"), message)
    val result = ActionResult(ActionStatus.Failed, Seq.empty, Some(message), Map.empty, 0)
    val common =
      CommonProducerConfig("paimon", "expire-snapshots", "paimon", Some("db"), None, None, 3, true)
    val json = TableActionJsonLog.render(common, TargetTable("db.t", "db", "t"), result, 0)
    assertFalse(json.contains("\n"))
    assertEquals(message, mapper.readTree(json).get("message").asText())
  }

  @Test def shortSecretsDoNotCorruptStackLocationsAndLongIds(): Unit = {
    val error = new IOException("credential=0 snapshot=102030 user value 123")
    error.setStackTrace(Array(new StackTraceElement("example.IO", "open", "IO.java", 2042)))
    val message = PaimonSnapshotErrors.format("failed", error, Seq("0", "123"))
    assertTrue(message.contains("snapshot=102030"), message)
    assertTrue(message.contains("IO.java:2042"), message)
    assertFalse(message.contains("credential=0"), message)
    assertFalse(message.contains("value 123"), message)
  }
  @Test def retainsRootCauseWithEmptyMessageAndItsLocation(): Unit = {
    val root = new NullPointerException()
    root.setStackTrace(Array(new StackTraceElement("example.IO", "open", "IO.java", 42)))
    val message =
      PaimonSnapshotErrors.format("Cleanup failed; no replay", new IOException("outer", root))
    assertTrue(message.contains("java.lang.NullPointerException"), message)
    assertTrue(message.contains("IO.java:42"), message)
    assertTrue(message.contains("java.io.IOException: outer"), message)
    assertTrue(message.indexOf("NullPointerException") < message.indexOf("IOException"), message)
  }

  @Test def boundsDeepChainsAndStackFramesButRetainsTheRoot(): Unit = {
    val root = new IOException("actual-root")
    root.setStackTrace((1 to 20).map(i =>
      new StackTraceElement("example.IO", "open", "IO.java", i)).toArray)
    val error =
      (1 to 20).foldLeft[Throwable](root)((cause, i) => new IOException(s"wrapper-$i", cause))
    val message = PaimonSnapshotErrors.format("failed", error)
    assertTrue(message.contains("actual-root"), message)
    assertTrue(message.contains("wrapper-20"), message)
    assertTrue(message.contains("IO.java:8"), message)
    assertFalse(message.contains("IO.java:9"), message)
    assertEquals(8, message.split("java.io.IOException", -1).length - 1)
    assertTrue(message.contains("[truncated]"), message)
    assertTrue(message.length <= 8192)
  }

  @Test def boundsLongTextAndSafelyTerminatesCyclicCauses(): Unit = {
    val first = new IOException("first")
    val second = new IOException("second", first)
    first.initCause(second)
    val cyclic = PaimonSnapshotErrors.format("failed", first)
    assertTrue(cyclic.contains("first"))
    assertTrue(cyclic.contains("second"))
    assertTrue(cyclic.contains("[truncated]"))
    val longChain = (1 to 8).foldLeft[Throwable](new IOException("root"))((cause, _) =>
      new IOException("x" * 20000, cause))
    val message = PaimonSnapshotErrors.format("failed", longChain)
    assertTrue(message.length <= 8192)
    assertTrue(message.contains("root"))
    assertTrue(message.endsWith("[truncated]"))
  }

  @Test def redactsAssignmentsUriUserInfoAndKnownSecretsBeforeTruncating(): Unit = {
    val input = "password=pass-one token=tok-one credential=cred-one " +
      "access.key=key-one private_key='private one' account-key=account-one " +
      "{\"secret\":\"json-secret\"} https://user:uri-secret@host/path?token=url-secret " +
      "Authorization: Bearer bearer-secret arbitrary known-value encoded known%20value"
    val message = PaimonSnapshotErrors.format(
      "failed",
      new IOException(input),
      Seq("known-value", "known value"))
    Seq(
      "pass-one",
      "tok-one",
      "cred-one",
      "key-one",
      "private one",
      "account-one",
      "json-secret",
      "uri-secret",
      "url-secret",
      "bearer-secret",
      "known-value",
      "known%20value")
      .foreach(secret => assertFalse(message.contains(secret), message))
    assertTrue(message.contains("https://[REDACTED]@host/path"), message)
    assertTrue(message.contains("[REDACTED]"), message)
    assertEquals(
      Seq("value"),
      PaimonSnapshotErrors.sensitiveValues(Map(
        "fs.s3a.secret.key" -> "value",
        "normal" -> "visible")))
  }

  @Test def detailedMessageRemainsAValidSingleLineJsonString(): Unit = {
    val message =
      PaimonSnapshotErrors.format("failed; no replay", new IOException("bad \"path\"\nnext line"))
    val result = ActionResult(ActionStatus.Failed, Seq.empty, Some(message), Map.empty, 0)
    val common =
      CommonProducerConfig("paimon", "expire-snapshots", "paimon", Some("db"), None, None, 3, true)
    val json = TableActionJsonLog.render(common, TargetTable("db.t", "db", "t"), result, 0)
    assertFalse(json.contains("\n"))
    assertEquals(
      message,
      new com.fasterxml.jackson.databind.ObjectMapper().readTree(json).get("message").asText())
  }
}
