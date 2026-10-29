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

import java.net.URLEncoder
import java.util.{Collections, IdentityHashMap}

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.core.io.JsonStringEncoder

/** Bounded diagnostic text; configuration objects and raw Throwable instances never enter results. */
private[paimon] object PaimonSnapshotErrors {
  private val Truncated = "[truncated]"
  private val Redacted = "[REDACTED]"
  private val MaxLength = 8192
  private val SensitiveKey =
    "(?i)password|passwd|secret|token|credential|access[._-]?key|private[._-]?key|account[._-]?key".r
  private val AssignmentKey =
    ("(?i)(?<![\\w.-])([\\\"']?[\\w.-]*(?:password|passwd|secret|token|credential|access[._-]?key|" +
      "private[._-]?key|account[._-]?key)[\\w.-]*[\\\"']?\\s*[:=]\\s*)").r
  private val UserInfo = "(?<![a-zA-Z0-9+.-])([a-zA-Z][a-zA-Z0-9+.-]*://)[^\\s/]+@".r
  private val Authorization = "(?i)(authorization\\s*[:=]\\s*(?:bearer|basic)\\s+)[^\\s,;]+".r

  def sensitiveValues(options: Iterable[(String, String)]): Seq[String] =
    options.collect {
      case (key, value)
          if key != null && value != null && value.nonEmpty &&
            SensitiveKey.findFirstIn(key).isDefined => value
    }.toSeq.distinct

  def format(prefix: String, error: Throwable, secrets: Seq[String] = Seq.empty): String = {
    val chain = ArrayBuffer.empty[Throwable]
    val visited = Collections.newSetFromMap(new IdentityHashMap[Throwable, java.lang.Boolean]())
    var current = error
    while (current != null && chain.size < 256 && visited.add(current)) {
      chain += current
      current = current.getCause
    }
    val selected = if (chain.size <= 8) chain.toVector else chain.take(7).toVector :+ chain.last
    val parts = selected.reverse.zipWithIndex.map { case (cause, index) =>
      val label = if (index == 0) "Cause" else "Wrapped by"
      val message = Option(cause.getMessage).filter(_.nonEmpty)
        .map(text => ": " + limit(redact(text, secrets), 1024)).getOrElse("")
      val frames = Option(cause.getStackTrace).toSeq.flatten
      val stack = frames.take(8).map(frame => "\n  at " + frame.toString).mkString
      val omitted = frames.length > 8 || cause.getSuppressed.nonEmpty
      s"$label: ${cause.getClass.getName}$message$stack" + (if (omitted) "\n" + Truncated else "")
    }
    val omitted = current != null || chain.size > 8
    limit(
      redact(
        (Seq(prefix) ++ parts ++ (if (omitted) Seq(Truncated) else Seq.empty))
          .mkString("\n"),
        secrets),
      MaxLength)
  }

  private def redact(text: String, secrets: Seq[String]): String = {
    val values = secrets.filter(value => value != null && value.nonEmpty).flatMap { value =>
      val encoded = URLEncoder.encode(value, "UTF-8")
      val json = new String(JsonStringEncoder.getInstance().quoteAsString(value))
      Seq(value, json, encoded, encoded.replace("+", "%20"))
    }.distinct.sortBy(value => -value.length)
    val known = values.foldLeft(text) { (result, value) =>
      // Short/numeric credentials must not erase digits inside unrelated line numbers or IDs.
      if (value.length <= 3 || value.forall(_.isDigit)) {
        ("(?<![\\w])" + java.util.regex.Pattern.quote(value) + "(?![\\w])").r
          .replaceAllIn(result, java.util.regex.Matcher.quoteReplacement(Redacted))
      } else result.replace(value, Redacted)
    }
    val assignments = redactAssignments(known)
    val userInfo = UserInfo.replaceAllIn(
      assignments,
      matched =>
        java.util.regex.Matcher.quoteReplacement(matched.group(1) + Redacted + "@"))
    Authorization.replaceAllIn(
      userInfo,
      matched =>
        java.util.regex.Matcher.quoteReplacement(matched.group(1) + Redacted))
  }

  private def redactAssignments(text: String): String = {
    val result = new java.lang.StringBuilder()
    val matcher = AssignmentKey.pattern.matcher(text)
    var cursor = 0
    while (cursor < text.length && matcher.find(cursor)) {
      val start = matcher.end()
      result.append(text, cursor, start)
      if (start == text.length) cursor = start
      else {
        result.append(Redacted)
        cursor = valueEnd(text, start)
      }
    }
    result.append(text, cursor, text.length).toString
  }

  private def valueEnd(text: String, start: Int): Int = {
    val quote = text.charAt(start)
    var index = start
    if (quote == '"' || quote == '\'') {
      index += 1
      while (index < text.length) {
        text.charAt(index) match {
          case '\\' => index = math.min(text.length, index + 2)
          case char if char == quote => return index + 1
          case _ => index += 1
        }
      }
      // An unterminated quoted credential must not expose its remaining suffix.
    } else {
      // Keep repeated redaction stable, but do not trust a marker followed by a secret suffix.
      if (text.startsWith(Redacted, start)) index += Redacted.length
      while (index < text.length && !Character.isWhitespace(text.charAt(index)) &&
        !",;&}]".contains(text.charAt(index))) index += 1
    }
    index
  }

  private def limit(text: String, size: Int): String =
    if (text.length <= size) text else text.take(size - Truncated.length) + Truncated
}
