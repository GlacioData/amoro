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

package org.apache.amoro.producer.core

final case class ActionOption(
    name: String,
    hasValue: Boolean = true,
    description: String = "",
    shortName: Option[String] = None) {
  require(name != null && name.trim.nonEmpty, "option name must not be empty")
  require(!name.startsWith("-"), s"option name must not start with '-': $name")
  shortName.foreach { value =>
    require(value.trim.nonEmpty, "short option name must not be empty")
    require(!value.startsWith("-"), s"short option name must not start with '-': $value")
  }
}

final case class ProducerParsedOptions private[core] (
    values: Map[String, String],
    flags: Set[String]) {

  def getOptionValue(name: String): String = {
    values.getOrElse(name, null)
  }

  def getOptionValue(name: String, defaultValue: String): String = {
    values.getOrElse(name, defaultValue)
  }

  def hasOption(name: String): Boolean = {
    values.contains(name) || flags.contains(name)
  }
}

final class ProducerCliParseException(message: String) extends IllegalArgumentException(message)
