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

package org.apache.amoro.producer.spi

import org.apache.amoro.producer.core.{ActionOption, ActionResult, CommonProducerConfig, ProducerContext, ProducerParsedOptions, RetryMode, TargetTable}

trait ActionConfig

trait ProducerAction {

  def name: String

  def retryMode: RetryMode

  def options: Seq[ActionOption]

  def parse(commandLine: ProducerParsedOptions): ActionConfig

  def validate(common: CommonProducerConfig, actionConfig: ActionConfig): Unit

  def execute(
      context: ProducerContext,
      table: TargetTable,
      actionConfig: ActionConfig): ActionResult
}
