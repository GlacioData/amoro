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

package org.apache.amoro.process.engine.simulated;

import org.apache.amoro.process.engine.LocalEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineFactory;
import org.apache.amoro.process.engine.ProcessEnginePort;
import org.apache.amoro.process.engine.ProviderMode;

/** Explicit local simulator; it never loads a table or invokes a format Action. */
public final class SimulatedLocalEngineFactory implements ProcessEngineFactory {

  @Override
  public String engineName() {
    return "local";
  }

  @Override
  public ProviderMode mode() {
    return ProviderMode.SIMULATED;
  }

  @Override
  public ProcessEnginePort create(Context context) {
    return new LocalEngineAdapter(
        context.workerThreads(),
        context.queueCapacity(),
        context.localActions(),
        context.localTerminalResultRetentionDays());
  }
}
