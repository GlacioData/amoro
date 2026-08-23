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

package org.apache.amoro.process.engine;

import java.util.Objects;

/**
 * Java SPI for a Process engine. Providers require a public no-argument constructor and are
 * registered through {@code META-INF/services}. The context deliberately exposes no Spring,
 * datasource, v1 AMS or table-format object.
 */
public interface ProcessEngineFactory {

  String engineName();

  ProviderMode mode();

  ProcessEnginePort create(Context context);

  /** Immutable framework-owned construction context. */
  final class Context {
    private final String instanceId;

    public Context(String instanceId) {
      this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
    }

    public String instanceId() {
      return instanceId;
    }
  }
}
