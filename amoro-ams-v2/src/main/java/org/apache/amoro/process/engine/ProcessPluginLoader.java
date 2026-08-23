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

import org.apache.amoro.process.trigger.ProcessActionPluginFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

/** Explicit-classloader Java SPI discovery for engine and action factories. */
public final class ProcessPluginLoader {

  private ProcessPluginLoader() {}

  public static List<ProcessEngineFactory> loadEngineFactories(ClassLoader classLoader) {
    return load(ProcessEngineFactory.class, classLoader);
  }

  public static List<ProcessActionPluginFactory> loadActionFactories(ClassLoader classLoader) {
    return load(ProcessActionPluginFactory.class, classLoader);
  }

  private static <T> List<T> load(Class<T> type, ClassLoader classLoader) {
    Objects.requireNonNull(classLoader, "classLoader");
    List<T> providers = new ArrayList<>();
    for (T provider : ServiceLoader.load(type, classLoader)) {
      providers.add(Objects.requireNonNull(provider, "SPI provider"));
    }
    return providers;
  }
}
