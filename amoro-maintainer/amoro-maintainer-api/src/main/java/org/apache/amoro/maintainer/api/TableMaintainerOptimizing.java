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

package org.apache.amoro.maintainer.api;

import java.io.Serializable;
import java.util.Map;

/**
 * The TableOptimizing interface defines the plan, execute, commit, and other processes required
 * during the optimizing process, as well as the input and output. Currently, these processes are
 * still scattered throughout various parts of AMS, so this interface has not yet been formally
 * used.
 */
public interface TableMaintainerOptimizing<
    I extends TableMaintainerOptimizing.MaintainerInput,
    O extends TableMaintainerOptimizing.MaintainerOutput> {

  /** Create an {@link MaintainerExecutorFactory}. */
  MaintainerExecutorFactory<I> createExecutorFactory();

  interface MaintainerInput extends Serializable {

    /** Set propertity for this OptimizingInput. */
    void option(String name, String value);

    /** Set properties for this OptimizingInput. */
    void options(Map<String, String> options);

    /** Get properties. */
    Map<String, String> getOptions();
  }

  /** Produced by {@link MaintainerExecutor} represent compaction result. */
  interface MaintainerOutput extends Serializable {
    Map<String, String> summary();
  }
}
