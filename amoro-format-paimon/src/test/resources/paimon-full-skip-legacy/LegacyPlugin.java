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

package org.apache.amoro.compat;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.config.OptimizingConfig;
import org.apache.amoro.optimizing.OptimizationContext;
import org.apache.amoro.optimizing.TableOptimizingCommitter;

/** Compiled with the interfaces from d5685bf, not the changed source tree. */
public class LegacyPlugin implements TableOptimizingCommitter, OptimizationContext {
  public int commits;
  public void commit() { commits++; }
  public ServerTableIdentifier getTableIdentifier() { return null; }
  public OptimizingConfig getOptimizingConfig() { return null; }
  public boolean isIdle() { return true; }
  public long getLastPlanTime() { return 0; }
  public long getLastMinorOptimizingTime() { return 0; }
  public long getLastMajorOptimizingTime() { return 0; }
  public long getLastFullOptimizingTime() { return 0; }
}
