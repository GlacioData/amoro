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

package org.apache.amoro.server.scheduler.external;

import org.apache.amoro.Action;
import org.apache.amoro.process.AmoroProcess;
import org.apache.amoro.process.TableProcessState;
import org.apache.amoro.resource.ExternalResourceContainer;
import org.apache.amoro.resource.ResourceManager;
import org.apache.amoro.server.scheduler.PeriodicExternalScheduler;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.TableService;

public class JavaProcessExecutor extends PeriodicExternalScheduler {

  public JavaProcessExecutor(
      ResourceManager resourceManager,
      ExternalResourceContainer resourceContainer,
      Action action,
      TableService tableService,
      int poolSize) {
    super(resourceManager, resourceContainer, action, tableService, poolSize);
  }

  @Override
  protected void trace(AmoroProcess<? extends TableProcessState> process) {
    TableProcessState state = process.getState();
  }

  @Override
  protected long getNextExecutingTime(DefaultTableRuntime tableRuntime) {
    return 1800000;
  }

}
