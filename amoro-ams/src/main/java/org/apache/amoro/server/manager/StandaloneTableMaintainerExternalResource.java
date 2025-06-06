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

package org.apache.amoro.server.manager;

import org.apache.amoro.OptimizerProperties;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.process.AmoroProcess;
import org.apache.amoro.process.TableProcessState;
import org.apache.amoro.resource.ExternalResourceContainer;
import org.apache.amoro.resource.Resource;
import org.apache.amoro.resource.ResourceStatus;
import org.apache.amoro.resource.ResourceType;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StandaloneTableMaintainerExternalResource implements ExternalResourceContainer {

  private static final Logger LOG =
      LoggerFactory.getLogger(StandaloneTableMaintainerExternalResource.class);

  private String amsHome;
  private String amsOptimizingUrl;

  @Override
  public void init(String name, Map<String, String> containerProperties) {
    this.amsHome = containerProperties.get(OptimizerProperties.AMS_HOME);
    this.amsOptimizingUrl = containerProperties.get(OptimizerProperties.AMS_OPTIMIZER_URI);
    Preconditions.checkNotNull(
        this.amsHome, "Container Property: %s is required", OptimizerProperties.AMS_HOME);
    Preconditions.checkNotNull(
        this.amsOptimizingUrl,
        "Container Property: %s is required",
        OptimizerProperties.AMS_OPTIMIZER_URI);
  }

  @Override
  public Resource submit(AmoroProcess<? extends TableProcessState> process) {
    ServerTableIdentifier tableIdentifier = process.getTableIdentifier();
    String startCmd =
        String.format(
            "%s/bin/maintainer.sh start %s", buildTableMaintainerStartupArgsString(process));
    LOG.info("Starting table maintainer cmd: " + startCmd);
    Resource.Builder builder =
        new Resource.Builder(
            tableIdentifier.toString(),
            "default",
            ResourceType.TABLE_MAINTAINER);
    builder.setThreadCount(4);
    builder.setMemoryMb(4098);
    return builder.build();
  }

  @Override
  public void release(String resourceId) {
    String os = System.getProperty("os.name").toLowerCase();
    String cmd;
    String[] finalCmd;
    if (os.contains("win")) {
      cmd = "task kill /PID " + resourceId + " /F ";
      finalCmd = new String[] {"cmd", "/c", cmd};
    } else {
      cmd = "kill -9 " + resourceId;
      finalCmd = new String[] {"/bin/sh", "-c", cmd};
    }
    try {
      Runtime runtime = Runtime.getRuntime();
      LOG.info("Stopping table maintainer using command:{}", cmd);
      runtime.exec(finalCmd);
    } catch (Exception e) {
      throw new RuntimeException("Failed to release table maintainer.", e);
    }
  }

  @Override
  public String name() {
    return "STANDALONE_TABLE_MAINTAINER";
  }

  @Override
  public ResourceStatus getStatus(String resourceId) {
    return null;
  }

  protected String buildTableMaintainerStartupArgsString(
      AmoroProcess<? extends TableProcessState> process) {
    ServerTableIdentifier tableIdentifier = process.getTableIdentifier();
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append(" -a ")
        .append(amsOptimizingUrl)
        .append(" -p ")
        .append(4)
        .append(" -g ")
        .append("default")
        .append(" -type ")
        .append(tableIdentifier.getFormat().name().toUpperCase())
        .append(" -c ")
        .append(tableIdentifier.getCatalog())
        .append(" -d ")
        .append(tableIdentifier.getDatabase())
        .append(" -t ")
        .append(tableIdentifier.getTableName());
    return stringBuilder.toString();
  }
}
