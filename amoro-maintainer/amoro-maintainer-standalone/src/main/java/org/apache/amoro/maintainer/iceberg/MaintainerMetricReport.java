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

package org.apache.amoro.maintainer.iceberg;

import org.apache.amoro.api.ExecutorTaskResult;
import org.apache.amoro.maintainer.api.TableMaintainerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * todo report will be add thread executor and retry fixable todo maybe can use mysql to connector
 * writer
 */
public class MaintainerMetricReport {

  private static final Logger LOG = LoggerFactory.getLogger(MaintainerMetricReport.class);

  private TableMaintainerExecutor executor;

  public MaintainerMetricReport(TableMaintainerExecutor executor) {
    this.executor = executor;
  }

  public void reportMetrics(ExecutorTaskResult taskResult) {
    try {
      executor.sendTaskResultToAms(taskResult);
    } catch (Exception e) {
      LOG.warn(
          "Now Unable to send task result to Ams server,Will Add Waiting Queue later Send it", e);
    }
  }
}
