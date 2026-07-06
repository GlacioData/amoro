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

package org.apache.amoro.server.process.paimon;

import org.apache.amoro.Action;
import org.apache.amoro.PaimonActions;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.process.ExecuteEngine;
import org.apache.amoro.process.HttpRemoteSparkStandAloneSubmit;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.TableProcess;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.cleanup.CleanupOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Remote table process that submits Paimon remove_orphan_files procedure to Spark via HTTP. */
public class PaimonCleanOrphansProcess extends TableProcess {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonCleanOrphansProcess.class);

  private final int sparkVersion;

  public PaimonCleanOrphansProcess(
      TableRuntime tableRuntime, ExecuteEngine engine, int sparkVersion) {
    super(tableRuntime, engine);
    this.sparkVersion = sparkVersion;
  }

  public static Optional<PaimonCleanOrphansProcess> trigger(
      TableRuntime tableRuntime, ExecuteEngine engine, int sparkVersion, Duration interval) {
    if (tableRuntime instanceof DefaultTableRuntime) {
      DefaultTableRuntime prt = (DefaultTableRuntime) tableRuntime;
      long lastExecuteTime = prt.getLastCleanTime(CleanupOperation.ORPHAN_FILES_CLEANING);
      if (System.currentTimeMillis() - lastExecuteTime < interval.toMillis()) {
        LOG.debug(
            "Skip clean orphans for table {}, last execute time: {}",
            tableRuntime.getTableIdentifier(),
            lastExecuteTime);
        return Optional.empty();
      }
    }
    return Optional.of(new PaimonCleanOrphansProcess(tableRuntime, engine, sparkVersion));
  }

  @Override
  public Action getAction() {
    return PaimonActions.CLEAN_ORPHANS;
  }

  @Override
  public Map<String, String> getProcessParameters() {
    String executeUser = getExecutionUser();
    Map<String, String> params = new HashMap<>();
    params.put("hql", buildCleanOrphansSql());
    params.put("curUser", executeUser);
    params.put("logUser", executeUser);
    params.put("group", executeUser);
    params.put("userId", "470");
    params.put("sparkVersion", String.valueOf(sparkVersion));
    params.put("sourceTag", "AMORO");
    params.put("conf", "{\"sparkVersion\":\"" + sparkVersion + "\",\"paimon.version\":\"1.3\"}");
    return params;
  }

  private String getExecutionUser() {
    if (executeEngine instanceof HttpRemoteSparkStandAloneSubmit) {
      return ((HttpRemoteSparkStandAloneSubmit) executeEngine).configuredExecuteUser();
    }
    return "sljdp";
  }

  @Override
  public Map<String, String> getSummary() {
    Map<String, String> summary = new HashMap<>();
    summary.put("table", getTableIdentifier().toString());
    summary.put("action", getAction().getName());
    return summary;
  }

  @Override
  public void afterComplete(ProcessStatus status) {
    if (status == ProcessStatus.SUCCESS && tableRuntime instanceof DefaultTableRuntime) {
      ((DefaultTableRuntime) tableRuntime)
          .updateLastCleanTime(CleanupOperation.ORPHAN_FILES_CLEANING, System.currentTimeMillis());
      LOG.info(
          "Updated lastOrphanFilesCleanTime for table {} after successful clean orphans",
          getTableIdentifier());
    }
  }

  String buildCleanOrphansSql() {
    ServerTableIdentifier tableId = getTableIdentifier();
    String fullTableName = String.format("%s.%s", tableId.getDatabase(), tableId.getTableName());
    String sql = String.format("CALL sys.remove_orphan_files(table => '%s')", fullTableName);

    LOG.info("Built clean orphans SQL for table {}: {}", tableId, sql);
    return sql;
  }
}
