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

package org.apache.amoro.optimizer.spark;

import org.apache.amoro.AmoroTable;
import org.apache.amoro.TableFormat;
import org.apache.amoro.optimizer.common.TableMaintainerConfig;
import org.apache.amoro.optimizing.DanglingDeleteFilesInput;
import org.apache.amoro.optimizing.DeleteFilesOutput;
import org.apache.amoro.optimizing.maintainer.DanglingDeleteFilesCleaningExecutor;
import org.apache.amoro.optimizing.maintainer.IcebergTableUtil;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class SparkTableMaintainer {

  private static final Logger LOG = LoggerFactory.getLogger(SparkOptimizer.class);
  private static final String APP_NAME_FORMAT = "amoro-spark-executor-%s-%s";

  public static void main(String[] args) throws Exception {
    TableMaintainerConfig config = new TableMaintainerConfig(args);
    SparkSession spark =
        SparkSession.builder()
            .appName(String.format(APP_NAME_FORMAT, config.getType(), "tableName"))
            .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    if (!jsc.getConf().getBoolean("spark.dynamicAllocation.enabled", false)) {
      LOG.warn(
          "To better utilize computing resources, it is recommended to enable 'spark.dynamicAllocation.enabled' "
              + "and set 'spark.dynamicAllocation.maxExecutors' equal to 'OPTIMIZER_EXECUTION_PARALLEL'");
    }

    SparkTableMaintainerExecutor executor = new SparkTableMaintainerExecutor(jsc, config);
    AmoroTable<?> amoroTable = executor.getAmoroTable();
    TableFormat format = amoroTable.format();

    // switch (config.getType()) {
    //   case "CLEAN_DANGLING":
    //     if (TableFormat.ICEBERG.equals(format)) {
    //       // get DanglingDeleteFilesInput
    //       DanglingDeleteFilesCleaningExecutor cleaningExecutor =
    //           new DanglingDeleteFilesCleaningExecutor(
    //               new DanglingDeleteFilesInput((Table) amoroTable.originalTable(), true, 1L));
    //       DeleteFilesOutput deleteFilesResult = cleaningExecutor.execute();
    //     }
    //   default:
    //     throw new IllegalStateException("UnSupport Table Maintainer Type: " + config.getType());
    // }

    // todo split task to do
    if (TableFormat.ICEBERG.equals(format)) {
      Set<DeleteFile> danglingDeleteFiles =
          IcebergTableUtil.getDanglingDeleteFiles((Table) amoroTable.originalTable());
      // jsc.parallelize(new ArrayList<>(danglingDeleteFiles),2).map();

      // get DanglingDeleteFilesInput
      DanglingDeleteFilesCleaningExecutor cleaningExecutor =
          new DanglingDeleteFilesCleaningExecutor(
              new DanglingDeleteFilesInput((Table) amoroTable.originalTable(), true, 1L));
      DeleteFilesOutput deleteFilesResult = cleaningExecutor.execute();
      LOG.info("deleteFilesResult :{}", deleteFilesResult);
    }
  }
}
