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

import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.iceberg.producer.RemoveDanglingDeleteFilesSparkExecutor;
import org.apache.amoro.optimizing.IcebergDanglingDeleteFilesInput;
import org.apache.amoro.optimizing.IcebergDeleteFilesOutput;
import org.apache.amoro.optimizing.maintainer.DanglingDeleteFilesCleaningFactory;
import org.apache.spark.sql.SparkSession;

/** Clean table dangling delete files */
public class SparkDanglingDeleteFilesCleaningFactory extends DanglingDeleteFilesCleaningFactory {

  private final SparkSession spark;

  public SparkDanglingDeleteFilesCleaningFactory(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public MaintainerExecutor<IcebergDeleteFilesOutput> createExecutor(
      IcebergDanglingDeleteFilesInput input) {
    return new RemoveDanglingDeleteFilesSparkExecutor(input, spark);
  }
}
