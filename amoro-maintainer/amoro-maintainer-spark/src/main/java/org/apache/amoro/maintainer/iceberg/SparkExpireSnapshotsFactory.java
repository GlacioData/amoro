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
import org.apache.amoro.maintainer.iceberg.producer.ExpireSnapshotsSparkExecutor;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;
import org.apache.amoro.optimizing.maintainer.ExpireSnapshotsFactory;
import org.apache.spark.sql.SparkSession;

/** SparkExpireSnapshotsFactory */
public class SparkExpireSnapshotsFactory extends ExpireSnapshotsFactory {

  private final SparkSession spark;

  public SparkExpireSnapshotsFactory(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public MaintainerExecutor<IcebergExpireSnapshotsOutput> createExecutor(
      IcebergExpireSnapshotInput input) {
    return new ExpireSnapshotsSparkExecutor(spark, input);
  }
}
