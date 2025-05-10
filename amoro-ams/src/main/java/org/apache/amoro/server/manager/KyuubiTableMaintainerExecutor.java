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

import org.apache.kyuubi.client.BatchRestApi;
import org.apache.kyuubi.client.KyuubiRestClient;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;

public class KyuubiTableMaintainerExecutor implements SubmitTableMaintainerExecutor {

  private KyuubiRestClient kyuubiRestClient;

  public KyuubiTableMaintainerExecutor(String kyuubiUrl) {
    this.kyuubiRestClient =
        KyuubiRestClient.builder(kyuubiUrl)
            .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.SPNEGO)
            .build();
  }

  /** https://kyuubi.readthedocs.io/en/v1.10.1/client/rest/rest_api.html#post-batches */
  @Override
  public void submit() {
    // 创建 BatchRestApi 实例
    BatchRestApi batchRestApi = new BatchRestApi(kyuubiRestClient);
    BatchRequest request =
        new BatchRequest(
            "Spark", "/path/to/your/spark-job.jar", "com.example.YourSparkJobClass", "123");
    Batch batch = batchRestApi.createBatch(request);
    // https://kyuubi.readthedocs.io/en/v1.10.1/client/rest/rest_api.html#batch
    batch.getState();
  }

  @Override
  public void cancel() {}
}
