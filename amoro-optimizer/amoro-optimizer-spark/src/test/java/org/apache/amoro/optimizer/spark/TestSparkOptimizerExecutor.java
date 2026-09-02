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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.amoro.api.OptimizingTask;
import org.apache.amoro.api.OptimizingTaskId;
import org.apache.amoro.formats.paimon.optimizing.PaimonCompactionInput;
import org.apache.amoro.utils.SerializationUtil;
import org.junit.jupiter.api.Test;

class TestSparkOptimizerExecutor {

  @Test
  void jobDescriptionShowsFlatProcessAndTaskIds() {
    OptimizingTask task = new OptimizingTask(new OptimizingTaskId(1464999674912768L, 1));
    task.setTaskInput(SerializationUtil.simpleSerialize(new PaimonCompactionInput()));

    String description = SparkOptimizerExecutor.jobDescription(task);

    assertEquals(
        "Amoro optimizing task, Amoro paimon compaction task, table:<unknown>, partition:<none>, "
            + "processId:1464999674912768, taskId:1",
        description);
    assertFalse(description.contains("OptimizingTaskId("));
  }
}
