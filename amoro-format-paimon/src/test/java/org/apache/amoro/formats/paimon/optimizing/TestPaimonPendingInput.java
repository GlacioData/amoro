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

package org.apache.amoro.formats.paimon.optimizing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestPaimonPendingInput {

  @Test
  public void constructorAndGetters() {
    PaimonPendingInput input =
        new PaimonPendingInput(47, 2147483648L, 5600000L, 23, 536870912L, 3, 0, 0L, 51);

    assertEquals(47, input.getDataFileCount());
    assertEquals(2147483648L, input.getDataFileSize());
    assertEquals(5600000L, input.getDataRecordCount());
    assertEquals(5600000L, input.getTotalFileRecords());
    assertEquals(23, input.getSmallFileCount());
    assertEquals(536870912L, input.getSmallFileSize());
    assertEquals(3, input.getPartitionCount());
    assertEquals(0, input.getFileWithDeleteCount());
    assertEquals(0L, input.getDeleteRecordCount());
    assertEquals(51, input.getHealthScore());
  }

  @Test
  public void defaultConstructorZeros() {
    PaimonPendingInput input = new PaimonPendingInput();
    assertEquals(0, input.getDataFileCount());
    assertEquals(0L, input.getDataFileSize());
    assertEquals(-1, input.getHealthScore());
  }

  @Test
  public void setters() {
    PaimonPendingInput input = new PaimonPendingInput();
    input.setDataFileCount(10);
    input.setDataFileSize(1024L);
    input.setHealthScore(85);
    assertEquals(10, input.getDataFileCount());
    assertEquals(1024L, input.getDataFileSize());
    assertEquals(85, input.getHealthScore());
  }
}
