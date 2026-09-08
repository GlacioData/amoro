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

package org.apache.amoro.server;

import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TMemoryBuffer;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransport;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransportFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestThriftMessageSize {

  @Test
  void testConfiguredMessageSizeIsAlsoAppliedToFrameSize() throws Exception {
    int maxMessageSize = 100 * 1024 * 1024;

    TTransportFactory factory = AmoroServiceContainer.createThriftTransportFactory(maxMessageSize);
    TTransport transport = factory.getTransport(new TMemoryBuffer(16));

    Assertions.assertEquals(maxMessageSize, transport.getConfiguration().getMaxFrameSize());
  }

  @Test
  void testFrameSizeRejectsValuesOutsideThriftRange() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AmoroServiceContainer.createThriftTransportFactory(0));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> AmoroServiceContainer.createThriftTransportFactory((long) Integer.MAX_VALUE + 1));
  }
}
