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

package org.apache.amoro.api;

import org.apache.amoro.shade.thrift.org.apache.thrift.TConfiguration;
import org.apache.amoro.shade.thrift.org.apache.thrift.TSerializer;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TCompactProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TJSONProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocolException;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocolFactory;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TTupleProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TMemoryBuffer;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Stream;

public class TestGeneratedThriftSerialization {

  private static final int MAX_MESSAGE_SIZE = 1024 * 1024;

  static Stream<Arguments> protocols() {
    return Stream.of(
        Arguments.of("binary", new TBinaryProtocol.Factory()),
        Arguments.of("compact", new TCompactProtocol.Factory()),
        Arguments.of("tuple", new TTupleProtocol.Factory()),
        Arguments.of("json", new TJSONProtocol.Factory()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("protocols")
  void testConsecutiveNestedMessagesWithinRecursionLimit(
      String protocolName, TProtocolFactory factory) throws Exception {
    TConfiguration config = new TConfiguration(MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE, 2);
    TMemoryBuffer buffer = new TMemoryBuffer(config, 1024);
    TProtocol writer = factory.getProtocol(buffer);
    OptimizingTask expected = task();
    expected.write(writer);
    expected.write(writer);

    TProtocol reader = factory.getProtocol(buffer);
    for (int message = 0; message < 2; message++) {
      OptimizingTask actual = new OptimizingTask();
      actual.read(reader);
      Assertions.assertEquals(expected, actual, protocolName + " message " + message);
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("protocols")
  void testNestedTaskRejectsInsufficientRecursionLimit(
      String protocolName, TProtocolFactory factory) throws Exception {
    byte[] bytes = new TSerializer(factory).serialize(task());
    TConfiguration config = new TConfiguration(MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE, 1);
    TProtocol reader = factory.getProtocol(new TMemoryInputTransport(config, bytes));

    TProtocolException failure =
        Assertions.assertThrows(
            TProtocolException.class, () -> new OptimizingTask().read(reader), protocolName);
    Assertions.assertEquals(TProtocolException.DEPTH_LIMIT, failure.getType());
  }

  private static OptimizingTask task() {
    return new OptimizingTask()
        .setTaskId(new OptimizingTaskId(250L, 7))
        .setTaskInput("Amoro 中文".getBytes(StandardCharsets.UTF_8))
        .setProperties(Collections.singletonMap("thrift", "0.25.0"));
  }
}
