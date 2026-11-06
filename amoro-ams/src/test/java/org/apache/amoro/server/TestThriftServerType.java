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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.amoro.api.OptimizingService;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.TServer;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TSocket;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestThriftServerType {

  private static int ephemeralPort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @Test
  public void testServerIsThreadedSelector() throws Exception {
    ExecutorService executor = Executors.newCachedThreadPool();
    TServer server =
        AmoroServiceContainer.createThriftServer(
            new OptimizingService.Processor<>(mock(OptimizingService.Iface.class)),
            "OptimizingService",
            "127.0.0.1",
            ephemeralPort(),
            executor,
            1,
            4,
            100 * 1024 * 1024,
            null);
    try {
      Assertions.assertTrue(
          server instanceof TThreadedSelectorServer,
          "expected TThreadedSelectorServer but was " + server.getClass().getName());
    } finally {
      server.stop();
      executor.shutdownNow();
    }
  }

  @Test
  public void testRejectsNonPositiveSelectorConfig() {
    ExecutorService executor = Executors.newCachedThreadPool();
    IllegalArgumentException zeroSelectors =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                AmoroServiceContainer.createThriftServer(
                    new OptimizingService.Processor<>(mock(OptimizingService.Iface.class)),
                    "OptimizingService",
                    "127.0.0.1",
                    1261,
                    executor,
                    0,
                    4,
                    100 * 1024 * 1024,
                    null));
    Assertions.assertTrue(
        zeroSelectors.getMessage().contains("thrift-server.selector-thread-count"),
        "message should name the config key: " + zeroSelectors.getMessage());

    IllegalArgumentException zeroQueue =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                AmoroServiceContainer.createThriftServer(
                    new OptimizingService.Processor<>(mock(OptimizingService.Iface.class)),
                    "OptimizingService",
                    "127.0.0.1",
                    1261,
                    executor,
                    1,
                    0,
                    100 * 1024 * 1024,
                    null));
    Assertions.assertTrue(
        zeroQueue.getMessage().contains("thrift-server.selector-queue-size"),
        "message should name the config key: " + zeroQueue.getMessage());
    executor.shutdownNow();
  }

  @Test
  public void testFramedBinaryRoundtrip() throws Exception {
    OptimizingService.Iface service = mock(OptimizingService.Iface.class);
    int port = ephemeralPort();
    ExecutorService executor = Executors.newCachedThreadPool();
    TServer server =
        AmoroServiceContainer.createThriftServer(
            new OptimizingService.Processor<>(service),
            "OptimizingService",
            "127.0.0.1",
            port,
            executor,
            1,
            4,
            100 * 1024 * 1024,
            null);
    Thread serveThread = new Thread(server::serve, "test-thrift-serve");
    serveThread.start();
    try (TFramedTransport transport = new TFramedTransport(new TSocket("127.0.0.1", port, 5000))) {
      transport.open();
      TMultiplexedProtocol protocol =
          new TMultiplexedProtocol(new TBinaryProtocol(transport), "OptimizingService");
      new OptimizingService.Client(protocol).ping();
      verify(service).ping();
    } finally {
      server.stop();
      serveThread.join(10_000);
      executor.shutdownNow();
    }
  }
}
