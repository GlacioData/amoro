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

import org.apache.amoro.api.OptimizingService;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.ServerContext;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.TServer;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TSocket;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestThriftConnectionEventHandler {

  @Test
  public void testContextStoresRemoteAddress() {
    ThriftConnectionEventHandler.ConnectionContext context =
        new ThriftConnectionEventHandler.ConnectionContext();
    SocketAddress address = new InetSocketAddress(12345);
    context.setRemoteAddress(address);
    Assertions.assertSame(address, context.getRemoteAddress());
  }

  @Test
  public void testDeleteContextToleratesNullAddress() {
    new ThriftConnectionEventHandler("test")
        .deleteContext(new ThriftConnectionEventHandler.ConnectionContext(), null, null);
  }

  @Test
  public void testUnwrap() {
    ThriftConnectionEventHandler.ConnectionContext context =
        new ThriftConnectionEventHandler.ConnectionContext();
    Assertions.assertTrue(context.isWrapperFor(ServerContext.class));
    Assertions.assertSame(context, context.unwrap(ServerContext.class));
  }

  @Test
  public void testServerPopulatesRemoteAddress() throws Exception {
    RecordingHandler handler = new RecordingHandler();
    OptimizingService.Iface service = mock(OptimizingService.Iface.class);
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
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
            handler);
    Thread serveThread = new Thread(server::serve, "test-thrift-serve-handler");
    serveThread.start();
    try (TFramedTransport transport = new TFramedTransport(new TSocket("127.0.0.1", port, 5000))) {
      transport.open();
      TMultiplexedProtocol protocol =
          new TMultiplexedProtocol(new TBinaryProtocol(transport), "OptimizingService");
      new OptimizingService.Client(protocol).ping();
    } finally {
      server.stop();
      serveThread.join(10_000);
      executor.shutdownNow();
    }
    Assertions.assertTrue(handler.closed.await(10, TimeUnit.SECONDS), "deleteContext not fired");
    Assertions.assertNotNull(handler.remoteAddress, "remote address not populated");
    Assertions.assertTrue(
        handler.remoteAddress.toString().contains("127.0.0.1"),
        "unexpected remote address: " + handler.remoteAddress);
  }

  private static class RecordingHandler extends ThriftConnectionEventHandler {
    final CountDownLatch closed = new CountDownLatch(1);
    volatile SocketAddress remoteAddress;

    RecordingHandler() {
      super("OptimizingService");
    }

    @Override
    public void deleteContext(ServerContext context, TProtocol input, TProtocol output) {
      if (context instanceof ConnectionContext) {
        remoteAddress = ((ConnectionContext) context).getRemoteAddress();
      }
      closed.countDown();
    }
  }
}
