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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import org.apache.amoro.api.OptimizingService;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.ServerContext;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.TServer;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TNonblockingSocket;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TSocket;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransportException;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public class TestThriftServerLifecycle {

  private static final String HOST = "127.0.0.1";
  private static final String SERVICE = "OptimizingService";
  private static final int TIMEOUT_MILLIS = 5000;

  @Test
  public void testInFlightRpcSurvivesSelectorRebuild() throws Exception {
    OptimizingService.Iface service = mock(OptimizingService.Iface.class);
    CountDownLatch processing = new CountDownLatch(1);
    CountDownLatch releaseRpc = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              processing.countDown();
              await(releaseRpc, "RPC was never released");
              return null;
            })
        .when(service)
        .ping();

    RebuildOnNextConnection handler = new RebuildOnNextConnection();
    RecordingServerSocket serverSocket = new RecordingServerSocket(2);
    ExecutorService workers = Executors.newSingleThreadExecutor();
    TServer server = createServer(serverSocket, service, workers, handler);
    Thread serving = startServer(server);
    try (TFramedTransport transport = openTransport(serverSocket.getPort())) {
      OptimizingService.Client client = client(transport);
      client.send_ping();
      await(processing, "worker never received the in-flight ping");

      handler.trigger.set(true);
      try (TFramedTransport wakeup = openTransport(serverSocket.getPort())) {
        Selector original = handler.originalSelector.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        awaitCondition(() -> !original.isOpen(), "selector was not rebuilt");
        releaseRpc.countDown();

        client.recv_ping();
        client.ping();
        client(wakeup).ping();
      }
    } finally {
      releaseRpc.countDown();
      closeServer(server, serving, serverSocket, workers);
    }
  }

  @Test
  public void testStopClosesConnectionsWhenAcceptQueueIsFull() throws Exception {
    CountDownLatch registering = new CountDownLatch(1);
    CountDownLatch releaseRegistration = new CountDownLatch(1);
    ThriftConnectionEventHandler handler =
        new ThriftConnectionEventHandler(SERVICE) {
          @Override
          public ServerContext createContext(TProtocol input, TProtocol output) {
            registering.countDown();
            await(releaseRegistration, "registration was never released");
            return super.createContext(input, output);
          }
        };
    RecordingServerSocket serverSocket = new RecordingServerSocket(3);
    ExecutorService workers = Executors.newSingleThreadExecutor();
    TServer server =
        createServer(serverSocket, mock(OptimizingService.Iface.class), workers, handler);
    Thread serving = startServer(server);
    try (Socket registeringSocket = connect(serverSocket.getPort())) {
      await(registering, "selector did not start registering the first connection");
      Set<?> selectors =
          (Set<?>) field(TThreadedSelectorServer.class, "selectorThreads").get(server);
      Assertions.assertEquals(1, selectors.size(), "server must use the configured selector count");
      Object selectorThread = selectors.iterator().next();
      BlockingQueue<?> queue =
          (BlockingQueue<?>) field(selectorThread.getClass(), "acceptedQueue").get(selectorThread);
      Assertions.assertEquals(1, queue.remainingCapacity(), "wrong accept queue capacity");

      try (Socket queuedSocket = connect(serverSocket.getPort());
          Socket overflowSocket = connect(serverSocket.getPort())) {
        await(serverSocket.acceptedConnections, "accept thread did not receive all connections");
        Assertions.assertEquals(
            1, queue.size(), "expected one connection waiting for registration");
        Thread acceptThread =
            (Thread) field(TThreadedSelectorServer.class, "acceptThread").get(server);
        awaitCondition(
            () ->
                Arrays.stream(acceptThread.getStackTrace())
                    .anyMatch(
                        frame ->
                            frame.getClassName().equals(ArrayBlockingQueue.class.getName())
                                && (frame.getMethodName().equals("offer")
                                    || frame.getMethodName().equals("put"))),
            "accept thread did not wait on the full registration queue");

        server.stop();
        releaseRegistration.countDown();

        serving.join(TIMEOUT_MILLIS);
        Assertions.assertFalse(serving.isAlive(), "serve is still waiting for the accept thread");
        Assertions.assertFalse(server.isServing());
        Assertions.assertTrue(workers.isTerminated());
        Assertions.assertEquals(-1, registeringSocket.getInputStream().read());
        Assertions.assertEquals(-1, queuedSocket.getInputStream().read());
        Assertions.assertEquals(-1, overflowSocket.getInputStream().read());
      }
    } finally {
      releaseRegistration.countDown();
      closeServer(server, serving, serverSocket, workers);
    }
  }

  private static TServer createServer(
      RecordingServerSocket serverSocket,
      OptimizingService.Iface service,
      ExecutorService workers,
      ThriftConnectionEventHandler handler) {
    return AmoroServiceContainer.createThriftServer(
        new OptimizingService.Processor<>(service),
        SERVICE,
        serverSocket,
        workers,
        1,
        1,
        100 * 1024 * 1024,
        handler);
  }

  private static Thread startServer(TServer server) {
    Thread serving = new Thread(server::serve, "test-thrift-server-lifecycle");
    serving.setDaemon(true);
    serving.start();
    return serving;
  }

  private static void closeServer(
      TServer server, Thread serving, RecordingServerSocket serverSocket, ExecutorService workers)
      throws Exception {
    try {
      server.stop();
      serving.join(TIMEOUT_MILLIS);
      if (serving.isAlive()) {
        // Recover even against the unpatched library, where stop cannot wake a blocked enqueue.
        Thread acceptThread =
            (Thread) field(TThreadedSelectorServer.class, "acceptThread").get(server);
        if (acceptThread != null) {
          acceptThread.interrupt();
        }
        serving.join(TIMEOUT_MILLIS);
      }
    } finally {
      workers.shutdownNow();
      serverSocket.closeAcceptedConnections();
      serverSocket.close();
    }
  }

  private static TFramedTransport openTransport(int port) throws TTransportException {
    TFramedTransport transport = new TFramedTransport(new TSocket(HOST, port, TIMEOUT_MILLIS));
    transport.open();
    return transport;
  }

  private static OptimizingService.Client client(TFramedTransport transport) {
    return new OptimizingService.Client(
        new TMultiplexedProtocol(new TBinaryProtocol(transport), SERVICE));
  }

  private static Socket connect(int port) throws Exception {
    Socket socket = new Socket(HOST, port);
    socket.setSoTimeout(TIMEOUT_MILLIS);
    return socket;
  }

  private static void await(CountDownLatch latch, String message) {
    try {
      Assertions.assertTrue(latch.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS), message);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(message, e);
    }
  }

  private static void awaitCondition(BooleanSupplier ready, String message)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS);
    while (!ready.getAsBoolean() && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(1);
    }
    Assertions.assertTrue(ready.getAsBoolean(), message);
  }

  private static Field field(Class<?> type, String name) throws NoSuchFieldException {
    Field field = type.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }

  private static class RebuildOnNextConnection extends ThriftConnectionEventHandler {
    private final AtomicBoolean trigger = new AtomicBoolean();
    private final CompletableFuture<Selector> originalSelector = new CompletableFuture<>();

    RebuildOnNextConnection() {
      super(SERVICE);
    }

    @Override
    public ServerContext createContext(TProtocol input, TProtocol output) {
      if (trigger.getAndSet(false)) {
        Thread selectorThread = Thread.currentThread();
        Class<?> type = selectorThread.getClass();
        try {
          // Start one empty select away from the rebuild threshold, on the selector's own thread.
          Field threshold = field(type, "SELECTOR_AUTO_REBUILD_THRESHOLD");
          field(type, "jvmBug").setInt(selectorThread, threshold.getInt(null));
          Selector selector =
              (Selector) field(type.getSuperclass(), "selector").get(selectorThread);
          selector.wakeup();
          originalSelector.complete(selector);
        } catch (ReflectiveOperationException e) {
          originalSelector.completeExceptionally(e);
        }
      }
      return super.createContext(input, output);
    }
  }

  private static class RecordingServerSocket extends TNonblockingServerSocket {
    private final Queue<TNonblockingSocket> sockets = new ConcurrentLinkedQueue<>();
    private final CountDownLatch acceptedConnections;

    RecordingServerSocket(int expectedConnections) throws TTransportException {
      super(new InetSocketAddress(HOST, 0));
      acceptedConnections = new CountDownLatch(expectedConnections);
    }

    @Override
    public TNonblockingSocket accept() throws TTransportException {
      TNonblockingSocket accepted = super.accept();
      if (accepted != null) {
        sockets.add(accepted);
        acceptedConnections.countDown();
      }
      return accepted;
    }

    void closeAcceptedConnections() {
      sockets.forEach(TNonblockingSocket::close);
    }
  }
}
