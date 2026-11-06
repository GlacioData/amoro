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

package org.apache.amoro.client;

import org.apache.amoro.Constants;
import org.apache.amoro.api.OptimizingService;
import org.apache.amoro.api.OptimizingTask;
import org.apache.amoro.api.OptimizingTaskId;
import org.apache.amoro.shade.thrift.org.apache.thrift.TException;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TField;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TMessage;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TMessageType;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TStruct;
import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TType;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TSocket;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransportException;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestMessageSizeViolation {
  private static final String HOST = "127.0.0.1";
  private static final int TIMEOUT_MS = 5000;

  private ServerSocket listener;
  private ExecutorService peerExecutor;

  @BeforeEach
  public void startPeer() throws IOException {
    listener = new ServerSocket(0, 1, InetAddress.getByName(HOST));
    listener.setSoTimeout(TIMEOUT_MS);
    peerExecutor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  public void stopPeer() throws IOException {
    try {
      listener.close();
    } finally {
      peerExecutor.shutdownNow();
    }
  }

  @Test
  public void testMalformedResponseKeepsOriginalReadBudgetError() throws Exception {
    Future<?> peer =
        servePollReply(
            protocol -> {
              protocol.writeStructBegin(new TStruct("pollTask_result"));
              protocol.writeFieldBegin(new TField("success", TType.STRUCT, (short) 0));
              protocol.writeStructBegin(new TStruct("OptimizingTask"));
              protocol.writeFieldBegin(new TField("taskInput", TType.STRING, (short) 2));
              // The declared binary length fits the configured maximum but not this frame.
              protocol.writeI32(512);
            });

    OptimizingService.Iface client = connect(1024);
    TTransportException failure =
        Assertions.assertThrows(TTransportException.class, () -> client.pollTask("token", 1));
    Assertions.assertEquals(TTransportException.MESSAGE_SIZE_LIMIT, failure.getType());
    peer.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testOversizedResponseFrameIdentifiesClientLimitAndPreservesCause() throws Exception {
    Future<?> peer =
        servePollReply(
            protocol -> {
              OptimizingTask task =
                  new OptimizingTask(new OptimizingTaskId(1L, 1)).setTaskInput(new byte[512]);
              new OptimizingService.pollTask_result().setSuccess(task).write(protocol);
            });

    OptimizingService.Iface client = connect(128);
    ThriftException failure =
        Assertions.assertThrows(ThriftException.class, () -> client.pollTask("token", 1));
    Assertions.assertTrue(
        failure.getMessage().contains("maxMessageSize=128"), failure.getMessage());
    TTransportException cause =
        Assertions.assertInstanceOf(TTransportException.class, failure.getCause());
    Assertions.assertEquals(TTransportException.CORRUPTED_DATA, cause.getType());
    peer.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private OptimizingService.Iface connect(int maxMessageSize) {
    return OptimizingClientPools.getClient(
        "thrift://"
            + HOST
            + ":"
            + listener.getLocalPort()
            + "?maxMessageSize="
            + maxMessageSize
            + "&autoReconnect=false&connectTimeout="
            + TIMEOUT_MS
            + "&socketTimeout="
            + TIMEOUT_MS);
  }

  private Future<?> servePollReply(ReplyWriter writer) {
    return peerExecutor.submit(
        () -> {
          try (Socket socket = listener.accept();
              TFramedTransport transport = new TFramedTransport(new TSocket(socket))) {
            socket.setSoTimeout(TIMEOUT_MS);
            TProtocol protocol = new TBinaryProtocol(transport);
            TMessage ping = readRequest(protocol, "ping");
            protocol.writeMessageBegin(new TMessage("ping", TMessageType.REPLY, ping.seqid));
            new OptimizingService.ping_result().write(protocol);
            protocol.writeMessageEnd();
            transport.flush();

            TMessage poll = readRequest(protocol, "pollTask");
            protocol.writeMessageBegin(new TMessage("pollTask", TMessageType.REPLY, poll.seqid));
            writer.write(protocol);
            protocol.writeMessageEnd();
            transport.flush();

            try {
              Assertions.assertEquals(
                  -1,
                  socket.getInputStream().read(),
                  "The failed RPC must close its pooled socket");
            } catch (SocketException reset) {
              // Closing before consuming an oversized frame can reset the socket instead of EOF.
            }
          }
          return null;
        });
  }

  private static TMessage readRequest(TProtocol protocol, String method) throws TException {
    TMessage request = protocol.readMessageBegin();
    Assertions.assertEquals(Constants.THRIFT_OPTIMIZING_SERVICE_NAME + ":" + method, request.name);
    protocol.skip(TType.STRUCT);
    protocol.readMessageEnd();
    return request;
  }

  private interface ReplyWriter {
    void write(TProtocol protocol) throws TException;
  }
}
