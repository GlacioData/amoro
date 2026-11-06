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

import org.apache.amoro.shade.thrift.org.apache.thrift.protocol.TProtocol;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.ServerContext;
import org.apache.amoro.shade.thrift.org.apache.thrift.server.TServerEventHandler;
import org.apache.amoro.shade.thrift.org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

/**
 * Logs the Thrift connection lifecycle of an AMS RPC service. The remote socket address is captured
 * through {@link ConnectionContext#setRemoteAddress(SocketAddress)}, which thrift invokes on the
 * context right after {@link #createContext(TProtocol, TProtocol)} for socket based transports; it
 * stays {@code null} for non-socket transports.
 */
public class ThriftConnectionEventHandler implements TServerEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftConnectionEventHandler.class);

  private final String processorName;

  public ThriftConnectionEventHandler(String processorName) {
    this.processorName = processorName;
  }

  @Override
  public void preServe() {
    LOG.info("Thrift server started: {}", processorName);
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    LOG.debug("Thrift connection established: {}", processorName);
    return new ConnectionContext();
  }

  @Override
  public void deleteContext(ServerContext context, TProtocol input, TProtocol output) {
    SocketAddress remoteAddress =
        context instanceof ConnectionContext
            ? ((ConnectionContext) context).getRemoteAddress()
            : null;
    LOG.info("Thrift connection closed: {}, remote: {}", processorName, remoteAddress);
  }

  @Override
  public void processContext(
      ServerContext context, TTransport inputTransport, TTransport outputTransport) {
    // no per-request handling needed
  }

  /** Connection context storing the remote address thrift assigns after connection accept. */
  public static class ConnectionContext implements ServerContext {
    private volatile SocketAddress remoteAddress;

    @Override
    public void setRemoteAddress(SocketAddress remoteAddress) {
      this.remoteAddress = remoteAddress;
    }

    public SocketAddress getRemoteAddress() {
      return remoteAddress;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      return iface.isInstance(this) ? iface.cast(this) : null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return iface.isInstance(this);
    }
  }
}
