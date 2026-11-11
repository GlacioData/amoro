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

package org.apache.amoro.server.persistence;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** A plaintext, packet-aware MySQL proxy for silent-network fault tests only. */
final class MySqlFaultProxy implements AutoCloseable {
  enum Mode {
    DROP_REQUEST,
    DROP_RESPONSE
  }

  private final String backendHost;
  private final int backendPort;
  private final ServerSocket listener;
  private final ExecutorService workers;
  private final Set<SocketPair> connections = ConcurrentHashMap.newKeySet();
  private final List<String> commands = new CopyOnWriteArrayList<>();
  private final List<Throwable> unexpectedFailures = new CopyOnWriteArrayList<>();
  private final AtomicInteger acceptedConnections = new AtomicInteger();
  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile Fault fault;

  MySqlFaultProxy(String backendHost, int backendPort) throws IOException {
    this.backendHost = backendHost;
    this.backendPort = backendPort;
    listener = new ServerSocket(0, 20, InetAddress.getLoopbackAddress());
    AtomicInteger sequence = new AtomicInteger();
    workers =
        Executors.newCachedThreadPool(
            runnable -> {
              Thread thread =
                  new Thread(runnable, "mysql-fault-proxy-" + sequence.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    workers.submit(this::accept);
  }

  int port() {
    return listener.getLocalPort();
  }

  int acceptedConnections() {
    return acceptedConnections.get();
  }

  int openConnections() {
    return connections.size();
  }

  long commandCount(String command) {
    String normalized = normalize(command);
    return commands.stream().filter(value -> value.startsWith(normalized)).count();
  }

  Fault arm(String command, Mode mode) {
    Fault next = new Fault(normalize(command), mode, false);
    fault = next;
    return next;
  }

  Fault armPersistent(String command, Mode mode) {
    Fault next = new Fault(normalize(command), mode, true);
    fault = next;
    return next;
  }

  void clearFault() {
    fault = null;
  }

  boolean awaitOpenConnections(int expected, long timeoutMillis) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    synchronized (connections) {
      while (connections.size() != expected && System.nanoTime() < deadline) {
        TimeUnit.NANOSECONDS.timedWait(connections, Math.max(1, deadline - System.nanoTime()));
      }
      return connections.size() == expected;
    }
  }

  boolean terminated() {
    return workers.isTerminated();
  }

  List<Throwable> unexpectedFailures() {
    return unexpectedFailures;
  }

  private void accept() {
    while (!closed.get()) {
      Socket frontend = null;
      try {
        frontend = listener.accept();
        Socket backend = new Socket(backendHost, backendPort);
        frontend.setTcpNoDelay(true);
        backend.setTcpNoDelay(true);
        SocketPair pair = new SocketPair(frontend, backend);
        synchronized (connections) {
          if (closed.get()) {
            pair.close();
          } else {
            connections.add(pair);
            acceptedConnections.incrementAndGet();
            workers.submit(() -> transfer(pair, true));
            workers.submit(() -> transfer(pair, false));
          }
        }
      } catch (IOException failure) {
        closeSocket(frontend);
        if (!closed.get()) {
          unexpectedFailures.add(failure);
        }
      }
    }
  }

  private void transfer(SocketPair pair, boolean clientToServer) {
    Socket source = clientToServer ? pair.frontend : pair.backend;
    Socket destination = clientToServer ? pair.backend : pair.frontend;
    try {
      InputStream input = source.getInputStream();
      while (!pair.closed.get()) {
        byte[] packet = packet(input);
        if (packet == null) {
          return;
        }
        if (clientToServer && pair.authenticated.get() && packet.length > 4) {
          int opcode = packet[4] & 0xff;
          String command = null;
          if (opcode == 3) {
            // MySQL 8 negotiates CLIENT_QUERY_ATTRIBUTES. These fixtures set no attributes,
            // so Connector/J prefixes SQL with parameter_count=0 and parameter_set_count=1.
            int offset = packet.length > 6 && packet[5] == 0 && packet[6] == 1 ? 7 : 5;
            command =
                normalize(
                    new String(packet, offset, packet.length - offset, StandardCharsets.UTF_8));
          } else if (opcode == 14) {
            command = "PING";
          }
          if (command != null) {
            commands.add(command);
            Fault current = fault;
            if (current != null && current.claim(command)) {
              if (current.mode == Mode.DROP_REQUEST) {
                continue;
              }
              pair.dropResponse.set(true);
            }
          }
        } else if (!clientToServer) {
          if (pair.dropResponse.compareAndSet(true, false)) {
            continue;
          }
          // An authentication OK packet precedes the command phase (including auth switches).
          if (!pair.authenticated.get() && packet.length > 4 && packet[4] == 0) {
            pair.authenticated.set(true);
          }
        }
        destination.getOutputStream().write(packet);
        destination.getOutputStream().flush();
      }
    } catch (SocketException | EOFException ignored) {
      // The driver closes a timed-out socket; closing either direction unblocks its peer.
    } catch (IOException failure) {
      if (!pair.closed.get() && !closed.get()) {
        unexpectedFailures.add(failure);
      }
    } finally {
      pair.close();
    }
  }

  private static byte[] packet(InputStream input) throws IOException {
    int first = input.read();
    if (first < 0) {
      return null;
    }
    byte[] header = new byte[4];
    header[0] = (byte) first;
    readFully(input, header, 1, 3);
    int size = first | ((header[1] & 0xff) << 8) | ((header[2] & 0xff) << 16);
    byte[] packet = new byte[4 + size];
    System.arraycopy(header, 0, packet, 0, 4);
    readFully(input, packet, 4, size);
    return packet;
  }

  private static void readFully(InputStream input, byte[] bytes, int offset, int count)
      throws IOException {
    while (count > 0) {
      int read = input.read(bytes, offset, count);
      if (read < 0) {
        throw new EOFException("Incomplete MySQL packet");
      }
      offset += read;
      count -= read;
    }
  }

  private static String normalize(String command) {
    return command.replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
  }

  private static void closeSocket(Socket socket) {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException ignored) {
        // Best-effort socket cleanup; worker termination is asserted by the caller.
      }
    }
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
    listener.close();
    synchronized (connections) {
      connections.forEach(SocketPair::close);
      workers.shutdown();
    }
    if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
      workers.shutdownNow();
      throw new IllegalStateException("MySQL fault proxy workers did not exit");
    }
  }

  static final class Fault {
    private final String command;
    private final Mode mode;
    private final boolean persistent;
    private final AtomicInteger hits = new AtomicInteger();

    private Fault(String command, Mode mode, boolean persistent) {
      this.command = command;
      this.mode = mode;
      this.persistent = persistent;
    }

    private boolean claim(String actual) {
      if (!actual.startsWith(command)) {
        return false;
      }
      if (persistent) {
        hits.incrementAndGet();
        return true;
      }
      return hits.compareAndSet(0, 1);
    }

    int hits() {
      return hits.get();
    }
  }

  private final class SocketPair {
    private final Socket frontend;
    private final Socket backend;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean authenticated = new AtomicBoolean();
    private final AtomicBoolean dropResponse = new AtomicBoolean();

    private SocketPair(Socket frontend, Socket backend) {
      this.frontend = frontend;
      this.backend = backend;
    }

    private void close() {
      if (closed.compareAndSet(false, true)) {
        closeSocket(frontend);
        closeSocket(backend);
        connections.remove(this);
        synchronized (connections) {
          connections.notifyAll();
        }
      }
    }
  }
}
