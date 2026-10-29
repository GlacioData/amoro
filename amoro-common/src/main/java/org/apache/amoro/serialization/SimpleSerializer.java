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

package org.apache.amoro.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class SimpleSerializer<R> implements ResourceSerde<R> {

  @Override
  public byte[] serializeResource(R resource) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    serializeResource(resource, bos);
    return bos.toByteArray();
  }

  /**
   * Serializes {@code resource} directly onto {@code out} without materializing an intermediate
   * byte array. The stream is flushed but not closed; closing remains the caller's choice so it can
   * chain compression around it.
   */
  public void serializeResource(R resource, OutputStream out) {
    try (ObjectOutputStream oos =
        new ObjectOutputStream(out) {
          // ObjectOutputStream.close() would close the caller's stream; flush instead so the
          // documented contract ("flushed but not closed") holds.
          @Override
          public void close() throws IOException {
            flush();
          }
        }) {
      oos.writeObject(resource);
      oos.flush();
    } catch (IOException e) {
      throw new IllegalArgumentException("serialization error of " + resource, e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public R deserializeResource(byte[] input) {
    if (input == null) {
      return null;
    }
    try (ByteArrayInputStream bis = new ByteArrayInputStream(input);
        ObjectInputStream ois = new ObjectInputStream(bis)) {
      return (R) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalArgumentException("deserialization error ", e);
    }
  }
}
