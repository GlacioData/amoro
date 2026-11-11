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

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DefaultPropertySet;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;

import java.util.List;
import java.util.Properties;

/** Checks the effective driver properties before the first physical connection is created. */
final class MySqlConnectionPolicy {
  private MySqlConnectionPolicy() {}

  static void validate(String url, String username, String password) {
    Properties connectionProperties = new Properties();
    // Match DBCP's explicit driver properties, including credential-dependent URL transforms.
    if (username != null) {
      connectionProperties.setProperty(PropertyKey.USER.getKeyName(), username);
    }
    if (password != null) {
      connectionProperties.setProperty(PropertyKey.PASSWORD.getKeyName(), password);
    }
    List<HostInfo> hosts;
    try {
      hosts = ConnectionUrl.getConnectionUrlInstance(url, connectionProperties).getHostsList();
    } catch (RuntimeException e) {
      // Driver URL parsing failures can include credentials. Do not propagate their text or cause.
      throw invalid("invalid MySQL connection properties");
    }
    if (hosts.isEmpty()) {
      throw invalid("a MySQL host is required");
    }
    for (HostInfo host : hosts) {
      DefaultPropertySet properties = new DefaultPropertySet();
      try {
        properties.initializeProperties(host.exposeAsProperties());
      } catch (RuntimeException e) {
        throw invalid("invalid MySQL connection properties");
      }
      if (properties.getBooleanProperty(PropertyKey.autoReconnect).getValue()
          || properties.getBooleanProperty(PropertyKey.autoReconnectForPools).getValue()) {
        throw invalid("autoReconnect and autoReconnectForPools must be false");
      }
      if (properties.getIntegerProperty(PropertyKey.connectTimeout).getValue() <= 0
          || properties.getIntegerProperty(PropertyKey.socketTimeout).getValue() <= 0) {
        throw invalid("connectTimeout and socketTimeout must be positive milliseconds");
      }
    }
  }

  private static IllegalArgumentException invalid(String detail) {
    return new IllegalArgumentException("Invalid database.url: " + detail);
  }
}
