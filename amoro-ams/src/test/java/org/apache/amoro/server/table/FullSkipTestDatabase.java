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

package org.apache.amoro.server.table;

import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.amoro.server.persistence.DataSourceFactory;
import org.apache.amoro.server.persistence.SqlSessionFactoryProvider;

/** Local disposable database fixture; defaults to the existing Derby test database. */
final class FullSkipTestDatabase {
  private static boolean initialized;

  private FullSkipTestDatabase() {}

  static synchronized void initialize() {
    if (initialized) {
      return;
    }
    String type = System.getProperty("jspt2717.test.db", "derby");
    if ("derby".equals(type)) {
      new DerbyPersistence();
    } else {
      if (!"mysql".equals(type) && !"postgres".equals(type)) {
        throw new IllegalArgumentException("Unsupported disposable test database: " + type);
      }
      int port = Integer.parseInt(System.getProperty("jspt2717.test.port"));
      String protocol = "mysql".equals(type) ? "mysql" : "postgresql";
      String url = "jdbc:" + protocol + "://127.0.0.1:" + port + "/jspt2717";
      if ("mysql".equals(type)) {
        url += "?useSSL=false&allowPublicKeyRetrieval=true";
      }
      Configurations config = new Configurations();
      config.set(AmoroManagementConf.DB_TYPE, type);
      config.set(AmoroManagementConf.DB_CONNECTION_URL, url);
      config.set(
          AmoroManagementConf.DB_DRIVER_CLASS_NAME,
          "mysql".equals(type) ? "com.mysql.cj.jdbc.Driver" : "org.postgresql.Driver");
      config.set(AmoroManagementConf.DB_USER_NAME, "amoro");
      config.set(AmoroManagementConf.DB_PASSWORD, "local-test-only");
      try {
        SqlSessionFactoryProvider.getInstance().init(DataSourceFactory.createDataSource(config));
      } catch (java.sql.SQLException failure) {
        throw new IllegalStateException("Failed to initialize disposable test database", failure);
      }
    }
    initialized = true;
  }
}
