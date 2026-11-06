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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mysql.cj.conf.ConnectionPropertiesTransform;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

public class TestDataSourceFactory {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:mysql://localhost/db",
        "jdbc:mysql://localhost/db?connectTimeout=5000&socketTimeout=0",
        "jdbc:mysql://localhost/db?connectTimeout=0&socketTimeout=10000",
        "jdbc:mysql://localhost/db?connectTimeout=oops&socketTimeout=10000&password=secret-test",
        "jdbc:mysql://localhost/db?connectTimeout=5000&socketTimeout=10000&autoReconnect=true",
        "jdbc:mysql://localhost/db?connectTimeout=5000&socketTimeout=10000&autoReconnectForPools=true",
        "jdbc:mysql://address=(host=localhost)(socketTimeout=0)/db?connectTimeout=5000&socketTimeout=10000"
      })
  void rejectsUnsafeEffectiveMySqlProperties(String url) {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> DataSourceFactory.createDataSource(mysqlConfig(url)));
    assertTrue(failure.getMessage().contains("database.url"));
    assertFalse(failure.toString().contains("secret-test"));
    assertNull(failure.getCause(), "Driver parsing errors may expose credentials");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:mysql://localhost/db?connectTimeout=5000&socketTimeout=10000",
        "jdbc:mysql://localhost/db?connectTimeout=5000&socketTimeout=10000&autoReconnect=false&autoReconnectForPools=false",
        "jdbc:mysql://address=(host=localhost)(socketTimeout=2000)/db?connectTimeout=5000&socketTimeout=10000"
      })
  void acceptsFiniteMySqlPropertiesWithoutConnecting(String url) throws Exception {
    try (BasicDataSource ds =
        (BasicDataSource) DataSourceFactory.createDataSource(mysqlConfig(url))) {
      assertEquals(url, ds.getUrl());
      assertTrue(ds.getTestOnBorrow());
      assertEquals(2, ds.getValidationQueryTimeout());
      assertNull(ds.getValidationQuery());
      assertTrue(ds.getFastFailValidation());
      assertFalse(ds.getAutoCommitOnReturn());
      assertTrue(ds.getRollbackOnReturn());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"socketTimeout", "connectTimeout", "autoReconnect", "autoReconnectForPools"})
  void rejectsUnsafePropertiesProducedFromConfiguredCredentials(String unsafeProperty) {
    Configurations config = mysqlConfig(transformUrl(unsafeProperty));
    config.set(AmoroManagementConf.DB_USER_NAME, "policy-test-user");
    config.set(AmoroManagementConf.DB_PASSWORD, "secret-transform-test");
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class, () -> DataSourceFactory.createDataSource(config));
    assertTrue(failure.getMessage().contains("database.url"));
    assertFalse(failure.toString().contains("secret-transform-test"));
    assertFalse(failure.toString().contains("policy-test-user"));
    assertNull(failure.getCause());
  }

  @Test
  void acceptsSafeCredentialDependentPropertiesWithoutConnecting() throws Exception {
    Configurations config = mysqlConfig(transformUrl("socketTimeout"));
    config.set(AmoroManagementConf.DB_USER_NAME, "safe-user");
    config.set(AmoroManagementConf.DB_PASSWORD, "secret-transform-test");
    try (BasicDataSource ds = (BasicDataSource) DataSourceFactory.createDataSource(config)) {
      assertEquals("safe-user", ds.getUsername());
      assertEquals(0, ds.getNumActive());
    }
  }

  private String transformUrl(String unsafeProperty) {
    return "jdbc:mysql://localhost/db?connectTimeout=5000&socketTimeout=10000"
        + "&propertiesTransform="
        + CredentialsDependentTransform.class.getName()
        + "&policyTestUnsafeProperty="
        + unsafeProperty;
  }

  public static class CredentialsDependentTransform implements ConnectionPropertiesTransform {
    @Override
    public Properties transformProperties(Properties input) {
      Properties transformed = new Properties();
      transformed.putAll(input);
      if ("policy-test-user".equals(input.getProperty("user"))
          && "secret-transform-test".equals(input.getProperty("password"))) {
        String property = input.getProperty("policyTestUnsafeProperty");
        transformed.setProperty(property, property.endsWith("Timeout") ? "0" : "true");
      }
      return transformed;
    }
  }

  private Configurations mysqlConfig(String url) {
    Configurations config = new Configurations();
    config.set(AmoroManagementConf.DB_TYPE, AmoroManagementConf.DB_TYPE_MYSQL);
    config.set(AmoroManagementConf.DB_CONNECTION_URL, url);
    config.set(AmoroManagementConf.DB_DRIVER_CLASS_NAME, "com.mysql.cj.jdbc.Driver");
    config.set(AmoroManagementConf.DB_AUTO_CREATE_TABLES, false);
    return config;
  }
}
