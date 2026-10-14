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

package org.apache.amoro.server.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.amoro.config.Configurations;
import org.apache.amoro.server.AmoroManagementConf;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CasbinAuthorizationManagerTest {
  private static final Set<String> ADMIN_PRIVILEGES =
      new HashSet<>(
          Arrays.asList(
              "VIEW_SYSTEM",
              "VIEW_CATALOG",
              "VIEW_TABLE",
              "VIEW_OPTIMIZER",
              "MANAGE_CATALOG",
              "MANAGE_TABLE",
              "MANAGE_OPTIMIZER",
              "MANAGE_PLATFORM"));

  @Test
  public void testResolvePrivilegesForBuiltInRoles() {
    Configurations conf = new Configurations();
    conf.set(AmoroManagementConf.AUTHORIZATION_ENABLED, true);
    CasbinAuthorizationManager manager = new CasbinAuthorizationManager(conf);
    assertEquals(
        new HashSet<>(Arrays.asList("VIEW_CATALOG", "VIEW_TABLE", "VIEW_OPTIMIZER")),
        manager.resolvePrivileges(Collections.singleton(Role.VIEWER)));
    assertEquals(
        ADMIN_PRIVILEGES, manager.resolvePrivileges(Collections.singleton(Role.SERVICE_ADMIN)));
    assertEquals(Collections.emptySet(), manager.resolvePrivileges(Collections.emptySet()));
  }

  @Test
  public void testAuthorizationDisabledRetainsAllManagementPrivileges() {
    Configurations conf = new Configurations();
    conf.set(AmoroManagementConf.AUTHORIZATION_ENABLED, false);
    CasbinAuthorizationManager manager = new CasbinAuthorizationManager(conf);
    assertEquals(ADMIN_PRIVILEGES, manager.resolvePrivileges(Collections.singleton(Role.VIEWER)));
    assertEquals(
        ADMIN_PRIVILEGES, manager.resolvePrivileges(Collections.singleton(Role.SERVICE_ADMIN)));
    assertEquals(ADMIN_PRIVILEGES, manager.resolvePrivileges(Collections.emptySet()));
  }
}
