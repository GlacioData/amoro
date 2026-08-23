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

package org.apache.amoro;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.amoro.process.engine.ProcessEngineRegistry;
import org.apache.amoro.process.rest.ProcessActionCatalog;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      // mybatis-spring-boot-starter's auto-configuration demands a DataSource; the skeleton
      // app has none of its own yet, so the smoke test supplies an embedded Derby one (T10
      // wires the real datasource assembly)
      "spring.datasource.url=jdbc:derby:memory:amoroV2AppSmoke;create=true",
      "spring.datasource.driver-class-name=org.apache.derby.iapi.jdbc.AutoloadedDriver",
      "spring.sql.init.mode=never"
    })
class AmoroAmsV2ApplicationTests {

  @LocalServerPort private int port;

  @Autowired private TestRestTemplate restTemplate;

  @Autowired private ProcessEngineRegistry engines;

  @Autowired private ProcessActionCatalog actions;

  @Test
  void contextLoadsAndHealthEndpointResponds() {
    String body = restTemplate.getForObject("/api/ams/v2/health", String.class);
    assertThat(body).contains("\"status\":\"UP\"").contains("amoro-ams-v2");
  }

  @Test
  void defaultContextPublishesNoProcessEngineOrAction() {
    assertThat(engines.engines()).isEmpty();
    assertThat(actions.actions()).isEmpty();
  }
}
