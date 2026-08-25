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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.engine.ProcessEngineRegistry;
import org.apache.amoro.process.rest.ApiError;
import org.apache.amoro.process.rest.ProcessActionCatalog;
import org.apache.amoro.resources.ProcessResource;
import org.apache.amoro.service.ProcessService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/** Proves that simulator providers are opt-in and format-neutral. */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
      "spring.datasource.url=jdbc:derby:memory:amoroV2SimulatedProviders;create=true",
      "spring.datasource.driver-class-name=org.apache.derby.iapi.jdbc.AutoloadedDriver",
      "spring.sql.init.mode=never",
      "amoro.process.simulation.enabled=true",
      "amoro.control.scheduler.delay-ms=25",
      "amoro.process.reconcile.poll-interval-ms=25"
    })
class SimulatedProcessProviderApplicationTests {

  @Autowired private ProcessEngineRegistry engines;

  @Autowired private ProcessActionCatalog actions;

  @Autowired private ProcessService rest;

  @Autowired private ProcessDomainAssembly assembly;

  @Test
  void explicitSimulationPublishesLocalAndRemoteDummyCapabilities() {
    assertThat(engines.engines().keySet()).containsExactlyInAnyOrder("local", "remote-spark");
    assertThat(actions.actions()).containsExactly("dummy-maintenance");
    assertThat(actions.supports("simulated", "dummy-maintenance", "local")).isTrue();
    assertThat(actions.supports("simulated", "dummy-maintenance", "remote-spark")).isTrue();
  }

  @Test
  void dummyActionBuilderDrivesLocalAndRemoteWhileUnknownTableIs404() throws Exception {
    ApiError missing =
        assertThrows(
            ApiError.class,
            () ->
                rest.create(
                    "simulated",
                    "other",
                    "table",
                    "missing-table",
                    "dummy-maintenance",
                    "local",
                    java.util.Collections.emptyMap()));
    assertEquals(404, missing.httpStatus());

    ProcessResource local =
        rest.create(
                "simulated",
                "demo",
                "table",
                "local-e2e",
                "dummy-maintenance",
                "local",
                java.util.Collections.singletonMap("value", 1))
            .toCompletableFuture()
            .join();
    ProcessResource localFinal = awaitFinal(local.name());
    assertEquals("simulated", localFinal.spec().table().tableFormat());
    assertEquals("action-plugin", localFinal.status().summary().result().get("submissionBuilder"));

    ProcessResource remote =
        rest.create(
                "simulated",
                "demo",
                "table",
                "remote-e2e",
                "dummy-maintenance",
                "remote-spark",
                java.util.Collections.singletonMap("value", 2))
            .toCompletableFuture()
            .join();
    ProcessResource remoteFinal = awaitFinal(remote.name());
    assertEquals("action-plugin", remoteFinal.status().summary().result().get("submissionBuilder"));
  }

  private ProcessResource awaitFinal(String name) throws Exception {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      ProcessResource resource = rest.get(name).toCompletableFuture().join();
      if ("SUCCESS".equals(resource.status().phase())) {
        while (assembly
            .indexProjection()
            .current()
            .activeProcessOf(resource.spec().table().tableId(), resource.spec().action())
            .isPresent()) {
          Thread.sleep(10L);
        }
        return resource;
      }
      Thread.sleep(10L);
    }
    throw new AssertionError("simulated process did not finish: " + name);
  }
}
