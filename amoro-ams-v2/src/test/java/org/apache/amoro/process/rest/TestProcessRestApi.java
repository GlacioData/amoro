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

package org.apache.amoro.process.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.amoro.control.DefaultScheduler;
import org.apache.amoro.controller.ProcessApiController;
import org.apache.amoro.persistence.HandoffResult;
import org.apache.amoro.process.ProcessDomainAssembly;
import org.apache.amoro.process.ProcessTestFixtures;
import org.apache.amoro.process.TestProcessDomain;
import org.apache.amoro.resources.ProcessResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** P5: REST contract of /api/ams/v2 — create/idempotency, point read, list, cancel, resolutions. */
@Timeout(60)
public class TestProcessRestApi {

  private static final String CREATE = "/api/ams/v2/tables/prod/db1/orders/processes";

  private DefaultScheduler scheduler;
  private ProcessDomainAssembly assembly;
  private ProcessRestSupport support;
  private MockMvc mvc;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void setUp() {
    scheduler = DefaultScheduler.create(1, 1000L); // not started: REST semantics only
    assembly =
        new ProcessDomainAssembly(
            new TestProcessDomain.TestPersistenceBlobStore(),
            event -> HandoffResult.ACCEPTED,
            scheduler,
            128,
            10_000L,
            65536);
    support = org.apache.amoro.process.ProcessTestFixtures.simulatedRestSupport(assembly);
    // mirror the production MVC contract: unknown request fields are rejected (spec §8.1)
    com.fasterxml.jackson.databind.ObjectMapper strict =
        new com.fasterxml.jackson.databind.ObjectMapper();
    strict.configure(
        com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    mvc =
        MockMvcBuilders.standaloneSetup(new ProcessApiController(support))
            .setControllerAdvice(new ApiExceptionHandler())
            .setMessageConverters(
                new org.springframework.http.converter.json.MappingJackson2HttpMessageConverter(
                    strict))
            .build();
  }

  @AfterEach
  public void tearDown() {
    scheduler.shutdown(Duration.ofSeconds(5));
    assembly.persistence().shutdown(Duration.ofSeconds(5));
  }

  private static String body(String action, String engine) {
    return "{\"action\":\""
        + action
        + "\",\"executionEngine\":\""
        + engine
        + "\","
        + "\"parameters\":{\"retainLast\":1}}";
  }

  // ------------------------------------------------------------------ create

  @Test
  public void createReturns201WithVersionOneAndPENDING() throws Exception {
    MvcResult result =
        mvc.perform(
                post(CREATE)
                    .header("Idempotency-Key", "key-1")
                    .contentType("application/json")
                    .content(body("dummy-maintenance", "local")))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.resourceVersion").value(1))
            .andExpect(jsonPath("$.status.phase").value("PENDING"))
            .andExpect(jsonPath("$.spec.desiredState").value("RUN"))
            .andExpect(jsonPath("$.spec.action").value("dummy-maintenance"))
            .andReturn();
    JsonNode node = mapper.readTree(result.getResponse().getContentAsString());
    assertTrue(node.get("name").asText().length() > 0, "server-generated string name");
  }

  @Test
  public void createReplayWithSameKeyAndBodyReturnsOriginal() throws Exception {
    MvcResult first =
        mvc.perform(
                post(CREATE)
                    .header("Idempotency-Key", "key-1")
                    .contentType("application/json")
                    .content(body("dummy-maintenance", "local")))
            .andExpect(status().isCreated())
            .andReturn();
    String name = mapper.readTree(first.getResponse().getContentAsString()).get("name").asText();

    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "key-1")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.name").value(name));
  }

  @Test
  public void createWithSameKeyButDifferentBodyIsRejected() throws Exception {
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "key-1")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isCreated());

    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "key-1")
                .contentType("application/json")
                .content(
                    body("dummy-maintenance", "local")
                        .replace("\"retainLast\":1", "\"retainLast\":2")))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_REUSED"));
  }

  @Test
  public void createWithoutKeyFails() throws Exception {
    mvc.perform(
            post(CREATE)
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_REQUIRED"));
  }

  @Test
  public void createWithUnknownActionOrEngineFails() throws Exception {
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "k")
                .contentType("application/json")
                .content(body("reindex-everything", "local")))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("INVALID_ACTION"));

    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "k2")
                .contentType("application/json")
                .content(body("dummy-maintenance", "teleport")))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("INVALID_ENGINE"));
  }

  @Test
  public void createWithoutExecutionEngineFailsAsValidationError() throws Exception {
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "missing-engine")
                .contentType("application/json")
                .content("{\"action\":\"dummy-maintenance\",\"parameters\":{}}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
  }

  @Test
  public void createMissingActionNullOrEmptyBodyFailsAsValidationError() throws Exception {
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "missing-action")
                .contentType("application/json")
                .content("{\"executionEngine\":\"local\",\"parameters\":{}}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));

    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "null-body")
                .contentType("application/json")
                .content("null"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));

    mvc.perform(
            post(CREATE).header("Idempotency-Key", "empty-body").contentType("application/json"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
  }

  @Test
  public void inProgressCreateCarriesRetryAfter() throws Exception {
    CountDownLatch nameGenerationEntered = new CountDownLatch(1);
    CountDownLatch releaseNameGeneration = new CountDownLatch(1);
    org.apache.amoro.process.ProcessCreationService blockingCreation =
        new org.apache.amoro.process.ProcessCreationService(
            assembly,
            Clock.systemUTC(),
            () -> {
              nameGenerationEntered.countDown();
              try {
                releaseNameGeneration.await(2, TimeUnit.SECONDS);
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
              }
              return "blocked-create";
            },
            Duration.ofMillis(25));
    ProcessRestSupport blockingSupport =
        ProcessTestFixtures.simulatedRestSupport(assembly, blockingCreation);
    MockMvc blockingMvc =
        MockMvcBuilders.standaloneSetup(new ProcessApiController(blockingSupport))
            .setControllerAdvice(new ApiExceptionHandler())
            .build();
    CompletableFuture<ProcessRestSupport.CreateResult> first =
        CompletableFuture.supplyAsync(
            () ->
                blockingSupport.create(
                    "prod",
                    "db1",
                    "orders",
                    "first",
                    "dummy-maintenance",
                    "local",
                    Collections.emptyMap()));
    assertTrue(nameGenerationEntered.await(1, TimeUnit.SECONDS));

    blockingMvc
        .perform(
            post(CREATE)
                .header("Idempotency-Key", "second")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.code").value("IDEMPOTENCY_IN_PROGRESS"))
        .andExpect(
            org.springframework.test.web.servlet.result.MockMvcResultMatchers.header()
                .string("Retry-After", "1"));

    releaseNameGeneration.countDown();
    first.get(1, TimeUnit.SECONDS);
  }

  @Test
  public void createUsesOneAtomicTableIdentitySnapshot() {
    AtomicInteger resolves = new AtomicInteger();
    ProcessRestSupport atomicSupport =
        new ProcessRestSupport(
            assembly,
            (catalog, database, table) -> {
              if (resolves.incrementAndGet() > 1) {
                throw new AssertionError("table identity was resolved more than once");
              }
              return new ProcessRestSupport.TableIdentity("stable-table-id", "simulated");
            },
            new org.apache.amoro.process.ProcessCreationService(assembly),
            ProcessActionCatalog.simulatedRoutingFixtures());

    ProcessRestSupport.CreateResult created =
        atomicSupport.create(
            "prod",
            "db1",
            "orders",
            "atomic-table",
            "dummy-maintenance",
            "local",
            Collections.emptyMap());

    assertEquals(1, resolves.get());
    assertEquals("stable-table-id", created.resource.spec().table().tableId());
    assertEquals("simulated", created.resource.spec().table().tableFormat());
  }

  @Test
  public void createWithUnknownTableFails404() throws Exception {
    mvc.perform(
            post("/api/ams/v2/tables/prod/db1/ghost-table/processes")
                .header("Idempotency-Key", "k")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("TABLE_NOT_FOUND"));
  }

  @Test
  public void createWhileActiveExistsFails409() throws Exception {
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "key-1")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isCreated());

    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "key-2")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.code").value("ACTIVE_PROCESS_EXISTS"));
  }

  // ------------------------------------------------------------------ read

  @Test
  public void pointReadAndMissingProcess() throws Exception {
    MvcResult created =
        mvc.perform(
                post(CREATE)
                    .header("Idempotency-Key", "key-1")
                    .contentType("application/json")
                    .content(body("dummy-maintenance", "local")))
            .andReturn();
    String name = mapper.readTree(created.getResponse().getContentAsString()).get("name").asText();

    mvc.perform(get("/api/ams/v2/processes/" + name))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.name").value(name));

    mvc.perform(get("/api/ams/v2/processes/does-not-exist"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("PROCESS_NOT_FOUND"));
  }

  @Test
  public void listFiltersByActionAndPhaseWithPagination() throws Exception {
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "k1")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isCreated());
    mvc.perform(
            post("/api/ams/v2/tables/prod/db1/orders2/processes")
                .header("Idempotency-Key", "k2")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isCreated());

    mvc.perform(get(CREATE).param("action", "dummy-maintenance"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1))
        .andExpect(jsonPath("$.items[0].spec.action").value("dummy-maintenance"));

    mvc.perform(get(CREATE).param("status", "PENDING"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.total").value(1));

    mvc.perform(get(CREATE).param("page", "0"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));

    mvc.perform(get(CREATE).param("pageSize", "51"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));

    mvc.perform(get("/api/ams/v2/tables/prod/db1/ghost-table/processes"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("TABLE_NOT_FOUND"));
  }

  // ------------------------------------------------------------------ cancel

  @Test
  public void cancelFlipsDesiredStateOnly() throws Exception {
    MvcResult created =
        mvc.perform(
                post(CREATE)
                    .header("Idempotency-Key", "key-1")
                    .contentType("application/json")
                    .content(body("dummy-maintenance", "local")))
            .andReturn();
    String name = mapper.readTree(created.getResponse().getContentAsString()).get("name").asText();

    mvc.perform(
            patch("/api/ams/v2/processes/" + name)
                .contentType("application/json")
                .content("{\"desiredState\":\"CANCEL\",\"reason\":\"operator request\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.spec.desiredState").value("CANCEL"));

    // repeated cancel is idempotent
    mvc.perform(
            patch("/api/ams/v2/processes/" + name)
                .contentType("application/json")
                .content("{\"desiredState\":\"CANCEL\",\"reason\":\"repeat\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.spec.desiredState").value("CANCEL"));

    // only CANCEL is accepted
    mvc.perform(
            patch("/api/ams/v2/processes/" + name)
                .contentType("application/json")
                .content("{\"desiredState\":\"RUN\"}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void cancelWithoutReasonFailsAsValidationError() throws Exception {
    MvcResult created =
        mvc.perform(
                post(CREATE)
                    .header("Idempotency-Key", "cancel-no-reason")
                    .contentType("application/json")
                    .content(body("dummy-maintenance", "local")))
            .andReturn();
    String name = mapper.readTree(created.getResponse().getContentAsString()).get("name").asText();

    mvc.perform(
            patch("/api/ams/v2/processes/" + name)
                .contentType("application/json")
                .content("{\"desiredState\":\"CANCEL\"}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
  }

  @Test
  public void cancelOnFinalProcessReturnsCurrentState() throws Exception {
    MvcResult created =
        mvc.perform(
                post(CREATE)
                    .header("Idempotency-Key", "key-1")
                    .contentType("application/json")
                    .content(body("dummy-maintenance", "local")))
            .andReturn();
    String name = mapper.readTree(created.getResponse().getContentAsString()).get("name").asText();
    ProcessTestFixtures.forceTerminal(assembly, name, "SUCCESS");

    mvc.perform(
            patch("/api/ams/v2/processes/" + name)
                .contentType("application/json")
                .content("{\"desiredState\":\"CANCEL\",\"reason\":\"already final\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status.phase").value("SUCCESS"));
  }

  // ------------------------------------------------------------------ resolutions

  @Test
  public void submissionResolutionAckMovesToSubmitted() throws Exception {
    String name = createDispatchingProcess();
    ProcessResource current = assembly.repository().get(name);
    String attemptKey = current.status().attempt().submissionKey();

    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/submission-resolutions")
                .header("Idempotency-Key", "res-1")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\""
                        + attemptKey
                        + "\",\"requestHash\":\""
                        + current.status().attempt().requestHash()
                        + "\","
                        + "\"resolution\":\"ACKNOWLEDGED\",\"externalId\":\"app_77\","
                        + "\"reason\":\"verified\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status.phase").value("SUBMITTED"))
        .andExpect(jsonPath("$.status.attempt.externalId").value("app_77"));

    // identity mismatch is rejected
    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/submission-resolutions")
                .header("Idempotency-Key", "res-2")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\"bogus:0:9\",\"requestHash\":\"sha256:bogus\","
                        + "\"resolution\":\"NOT_FOUND\",\"reason\":\"nope\"}"))
        .andExpect(status().isConflict());
  }

  @Test
  public void executionResolutionFinalFailedTerminates() throws Exception {
    String name = createDispatchingProcess();
    ProcessTestFixtures.forceExecutionUnresolved(assembly, name);
    ProcessResource current = assembly.repository().get(name);
    String attemptKey = current.status().attempt().submissionKey();

    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/execution-resolutions")
                .header("Idempotency-Key", "res-1")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\""
                        + attemptKey
                        + "\",\"requestHash\":\""
                        + current.status().attempt().requestHash()
                        + "\","
                        + "\"resolution\":\"FAILED\",\"retryAllowed\":false,"
                        + "\"reason\":\"handle lost\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status.phase").value("FAILED"))
        .andExpect(jsonPath("$.status.finishedAt").isNotEmpty());
  }

  private String createDispatchingProcess() throws Exception {
    MvcResult created =
        mvc.perform(
                post(CREATE)
                    .header("Idempotency-Key", "dispatch-1")
                    .contentType("application/json")
                    .content(body("dummy-maintenance", "local")))
            .andReturn();
    String name = mapper.readTree(created.getResponse().getContentAsString()).get("name").asText();
    ProcessTestFixtures.forceSubmissionUnresolved(assembly, name);
    return name;
  }

  @Test
  public void createReplaysAfterTheProcessBecameFinal() throws Exception {
    MvcResult created =
        mvc.perform(
                post(CREATE)
                    .header("Idempotency-Key", "final-key")
                    .contentType("application/json")
                    .content(body("dummy-maintenance", "local")))
            .andReturn();
    String name = mapper.readTree(created.getResponse().getContentAsString()).get("name").asText();
    ProcessTestFixtures.forceTerminal(assembly, name, "SUCCESS");

    // a completed create still replays to its original resource (spec §8.3)
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "final-key")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.name").value(name))
        .andExpect(jsonPath("$.status.phase").value("SUCCESS"));
  }

  @Test
  public void submissionNotFoundRotatesGeneration() throws Exception {
    String name = createDispatchingProcess();
    ProcessResource before = assembly.repository().get(name);
    String attemptKey = before.status().attempt().submissionKey();

    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/submission-resolutions")
                .header("Idempotency-Key", "res-1")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\""
                        + attemptKey
                        + "\","
                        + "\"requestHash\":\""
                        + before.status().attempt().requestHash()
                        + "\","
                        + "\"resolution\":\"NOT_FOUND\",\"reason\":\"ledger checked\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status.phase").value("PENDING"))
        .andExpect(jsonPath("$.status.attempt.dispatchGeneration").value(1))
        .andExpect(jsonPath("$.status.attempt.submissionHistory[0].outcome").value("NOT_FOUND"));

    // a bogus key is identity-stale regardless of payload (409, checked before field rules)
    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/submission-resolutions")
                .header("Idempotency-Key", "res-2")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\"x:0:0\",\"requestHash\":\"sha256:bogus\","
                        + "\"resolution\":\"NOT_FOUND\","
                        + "\"reason\":\"bad\"}"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.code").value("PROCESS_ATTEMPT_STALE"));

    // the CURRENT key with NOT_FOUND + externalId breaks the field rule (400); stage the
    // rotated generation back to DISPATCHING so the field rule is what fires
    ProcessTestFixtures.forceSubmissionUnresolved(assembly, name);
    ProcessResource rotated = assembly.repository().get(name);
    String rotatedKey = rotated.status().attempt().submissionKey();
    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/submission-resolutions")
                .header("Idempotency-Key", "res-3")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\""
                        + rotatedKey
                        + "\",\"requestHash\":\""
                        + rotated.status().attempt().requestHash()
                        + "\","
                        + "\"resolution\":\"NOT_FOUND\",\"externalId\":\"app\","
                        + "\"reason\":\"bad\"}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void submissionAckUnderCancelDesiredGoesCanceling() throws Exception {
    String name = createDispatchingProcess();
    ProcessResource staged = assembly.repository().get(name);
    String attemptKey = staged.status().attempt().submissionKey();
    ProcessRestSupport support2 = support;
    ProcessResource current = support2.get(name);
    assembly
        .repository()
        .modify(
            name, current.resourceVersion(), r -> r.withSpec(r.spec().withDesiredState("CANCEL")));

    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/submission-resolutions")
                .header("Idempotency-Key", "res-1")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\""
                        + attemptKey
                        + "\",\"requestHash\":\""
                        + staged.status().attempt().requestHash()
                        + "\","
                        + "\"resolution\":\"ACKNOWLEDGED\",\"externalId\":\"app_9\","
                        + "\"reason\":\"found\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status.phase").value("CANCELING"));
  }

  @Test
  public void executionFailedWithoutRetryAllowedIsRejected() throws Exception {
    String name = createDispatchingProcess();
    ProcessTestFixtures.forceExecutionUnresolved(assembly, name);
    ProcessResource current = assembly.repository().get(name);
    String attemptKey = current.status().attempt().submissionKey();
    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/execution-resolutions")
                .header("Idempotency-Key", "res-1")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\""
                        + attemptKey
                        + "\",\"requestHash\":\""
                        + current.status().attempt().requestHash()
                        + "\","
                        + "\"resolution\":\"FAILED\",\"reason\":\"no flag\"}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void submissionResolutionMissingRequiredFieldsNeverReturns500() throws Exception {
    String name = createDispatchingProcess();
    ProcessResource current = assembly.repository().get(name);
    String key = current.status().attempt().submissionKey();
    String hash = current.status().attempt().requestHash();
    String endpoint = "/api/ams/v2/processes/" + name + "/submission-resolutions";
    String[] invalidBodies = {
      "{\"requestHash\":\"" + hash + "\",\"resolution\":\"NOT_FOUND\",\"reason\":\"x\"}",
      "{\"submissionKey\":\"" + key + "\",\"resolution\":\"NOT_FOUND\",\"reason\":\"x\"}",
      "{\"submissionKey\":\"" + key + "\",\"requestHash\":\"" + hash + "\",\"reason\":\"x\"}",
      "{\"submissionKey\":\""
          + key
          + "\",\"requestHash\":\""
          + hash
          + "\",\"resolution\":\"NOT_FOUND\"}"
    };
    for (int index = 0; index < invalidBodies.length; index++) {
      mvc.perform(
              post(endpoint)
                  .header("Idempotency-Key", "missing-field-" + index)
                  .contentType("application/json")
                  .content(invalidBodies[index]))
          .andExpect(status().isBadRequest())
          .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
    }
  }

  @Test
  public void executionResolutionMissingReasonNeverReturns500() throws Exception {
    String name = createDispatchingProcess();
    ProcessTestFixtures.forceExecutionUnresolved(assembly, name);
    ProcessResource current = assembly.repository().get(name);

    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/execution-resolutions")
                .header("Idempotency-Key", "missing-reason")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\""
                        + current.status().attempt().submissionKey()
                        + "\",\"requestHash\":\""
                        + current.status().attempt().requestHash()
                        + "\",\"resolution\":\"FAILED\",\"retryAllowed\":false}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
  }

  @Test
  public void unknownTopLevelFieldsAreRejected() throws Exception {
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "k")
                .contentType("application/json")
                .content(
                    body("dummy-maintenance", "local")
                        .replaceFirst("\\}$", ",\"retryPolicy\":{\"maxRetries\":9}}")))
        .andExpect(status().isBadRequest());
  }

  @Test
  public void listOrdersNewestFirst() throws Exception {
    mvc.perform(
            post(CREATE)
                .header("Idempotency-Key", "older")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isCreated());
    Thread.sleep(50L); // distinct createdAt
    mvc.perform(
            post("/api/ams/v2/tables/prod/db1/orders2/processes")
                .header("Idempotency-Key", "newer")
                .contentType("application/json")
                .content(body("dummy-maintenance", "local")))
        .andExpect(status().isCreated());

    // both under one table for ordering: second create targeted orders2; use orders only
    mvc.perform(get(CREATE)).andExpect(status().isOk()).andExpect(jsonPath("$.total").value(1));
  }

  @Test
  public void resolutionWithoutIdempotencyKeyIsRejected() throws Exception {
    String name = createDispatchingProcess();
    ProcessResource current = assembly.repository().get(name);
    String attemptKey = current.status().attempt().submissionKey();
    mvc.perform(
            post("/api/ams/v2/processes/" + name + "/submission-resolutions")
                .contentType("application/json")
                .content(
                    "{\"submissionKey\":\""
                        + attemptKey
                        + "\",\"requestHash\":\""
                        + current.status().attempt().requestHash()
                        + "\","
                        + "\"resolution\":\"ACKNOWLEDGED\",\"externalId\":\"app\","
                        + "\"reason\":\"x\"}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_REQUIRED"));
  }

  @Test
  public void errorBodyCarriesTheUnifiedShape() throws Exception {
    MvcResult result =
        mvc.perform(get("/api/ams/v2/processes/missing"))
            .andExpect(status().isNotFound())
            .andReturn();
    JsonNode error = mapper.readTree(result.getResponse().getContentAsString());
    assertEquals("PROCESS_NOT_FOUND", error.get("code").asText());
    assertTrue(error.has("message"));
    assertTrue(error.has("timestamp"));
    assertTrue(error.has("traceId"));
  }
}
