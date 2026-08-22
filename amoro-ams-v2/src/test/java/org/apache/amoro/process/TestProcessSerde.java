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

package org.apache.amoro.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.serde.DeserializedResource;
import org.apache.amoro.serde.SerdeRegistry;
import org.apache.amoro.serde.VersionAwareJacksonSerde;
import org.apache.amoro.serde.VersionedResourceConverter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** P1: process/v1 YAML serde round-trip and the max-legal-shape bound proof. */
public class TestProcessSerde {

  private static VersionAwareJacksonSerde<ProcessResource> serde() {
    return new VersionAwareJacksonSerde<ProcessResource>(
        ProcessResource.class,
        new SerdeRegistry(ProcessResource.API_VERSION, new ArrayList<VersionedResourceConverter>()),
        SerdeFormat.YAML,
        65536);
  }

  private static String repeat(String seed, int targetBytes) {
    StringBuilder builder = new StringBuilder(targetBytes + seed.length());
    while (builder.length() < targetBytes) {
      builder.append(seed);
    }
    return builder.toString();
  }

  static ProcessResource sample() {
    Map<String, Object> parameters = new LinkedHashMap<String, Object>();
    parameters.put("olderThanMillis", 1_724_284_800_000L);
    parameters.put("retainLast", 1);

    return new ProcessResource(
        "1948372910284737281",
        new ProcessResource.ProcessSpec(
            new ProcessResource.TableRef("prod", "db1", "orders", "42"),
            "expire-snapshots",
            "remote-spark",
            "MANUAL",
            "2026-08-22T10:00:00Z",
            "RUN",
            new ProcessResource.RequestIdentity("sha256:2a4f", "sha256:87bc"),
            parameters,
            new ProcessResource.RetryPolicy(3, 2, 30)),
        new ProcessResource.ProcessStatus(
            "RUNNING",
            1,
            new ProcessResource.ProcessAttempt(
                1,
                "1948372910284737281:1:1",
                "sha256:9f3a",
                "ACKNOWLEDGED",
                "application_001",
                "2026-08-22T10:00:05Z",
                "AUTO",
                null,
                new ArrayList<ProcessResource.SubmissionSummary>(),
                new ProcessResource.ManualResolutions(null, null)),
            new ArrayList<ProcessResource.AttemptSummary>(),
            "2026-08-22T10:05:00Z",
            "2026-08-22T10:05:03Z",
            new ProcessResource.EngineBackoff(0, 1, 0, 0),
            new ArrayList<ProcessResource.Condition>(),
            new ProcessResource.Summary(
                "https://spark.example/jobs/application_001", new LinkedHashMap<String, Object>()),
            null,
            "2026-08-22T10:00:06Z",
            "2026-08-22T10:00:40Z",
            null));
  }

  @Test
  public void yamlRoundTripPreservesTheResource() {
    ProcessResource resource = sample();
    VersionAwareJacksonSerde<ProcessResource> serde = serde();
    byte[] document = serde.serialize(resource);
    DeserializedResource<ProcessResource> back = serde.deserialize(document);

    assertEquals(resource, back.resource());
    assertFalse(back.modifiedDuringDeserialization());
    assertNotSame(resource, back.resource());
  }

  @Test
  public void detachedCopyIsolatesAliases() {
    ProcessResource resource = sample();
    ProcessResource copy = serde().detachedCopy(resource);
    assertEquals(resource, copy);
    assertNotSame(resource, copy);
    assertNotSame(resource.status(), copy.status());
  }

  /**
   * The max-legal shape (process spec §3.1): every bounded history at its cap with all text fields
   * at their caps — the final terminal CAS must still serialize under 64KiB in BOTH the persistence
   * YAML and an equivalent JSON view.
   */
  @Test
  public void maxLegalShapeStaysUnderTheDocumentBound() {
    String maxConditionMessage = repeat("c", 512);
    String maxFailure = repeat("f", 512);
    String maxParameter = repeat("p", 16 * 1024);
    String maxResultValue = repeat("r", 8 * 1024);
    String maxTrackUri = repeat("https://j.example/", 2048 - 19);
    String maxExternalId = repeat("e", 512);

    // histories at cap: (maxRetries + 1) attempts x (maxSubmissionRetries + 1) generations
    List<ProcessResource.SubmissionSummary> fullSubmissionHistory =
        new ArrayList<ProcessResource.SubmissionSummary>();
    for (int generation = 0; generation <= 2; generation++) {
      fullSubmissionHistory.add(
          new ProcessResource.SubmissionSummary(
              generation,
              "p:0:" + generation,
              "sha256:abcd",
              "NOT_FOUND",
              null,
              "2026-08-22T10:00:0" + generation + "Z"));
    }
    List<ProcessResource.AttemptSummary> fullHistory =
        new ArrayList<ProcessResource.AttemptSummary>();
    for (int retry = 0; retry <= 3; retry++) {
      fullHistory.add(
          new ProcessResource.AttemptSummary(
              retry,
              0,
              "p:" + retry + ":0",
              "sha256:4e8b",
              "FAILED",
              retry == 0 ? maxExternalId : "app_" + retry,
              "ALLOW",
              fullSubmissionHistory,
              null,
              "2026-08-22T10:00:03Z",
              "ENGINE_FAILED"));
    }
    List<ProcessResource.Condition> eightConditions = new ArrayList<ProcessResource.Condition>();
    for (int i = 0; i < 8; i++) {
      eightConditions.add(
          new ProcessResource.Condition(
              "Type" + i,
              "True",
              "Reason" + i,
              maxConditionMessage,
              "2026-08-22T10:05:00Z",
              "2026-08-22T10:05:00Z"));
    }

    Map<String, Object> parameters = new LinkedHashMap<String, Object>();
    parameters.put("blob", maxParameter);
    Map<String, Object> result = new LinkedHashMap<String, Object>();
    result.put("out", maxResultValue);

    ProcessResource maxShape =
        new ProcessResource(
            "1948372910284737281",
            new ProcessResource.ProcessSpec(
                new ProcessResource.TableRef("prod", "db1", "orders", "42"),
                "expire-snapshots",
                "remote-spark",
                "MANUAL",
                "2026-08-22T10:00:00Z",
                "CANCEL",
                new ProcessResource.RequestIdentity("sha256:2a4f", "sha256:87bc"),
                parameters,
                new ProcessResource.RetryPolicy(3, 2, 30)),
            new ProcessResource.ProcessStatus(
                "FAILED",
                3,
                new ProcessResource.ProcessAttempt(
                    1,
                    "1948372910284737281:3:2",
                    "sha256:9f3a",
                    "ACKNOWLEDGED",
                    maxExternalId,
                    "2026-08-22T10:00:05Z",
                    "FINAL",
                    "2026-08-22T11:00:00Z",
                    fullSubmissionHistory,
                    new ProcessResource.ManualResolutions(
                        "{\"idempotencyKeyHash\":\"sha256:x\",\"commandHash\":\"sha256:y\","
                            + "\"submissionKey\":\"k\",\"requestHash\":\"sha256:z\","
                            + "\"outcome\":\"NOT_FOUND\",\"resolvedAt\":\"2026-08-22T10:30:00Z\"}",
                        null)),
                fullHistory,
                "2026-08-22T11:00:00Z",
                "2026-08-22T11:00:00Z",
                new ProcessResource.EngineBackoff(7, 7, 7, 7),
                eightConditions,
                new ProcessResource.Summary(maxTrackUri, result),
                maxFailure,
                "2026-08-22T10:00:06Z",
                "2026-08-22T10:00:40Z",
                "2026-08-22T11:00:00Z"));

    byte[] yaml = serde().serialize(maxShape);
    assertTrue(
        yaml.length < 65536,
        "max-legal-shape persistence YAML must stay under 65536 raw bytes, was " + yaml.length);

    // an equivalent JSON view (the REST wire form) must also stay under the bound
    VersionAwareJacksonSerde<ProcessResource> jsonSerde =
        new VersionAwareJacksonSerde<ProcessResource>(
            ProcessResource.class,
            new SerdeRegistry(
                ProcessResource.API_VERSION, new ArrayList<VersionedResourceConverter>()),
            SerdeFormat.JSON,
            65536);
    byte[] json = jsonSerde.serialize(maxShape);
    assertTrue(
        json.length < 65536,
        "max-legal-shape REST JSON must stay under 65536 raw bytes, was " + json.length);

    // and the bound is real: four 16KiB parameters (~64KiB alone) exceed it
    Map<String, Object> oversized = new LinkedHashMap<String, Object>(parameters);
    oversized.put("blob2", maxParameter);
    oversized.put("blob3", maxParameter);
    oversized.put("blob4", maxParameter);
    ProcessResource over = maxShape.withSpec(maxShape.spec().withDesiredState("RUN"));
    ProcessResource oversizedResource =
        new ProcessResource(
            over.name(),
            new ProcessResource.ProcessSpec(
                over.spec().table(),
                over.spec().action(),
                over.spec().executionEngine(),
                over.spec().triggerSource(),
                over.spec().createdAt(),
                over.spec().desiredState(),
                over.spec().request(),
                oversized,
                over.spec().retryPolicy()),
            over.status());
    try {
      serde().serialize(oversizedResource);
      throw new AssertionError("double-cap document must exceed the bound");
    } catch (org.apache.amoro.persistence.exception.PersistenceException expected) {
      // the 64KiB bound is enforced before the DB write, not by column truncation
    }
  }

  @Test
  public void base64WrappedYamlRoundTrip() {
    // the blob layer stores Base64(document); that wrapping is transparent
    ProcessResource resource = sample();
    byte[] document = serde().serialize(resource);
    String stored = java.util.Base64.getEncoder().encodeToString(document);
    ProcessResource back =
        serde().deserialize(java.util.Base64.getDecoder().decode(stored)).resource();
    assertEquals(resource, back);
  }

  @Test
  public void unknownFieldsAreToleratedForForwardCompatibility() {
    ProcessResource resource = sample();
    byte[] document = serde().serialize(resource);
    String padded =
        new String(document, java.nio.charset.StandardCharsets.UTF_8)
            + "\nbrandNewField: whatever\n";
    ProcessResource back = serde().deserialize(padded.getBytes()).resource();
    assertEquals(resource.name(), back.name());
  }
}
