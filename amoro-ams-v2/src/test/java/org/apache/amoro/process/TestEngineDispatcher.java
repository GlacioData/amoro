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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.apache.amoro.process.engine.FakeEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/** P3: single-flight dispatcher semantics and the bounded-timeout degradations. */
public class TestEngineDispatcher {

  @Test
  public void submitAcknowledgeRoundTrip() throws Exception {
    FakeEngineAdapter adapter = new FakeEngineAdapter();
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 5_000L);

    SubmissionOutcome outcome =
        dispatcher
            .submit("p", "p:0:0", "sha256:r", new byte[] {1})
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals(SubmissionOutcome.Kind.ACKNOWLEDGED, outcome.kind());
    assertTrue(outcome.externalId().startsWith("fake-app-"));
  }

  @Test
  public void inFlightIdentityRejectsSecondCaller() {
    FakeEngineAdapter adapter =
        new FakeEngineAdapter() {
          @Override
          public java.util.concurrent.CompletionStage<SubmissionOutcome> submit(
              String submissionKey, String requestHash, byte[] payload) {
            return new CompletableFuture<SubmissionOutcome>(); // never completes
          }
        };
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 60_000L);
    dispatcher.submit("p", "p:0:0", "sha256:r", new byte[] {1});

    assertThrows(
        ProcessEngineDispatcher.CommandInFlightException.class,
        () -> dispatcher.submit("p", "p:0:0", "sha256:r", new byte[] {1}));
    // a different key is independent
    ProcessObservation observation =
        dispatcher
            .observe("p", "other-external")
            .toCompletableFuture()
            .join(); // completes with NOT_FOUND
    assertEquals(ProcessObservation.Kind.NOT_FOUND, observation.kind());
  }

  @Test
  public void completedFlightFreesTheIdentity() throws Exception {
    FakeEngineAdapter adapter = new FakeEngineAdapter();
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 5_000L);

    dispatcher
        .submit("p", "p:0:0", "sha256:r", new byte[] {1})
        .toCompletableFuture()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    assertEquals(0, dispatcher.inFlightCount());
    // the same identity can fly again (e.g. a later reconcile round)
    SubmissionOutcome again =
        dispatcher
            .submit("p", "p:0:0", "sha256:r", new byte[] {1})
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals(SubmissionOutcome.Kind.ACKNOWLEDGED, again.kind());
  }

  @Test
  public void submitTimeoutDegradesToUnknownOthersToUnavailable() throws Exception {
    FakeEngineAdapter hung =
        new FakeEngineAdapter() {
          @Override
          public java.util.concurrent.CompletionStage<SubmissionOutcome> submit(
              String submissionKey, String requestHash, byte[] payload) {
            return new CompletableFuture<SubmissionOutcome>();
          }

          @Override
          public java.util.concurrent.CompletionStage<ProcessObservation> observe(
              String externalId) {
            return new CompletableFuture<ProcessObservation>();
          }
        };
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(hung, 50L);

    SubmissionOutcome submitOutcome =
        dispatcher
            .submit("p", "p:0:0", "sha256:r", new byte[] {1})
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals(
        SubmissionOutcome.Kind.UNKNOWN,
        submitOutcome.kind(),
        "submit timeout is conservative: side effects may have happened");

    ProcessObservation observation =
        dispatcher.observe("p", "ext").toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(ProcessObservation.Kind.UNAVAILABLE, observation.kind());
  }

  @Test
  public void adapterFailurePropagatesExceptionally() {
    FakeEngineAdapter failing =
        new FakeEngineAdapter() {
          @Override
          public java.util.concurrent.CompletionStage<SubmissionResolution> resolveSubmission(
              String submissionKey, String requestHash) {
            return CompletableFuture.failedFuture(new IllegalStateException("adapter broke"));
          }
        };
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(failing, 5_000L);
    CompletionException thrown =
        assertThrows(
            CompletionException.class,
            () ->
                dispatcher
                    .resolveSubmission("p", "p:0:0", "sha256:r")
                    .toCompletableFuture()
                    .join());
    assertTrue(thrown.getCause() instanceof IllegalStateException);
  }

  @Test
  public void cancelAndResolutionRoundTrip() throws Exception {
    FakeEngineAdapter adapter = new FakeEngineAdapter();
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 5_000L);

    SubmissionOutcome submitted =
        dispatcher
            .submit("p", "p:0:0", "sha256:r", new byte[] {1})
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    String externalId = submitted.externalId();

    SubmissionResolution resolution =
        dispatcher
            .resolveSubmission("p", "p:0:0", "sha256:r")
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals(SubmissionResolution.Kind.ACKNOWLEDGED, resolution.kind());
    assertEquals(externalId, resolution.externalId());

    CancellationOutcome cancel =
        dispatcher.cancel("p", externalId).toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(CancellationOutcome.Kind.ACCEPTED, cancel.kind());
    ProcessObservation after =
        dispatcher.observe("p", externalId).toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("CANCELED", after.observation().remotePhase());
  }
}
