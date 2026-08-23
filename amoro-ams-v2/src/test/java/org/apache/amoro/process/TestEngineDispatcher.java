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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.apache.amoro.process.engine.FakeEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineDispatcher;
import org.apache.amoro.process.engine.ProcessEngineDispatcher.CommandFlight;
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
            .result()
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
        () -> dispatcher.resolveSubmission("p", "p:0:0", "sha256:r"));
    // a different key is independent
    ProcessObservation observation =
        dispatcher
            .observe("p", "other-external")
            .result()
            .toCompletableFuture()
            .join(); // completes with NOT_FOUND
    assertEquals(ProcessObservation.Kind.NOT_FOUND, observation.kind());
  }

  @Test
  public void completedResultKeepsIdentityUntilDurablyHandled() throws Exception {
    FakeEngineAdapter adapter = new FakeEngineAdapter();
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 5_000L);

    CommandFlight<SubmissionOutcome> first =
        dispatcher.submit("p", "p:0:0", "sha256:r", new byte[] {1});
    first
        .result()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    assertEquals(1, dispatcher.inFlightCount());
    assertThrows(
        ProcessEngineDispatcher.CommandInFlightException.class,
        () -> dispatcher.resolveSubmission("p", "p:0:0", "sha256:r"));

    first.markDurablyHandled();
    assertEquals(0, dispatcher.inFlightCount());
    SubmissionOutcome again =
        dispatcher
            .submit("p", "p:0:0", "sha256:r", new byte[] {1})
            .result()
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
            .result()
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
    assertEquals(
        SubmissionOutcome.Kind.UNKNOWN,
        submitOutcome.kind(),
        "submit timeout is conservative: side effects may have happened");

    ProcessObservation observation =
        dispatcher.observe("p", "ext").result().toCompletableFuture().get(5, TimeUnit.SECONDS);
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
                    .result()
                    .toCompletableFuture()
                    .join());
    assertTrue(thrown.getCause() instanceof IllegalStateException);
  }

  @Test
  public void cancelAndResolutionRoundTrip() throws Exception {
    FakeEngineAdapter adapter = new FakeEngineAdapter();
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 5_000L);

    CommandFlight<SubmissionOutcome> submitFlight =
        dispatcher.submit("p", "p:0:0", "sha256:r", new byte[] {1});
    SubmissionOutcome submitted =
        submitFlight.result().toCompletableFuture().get(5, TimeUnit.SECONDS);
    submitFlight.markDurablyHandled();
    String externalId = submitted.externalId();

    CommandFlight<SubmissionResolution> resolutionFlight =
        dispatcher.resolveSubmission("p", "p:0:0", "sha256:r");
    SubmissionResolution resolution =
        resolutionFlight.result().toCompletableFuture().get(5, TimeUnit.SECONDS);
    resolutionFlight.markDurablyHandled();
    assertEquals(SubmissionResolution.Kind.ACKNOWLEDGED, resolution.kind());
    assertEquals(externalId, resolution.externalId());

    CommandFlight<CancellationOutcome> cancelFlight = dispatcher.cancel("p", externalId);
    CancellationOutcome cancel =
        cancelFlight.result().toCompletableFuture().get(5, TimeUnit.SECONDS);
    cancelFlight.markDurablyHandled();
    assertEquals(CancellationOutcome.Kind.ACCEPTED, cancel.kind());
    ProcessObservation after =
        dispatcher.observe("p", externalId).result().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("CANCELED", after.observation().remotePhase());
  }

  @Test
  public void observeAndCancelShareExecutionIdentity() throws Exception {
    CompletableFuture<ProcessObservation> pending = new CompletableFuture<>();
    FakeEngineAdapter adapter =
        new FakeEngineAdapter() {
          @Override
          public java.util.concurrent.CompletionStage<ProcessObservation> observe(String externalId) {
            return pending;
          }
        };
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 5_000L);
    CommandFlight<ProcessObservation> observation = dispatcher.observe("p", "external-1");

    assertThrows(
        ProcessEngineDispatcher.CommandInFlightException.class,
        () -> dispatcher.cancel("p", "external-1"));
    pending.complete(ProcessObservation.notFound());
    observation.result().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertThrows(
        ProcessEngineDispatcher.CommandInFlightException.class,
        () -> dispatcher.cancel("p", "external-1"));
    observation.markDurablyHandled();
    assertEquals(
        CancellationOutcome.Kind.NOT_FOUND,
        dispatcher
            .cancel("p", "external-1")
            .result()
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .kind());
  }

  @Test
  public void duplicateReleaseMergesAndTimeoutIsExceptional() {
    java.util.concurrent.atomic.AtomicInteger releases = new java.util.concurrent.atomic.AtomicInteger();
    FakeEngineAdapter adapter =
        new FakeEngineAdapter() {
          @Override
          public java.util.concurrent.CompletionStage<Void> release(String externalId) {
            releases.incrementAndGet();
            return new CompletableFuture<>();
          }
        };
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 50L);

    CommandFlight<Void> first = dispatcher.release("local", "external-1");
    CommandFlight<Void> duplicate = dispatcher.release("local", "external-1");

    assertSame(first, duplicate);
    assertEquals(1, releases.get());
    CompletionException timeout =
        assertThrows(CompletionException.class, () -> first.result().toCompletableFuture().join());
    assertTrue(
        timeout.getCause() instanceof ProcessEngineDispatcher.EngineCommandTimeoutException);
    first.markDurablyHandled();
  }

  @Test
  public void invalidTrackUriIsDroppedButValidPhaseIsRetained() throws Exception {
    FakeEngineAdapter adapter =
        new FakeEngineAdapter() {
          @Override
          public java.util.concurrent.CompletionStage<ProcessObservation> observe(String externalId) {
            return CompletableFuture.completedFuture(
                ProcessObservation.known(
                    new org.apache.amoro.process.engine.EngineTypes.EngineObservation(
                        "RUNNING", "javascript:alert(1)", java.util.Collections.emptyMap(), null)));
          }
        };
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 5_000L);

    ProcessObservation sanitized =
        dispatcher
            .observe("p", "external-1")
            .result()
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);

    assertEquals(ProcessObservation.Kind.KNOWN, sanitized.kind());
    assertEquals("RUNNING", sanitized.observation().remotePhase());
    assertNull(sanitized.observation().trackUri());
  }

  @Test
  public void malformedEngineResultsDegradeConservatively() throws Exception {
    FakeEngineAdapter adapter =
        new FakeEngineAdapter() {
          @Override
          public java.util.concurrent.CompletionStage<SubmissionOutcome> submit(
              String submissionKey, String requestHash, byte[] payload) {
            return CompletableFuture.completedFuture(SubmissionOutcome.acknowledged(""));
          }

          @Override
          public java.util.concurrent.CompletionStage<CancellationOutcome> cancel(
              String externalId) {
            return CompletableFuture.completedFuture(
                CancellationOutcome.alreadyTerminal(
                    new org.apache.amoro.process.engine.EngineTypes.EngineObservation(
                        "RUNNING", null, java.util.Collections.emptyMap(), null)));
          }
        };
    ProcessEngineDispatcher dispatcher = new ProcessEngineDispatcher(adapter, 5_000L);

    assertEquals(
        SubmissionOutcome.Kind.UNKNOWN,
        dispatcher
            .submit("p", "p:0:0", "sha256:r", new byte[0])
            .result()
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .kind());
    assertEquals(
        CancellationOutcome.Kind.UNAVAILABLE,
        dispatcher
            .cancel("p", "external-1")
            .result()
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .kind());
  }
}
