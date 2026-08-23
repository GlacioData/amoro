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

package org.apache.amoro.process.engine.simulated;

import org.apache.amoro.process.engine.EngineTypes.CancellationOutcome;
import org.apache.amoro.process.engine.EngineTypes.EngineCapabilities;
import org.apache.amoro.process.engine.EngineTypes.ProcessObservation;
import org.apache.amoro.process.engine.EngineTypes.SubmissionOutcome;
import org.apache.amoro.process.engine.EngineTypes.SubmissionResolution;
import org.apache.amoro.process.engine.LocalEngineAdapter;
import org.apache.amoro.process.engine.ProcessEngineFactory;
import org.apache.amoro.process.engine.ProcessEngineLifecycle;
import org.apache.amoro.process.engine.ProcessEnginePort;
import org.apache.amoro.process.engine.ProviderMode;
import org.apache.amoro.process.engine.SubmissionCommand;

import java.util.concurrent.CompletionStage;

/**
 * In-memory remote-spark simulator. Its name tests routing only: there is no Spark client, HTTP
 * endpoint or external job submission in this provider.
 */
public final class SimulatedRemoteSparkEngineFactory implements ProcessEngineFactory {

  @Override
  public String engineName() {
    return "remote-spark";
  }

  @Override
  public ProviderMode mode() {
    return ProviderMode.SIMULATED;
  }

  @Override
  public ProcessEnginePort create(Context context) {
    return new SimulatedRemotePort(context);
  }

  private static final class SimulatedRemotePort
      implements ProcessEnginePort, ProcessEngineLifecycle {
    private final LocalEngineAdapter delegate;

    private SimulatedRemotePort(Context context) {
      this.delegate =
          new LocalEngineAdapter(
              context.workerThreads(),
              context.queueCapacity(),
              context.localActions(),
              context.localTerminalResultRetentionDays());
    }

    @Override
    public EngineCapabilities capabilities() {
      return new EngineCapabilities(true, true, "remote-spark-simulated-v1");
    }

    @Override
    public CompletionStage<SubmissionOutcome> submit(
        String submissionKey, String requestHash, byte[] submissionPayload) {
      return delegate.submit(submissionKey, requestHash, submissionPayload);
    }

    @Override
    public CompletionStage<SubmissionOutcome> submit(SubmissionCommand command) {
      return delegate.submit(command);
    }

    @Override
    public CompletionStage<SubmissionResolution> resolveSubmission(
        String submissionKey, String requestHash) {
      return delegate.resolveSubmission(submissionKey, requestHash);
    }

    @Override
    public CompletionStage<ProcessObservation> observe(String externalId) {
      return delegate.observe(externalId);
    }

    @Override
    public CompletionStage<CancellationOutcome> cancel(String externalId) {
      return delegate.cancel(externalId);
    }

    @Override
    public CompletionStage<Void> release(String externalId) {
      return delegate.release(externalId);
    }

    @Override
    public void shutdown(long timeoutMillis) {
      delegate.shutdown(timeoutMillis);
    }
  }
}
