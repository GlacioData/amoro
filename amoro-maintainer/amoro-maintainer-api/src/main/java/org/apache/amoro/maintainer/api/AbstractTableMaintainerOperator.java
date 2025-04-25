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

package org.apache.amoro.maintainer.api;

import org.apache.amoro.ErrorCodes;
import org.apache.amoro.api.AmoroException;
import org.apache.amoro.api.MaintainerService;
import org.apache.amoro.client.TableMaintainerClientPools;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.thrift.org.apache.thrift.TApplicationException;
import org.apache.amoro.shade.thrift.org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class AbstractTableMaintainerOperator implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTableMaintainerOperator.class);

  // Call ams every 5 seconds by default
  private static long callAmsInterval = TimeUnit.SECONDS.toMillis(5);

  private final TableMaintainerConfig config;
  private volatile boolean stopped = false;

  public AbstractTableMaintainerOperator(TableMaintainerConfig config) {
    Preconditions.checkNotNull(config);
    this.config = config;
  }

  private boolean shouldRetryLater(Throwable t) {
    if (t instanceof AmoroException) {
      AmoroException amoroException = (AmoroException) t;
      // Call ams again when got a persistence/undefined error
      return ErrorCodes.PERSISTENCE_ERROR_CODE == amoroException.getErrorCode()
          || ErrorCodes.UNDEFINED_ERROR_CODE == amoroException.getErrorCode();
    } else {
      // Call ams again when got an unexpected error
      return true;
    }
  }

  // Return null if got MISSING_RESULT error
  private boolean shouldReturnNull(Throwable t) {
    if (t instanceof TApplicationException) {
      TApplicationException applicationException = (TApplicationException) t;
      return applicationException.getType() == TApplicationException.MISSING_RESULT;
    }
    return false;
  }

  public <T> T callAms(AmsCallOperation<T> operation) throws TException {
    try {
      return operation.call(TableMaintainerClientPools.getClient(config.getAmsUrl()));
    } catch (Throwable t) {
      if (t instanceof AmoroException
          && ErrorCodes.PLUGIN_RETRY_AUTH_ERROR_CODE == ((AmoroException) (t)).getErrorCode()) {
        // Reset the token when got a authorization error
        LOG.error(
            "Got a authorization error while calling ams, reset token and wait for a new one", t);
      } else if (shouldReturnNull(t)) {
        return null;
      } else if (shouldRetryLater(t)) {
        LOG.error("Call ams got an error and will try again later", t);
        waitAShortTime();
      } else {
        throw t;
      }
    }
    throw new IllegalStateException("Operator is stopped");
  }

  public static void setCallAmsInterval(long callAmsInterval) {
    AbstractTableMaintainerOperator.callAmsInterval = callAmsInterval;
  }

  protected TableMaintainerConfig getConfig() {
    return config;
  }

  public boolean isStarted() {
    return !stopped;
  }

  public void stop() {
    this.stopped = true;
  }

  protected void waitAShortTime() {
    waitAShortTime(callAmsInterval);
  }

  protected void waitAShortTime(long waitTime) {
    try {
      TimeUnit.MILLISECONDS.sleep(waitTime);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  public interface AmsCallOperation<T> {
    T call(MaintainerService.Iface client) throws TException;
  }
}
