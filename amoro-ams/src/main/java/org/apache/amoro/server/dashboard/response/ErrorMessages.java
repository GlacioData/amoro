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

package org.apache.amoro.server.dashboard.response;

import org.apache.amoro.exception.AlreadyExistsException;
import org.apache.amoro.exception.ObjectNotExistsException;
import org.apache.amoro.server.dashboard.DashboardServer;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;

import javax.ws.rs.BadRequestException;

import java.util.regex.Pattern;

/** Decides which exception messages may be returned to an HTTP client. */
public final class ErrorMessages {

  /** The only text a client gets for a failure that is not a problem with its request. */
  public static final String INTERNAL_ERROR = "Internal server error";

  private static final String DASHBOARD_LAYER = DashboardServer.class.getPackage().getName() + ".";

  /**
   * Validation messages are short. A longer text is not one, and scanning a huge one would cost CPU
   * on the error path, so it is neither scanned nor returned.
   */
  private static final int MAX_CLIENT_MESSAGE_LENGTH = 1000;

  /**
   * Text that only shows up when a message comes from infrastructure or wraps another failure.
   * Validation messages are single-line sentences built from fixed text and the client's input.
   */
  private static final Pattern INTERNAL_DETAIL =
      Pattern.compile(
          "\\b[A-Z][\\w$]*(?:Exception|Error)\\b" // exception class name
              + "|\\R" // stack traces and multi-line SQL
              + "|\\bat\\s+[\\w$]+(?:\\.[\\w$]+)+\\(" // stack frame
              + "|Caused by:"
              + "|Cannot invoke " // detailed NullPointerException text of newer JDKs
              + "|jdbc:"
              + "|[a-z][a-z0-9+.-]*://[A-Za-z0-9.\\-]+(?::\\d+)?"); // URL that names a host

  private ErrorMessages() {}

  /**
   * The message a client may see for {@code failure}; {@link #INTERNAL_ERROR} for anything else.
   */
  public static String of(Exception failure) {
    return isClientFacing(failure) ? failure.getMessage() : INTERNAL_ERROR;
  }

  /**
   * Whether the text carries a sign of an infrastructure failure instead of describing a problem
   * with the request, or is too long to be such a description.
   */
  public static boolean carriesInternalDetail(String message) {
    return message == null
        || message.length() > MAX_CLIENT_MESSAGE_LENGTH
        || INTERNAL_DETAIL.matcher(message).find();
  }

  /**
   * Whether the failure is a validation error raised for the client. Only the exception types the
   * REST layer throws on purpose qualify, and only when the message carries no internal detail.
   */
  public static boolean isClientFacing(Throwable failure) {
    if (carriesInternalDetail(failure.getMessage())) {
      return false;
    }
    return failure instanceof IllegalArgumentException
        || failure instanceof IllegalStateException
        || failure instanceof NullPointerException
        || failure instanceof BadRequestException
        || failure instanceof ObjectNotExistsException
        || failure instanceof AlreadyExistsException
        || failure instanceof NotFoundException
        || failure instanceof NoSuchTableException
        || failure instanceof NoSuchNamespaceException
        || failure instanceof org.apache.iceberg.exceptions.AlreadyExistsException
        // A plain RuntimeException is a validation message only when the dashboard layer raised it.
        || (failure.getClass() == RuntimeException.class && raisedInDashboardLayer(failure));
  }

  private static boolean raisedInDashboardLayer(Throwable failure) {
    StackTraceElement[] stackTrace = failure.getStackTrace();
    return stackTrace.length > 0 && stackTrace[0].getClassName().startsWith(DASHBOARD_LAYER);
  }
}
