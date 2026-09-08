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

package org.apache.amoro.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class TestExceptionUtil {

  @Test
  public void testGetCompleteErrorMessagePreservesLongUnicodeStackTrace() {
    String tailMarker = "完整堆栈尾部-🚀";
    IllegalArgumentException cause =
        new IllegalArgumentException("原因-" + repeat("多字节内容", 1_000) + tailMarker);
    cause.addSuppressed(new IllegalStateException("suppressed-异常-😃"));
    RuntimeException failure = new RuntimeException("顶层异常", cause);

    String stackTrace = ExceptionUtil.getErrorMessage(failure);

    Assertions.assertTrue(stackTrace.length() > 4_000);
    Assertions.assertTrue(stackTrace.contains("顶层异常"));
    Assertions.assertTrue(stackTrace.contains("suppressed-异常-😃"));
    Assertions.assertTrue(stackTrace.contains(tailMarker));
    Assertions.assertEquals(
        stackTrace,
        new String(stackTrace.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));
  }

  @Test
  public void testGetCompleteErrorMessageHandlesNull() {
    Assertions.assertEquals(ExceptionUtil.EMPTY_ERROR_MESSAGE, ExceptionUtil.getErrorMessage(null));
  }

  private static String repeat(String value, int count) {
    StringBuilder builder = new StringBuilder(value.length() * count);
    for (int i = 0; i < count; i++) {
      builder.append(value);
    }
    return builder.toString();
  }
}
