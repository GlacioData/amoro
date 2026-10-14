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

package org.apache.amoro.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

class TestAmsLogging {
  @Test
  void slf4jEventsReachTheLog4jBackend() {
    String loggerName = "org.apache.amoro.fixture.logging";
    Logger backend = (Logger) LogManager.getLogger(loggerName);
    Level previousLevel = backend.getLevel();
    AtomicReference<String> message = new AtomicReference<>();
    AbstractAppender capture =
        new AbstractAppender("fixture-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            message.set(event.getMessage().getFormattedMessage());
          }
        };
    capture.start();
    backend.addAppender(capture);
    backend.setLevel(Level.INFO);
    try {
      // Verify observable delivery rather than merely asserting a dependency/class is present.
      // https://logging.apache.org/log4j/2.x/manual/installation.html#impl-bridge-slf4j
      LoggerFactory.getLogger(loggerName).info("AMS fixture {}", "event");
      assertEquals("AMS fixture event", message.get());
    } finally {
      backend.removeAppender(capture);
      backend.setLevel(previousLevel);
      capture.stop();
    }
  }
}
