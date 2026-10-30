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

package org.apache.amoro.server.dashboard.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.amoro.server.dashboard.model.VersionInfo;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;

public class TestVersionController {

  /**
   * The cached version info must equal what the original per-request computation produced;
   * git.properties is a build artifact and is fixed for the process lifetime.
   */
  @Test
  void testCachedVersionInfoMatchesPerRequestComputation() throws Exception {
    VersionInfo expected = computeVersionInfoPerRequest();
    VersionInfo actual = loadVersionInfo();

    assertEquals(expected.getVersion(), actual.getVersion());
    assertEquals(expected.getCommitTime(), actual.getCommitTime());
  }

  /** Verbatim copy of the pre-cache per-request logic, used as the oracle. */
  private static VersionInfo computeVersionInfoPerRequest() {
    Properties prop = new Properties();
    InputStream is =
        VersionController.class.getClassLoader().getResourceAsStream("amoro/git.properties");
    String version = "UNKNOWN";
    String commitTime = "UNKNOWN";
    String abbrev = "UNKNOWN";
    if (is != null) {
      try {
        prop.load(is);
        version = prop.getProperty("git.build.version");
        abbrev = prop.getProperty("git.commit.id.abbrev");
        commitTime = prop.getProperty("git.commit.time");
      } catch (Exception e) {
        // swallow, as the production code does
      }
    }
    VersionInfo versionInfo = new VersionInfo();
    versionInfo.setVersion(String.format("%s(%s)", version, abbrev));
    versionInfo.setCommitTime(commitTime);
    return versionInfo;
  }

  private static VersionInfo loadVersionInfo() throws Exception {
    Method load = VersionController.class.getDeclaredMethod("loadVersionInfo");
    load.setAccessible(true);
    return (VersionInfo) load.invoke(null);
  }
}
