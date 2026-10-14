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

package org.apache.amoro.table.tag;

/** Change retention in place; name and token are supplied by the route and If-Match header. */
public class UpdateTagRetentionRequest {
  private String name;
  private TagScope scope = new TagScope();
  private TagRetentionRequest retention;
  private String expectedToken;
  private String expectedRetentionModel;
  private String expectedRetentionConfigToken;
  private String expectedUpdateClock;

  @org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore
  public String getName() {
    return name;
  }

  public void setName(String value) {
    name = value;
  }

  public TagScope getScope() {
    return scope;
  }

  public void setScope(TagScope value) {
    scope = value;
  }

  public TagRetentionRequest getRetention() {
    return retention;
  }

  public void setRetention(TagRetentionRequest value) {
    retention = value;
  }

  @org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore
  public String getExpectedToken() {
    return expectedToken;
  }

  public void setExpectedToken(String value) {
    expectedToken = value;
  }

  public String getExpectedRetentionModel() {
    return expectedRetentionModel;
  }

  public void setExpectedRetentionModel(String value) {
    expectedRetentionModel = value;
  }

  public String getExpectedRetentionConfigToken() {
    return expectedRetentionConfigToken;
  }

  public void setExpectedRetentionConfigToken(String value) {
    expectedRetentionConfigToken = value;
  }

  public String getExpectedUpdateClock() {
    return expectedUpdateClock;
  }

  public void setExpectedUpdateClock(String value) {
    expectedUpdateClock = value;
  }
}
