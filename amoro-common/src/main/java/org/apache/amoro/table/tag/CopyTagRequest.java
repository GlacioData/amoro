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

import org.apache.amoro.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

/** Format-neutral Tag contract; version IDs remain opaque strings. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CopyTagRequest {
  private String sourceName;
  private String newName;
  private TagScope scope = new TagScope();
  private TagRetentionRequest retention = new TagRetentionRequest();
  private String expectedSourceToken;
  private String expectedRetentionModel;
  private String expectedRetentionConfigToken;

  public String getSourceName() {
    return sourceName;
  }

  public void setSourceName(String value) {
    sourceName = value;
  }

  public String getNewName() {
    return newName;
  }

  public void setNewName(String value) {
    newName = value;
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

  public String getExpectedSourceToken() {
    return expectedSourceToken;
  }

  public void setExpectedSourceToken(String value) {
    expectedSourceToken = value;
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
}
