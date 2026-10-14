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
public class DeleteTagRequest {
  private String name;
  private TagScope scope = new TagScope();
  private String expectedToken;

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

  public String getExpectedToken() {
    return expectedToken;
  }

  public void setExpectedToken(String value) {
    expectedToken = value;
  }
}
