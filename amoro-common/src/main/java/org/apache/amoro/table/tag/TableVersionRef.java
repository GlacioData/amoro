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
public class TableVersionRef {
  private String id;
  private String branch;

  public String getId() {
    return id;
  }

  public void setId(String value) {
    id = value;
  }

  public String getBranch() {
    return branch;
  }

  public void setBranch(String value) {
    branch = value;
  }

  public static TableVersionRef of(String id, String branch) {
    TableVersionRef ref = new TableVersionRef();
    ref.setId(id);
    ref.setBranch(branch);
    return ref;
  }
}
