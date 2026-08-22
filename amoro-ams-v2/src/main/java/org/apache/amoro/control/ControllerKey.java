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

package org.apache.amoro.control;

import java.util.Objects;

/**
 * Value object identifying a scheduled controller by resource domain plus resource id. A bare
 * resource id is not unique across domains, so the domain component is mandatory for the
 * single-flight registry: two domains may independently schedule the same resource id.
 */
public final class ControllerKey {

  private final String domain;
  private final String resourceId;

  private ControllerKey(String domain, String resourceId) {
    this.domain = Objects.requireNonNull(domain, "domain");
    this.resourceId = Objects.requireNonNull(resourceId, "resourceId");
  }

  public static ControllerKey of(String domain, String resourceId) {
    return new ControllerKey(domain, resourceId);
  }

  public String domain() {
    return domain;
  }

  public String resourceId() {
    return resourceId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ControllerKey that = (ControllerKey) o;
    return domain.equals(that.domain) && resourceId.equals(that.resourceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(domain, resourceId);
  }

  @Override
  public String toString() {
    return "ControllerKey{domain='" + domain + "', resourceId='" + resourceId + "'}";
  }
}
