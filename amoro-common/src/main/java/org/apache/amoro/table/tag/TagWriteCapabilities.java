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
public class TagWriteCapabilities {
  private java.util.Set<String> actions = java.util.Collections.emptySet();
  private java.util.List<TagScope> supportedScopes = java.util.Collections.emptyList();
  private Retention retention = new Retention();
  private String deleteEffect = "UNKNOWN";
  private String preconditionGuarantee = "UNSUPPORTED";
  private String unavailableReason;

  public java.util.Set<String> getActions() {
    return actions;
  }

  public void setActions(java.util.Set<String> value) {
    actions = value;
  }

  public java.util.List<TagScope> getSupportedScopes() {
    return supportedScopes;
  }

  public void setSupportedScopes(java.util.List<TagScope> value) {
    supportedScopes = value;
  }

  public Retention getRetention() {
    return retention;
  }

  public void setRetention(Retention value) {
    retention = value;
  }

  public String getDeleteEffect() {
    return deleteEffect;
  }

  public void setDeleteEffect(String value) {
    deleteEffect = value;
  }

  public String getPreconditionGuarantee() {
    return preconditionGuarantee;
  }

  public void setPreconditionGuarantee(String value) {
    preconditionGuarantee = value;
  }

  public String getUnavailableReason() {
    return unavailableReason;
  }

  public void setUnavailableReason(String value) {
    unavailableReason = value;
  }

  public static class Retention {
    private java.util.Set<String> supportedModes = java.util.Collections.emptySet();
    private String model = "UNKNOWN";
    // RESET_ON_UPDATE, PRESERVE_ANCHOR, or UNKNOWN. No clock semantics are inferred from format.
    private String updateClock = "UNKNOWN";

    public String getUpdateClock() {
      return updateClock;
    }

    public void setUpdateClock(String value) {
      updateClock = value;
    }

    private TagRetentionInfo nativeDefault;
    private String configObservationToken;

    public java.util.Set<String> getSupportedModes() {
      return supportedModes;
    }

    public void setSupportedModes(java.util.Set<String> value) {
      supportedModes = value;
    }

    public String getModel() {
      return model;
    }

    public void setModel(String value) {
      model = value;
    }

    public TagRetentionInfo getNativeDefault() {
      return nativeDefault;
    }

    public void setNativeDefault(TagRetentionInfo value) {
      nativeDefault = value;
    }

    public String getConfigObservationToken() {
      return configObservationToken;
    }

    public void setConfigObservationToken(String value) {
      configObservationToken = value;
    }
  }
}
