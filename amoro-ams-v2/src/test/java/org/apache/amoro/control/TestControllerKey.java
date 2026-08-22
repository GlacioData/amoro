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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestControllerKey {

  @Test
  public void valueSemanticsIncludeDomainAndResourceId() {
    ControllerKey a1 = ControllerKey.of("domain-a", "1");
    ControllerKey a1Again = ControllerKey.of("domain-a", "1");
    ControllerKey b1 = ControllerKey.of("domain-b", "1");

    assertEquals(a1, a1Again);
    assertEquals(a1.hashCode(), a1Again.hashCode());
    assertNotEquals(a1, b1);
  }

  @Test
  public void sameDomainDifferentResourceIdsAreNotEqual() {
    ControllerKey a1 = ControllerKey.of("domain-a", "1");
    ControllerKey a2 = ControllerKey.of("domain-a", "2");

    assertNotEquals(a1, a2);
  }

  @Test
  public void crossDomainKeysWithBareIdCollisionAreIsolated() {
    // the single-flight registry is keyed by ControllerKey; a bare resourceId would collide
    // across domains, the domain component must prevent that (Spec §5.1 of the process spec).
    ControllerKey processOne = ControllerKey.of("process", "1");
    ControllerKey otherOne = ControllerKey.of("other-domain", "1");

    assertNotEquals(processOne, otherOne);
    assertNotNull(processOne.domain());
    assertNotNull(processOne.resourceId());
    assertEquals("process", processOne.domain());
    assertEquals("1", processOne.resourceId());
  }

  @Test
  public void nullArgumentsAreRejected() {
    assertThrows(NullPointerException.class, () -> ControllerKey.of(null, "1"));
    assertThrows(NullPointerException.class, () -> ControllerKey.of("domain-a", null));
  }

  @Test
  public void toStringMentionsBothComponents() {
    ControllerKey key = ControllerKey.of("process", "42");
    String text = key.toString();

    assertTrue(text.contains("process"));
    assertTrue(text.contains("42"));
  }
}
