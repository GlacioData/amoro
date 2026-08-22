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

package org.apache.amoro.serde;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.amoro.persistence.ControlledResource;
import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.persistence.exception.PersistenceException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;

public class TestVersionAwareSerde {

  // ------------------------------------------------------------------ fake model

  /** Latest (v3) fake resource shape. */
  public static final class FakeResource implements ControlledResource {
    private final String apiVersion;
    private final String name;
    private final String collection;
    private final long resourceVersion;
    private final String payload;
    private final String renamedValue; // introduced in v2 as "addedValue", renamed in v3

    public FakeResource() {
      this("v3", "unknown", "fake", 0L, null, null);
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public FakeResource(
        @com.fasterxml.jackson.annotation.JsonProperty("apiVersion") String apiVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("name") String name,
        @com.fasterxml.jackson.annotation.JsonProperty("collection") String collection,
        @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion") long resourceVersion,
        @com.fasterxml.jackson.annotation.JsonProperty("payload") String payload,
        @com.fasterxml.jackson.annotation.JsonProperty("renamedValue") String renamedValue) {
      this.apiVersion = apiVersion;
      this.name = name;
      this.collection = collection;
      this.resourceVersion = resourceVersion;
      this.payload = payload;
      this.renamedValue = renamedValue;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String collection() {
      return collection;
    }

    @Override
    public long resourceVersion() {
      return resourceVersion;
    }

    public String apiVersion() {
      return apiVersion;
    }

    public String payload() {
      return payload;
    }

    public String renamedValue() {
      return renamedValue;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("apiVersion")
    public String getApiVersion() {
      return apiVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("name")
    public String getName() {
      return name;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("collection")
    public String getCollection() {
      return collection;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("resourceVersion")
    public long getResourceVersion() {
      return resourceVersion;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("payload")
    public String getPayload() {
      return payload;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("renamedValue")
    public String getRenamedValue() {
      return renamedValue;
    }

    @Override
    public ControlledResource withResourceVersion(long newResourceVersion) {
      return new FakeResource(
          apiVersion, name, collection, newResourceVersion, payload, renamedValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FakeResource that = (FakeResource) o;
      return resourceVersion == that.resourceVersion
          && java.util.Objects.equals(apiVersion, that.apiVersion)
          && java.util.Objects.equals(name, that.name)
          && java.util.Objects.equals(collection, that.collection)
          && java.util.Objects.equals(payload, that.payload)
          && java.util.Objects.equals(renamedValue, that.renamedValue);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(
          apiVersion, name, collection, resourceVersion, payload, renamedValue);
    }
  }

  /** v1 -> v2: add "addedValue" with a default (additive evolution with default). */
  static final class V1ToV2Converter implements VersionedResourceConverter {
    @Override
    public String fromVersion() {
      return "v1";
    }

    @Override
    public String toVersion() {
      return "v2";
    }

    @Override
    public JsonNode upgrade(JsonNode thisVersion) {
      ObjectNode node = (ObjectNode) thisVersion.deepCopy();
      if (!node.has("addedValue")) {
        node.put("addedValue", "default-from-converter");
      }
      node.put("apiVersion", "v2");
      return node;
    }
  }

  /** v2 -> v3: rename "addedValue" to "renamedValue" (rename = move + drop old). */
  static final class V2ToV3Converter implements VersionedResourceConverter {
    @Override
    public String fromVersion() {
      return "v2";
    }

    @Override
    public String toVersion() {
      return "v3";
    }

    @Override
    public JsonNode upgrade(JsonNode thisVersion) {
      ObjectNode node = (ObjectNode) thisVersion.deepCopy();
      if (node.has("addedValue") && !node.has("renamedValue")) {
        node.put("renamedValue", node.get("addedValue").asText());
      }
      node.remove("addedValue");
      node.put("apiVersion", "v3");
      return node;
    }
  }

  // ------------------------------------------------------------------ golden fixtures
  // Locked per-version samples. Kept as Java constants instead of .json resource files: JSON
  // cannot carry the Apache license header that rat enforces on repository files.

  static final String GOLDEN_V1_JSON =
      "{\"apiVersion\":\"v1\",\"name\":\"res-1\",\"collection\":\"fake\","
          + "\"resourceVersion\":4,\"payload\":\"v1-payload\"}";
  static final String GOLDEN_V2_JSON =
      "{\"apiVersion\":\"v2\",\"name\":\"res-1\",\"collection\":\"fake\","
          + "\"resourceVersion\":4,\"payload\":\"v1-payload\","
          + "\"addedValue\":\"set-in-v1-era\"}";
  static final String GOLDEN_V3_JSON =
      "{\"apiVersion\":\"v3\",\"name\":\"res-1\",\"collection\":\"fake\","
          + "\"resourceVersion\":4,\"payload\":\"v1-payload\","
          + "\"renamedValue\":\"set-in-v1-era\"}";
  static final String GOLDEN_V1_YAML =
      "apiVersion: \"v1\"\nname: \"res-1\"\ncollection: \"fake\"\n"
          + "resourceVersion: 4\npayload: \"v1-payload\"\n";

  private static SerdeRegistry fakeRegistry() {
    return new SerdeRegistry("v3", Arrays.asList(new V1ToV2Converter(), new V2ToV3Converter()));
  }

  private static VersionAwareJacksonSerde<FakeResource> jsonSerde() {
    return new VersionAwareJacksonSerde<FakeResource>(
        FakeResource.class, fakeRegistry(), SerdeFormat.JSON, 65536);
  }

  private static VersionAwareJacksonSerde<FakeResource> yamlSerde() {
    return new VersionAwareJacksonSerde<FakeResource>(
        FakeResource.class, fakeRegistry(), SerdeFormat.YAML, 65536);
  }

  // ------------------------------------------------------------------ chain tests

  @Test
  public void oldestVersionUpgradesThroughTheWholeChain() {
    for (VersionAwareJacksonSerde<FakeResource> serde : Arrays.asList(jsonSerde(), yamlSerde())) {
      byte[] input =
          serde.serdeFormat() == SerdeFormat.JSON
              ? GOLDEN_V1_JSON.getBytes()
              : GOLDEN_V1_YAML.getBytes();
      DeserializedResource<FakeResource> result = serde.deserialize(input);

      assertEquals("v3", result.resource().apiVersion());
      assertEquals("res-1", result.resource().name());
      assertEquals("fake", result.resource().collection());
      assertEquals(4L, result.resource().resourceVersion());
      assertEquals("v1-payload", result.resource().payload());
      // v1 lacked addedValue -> converter default -> v3 renamed field carries it
      assertEquals("default-from-converter", result.resource().renamedValue());
      assertTrue(
          result.modifiedDuringDeserialization(),
          "reading an old version must flag the lazy upgrade for write-back");
    }
  }

  @Test
  public void middleVersionUpgradesOnlyTheMissingLinks() {
    DeserializedResource<FakeResource> result = jsonSerde().deserialize(GOLDEN_V2_JSON.getBytes());

    assertEquals("v3", result.resource().apiVersion());
    assertEquals("set-in-v1-era", result.resource().renamedValue());
    assertTrue(result.modifiedDuringDeserialization());
  }

  @Test
  public void latestVersionDeserializesWithoutUpgrade() {
    DeserializedResource<FakeResource> result = jsonSerde().deserialize(GOLDEN_V3_JSON.getBytes());

    assertEquals("v3", result.resource().apiVersion());
    assertEquals("set-in-v1-era", result.resource().renamedValue());
    assertFalse(result.modifiedDuringDeserialization(), "no conversion, no write-back flag");
  }

  @Test
  public void missingApiVersionIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> jsonSerde().deserialize("{\"name\":\"x\"}".getBytes()));
    assertThrows(
        IllegalArgumentException.class,
        () -> jsonSerde().deserialize("{\"apiVersion\":\"\",\"name\":\"x\"}".getBytes()));
  }

  @Test
  public void unknownVersionIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            jsonSerde()
                .deserialize(
                    "{\"apiVersion\":\"v99\",\"name\":\"x\",\"collection\":\"fake\"}".getBytes()));
    // the YAML surface has its own malformed-document shapes; errors must be equally explicit
    assertThrows(
        IllegalArgumentException.class,
        () -> yamlSerde().deserialize("name: no-version".getBytes()));
    assertThrows(
        IllegalArgumentException.class,
        () -> yamlSerde().deserialize("apiVersion: \"v99\"\nname: \"x\"".getBytes()));
  }

  @Test
  public void serializeRefusesStaleApiVersionInsteadOfRelabelling() {
    // a model that still carries an old apiVersion constant must fail fast: silently writing it
    // as-is (or relabelling it latest) would bypass the converter chain and brick the row
    FakeResource stale = new FakeResource("v1", "res-stale", "fake", 2L, "p", "r");
    PersistenceException thrown =
        assertThrows(PersistenceException.class, () -> jsonSerde().serialize(stale));
    assertTrue(thrown.getMessage().contains("v3"), "message names the latest version");

    FakeResource typo = new FakeResource("v4", "res-typo", "fake", 2L, "p", "r");
    assertThrows(PersistenceException.class, () -> yamlSerde().serialize(typo));
  }

  @Test
  public void serializationAlwaysWritesTheLatestVersionAndRoundtrips() {
    FakeResource resource = new FakeResource("v3", "res-9", "fake", 7L, "payload-9", "renamed-9");

    for (VersionAwareJacksonSerde<FakeResource> serde : Arrays.asList(jsonSerde(), yamlSerde())) {
      byte[] bytes = serde.serialize(resource);
      DeserializedResource<FakeResource> back = serde.deserialize(bytes);
      assertEquals(resource, back.resource());
      assertFalse(back.modifiedDuringDeserialization());
    }
  }

  @Test
  public void yamlMiddleAndLatestGoldensAlsoUpgradeAndBind() {
    DeserializedResource<FakeResource> v2 =
        yamlSerde()
            .deserialize(
                ("apiVersion: \"v2\"\nname: \"res-1\"\ncollection: \"fake\"\n"
                        + "resourceVersion: 4\npayload: \"v1-payload\"\n"
                        + "addedValue: \"set-in-v1-era\"\n")
                    .getBytes());
    assertEquals("v3", v2.resource().apiVersion());
    assertEquals("set-in-v1-era", v2.resource().renamedValue());
    assertTrue(v2.modifiedDuringDeserialization());

    DeserializedResource<FakeResource> v3 =
        yamlSerde()
            .deserialize(
                ("apiVersion: \"v3\"\nname: \"res-1\"\ncollection: \"fake\"\n"
                        + "resourceVersion: 4\npayload: \"v1-payload\"\n"
                        + "renamedValue: \"set-in-v1-era\"\n")
                    .getBytes());
    assertEquals("v3", v3.resource().apiVersion());
    assertFalse(v3.modifiedDuringDeserialization());
  }

  @Test
  public void base64WrappedBytesRoundTripForBothFormats() {
    // the blob layer stores Base64(document); that wrapping must be transparent to serde
    FakeResource resource = new FakeResource("v3", "res-b64", "fake", 3L, "payload", "value");
    for (VersionAwareJacksonSerde<FakeResource> serde : Arrays.asList(jsonSerde(), yamlSerde())) {
      byte[] document = serde.serialize(resource);
      String stored = Base64.getEncoder().encodeToString(document);
      byte[] loaded = Base64.getDecoder().decode(stored);
      assertEquals(resource, serde.deserialize(loaded).resource());
    }
  }

  @Test
  public void oversizeResourceFailsSerializationWithTheLimitInTheMessage() {
    VersionAwareJacksonSerde<FakeResource> small =
        new VersionAwareJacksonSerde<FakeResource>(
            FakeResource.class, fakeRegistry(), SerdeFormat.JSON, 64);
    FakeResource resource =
        new FakeResource(
            "v3",
            "res-big",
            "fake",
            1L,
            "this-payload-is-long-enough-to-blow-past-sixty-four-bytes",
            "x");

    PersistenceException thrown =
        assertThrows(PersistenceException.class, () -> small.serialize(resource));
    assertTrue(
        thrown.getMessage().contains("64"), "message must carry the limit: " + thrown.getMessage());
  }

  @Test
  public void exactlyAtTheLimitPassesAndOneByteOverFails() {
    // the limit is inclusive: a document of exactly maxResourceBytes is legal, locking the
    // boundary so nobody quietly turns '>' into '>=' later
    FakeResource resource = new FakeResource("v3", "res-edge", "fake", 1L, "payload", "value");
    byte[] document = jsonSerde().serialize(resource);
    VersionAwareJacksonSerde<FakeResource> exact =
        new VersionAwareJacksonSerde<FakeResource>(
            FakeResource.class, fakeRegistry(), SerdeFormat.JSON, document.length);
    assertArrayEquals(document, exact.serialize(resource));

    VersionAwareJacksonSerde<FakeResource> oneBelow =
        new VersionAwareJacksonSerde<FakeResource>(
            FakeResource.class, fakeRegistry(), SerdeFormat.JSON, document.length - 1);
    assertThrows(PersistenceException.class, () -> oneBelow.serialize(resource));
  }

  @Test
  public void defaultMaxResourceBytesStaysAtSixtyFourKib() {
    // the MEDIUMTEXT DDL sizes assume this default; drift would need a conscious decision
    assertEquals(65536, VersionAwareJacksonSerde.DEFAULT_MAX_RESOURCE_BYTES);
  }

  @Test
  public void unknownFieldsAreToleratedForForwardCompatibility() {
    // newer writers may add fields; older readers must not fail (upgrade-window compatibility)
    String withUnknown =
        "{\"apiVersion\":\"v3\",\"name\":\"res-u\",\"collection\":\"fake\","
            + "\"resourceVersion\":2,\"payload\":\"p\",\"renamedValue\":\"r\","
            + "\"brandNewField\":{\"nested\":true}}";
    FakeResource result = jsonSerde().deserialize(withUnknown.getBytes()).resource();
    assertEquals("res-u", result.name());
  }

  @Test
  public void sameChainProducesEquivalentEntitiesAcrossFormats() {
    FakeResource fromJson = jsonSerde().deserialize(GOLDEN_V1_JSON.getBytes()).resource();
    FakeResource fromYaml = yamlSerde().deserialize(GOLDEN_V1_YAML.getBytes()).resource();
    assertEquals(fromJson, fromYaml, "JSON and YAML upgrades must converge to equal entities");
  }

  @Test
  public void detachedCopyIsolatesAliases() {
    VersionAwareJacksonSerde<FakeResource> serde = jsonSerde();
    FakeResource original = new FakeResource("v3", "res-copy", "fake", 5L, "payload", "value");

    FakeResource copy = serde.detachedCopy(original);
    assertEquals(original, copy);
    assertNotSame(original, copy);
    // the fake is immutable, so aliasing cannot be shown here directly; the round-trip itself is
    // the framework guarantee for untrusted mutable models
  }

  // ------------------------------------------------------------------ registry validation

  @Test
  public void duplicateFromVersionFailsRegistration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SerdeRegistry("v3", Arrays.asList(new V1ToV2Converter(), new V1ToV2Converter())));
  }

  @Test
  public void brokenChainFailsRegistration() {
    // v1 -> v2 exists but v2 -> v3 is missing: the chain cannot reach the latest version
    assertThrows(
        IllegalArgumentException.class,
        () -> new SerdeRegistry("v3", Arrays.asList(new V1ToV2Converter())));
  }

  @Test
  public void converterLeavingTheLatestVersionFailsRegistration() {
    // v3 is the latest version; a converter claiming to upgrade v3 would be a dead link that
    // no walk ever reaches, so registration must reject it outright
    VersionedResourceConverter v3ToV4 =
        new VersionedResourceConverter() {
          @Override
          public String fromVersion() {
            return "v3";
          }

          @Override
          public String toVersion() {
            return "v4";
          }

          @Override
          public JsonNode upgrade(JsonNode thisVersion) {
            return thisVersion;
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SerdeRegistry(
                "v3", Arrays.asList(new V1ToV2Converter(), new V2ToV3Converter(), v3ToV4)));
  }

  @Test
  public void cyclicChainFailsRegistration() {
    VersionedResourceConverter v2ToV1 =
        new VersionedResourceConverter() {
          @Override
          public String fromVersion() {
            return "v2";
          }

          @Override
          public String toVersion() {
            return "v1";
          }

          @Override
          public JsonNode upgrade(JsonNode thisVersion) {
            return thisVersion;
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () -> new SerdeRegistry("v3", Arrays.asList(new V1ToV2Converter(), v2ToV1)));
  }

  @Test
  public void emptyRegistryForLatestOnlyIsValid() {
    SerdeRegistry registry = new SerdeRegistry("v3", new ArrayList<VersionedResourceConverter>());
    VersionAwareJacksonSerde<FakeResource> serde =
        new VersionAwareJacksonSerde<FakeResource>(
            FakeResource.class, registry, SerdeFormat.JSON, 65536);
    assertEquals("v3", serde.deserialize(GOLDEN_V3_JSON.getBytes()).resource().apiVersion());
    // but any historical version is now unresolvable
    assertThrows(
        IllegalArgumentException.class, () -> serde.deserialize(GOLDEN_V1_JSON.getBytes()));
  }

  @Test
  public void serdeExposesItsFormat() {
    assertEquals(SerdeFormat.JSON, jsonSerde().serdeFormat());
    assertEquals(SerdeFormat.YAML, yamlSerde().serdeFormat());
  }
}
