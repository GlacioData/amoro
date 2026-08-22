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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.amoro.persistence.PersistenceDomain.SerdeFormat;
import org.apache.amoro.persistence.exception.PersistenceException;

import java.io.IOException;
import java.util.Objects;

/**
 * Version-aware resource serde for one domain format (framework spec §5.4). Format is strategized
 * per {@link PersistenceDomain}: JSON by default, YAML for domains that chose it — apiVersion
 * handling and the converter chain are identical across formats because both parse into the same
 * {@link JsonNode} trees. The serialized size cap counts the raw document bytes (before any Base64
 * wrapping, which grows them ~33%); the reference implementation hard-codes 64KiB and silently
 * overflows — here it is configurable and enforced (fidelity ledger #5).
 *
 * <p>Unknown fields are tolerated ({@code FAIL_ON_UNKNOWN_PROPERTIES=false}) so a rolling upgrade
 * window stays bidirectionally compatible; unknown fields are dropped, not preserved.
 */
public final class VersionAwareJacksonSerde<R> implements ResourceSerde<R> {

  public static final int DEFAULT_MAX_RESOURCE_BYTES = 65536;

  private final Class<R> resourceClass;
  private final SerdeRegistry registry;
  private final SerdeFormat serdeFormat;
  private final int maxResourceBytes;
  private final ObjectMapper mapper;

  public VersionAwareJacksonSerde(
      Class<R> resourceClass,
      SerdeRegistry registry,
      SerdeFormat serdeFormat,
      int maxResourceBytes) {
    this.resourceClass = Objects.requireNonNull(resourceClass, "resourceClass");
    this.registry = Objects.requireNonNull(registry, "registry");
    this.serdeFormat = Objects.requireNonNull(serdeFormat, "serdeFormat");
    if (maxResourceBytes <= 0) {
      throw new IllegalArgumentException("maxResourceBytes must be > 0, got " + maxResourceBytes);
    }
    this.maxResourceBytes = maxResourceBytes;
    this.mapper =
        serdeFormat == SerdeFormat.YAML ? new ObjectMapper(new YAMLFactory()) : new ObjectMapper();
    // creator binding by constructor parameter name (kept by the -parameters compile flag);
    // without the module Jackson cannot name multi-arg constructor arguments
    this.mapper.registerModule(
        new com.fasterxml.jackson.module.paramnames.ParameterNamesModule(
            com.fasterxml.jackson.annotation.JsonCreator.Mode.PROPERTIES));
    this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  /** The document format this instance reads and writes. */
  public SerdeFormat serdeFormat() {
    return serdeFormat;
  }

  /**
   * Serializes the resource as the latest version document.
   *
   * @throws PersistenceException when encoding fails or the document exceeds the size limit
   */
  public byte[] serialize(R resource) {
    Objects.requireNonNull(resource, "resource");
    JsonNode tree;
    byte[] bytes;
    try {
      tree = mapper.valueToTree(resource);
      JsonNode versionNode = tree.get("apiVersion");
      if (versionNode == null
          || !versionNode.isTextual()
          || !registry.latestVersion().equals(versionNode.asText().trim())) {
        // fail-fast, never relabel: silently writing an old shape as latest would bypass the
        // converter chain and corrupt the durable history (framework spec §5.4)
        throw new PersistenceException(
            "refusing to serialize resource with apiVersion "
                + (versionNode == null ? "<missing>" : "'" + versionNode.asText() + "'")
                + ": every write path must produce the latest version '"
                + registry.latestVersion()
                + "'");
      }
      bytes = mapper.writeValueAsBytes(tree);
    } catch (IOException e) { // JsonProcessingException extends IOException
      throw new PersistenceException("failed to serialize resource " + resource, e);
    }
    if (bytes.length > maxResourceBytes) {
      throw new PersistenceException(
          "serialized resource is "
              + bytes.length
              + " bytes, exceeding the "
              + maxResourceBytes
              + "-byte limit");
    }
    return bytes;
  }

  /**
   * Reads the document tree, resolves the apiVersion, walks the converter chain to the latest
   * version when needed, then binds the tree to the latest resource class.
   *
   * @throws IllegalArgumentException when the document is not a versioned resource (missing or
   *     blank apiVersion) or its version has no path to the latest
   * @throws PersistenceException on malformed input or binding failures
   */
  public DeserializedResource<R> deserialize(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    JsonNode tree;
    try {
      tree = mapper.readTree(bytes);
    } catch (IOException e) {
      throw new PersistenceException("malformed resource document", e);
    }
    if (tree == null || !tree.isObject()) {
      throw new IllegalArgumentException("not a versioned resource: expected a JSON/YAML object");
    }
    JsonNode versionNode = tree.get("apiVersion");
    if (versionNode == null || !versionNode.isTextual() || versionNode.asText().trim().isEmpty()) {
      throw new IllegalArgumentException("not a versioned resource: apiVersion is missing");
    }
    String version = versionNode.asText().trim();
    boolean upgraded = false;
    if (!version.equals(registry.latestVersion())) {
      tree = upgradeToLatest(tree, version);
      upgraded = true;
    }
    R resource;
    try {
      resource = mapper.treeToValue(tree, resourceClass);
    } catch (IOException e) {
      throw new PersistenceException(
          "failed to bind versioned document of " + resourceClass.getSimpleName(), e);
    }
    return upgraded
        ? DeserializedResource.upgraded(resource)
        : DeserializedResource.current(resource);
  }

  private JsonNode upgradeToLatest(JsonNode tree, String version) {
    int hops = 0;
    while (!version.equals(registry.latestVersion())) {
      VersionedResourceConverter link = registry.linkFrom(version);
      if (link == null) {
        throw new IllegalArgumentException(
            "unable to find a converter for version '"
                + version
                + "' on the chain to '"
                + registry.latestVersion()
                + "'");
      }
      tree = link.upgrade(tree);
      if (tree == null || !tree.isObject()) {
        throw new PersistenceException(
            "converter "
                + link.fromVersion()
                + " -> "
                + link.toVersion()
                + " produced a non-object document");
      }
      version = link.toVersion().trim();
      if (++hops > registry.linkCount() + 1) {
        throw new PersistenceException("converter chain did not converge to the latest version");
      }
    }
    return tree;
  }

  /**
   * Alias isolation for untrusted resource models: a full serialize/deserialize round-trip, so the
   * returned instance shares no mutable state reachable from {@code resource}.
   */
  public R detachedCopy(R resource) {
    return deserialize(serialize(resource)).resource();
  }
}
