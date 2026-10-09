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

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.paimon.CoreOptions;

import java.util.Map;
import java.util.Objects;

/** Explicit FULL rewrite policy for fixed Append tables, before config defaults erase presence. */
public final class PaimonAppendRewriteOptions {
  public static final String AMORO_FULL_REWRITE = "self-optimizing.full.rewrite-all-files";

  public enum Source {
    AMORO_EXPLICIT,
    PAIMON_EXPLICIT,
    AMORO_DEFAULT
  }

  private final boolean fullRewriteAllFiles;
  private final Source source;

  private PaimonAppendRewriteOptions(boolean fullRewriteAllFiles, Source source) {
    this.fullRewriteAllFiles = fullRewriteAllFiles;
    this.source = source;
  }

  public static PaimonAppendRewriteOptions from(Map<String, String> mergedProperties) {
    Objects.requireNonNull(mergedProperties, "mergedProperties");
    if (mergedProperties.containsKey(AMORO_FULL_REWRITE)) {
      return new PaimonAppendRewriteOptions(
          parseBoolean(mergedProperties, AMORO_FULL_REWRITE), Source.AMORO_EXPLICIT);
    }
    // Source:
    // https://github.com/apache/paimon/blob/ac223f47d916f4c8e9c07fd1745db448e4e7d0d8/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L236-L241
    // Native default false must not replace Amoro's default true when neither key is explicit.
    String nativeKey = CoreOptions.COMPACTION_FORCE_REWRITE_ALL_FILES.key();
    if (mergedProperties.containsKey(nativeKey)) {
      return new PaimonAppendRewriteOptions(
          parseBoolean(mergedProperties, nativeKey), Source.PAIMON_EXPLICIT);
    }
    return new PaimonAppendRewriteOptions(true, Source.AMORO_DEFAULT);
  }

  private static boolean parseBoolean(Map<String, String> properties, String key) {
    String value = properties.get(key);
    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
      throw new IllegalArgumentException(key + " must be true or false");
    }
    return Boolean.parseBoolean(value);
  }

  public boolean fullRewriteAllFiles() {
    return fullRewriteAllFiles;
  }

  public Source source() {
    return source;
  }
}
