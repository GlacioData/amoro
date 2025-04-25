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

package org.apache.amoro.maintainer.iceberg;

import org.apache.amoro.config.ConfigHelpers;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.table.TableProperties;
import org.apache.amoro.utils.CompatiblePropertyUtil;

import java.time.temporal.ChronoUnit;
import java.util.Map;

public class ConfigUtils {

  public static TableConfiguration parseTableConfig(Map<String, String> properties) {
    boolean gcEnabled = CompatiblePropertyUtil.propertyAsBoolean(properties, "gc.enabled", true);
    return new TableConfiguration()
        .setExpireSnapshotEnabled(
            gcEnabled
                && CompatiblePropertyUtil.propertyAsBoolean(
                    properties,
                    TableProperties.ENABLE_TABLE_EXPIRE,
                    TableProperties.ENABLE_TABLE_EXPIRE_DEFAULT))
        .setSnapshotTTLMinutes(
            ConfigHelpers.TimeUtils.parseDuration(
                        CompatiblePropertyUtil.propertyAsString(
                            properties,
                            TableProperties.SNAPSHOT_KEEP_DURATION,
                            TableProperties.SNAPSHOT_KEEP_DURATION_DEFAULT),
                        ChronoUnit.MINUTES)
                    .getSeconds()
                / 60)
        .setSnapshotMinCount(
            CompatiblePropertyUtil.propertyAsInt(
                properties,
                TableProperties.SNAPSHOT_MIN_COUNT,
                TableProperties.SNAPSHOT_MIN_COUNT_DEFAULT))
        .setChangeDataTTLMinutes(
            CompatiblePropertyUtil.propertyAsLong(
                properties,
                TableProperties.CHANGE_DATA_TTL,
                TableProperties.CHANGE_DATA_TTL_DEFAULT))
        .setCleanOrphanEnabled(
            gcEnabled
                && CompatiblePropertyUtil.propertyAsBoolean(
                    properties,
                    TableProperties.ENABLE_ORPHAN_CLEAN,
                    TableProperties.ENABLE_ORPHAN_CLEAN_DEFAULT))
        .setOrphanExistingMinutes(
            CompatiblePropertyUtil.propertyAsLong(
                properties,
                TableProperties.MIN_ORPHAN_FILE_EXISTING_TIME,
                TableProperties.MIN_ORPHAN_FILE_EXISTING_TIME_DEFAULT))
        .setDeleteDanglingDeleteFilesEnabled(
            gcEnabled
                && CompatiblePropertyUtil.propertyAsBoolean(
                    properties,
                    TableProperties.ENABLE_DANGLING_DELETE_FILES_CLEAN,
                    TableProperties.ENABLE_DANGLING_DELETE_FILES_CLEAN_DEFAULT));
  }
}
