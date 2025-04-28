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

package org.apache.amoro.optimizing.maintainer;

import org.apache.amoro.maintainer.api.TableMaintainer;
import org.apache.amoro.maintainer.output.CleanOrphanOutPut;
import org.apache.amoro.optimizing.IcebergCleanOrphanInput;
import org.apache.amoro.optimizing.IcebergDanglingDeleteFilesInput;
import org.apache.amoro.optimizing.IcebergDeleteFilesOutput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;

/**
 * API for maintaining table V2 V2 return result input and output
 *
 * <p>Includes: clean content files, clean metadata, clean dangling delete files, expire snapshots,
 * auto create tags.
 */
public interface IcebergTableMaintainerV2 extends TableMaintainer {

  /**
   * Clean table orphan files. Includes: data files, metadata files.
   *
   * @param input cleanOrphanInput
   * @return CleanOrphanOutPut
   */
  CleanOrphanOutPut cleanOrphanFiles(IcebergCleanOrphanInput input);

  /** Clean table dangling delete files. */
  IcebergDeleteFilesOutput cleanDanglingDeleteFiles(IcebergDanglingDeleteFilesInput input);

  /**
   * Expire snapshots. The optimizing based on the snapshot that the current table relies on will
   * not expire according to TableRuntime.
   */
  IcebergExpireSnapshotsOutput expireSnapshots(IcebergExpireSnapshotInput input);

  /**
   * Expire historical data based on the expiration field, and data that exceeds the retention
   * period will be purged
   */
  void expireData();

  /** Auto create tags for table. */
  void autoCreateTags();
}
