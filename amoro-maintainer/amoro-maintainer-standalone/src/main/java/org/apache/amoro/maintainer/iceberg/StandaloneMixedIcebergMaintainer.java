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

import org.apache.amoro.maintainer.output.CleanOrphanOutPut;
import org.apache.amoro.optimizing.IcebergCleanOrphanInput;
import org.apache.amoro.optimizing.IcebergDanglingDeleteFilesInput;
import org.apache.amoro.optimizing.IcebergDeleteFilesOutput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;
import org.apache.amoro.optimizing.maintainer.IcebergTableMaintainerV2;
import org.apache.amoro.table.MixedTable;
import org.apache.amoro.table.UnkeyedTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneMixedIcebergMaintainer implements IcebergTableMaintainerV2 {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneMixedIcebergMaintainer.class);

  private final MixedTable mixedTable;

  private ChangeTableMaintainer changeMaintainer;

  private final BaseTableMaintainer baseMaintainer;

  public StandaloneMixedIcebergMaintainer(MixedTable mixedTable) {
    this.mixedTable = mixedTable;
    if (mixedTable.isKeyedTable()) {
      changeMaintainer = new ChangeTableMaintainer(mixedTable.asKeyedTable().changeTable());
      baseMaintainer = new BaseTableMaintainer(mixedTable.asKeyedTable().baseTable());
    } else {
      baseMaintainer = new BaseTableMaintainer(mixedTable.asUnkeyedTable());
    }
  }

  @Override
  public CleanOrphanOutPut cleanOrphanFiles(IcebergCleanOrphanInput input) {
    if (changeMaintainer != null) {
      changeMaintainer.cleanOrphanFiles(input);
    }
    return baseMaintainer.cleanOrphanFiles(input);
  }

  @Override
  public IcebergDeleteFilesOutput cleanDanglingDeleteFiles(IcebergDanglingDeleteFilesInput input) {
    if (changeMaintainer != null) {
      changeMaintainer.cleanDanglingDeleteFiles(input);
    }
    return baseMaintainer.cleanDanglingDeleteFiles(input);
  }

  @Override
  public IcebergExpireSnapshotsOutput expireSnapshots(IcebergExpireSnapshotInput input) {
    if (changeMaintainer != null) {
      changeMaintainer.expireSnapshots(input);
    }
    return baseMaintainer.expireSnapshots(input);
  }

  @Override
  public void expireData() {}

  @Override
  public void autoCreateTags() {}

  public class ChangeTableMaintainer extends StandaloneIcebergMaintainer {

    public ChangeTableMaintainer(UnkeyedTable table) {
      super(table);
    }
  }

  public class BaseTableMaintainer extends StandaloneIcebergMaintainer {

    public BaseTableMaintainer(UnkeyedTable table) {
      super(table);
    }
  }
}
