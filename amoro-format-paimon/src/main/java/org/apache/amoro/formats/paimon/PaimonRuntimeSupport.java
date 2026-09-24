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

package org.apache.amoro.formats.paimon;

import org.apache.amoro.TableRuntime;
import org.apache.amoro.formats.paimon.optimizing.PaimonPendingInput;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableRuntimeSupport;
import org.apache.amoro.table.descriptor.ServerTableMeta;

import java.util.Collections;
import java.util.List;

/** Paimon owns its state codecs and the cached schema-time detail projection. */
public class PaimonRuntimeSupport implements TableRuntimeSupport {
  private final StateKey<PaimonPendingInput> pendingInputKey =
      StateKey.stateKey("pending_input")
          .jsonType(PaimonPendingInput.class)
          .defaultValue(new PaimonPendingInput());

  @Override
  public StateKey<PaimonPendingInput> pendingInputKey() {
    return pendingInputKey;
  }

  @Override
  public List<StateKey<?>> requiredStates() {
    return Collections.singletonList(PaimonTableCreationTime.KEY);
  }

  @Override
  public void projectDetail(TableRuntime runtime, ServerTableMeta target) {
    PaimonTableCreationTime.project(runtime, target);
  }
}
