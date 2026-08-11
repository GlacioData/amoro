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

package org.apache.amoro.optimizing;

import org.apache.amoro.table.FormatPendingInput;
import org.apache.amoro.table.health.TableAnalysisKey;
import org.apache.amoro.table.health.TableHealthDetails;

/**
 * Format-agnostic result of evaluating one fixed table state.
 *
 * <p>Format implementations may extend this interface with immutable structural facts used by their
 * planner.
 */
public interface FormatTableAnalysis {

  TableAnalysisKey key();

  FormatPendingInput pendingInput();

  TableHealthDetails healthDetails();
}
