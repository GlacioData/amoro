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

package org.apache.amoro.maintainer.api;

public class OptimizerTableMaintainerProperties {

  // Resource properties
  public static final String AMS_MAINTAINER_URI = "ams-optimizing-uri";

  // Resource container properties
  public static final String EXPORT_PROPERTY_PREFIX = "export.";

  // Resource group properties
  public static final String MAINTAINER_EXECUTION_PARALLEL = "execution-parallel";
  public static final String TABLE_MAINTAINER_TYPE = "type";
  public static final String OPTIMIZER_CATALOG = "catalog";
  public static final String OPTIMIZER_DATABASE = "database";
  public static final String OPTIMIZER_TABLE = "table";
}
