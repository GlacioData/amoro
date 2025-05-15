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

package org.apache.amoro.server.persistence.mapper;

import org.apache.amoro.maintainer.api.MaintainerResult;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface MaintainerMapper {

  @Insert(
      "INSERT INTO table_runtime_maintainer (catalog_name, db_name,table_name,table_format,maintainer_type,status,table_summary) VALUES ("
          + "#{result.catalog_name},"
          + "#{result.db_name},"
          + "#{result.table_name},"
          + "#{result.table_format},"
          + "#{result.maintainer_type},"
          + "#{result.status},"
          + "#{result.summary,typeHandler=org.apache.amoro.server.persistence.converter.Map2StringConverter}")
  void insertMaintainerReport(@Param("result") MaintainerResult result);
}
