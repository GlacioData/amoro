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

package org.apache.amoro.persistence.blob;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.sql.Timestamp;
import java.util.List;

/**
 * L7: the five SQL statements of one homogeneous KV table (framework spec §5.3). The physical table
 * name is bound per persistence domain through {@code ${table}} substitution — callers must pass
 * only names from the {@link org.apache.amoro.persistence.PersistenceDomain.Table} whitelist, which
 * makes the substitution injection-safe by construction.
 */
public interface ResourceBlobMapper {

  @Insert(
      "INSERT INTO ${table} (name, collection, value, last_updated) "
          + "VALUES (#{name}, #{collection}, #{value}, #{lastUpdated})")
  int insert(
      @Param("table") String table,
      @Param("name") String name,
      @Param("collection") String collection,
      @Param("value") String value,
      @Param("lastUpdated") Timestamp lastUpdated);

  @Update(
      "UPDATE ${table} SET value = #{value}, last_updated = #{lastUpdated} "
          + "WHERE name = #{name} AND collection = #{collection}")
  int update(
      @Param("table") String table,
      @Param("name") String name,
      @Param("collection") String collection,
      @Param("value") String value,
      @Param("lastUpdated") Timestamp lastUpdated);

  @Delete("DELETE FROM ${table} WHERE name = #{name} AND collection = #{collection}")
  int delete(
      @Param("table") String table,
      @Param("name") String name,
      @Param("collection") String collection);

  @Select("SELECT value FROM ${table} WHERE name = #{name} AND collection = #{collection}")
  String find(
      @Param("table") String table,
      @Param("name") String name,
      @Param("collection") String collection);

  @Select("SELECT name, value FROM ${table} WHERE collection = #{collection} ORDER BY name")
  List<BlobRow> selectAll(@Param("table") String table, @Param("collection") String collection);
}
