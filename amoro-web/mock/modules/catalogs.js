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

export default [
  {
    url: '/mock/api/ams/v1/catalogs',
    method: 'get',
    response: () => ({
      "message": "success",
      "code": 200,
      "result": [
        {
          "catalogName": "test_catalog",
          "catalogType": "hadoop"
        }
      ]
    }),
  },
  {
    url: '/mock/api/ams/v1/catalogs',
    method: 'post',
    response: () => ({"message":"success","code":200,"result":""}),
  },

  {
    url: '/mock/api/ams/v1/catalogs/test_catalog/databases',
    method: 'get',
    response: () => {
      return { "message": "success", "code": 200, "result": ["db", "test", "acc"] }
    },
  },
  {
    url: '/mock/api/ams/v1/catalogs/test_catalog/databases/db/tables',
    method: 'get',
    response: () => {
      return { "message": "success", "code": 200, "result": [{ "name": "user", "type": "ICEBERG" },{ "name": "wf", "type": "ICEBERG" }, { "name": "xcvz", "type": "ICEBERG" }] };
    },
  },
  {
    url: '/mock/api/ams/v1/catalogs/:id',
    method: 'get',
    response: () => ({
      "message": "success",
      "code": 200,
      "result": {
        "name": "test_catalog",
        "type": "hadoop",
        "optimizerGroup": "local",
        "tableFormatList": [
          "ICEBERG"
        ],
        "storageConfig": {
          "storage.type": "Hadoop",
          "hive.site": {
            "fileName": "hive-site.xml"
          },
          "hadoop.core.site": {
            "fileName": "core-site.xml"
          },
          "hadoop.hdfs.site": {
            "fileName": "hdfs-site.xml"
          }
        },
        "authConfig": {
          "auth.type": "SIMPLE",
          "auth.simple.hadoop_username": "root"
        },
        "properties": {
          "warehouse": "/mnt/dfs/4/warehouse_public",
          "type": "hadoop"
        },
        "tableProperties": {}
      }
    }),
  },
  {
    url: '/mock/api/ams/v1/catalogs/:id',
    method: 'put',
    response: () => ({ "message": "success", "code": 200, "result": null }),
  },
  {
    url: '/mock/api/ams/v1/catalogs/:id',
    method: 'delete',
    response: () => ({ "message": "success", "code": 200, "result": true }),
  },
  {
    url: '/mock/api/ams/v1/catalogs/:id/delete/check',
    method: 'get',
    response: () => ({ "message": "success", "code": 200, "result": true }),
  },
  {
    url: '/mock/api/ams/v1/catalogs/metastore/types',
    method: 'get',
    response: () => ({
      "message": "success",
      "code": 200,
      "result": [
        {
          "value": "ams",
          "display": "Arctic Metastore"
        },
        {
          "value": "hive",
          "display": "Hive Metastore"
        },
        {
          "value": "hadoop",
          "display": "Hadoop"
        },
        {
          "value": "glue",
          "display": "Glue"
        },
        {
          "value": "custom",
          "display": "Custom"
        }
      ]
    }),
  },
  {
    url: '/mock/api/ams/v1/tables/catalogs/:catalog/dbs/:dbId/tables/:tableName/optimizing-processes/:processesId/tasks',
    method: 'get',
    response: () => ({
      "message": "success",
      "code": 200,
      "result": [
        {
          "value": "ams",
          "display": "Arctic Metastore"
        },
        {
          "value": "hive",
          "display": "Hive Metastore"
        },
        {
          "value": "hadoop",
          "display": "Hadoop"
        },
        {
          "value": "glue",
          "display": "Glue"
        },
        {
          "value": "custom",
          "display": "Custom"
        }
      ]
    }),
  },
]
