<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to You under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# 已归档：amoro-ams-v2 Process 旧任务清单

此文件不再跟踪任务状态。当前可执行任务及验收命令统一维护在
[`tasks/todo.md`](todo.md)。

硬边界：本轮只用 `simulated` / `dummy-maintenance` 跑通 Process 创建、调度、Engine SPI、回调、
取消、人工消解、重启恢复、release 与 TTL；不实现或注册任何 Iceberg/Paimon Action，不提交真实
Spark 作业。任何真实格式或真实远端执行任务都必须先建立独立 Spec。
