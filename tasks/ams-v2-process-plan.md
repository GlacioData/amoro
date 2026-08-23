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

# 已归档：amoro-ams-v2 Process 旧实施计划

此文件仅保留旧路径，**不再是实施依据**。当前权威计划为
[`tasks/plan.md`](plan.md)，权威规格为
[`tasks/amoro-ams-v2-process-spec.md`](amoro-ams-v2-process-spec.md)。

本次范围只有 `amoro-ams-v2` 的 Process 控制面与模拟全生命周期：

- 默认配置不装配任何 Engine、Action 或模拟表；
- 显式开启模拟后，只注册 `simulated` format、`dummy-maintenance` action，以及 Local/Remote
  simulator；
- 不接入、依赖、加载或执行任何 Iceberg/Paimon Action；
- 不读取真实表，不修改元数据或文件，不提交真实 Spark 作业；
- 真实格式 Action 与真实 Remote 协议只能由后续独立 Spec 规划和实现。

旧计划中涉及 v1 表读取、真实 Iceberg/Paimon Action、真实 Remote Spark adapter 或生产流量迁移的
条目全部作废，禁止从 Git 历史恢复后继续实施。
