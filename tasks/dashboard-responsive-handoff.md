<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# AMS Dashboard 响应式交付说明

**更新日期** 2026-11-21（新加坡时间）

使用旧版 `sl/dev` 的 `591b90ca5` 做同数据对照。交付分支为 `codex/element-plus-migration`，前端组件为 Element Plus 2.14.8。业务 API、标签写入确认和快照范围继续遵循既有契约。复现时使用隔离环境的测试数据和账号。

## 页面行为

| 页面 | 桌面 | 窄屏 |
| --- | --- | --- |
| 全局导航和账号菜单 | 可手动收起侧栏，账号菜单支持鼠标与键盘 | 1024 px 起收为图标导航，图标保留文字名称，账号菜单支持触摸 |
| 表浏览和表详情 | 保留左右布局和横向拖动 | 平板限制表树所占宽度，760 px 起通过表按钮展开目录，选表后收起目录 |
| Snapshot | 保留引用、筛选、展开、分页与文件详情 | 工具栏换行，图表按内容区宽度堆叠，长表格在组件内部滚动，浮层限制在视口内 |
| Tag | 保留列表列顺序、筛选和操作 | 760 px 起使用卡片读出名称、Snapshot、提交时间和保留期。复用同一组复制、保留期及删除按钮和权限判断 |
| Catalog 与表单 | 保留原分组、校验、上传和操作 | 清单及详情上下排列，表单标签与输入上下排列。短屏弹窗只滚动内容区，确认按钮保持可达 |
| 概览、优化、资源与设置 | 保留卡片、图表、各标签页和原有交互 | 汇总卡片与过滤项自动换行。宽表的滚动限制在表组件内 |

## 后端联调和回归入口

1. 使用 JDK 11 构建当前分支的 AMS 发行包。Maven 前端构建会安装 POM 固定的 Node 和 pnpm。

   ```bash
   CI=true ./mvnw -B -ntp -pl dist -am package -DskipTests
   ```

2. 用独立的配置、Derby 和仓库目录启动生成包。参考 [Spec 的本地运行步骤](../SPEC-element-plus-migration.md#9-本地真实-ams-验证)。
3. 连接至少一张含多次提交及 Tag 的 Paimon 表，以及一张含 DDL 历史的 Iceberg 表。管理员登录后按 [任务清单](todo.md) 重放。
4. Snapshot 从列表打开图表、引用选择、展开摘要和文件详情。Tag 分别操作搜索、创建、复制、保留期与删除。在 390×520 特别核对弹窗取消及提交按钮，在桌面核对原有信息顺序和交互。
5. 核对 Overview、Catalog 读写、Files、Operations、优化器及资源组、系统与容器设置、Swagger、登录和 404。视口使用 320、390、768、1024、1440 px。完整页面宽度应等于视口宽度，宽表允许内部横向滚动。

启动、角色鉴权和表格式集成沿用 AMS 既有行为。Hive/Mixed、外置认证、其他权限组合及浏览器兼容性使用对应环境继续联调。测试统计和截图收录在本地 `output/element-plus-verification` 的 [验证报告](../output/element-plus-verification/report.md)。


## 本次交付验证

- 本地 Amoro 1.0.4 发行包已构建。包内前端 JAR 与运行实例的 HTTP 静态资源均已按 SHA-256 核验。
- 旧版和候选各完成 155 个浏览器状态检查。候选另通过 16 个中文页面状态、5 条短屏触摸流程和 18 条桌面操作回归。
- 每端 10 个有效性能样本。首屏 JS 解析与编译中位数降低 34.8%，Snapshot 最后必要响应至可操作绘制中位数降低 59.3%。
