<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
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

# Implementation Plan: amoro-ams-v2 控制面框架（调度 + 七层存储）

> 2026-08-22。依据 `tasks/amoro-ams-v2-framework-spec.md`（下称 Spec）编制。
> 任务清单在 `tasks/ams-v2-framework-todo.md`（本仓库 tasks/ 为多专题共用目录，故带专题前缀，偏离技能默认的 tasks/todo.md 命名——与 dev 分支 reconciler 轮的 plan.md/todo.md 命名同理区分）。
> **已并入独立评审轮（Review R1）的全部 REQUIRED 修复与 RECOMMENDED 采纳项**：single-flight 调度语义、写路径统一顺序、Clock/RandomSupplier 时基决策、docker-it 激活机制、mybatis 显式定版、MEDIUMTEXT、toolchains 风险、L1 Repository 落地、离线重放用例、登记表回收。
> **评审 R3（Process Spec 审核）修订已并入**：durable-first、ControllerKey、earliest-deadline single-flight、精确退避/抖动、listener 修复与 1024 mailbox。
> **交付门禁**：T1-T12 每个 Task 均独立执行 JUnit 5 RED→GREEN、五轴 Review、相关离线/集成验证和本地原子提交；前一实施序列节点未提交不得开始下一节点。

## Overview

在 amoro-ams-v2（Spring Boot 3.5.16 / Java 17 toolchains）内语义重实现 SSP AppManager 的通用资源控制面框架：DelayQueue 调度循环 + 七层持久化（**数据库事实源、内存可重建读缓存** + 每域一表 KV blob + actor 单写者 + 版本化 serde + resourceVersion）。纯框架、不接真实资源；JUnit5 全覆盖（离线）；Docker MySQL 5.7 完成落库集成与端到端流程验证。

## Architecture Decisions

| 决策 | 依据 |
|---|---|
| 垂直切片而非逐层横切：先"调度循环可跑"（无存储）→ 再"内存资源生命周期闭环"（fake blob）→ 最后"真库落盘 + 重启重放" | 每片独立可验证，风险前置（调度语义最核心） |
| 契约层与实现同包分置（`org.apache.amoro.control` 接口在前，实现类随后）；L1 Repository + BlobStore 接口归入契约任务 T4 | Spec §2；无独立 Maven 子模块（采访确认）；R1-B1/C2 |
| **single-flight 调度**：每 key `ScheduledEntry` 状态机和锁串行 updater/worker/unschedule；同 key 至多一个包装在飞，deadline 取最早；entry identity 隔离删除后重建 | 防 remove/reinsert 竞态推迟取消；删除后旧 worker 不影响同 key 新 generation；无 tombstone |
| **时基决策**：自定义 `Clock`（基于 System.nanoTime 的单调毫秒）+ 可注入 `RandomSupplier`；`DelayQueue` 只排序/非阻塞 poll，worker 用带 signal version 的 `SchedulerWaitStrategy` 等待，新增/缩短/删除/停机显式 signal；测试用 `advanceClockAndSignal` | 裸 take/真实 timed poll 不能被虚拟时钟正确唤醒；version 检查避免 peek 与 await 间丢信号 |
| **durable-first mutation lane + alias 隔离**：调用线程只入队逻辑命令；actor 内 read latest→detached copy→apply→版本自增→DB→发布 canonical snapshot→listener event；create/get/select/updateFn/listener 都不暴露 cache 可变引用 | 防并发丢更新，也防调用方原地 mutation 在异常/DB 失败时旁路 durable-first |
| T4 先定义 `ListenerEventSink/ListenerEnvelope` port，T5 对 fake sink 做 durable handoff，T6 再提供有界 `ListenerDispatcher` 实现 | 保证 T5 可独立 RED→GREEN，不偷做后续 Task |
| 域二级索引走 `DurableStateProjection.prepare → PreparedProjectionUpdate.commit`：DB 前完成 immutable snapshot/固定上界 key delta 的全部计算，DB 后在 lane 内做 O(1) 原子切换或有声明上界的 non-throwing commit；same-lane 只给出发布顺序，需跨正文/多索引一致读取的领域必须提供包含 canonical read map 的单一 aggregate snapshot 或等价 read barrier | 避免 listener 丢失/延迟导致 admission/list 读到错误索引，也避免把两个独立容器的顺序发布误写成跨对象原子性 |
| delete 的 key-only unschedule 作为 `DurableDeletionHook` 在 mutation lane 内同步执行；hook 完成前 delete stage 不成功、同名 create 不出队，hook 异常则 fence name | 消除 durable delete 后晚到 unschedule 误杀同名新资源的竞态 |
| **listener 修复**：mutation/startup handoff 后回调独立异步执行；同 listener+resource 保序、跨 key/listener 隔离，默认 workers=4/队列=1024/失败重试=3/间隔=1s，耗尽告警，不反转 mutation stage；资源域按自身最终谓词提供有界分页 repair sweep | 框架不知道通用最终谓词；首个 Process 资源在 P4 提供 `ActiveProcessRescheduler` |
| **退避与 mailbox**：实际序列 `{3,3,5,8,13,21,34,55}s`，jitter `[0,250)ms`；mailbox 1024 | 修复 pre-increment off-by-one；与参考 actor 容量一致 |
| 断言策略：mutation future 只证明 durable+内存发布；listener 是异步 dispatcher，所有 listener/schedule 副作用统一用 Awaitility（spring-boot-starter-test 自带，非新增依赖） | listener 失败/队列满不反转 mutation stage，测试不能依赖同步偶然性 |
| **docker-it 激活机制**：pom 属性驱动 `<excludedGroups>${docker-mysql.excluded}</excludedGroups>`（默认 docker-mysql），`-Pdocker-it` 置空属性 + 注入 `spring.profiles.active=mysql57`；assumption 探测 3307 为第二道保险 | R1-D3：pom 字面量 excludedGroups 会被 `-Dgroups` 无法覆盖 → 静默假绿 |
| 测试命令统一走 `JAVA_HOME=jdk-11 ./mvnw -pl amoro-ams-v2 test`（surefire 经 toolchain fork JDK17） | 已验证的混编流程；GJF1.7/spotless 在 JDK11 侧校验 |
| 语法约束：全程 GJF 1.7 可解析（**无** record / switch 表达式 / 文本块 / instanceof 模式匹配 / sealed / 模式 switch）；新增 sql/yaml 同样带 Apache 协议头 | Spec §10.3（R1-E2/E3 扩充）；rat verify 检查 |
| serde Converter 注册用 Spring classpath 扫描；**v2 依赖自包含**：mybatis-spring-boot-starter、mysql-connector-j(8.x)、jackson-dataformat-yaml 等第三方版本一律在 amoro-ams-v2 pom 内显式定义，不复用父 pom 属性/管理 | R1-F1 + 用户确认（2026-08-22）：v2 与 v1 解耦演进，父 pom 版本调整不波及 v2 |
| **serde 格式策略化（R2）**：默认 JSON，按域可选 YAML（Base64(YAML)），apiVersion/Converter 链两格式语义一致 | 用户需求：Process 以 Base64(YAML) 持久化；Spec §5.4 / 台账 #13 |
| **每持久化域一表（R2）**：BlobStore 表名经域绑定注入（枚举白名单），默认域 amoro_resource，Process 域未来绑独立新表 | 用户需求：Process 独立保留期/清理生命周期；Spec §5.3 / 台账 #14 |

## Dependency Graph

```
T1 契约+Backoff+Clock+RandomSupplier ──> T2 DefaultScheduler(single-flight) ──> T3 停机/登记表回收
T4 Persistence 契约 ──> T7 serde ──> T8 mutation sequencer/actor ──> T5 InMemoryPersistence ──> T6 Listener/facade
                                             └──────────────────────> T9 MyBatisBlobStore+DDL+docker-it
T1 Scheduler/ControllerKey 契约 ───────────────────────────────────────────────> T6
T2 + T3 + T6 + T9 ────────────────────────────────────────────────────────────> T10 Spring装配
T10 ──> T11 端到端流程验证(含重启重放+第二fake资源) ──> T12 文档与回归收尾
```

实施固定串行为 **T1 → T2 → T3 → T4 → T7 → T8 → T5 → T6 → T9 → T10 → T11 → T12**。依赖图仍解释技术来源，但不授权并行或越过未提交节点；该顺序确保 InMemoryPersistence 不绕过 mutation lane，且 T5 通过 T4 的 event sink port 独立验证。

## Task List（摘要，详情见 todo）

### Phase 1: 调度循环（无存储）
- [ ] T1 契约层骨架 + BackoffPolicy + Clock/RandomSupplier（S）
- [ ] T2 DefaultScheduler/ScheduledController/Worker（single-flight）（M）
- [ ] T3 优雅停机 + 登记表回收（S）

### Checkpoint 1: 调度器语义全绿（离线），fake Controller 直连可用
- [ ] T1-T3 单测全绿；JDK11 全仓 compile 不回归

### Phase 2: 持久化编排 + 事件链（fake durable store）
- [ ] T4 Persistence 契约（含 L1 Repository、BlobStore 接口）+ 异常体系 + Selector（M）
- [ ] T7 版本化 serde（JSON/YAML 格式策略化）（M）
- [ ] T8 BlobStoreActor/mutation sequencer mailbox（M）
- [ ] T5 InMemoryPersistence 核心（actor 内 read-apply-write）（M）
- [ ] T6 Listener 分发/修复 + L2/L3 facade + 离线重放用例（M）

### Checkpoint 2: fake durable store 资源生命周期闭环
- [ ] create 的 fake durable write 成功后才 listener→schedule→invoke；失败时内存不变；serde 与 postStart 离线重放用例绿

### Phase 3: 落库 + 装配 + Docker 验证
- [ ] T9 MyBatisBlobStore + 三库方言 DDL + 表名参数化 + docker-it 机制（M）
- [ ] T10 Spring 装配 + 配置键 + SmartLifecycle（S）
- [ ] T11 端到端流程验证 + 资源无关性证明（M）

### Checkpoint 3: Spec §10 验收闭环
- [ ] 四条验收全过（离线单测 + `-Pdocker-it` 流程验证 + 双 JDK 构建回归 + spotless/checkstyle/rat）

### Phase 4: 收尾
- [ ] T12 文档/README（含 toolchains 前置保全）/提交切分（S）

## Risks and Mitigations

| 风险 | 影响 | 缓解 |
|---|---|---|
| **当前 core-hadoop2/core-hadoop3 CI 只安装 JDK11，未生成 JDK17 toolchain，且 path filter 未包含 `amoro-ams-v2/**`**；全仓 validate 会在 v2 toolchain 门禁失败或根本不因 v2 文件触发 | 高 | 本地按 README 双 JDK 继续逐 Task 验证；T12 必须更新/新增 v2 workflow，安装 JDK11+17、生成 toolchains、覆盖 v2 path 并运行离线 verify；发布前必须有真实 CI 证据 |
| **mybatis-spring-boot-starter 需显式定版**，与 Boot 3.5 的适配版本需验证 | 高 | T9 首轮编译即验证；定版 3.0.x 线；失败则降 Boot 兼容矩阵排查 |
| mysql:5.7 无 arm64 镜像，拉取/模拟慢或失败 | 中 | 备选 mysql:5.7.44；docker-it 独立 profile 不阻塞主线；本地已验证 docker 29.6.2 可用 |
| Boot BOM mysql-connector-j 9.x 不支持 5.7 | 高 | 模块 pom 独立定版 8.x（依赖自包含原则，用户确认）；集成测试首轮即验证连通 |
| TEXT 64KB × Base64 膨胀溢出（JSON 65536 → Base64 ~87KB） | 高 | MySQL DDL 用 `MEDIUMTEXT/DATETIME`；PG 用 `TEXT/TIMESTAMP(3) WITHOUT TIME ZONE`；Derby 用 `CLOB/TIMESTAMP` |
| single-flight 登记表泄漏或删除后旧 worker 复活 | 高 | `unschedule` + entry identity-aware remove；排队/在途/同 key 重建竞态测试 |
| durable delete 返回后才 key-only unschedule，可能误杀已同名重建的新 entry | 高 | unschedule 放入 same-lane `DurableDeletionHook`，delete stage/下一 mutation 之前完成；hook 失败 fence name |
| listener 在 durable write 后失败或 dispatcher 满导致资源永不调度 | 高 | mutation 仍成功；dropped 指标/告警；资源域 repair sweep；T6 与 Process P4 覆盖 |
| CompletionStage 全异步 + L2 deref/timeout 实现复杂（死锁/漏异常） | 中 | 统一断言策略（join 直断 + Awaitility）；actor 单写者无锁化；超时路径单测 |
| JDBC 连接中断时提交结果未知，旧内存继续写会扩大分歧 | 高 | 新连接点读 previous/candidate；不可判定则按 key fence、告警，repair reload 后解除 |
| Spring 停机顺序（actor 排空 vs scheduler shutdown 先后） | 中 | T10 SmartLifecycle phase 显式定序 + 停机顺序单测 |
| Docker 集成测试间数据残留 | 低 | 每测试类独立 collection 或 @BeforeEach 清表；schema initializer 幂等 |
| YAML 序列化分支的体积与一致性（空白敏感、注解覆盖） | 低 | Base64 编码规避空白敏感；MEDIUMTEXT 余量；T7 双格式往返+转换链单测锁定 |
| GJF 1.7 语法约束被无意违反 | 中 | 每任务验收含 spotless:check；禁用清单含 instanceof 模式/sealed 等 |
| Process 独立表被运行时 truncate 误清 | 高 | Process Spec 已定为最终谓词 + cutoff + bounded batch delete；runtime truncate 禁止 |

## 已核实环境缺口与 Open Questions

- [ ] JIRA issue 号（提交信息前缀仍为占位 `[ams-v2]`）。
- [x] 当前 core CI **不具备** v2 所需的 JDK17 toolchain/path trigger；已转为 T12 必做项，不再作为未知问题。
- [ ] docker-it 是否纳入 CI（或仅本地/发布前手工门禁）——待 CI 现状确认。
