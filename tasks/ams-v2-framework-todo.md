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

# amoro-ams-v2 控制面框架 — 任务清单

> 计划文档：`tasks/ams-v2-framework-plan.md`；规格：`tasks/amoro-ams-v2-framework-spec.md`（Spec）。
> 已并入评审 R1 全部 REQUIRED 修复与 RECOMMENDED 采纳项；依赖版本遵循**自包含原则**（v2 模块 pom 内独立定版，不用父 pom 属性）。
>
> 统一测试命令（离线，docker-mysql 组默认排除）：
> `JAVA_HOME=<jdk11> ./mvnw -pl amoro-ams-v2 test`
> Docker 集成（先按 Spec §8 启动 amoro-mysql57 容器）：
> `JAVA_HOME=<jdk11> ./mvnw -pl amoro-ams-v2 test -Pdocker-it`（profile 置空 excludedGroups 属性 + 注入 spring.profiles.active=mysql57；严禁 pom 字面量 excludedGroups）
>
> 门禁约束：每个 Task 都必须先写会失败的 JUnit 5 行为测试（纯文档/配置除外），再实现 GREEN，完成五轴 Review 和相关验证后才本地原子提交；固定实施序列为 **T1 → T2 → T3 → T4 → T7 → T8 → T5 → T6 → T9 → T10 → T11 → T12**，前一实施序列节点未提交不进入下一节点（本清单仍按 Task 编号展示）。全部 Java/sql/yaml 文件带 Apache 协议头（rat verify 检查）；语法 GJF 1.7 可解析——**禁用** record / switch 表达式 / 文本块 / instanceof 模式匹配 / sealed / 模式 switch；包名 org.apache.amoro.*。

---

## Task 1: 契约层骨架 + BackoffPolicy + Clock/RandomSupplier

**Description:** 建 `org.apache.amoro.control` 包：`Controller`（`key()+invoke()`）、`ControllerKey`（普通 immutable value class，domain+resourceId；不使用 record）、`Scheduler`（含 schedule/unschedule）、`TerminalState`（单例、writableStackTrace=false）、`Clock`（**自定义接口，基于 System.nanoTime 暴露单调毫秒**——`java.time.Clock` 无单调时基，不用）、`RandomSupplier`、`BackoffPolicy`，以及带 signal version 的 `SchedulerWaitStrategy` 契约。实际退避序列必须是 `{3,3,5,8,13,21,34,55}s`，超尾封顶 55s；实现先取当前索引再递增；抖动范围 `[0,250)ms`。

**Acceptance criteria:**
- [ ] 类型编译通过，零外部依赖（仅 JDK）
- [ ] BackoffPolicy 单测：实际发出序列逐值精确、封顶、重置归零；注入固定 RandomSupplier 后断言抖动 0 与 249ms，明确 250ms 不可达
- [ ] `ControllerKey("domain-a","1") != ControllerKey("domain-b","1")`
- [ ] Clock 提供单调毫秒；TerminalState 单例、无栈追踪
- [ ] SchedulerWaitStrategy 在 observed version 已变化时不得进入等待；有/无 deadline 两种 await 语义明确
- [ ] `unschedule(ControllerKey)` 契约幂等，删除后旧 generation 不得重排同 key 新 Controller

**Verification:**
- [ ] 测试过：`./mvnw -pl amoro-ams-v2 test -Dtest=TestBackoffPolicy`
- [ ] `./mvnw -pl amoro-ams-v2 validate`（spotless/checkstyle）

**Dependencies:** None
**Files likely touched:** `control/{Controller,ControllerKey,Scheduler,TerminalState,Clock,RandomSupplier,BackoffPolicy,SchedulerWaitStrategy}.java`、`test/.../control/{TestControllerKey,TestBackoffPolicy,TestSchedulerWaitStrategy}.java`
**Estimated scope:** S

---

## Task 2: DefaultScheduler + ScheduledController + SchedulerWorker（single-flight）

**Description:** DelayQueue 调度核心。`ScheduledEntry`（generation identity + QUEUED/CLAIMED/TERMINATED + per-entry lock）、`ScheduledController`（Delayed；显式并发可见字段；getDelay 消费注入 Clock）、`SchedulerWorker`（daemon `amoro-control-worker-%d`；DelayQueue 只做排序/非阻塞 poll，deadline 等待走 signal-version SchedulerWaitStrategy；异常退避不丢弃、仅 TerminalState 终止）、`DefaultScheduler`（`ConcurrentHashMap<ControllerKey,ScheduledEntry>` single-flight）。排队 updater、worker claim、unschedule 在同一 entry 锁内扭转；pending deadline 只取最早。`map.remove(key,entry)` 隔离删除后同 key 新 generation。

**Acceptance criteria:**
- [ ] 同 ControllerKey 串行：并发 schedule 同 key，invoke 无重叠，含在途期间再次 schedule
- [ ] 跨域隔离：相同 resourceId、不同 domain 可独立调度
- [ ] earliest deadline：先 60s 后 0s 必须立即缩短；先 0s 后 60s 不得推迟；排队与在途两分支都覆盖
- [ ] 排队包装缩短 deadline 时 remove/update/reinsert 同一对象；重复 schedule 后队列 cardinality 不增长、无 superseded/tombstone；remove/take 竞态转为 rescheduleRequested
- [ ] 两个并发 earlier-deadline updater + worker take 竞态下，最终 deadline 为全局最早且 invoke 不重叠
- [ ] unschedule 覆盖 QUEUED/CLAIMED；在途返回后不重排；随后同 key 新 entry 不被旧 worker remove/requeue
- [ ] schedule 读到旧 TERMINATED entry 时重查/putIfAbsent 新 identity；竞争失败候选不入队，旧包装不得复活
- [ ] TerminalState 抛出后永久不再调度**且登记表条目被移除**（无泄漏断言）
- [ ] 普通异常按退避序列无限重试、控制器不丢失；成功后 backOffAttempts 归零按 delay 重排
- [ ] 新增/缩短 deadline、unschedule、shutdown 均 signal；signal 发生在 peek 与 await 之间也不丢失
- [ ] 虚拟时钟快进：`FakeWaitStrategy.advanceClockAndSignal` 才触发到期；仅推进假 Clock 不宣称能唤醒（测试不真实等待秒级 delay）

**Verification:**
- [ ] 测试过：`./mvnw -pl amoro-ams-v2 test -Dtest=TestDefaultScheduler`

**Dependencies:** T1
**Files likely touched:** `control/{DefaultScheduler,ScheduledEntry,ScheduledController,SchedulerWorker}.java`、`test/.../control/TestDefaultScheduler.java`
**Estimated scope:** M

---

## Task 3: 优雅停机

**Description:** `Scheduler.shutdown(Duration timeout)`：拒新 schedule → 停止取件 → 限时等在途 invoke → 超时放行；幂等可重复调用。（`Scheduler.postStart` 为接口形状保留的 no-op；worker 仅由 `DefaultScheduler` 生命周期 start 启动，重放入口在 PersistenceService.postStart——保真台账 #11 语义。）

**Acceptance criteria:**
- [ ] shutdown 后 schedule() 抛 `RejectedExecutionException`；unschedule 在 shutdown 期间及之后幂等且不抛
- [ ] 在途 invoke 完成后才退出；超时场景放行退出；重复调用不抛
- [ ] 停机路径不产生登记表残留
- [ ] unschedule 与 shutdown 并发时幂等，不复活 entry

**Verification:**
- [ ] 测试过：`./mvnw -pl amoro-ams-v2 test -Dtest=TestSchedulerShutdown`

**Dependencies:** T2
**Files likely touched:** `control/DefaultScheduler.java`（扩展）、`test/.../control/TestSchedulerShutdown.java`
**Estimated scope:** S

---

### Checkpoint 1（T1-T3 后）
- [ ] `./mvnw -pl amoro-ams-v2 test`（离线全绿）
- [ ] `JAVA_HOME=<jdk11> ./mvnw clean compile -Pskip-dashboard-build` 不回归
- [ ] fake Controller 直连 Scheduler 的最小心跳 demo 可跑（测试内即可）

---

## Task 4: Persistence 契约 + 异常体系 + Selector + L1/BlobStore/域描述符接口

**Description:** `org.apache.amoro.persistence` 包：`PersistenceService<R>`（全异步 CompletionStage：create、无条件原子 modify/delete、带 expectedResourceVersion 的 modify/delete、get/select/listener/postStart）、`PersistenceListener<R>`、`ListenerEventSink<R>`/`ListenerEnvelope<R>`/`HandoffResult` port、`DurableStateProjection<R>`/`PersistenceChange<R>`/`PreparedProjectionUpdate`、同步 `DurableDeletionHook<R>`、`Selector<R>`、`Repository<R>`、`PersistenceDomain`、`ControlledResource`、逻辑 `MutationCommand`；`BlobStore` 唯一位于 `org.apache.amoro.persistence.blob`。异常至少含 `ResourceAlreadyExists`、`ResourceDoesNotExist`、`PreconditionFailedException`、`PersistenceOutcomeUnknownException`、`PostCommitCleanupException`。`ControlledResource` 声明深不可变，但框架仍要求 serde detached-copy 隔离 create/updateFn/get/select/listener alias。command 携带操作类型、可选 expected version 与 deferred updateFn，禁止携带调用线程预计算的候选。outcome unknown 要求按 `(domain,name)` fence 后续写，直至 DB 点读/reload 按 INSERT/UPDATE/DELETE 各自 previous/candidate 语义确定并解除。durable delete 后的同步 hook 必须在同一 mutation lane、delete stage 完成及同名 create 出队之前执行，用于 key-only scheduler unschedule；hook 异常时 fence name，修复完成前禁止同名重建。

**Acceptance criteria:**
- [ ] 契约编译通过（R extends ControlledResource）；异常语义注释齐全（何时抛、谁翻译）
- [ ] PersistenceDomain 含表名白名单校验（非法表名构造即拒绝）
- [ ] outcome unknown、fenced key 和 repair/unfence 契约注释完整
- [ ] 公开签名同时覆盖 lane 内无条件原子写和 expectedResourceVersion CAS；Process 等 Controller 域不得用无版本重载旁路 CAS
- [ ] create 只接受 resourceVersion=0，首次持久化版本由框架分配为 1；modify 成功后恰好 +1
- [ ] ListenerEnvelope 固定包含 listener identity、domain/name/version/type 与 detached resource；sink 只做 handoff，不同步执行 listener
- [ ] projection prepare 只接 detached previous/current，DB 前完成 immutable snapshot 或固定上界 key delta 的全部可失败计算；commit 只能 O(1) 原子切换或域契约已声明 key 数上界的 non-throwing 更新，不得随资源总量遍历/分配；文档/API 明示 same-lane 只保证写顺序，不承诺 framework cache 与独立 projection 的跨对象原子读取
- [ ] DurableDeletionHook 在 delete DB commit 后、delete stage 完成/同名 create 出队前运行；异常 fence name 直至 repair 完成 cleanup
- [ ] hook 禁止 DB/网络/等待 future，只允许 O(1) 进程内 cleanup 或 queue/index handoff；阻塞行为通过测试 double 检出并记录为契约违例

**Verification:**
- [ ] `./mvnw -pl amoro-ams-v2 test-compile`

**Dependencies:** 技术依赖 None；实施序列前置 T3
**Files likely touched:** `persistence/{PersistenceService,PersistenceListener,ListenerEventSink,ListenerEnvelope,HandoffResult,DurableStateProjection,PersistenceChange,PreparedProjectionUpdate,DurableDeletionHook,Selector,Repository,PersistenceDomain,ControlledResource,MutationCommand}.java`、`persistence/blob/BlobStore.java`、`persistence/exception/*.java`
**Estimated scope:** M

---

## Task 5: InMemoryPersistence 核心（统一写路径顺序）

**Description:** L5 内存实现只构造逻辑 mutation 并交给 T8 lane；lane 内读取最新 canonical 值 → detached copy → 校验 expected version → apply updateFn（必须返回新实例）→ 生成 resourceVersion+1 候选 → serde → projection prepare → DB → **durable success 后**发布新的 canonical snapshot并 commit prepared index snapshot/固定上界 delta → 通过 T4 `ListenerEventSink` handoff detached envelope → mutation stage 返回 detached copy。commit 必须 non-throwing，且只能 O(1) 原子切换或域已声明 key 数上界的更新。T5 注入 fake sink/projection，不实现 T6 dispatcher。create 入参、get/select/updateFn/listener 均不得保留 cache alias；mailbox/copy/projection prepare/serde/DB 明确失败时 stage exceptional，内存/resourceVersion/index/listener 均不变。delete 的 synchronous `DurableDeletionHook` 在同一 lane 中位于 DB commit/cache/index publish 之后、delete stage/handoff/下一 mutation 之前；唯一名称由域写序列化 + DB 主键保证。

**Acceptance criteria:**
- [ ] CRUD/select 全走内存：fake BlobStore 断言读路径零调用
- [ ] create 输入在入队时 detached；调用方随后修改原对象不能改变待写候选或 cache；成功 create 的初始 resourceVersion=1
- [ ] get/select/stage 返回值及 listener envelope 均为 detached copy；调用方原地修改不能改变 cache
- [ ] 并发 create 同名：恰一次成功，其余 ResourceAlreadyExists
- [ ] updateFn 改 id/collection → 拒绝且无副作用
- [ ] resourceVersion 每次成功 modify +1；带 expectedResourceVersion 的 modify/delete 冲突 → PreconditionFailed，不自动重试且 DB/内存/listener 无副作用
- [ ] N 个并发无 expected-version modify 各自 `counter+1`，最终增加 N 且版本连续；证明 read/apply 在 lane 内，不是只证明候选写入 FIFO
- [ ] updateFn 递归调用同 PersistenceService 由 lane reentrancy guard fail-fast；updateFn 抛异常仅失败当前命令，后续 mailbox 消息继续处理（任意 I/O 由契约禁止，不虚构通用检测）
- [ ] updateFn 原地修改其 detached 输入后抛错、返回候选后 serde/DB 失败，canonical cache 均保持逐字段不变
- [ ] 队列满、serde 失败、fake blob 抛异常：stage 失败且内存/resourceVersion/listener 均无变化
- [ ] fake durable store 完成前，get/select 看不到候选值；完成成功后才同时可见
- [ ] projection prepare 失败发生在 DB 前，DB/cache/index/listener 均不变；DB 失败时 prepared update 被丢弃；DB 成功后 cache 与 projection 在同一 lane 按序发布且 mutation stage 最后完成，无 listener 时序依赖；并发读测试证明框架不虚构跨两个容器的原子快照，需该语义的 fake domain 使用 aggregate snapshot/read barrier 后只能看见完整旧版或完整新版
- [ ] fake unknown outcome 分别覆盖 INSERT/UPDATE/DELETE：各自 candidate→按成功发布、previous→失败且内存不变、第三值/点读不可用→fence，repair reload 后解除；DELETE 缺失必须判成功
- [ ] 不存在的 id：get/delete → ResourceDoesNotExist
- [ ] delete DB commit 后阻塞 hook 时同名 create 只能排队；释放 hook 完成旧 key unschedule 后才允许 create/新 listener schedule，旧 delete 绝不终止新 entry
- [ ] DurableDeletionHook 异常时 delete stage 抛 PostCommitCleanupException、name fenced、同名 create 拒绝；repair cleanup 后才解除

**Verification:**
- [ ] 测试过：`./mvnw -pl amoro-ams-v2 test -Dtest=TestInMemoryPersistence`

**Dependencies:** T4、T7、T8
**Files likely touched:** `persistence/InMemoryPersistence.java`、`test/.../persistence/TestInMemoryPersistence.java`
**Estimated scope:** M

---

## Task 6: Listener 分发 + L2/L3 facade + 离线重放

**Description:** 实现 T4 `ListenerEventSink` port：afterCreated/afterModified/afterDeleted/postStart 在 durable success 与内存发布/重建后尝试交给有界 `ListenerDispatcher`；mutation/startup handoff 不等待 listener，listener 异步执行。event envelope 携带 domain/name/resourceVersion/type/listener identity 与 detached snapshot；同 listener+resource pair 串行保序，失败重试不越过该 pair 后续事件，但不阻断其他 listener/key。listener 必须幂等、level-triggered，并容忍 crash/repair 重复。单 listener 异常不阻断其他 listener、不回滚 durable write、也不把 mutation stage 改成失败，进入有界重试和告警。默认 workers=4、queue-capacity=1024、首次失败后 max-retries=3、retry-delay=1s，全部可配置并在 T10 校验。dispatcher 队列满同样不反转 mutation，必须递增 dropped 指标/告警并由资源域有界 repair sweep 补偿（Process P4）。delete 的立即 unschedule 由 T4/T5 synchronous hook 保证，afterDeleted 仅作幂等补偿。

**Acceptance criteria:**
- [ ] create/modify/delete 回调严格发生在 DB 成功和内存发布之后；单 listener 抛异常不阻断其他 listener
- [ ] listener 抛异常时 mutation stage 仍成功；只有 listener retry/alert 记录失败
- [ ] dispatcher 队列满时 mutation stage 仍成功，dropped 指标/告警可断言，repair sweep 后 listener 副作用最终发生
- [ ] 失败 listener 首次调用 + 3 次重试后耗尽并告警；retry 配置边界可确定性测试，且不会阻塞其他 listener
- [ ] 同 listener+resource 的 create→modify→delete 保序；失败重试期间同 pair 后续事件不越过，但其他 listener/key 可继续；重复投递保持幂等
- [ ] select 按 collection 过滤正确
- [ ] RepositoryFacade：正常返回、超时上抛（可配 timeout）
- [ ] 离线重放：预置 N 个存量 → postStart → N 次 listener.postStart + ≥N 次 schedule；所有异步 listener 断言用 Awaitility，重复 schedule 被 single-flight 合并

**Verification:**
- [ ] 测试过：`./mvnw -pl amoro-ams-v2 test "-Dtest=TestPersistenceListener,TestRepositoryFacade"`

**Dependencies:** T5, T1（Scheduler/ControllerKey 契约）
**Files likely touched:** `persistence/{ListenerDispatcher}.java`、`persistence/facade/*.java`、`test/.../persistence/{TestPersistenceListener,TestRepositoryFacade}.java`
**Estimated scope:** M

---

## Task 7: 版本化 serde（JSON/YAML 格式策略化）

**Description:** `org.apache.amoro.serde`：`VersionAwareJacksonSerde`（**格式策略化：默认 JSON，按域/资源类型可选 YAML**——Jackson `YAMLFactory`，`jackson-dataformat-yaml` 在模块 pom 自包含定版；两种格式下 apiVersion 读取与转换链语义一致；Base64 编码规避 YAML 空白敏感）、`SerdeRegistry`（Spring classpath 扫描注册 `VersionedResourceConverter`）、序列化上限可配置（默认 65536，指原始字节；DDL 用 MEDIUMTEXT 容纳 Base64 膨胀）。

**Acceptance criteria:**
- [ ] v1→v2→v3 转换链正确（含字段修补标记）——**JSON 与 YAML 两格式各自验证**；**每版本 golden fixture 永久锁死（含最旧→最新全链）**
- [ ] 未知/缺失 apiVersion 报错语义明确（两格式）
- [ ] Base64(JSON) 与 Base64(YAML) 往返一致；超限抛明确异常（含上限值）；resourceVersion 语义不受 serde/格式影响
- [ ] 同一 Converter 链对两种格式产出等价实体（互转断言）
- [ ] **注册自检**：同 (资源类型, 输入版本) 重复注册 → 启动失败；通往最新的链缺环 → 启动失败
- [ ] **未知字段容忍**：含未知字段的新版本数据被旧模型读取不抛异常（FAIL_ON_UNKNOWN_PROPERTIES=false 验证）

**Verification:**
- [ ] 测试过：`./mvnw -pl amoro-ams-v2 test -Dtest=TestVersionAwareSerde`

**Dependencies:** T4（完成后供 T8/T5 使用）
**Files likely touched:** `serde/{VersionAwareJacksonSerde,SerdeRegistry,VersionedResourceConverter}.java`、`amoro-ams-v2/pom.xml`、`test/.../serde/TestVersionAwareSerde.java`（含 FakeV1/V2/V3 实体与 converter）
**Estimated scope:** M

---

### Checkpoint 2（实施序列 T4→T7→T8→T5→T6 完成后；清单按编号展示）
- [ ] 离线全量单测绿
- [ ] fake durable store 生命周期通过：create durable success→内存发布→afterCreated→schedule→invoke→modify；durable failure 分支内存与 listener 均无副作用

---

## Task 8: BlobStoreActor mailbox

**Description:** L6：每域单线程 mutation sequencer/mailbox（有界队列，默认 1024，与参考 actor 一致）；每个 `MutationCommand` 是 deferred 逻辑命令，只有进入 lane 后才读取最新值、apply updateFn、分配版本并调用底层 BlobStore。队列满或 DB 明确失败完成 exceptional；不得先预计算候选，也不得先向上层确认成功再后台补写。消息还包含点查/全量游标；FIFO；停机排空。

**Acceptance criteria:**
- [ ] 串行性：N 线程并发提交 deferred `counter+1` 命令，命令在 lane 内读/apply，最终状态增加 N；不能只提交 N 个已算好的候选
- [ ] FIFO 顺序（同一 key 连续写按序生效）；队列满 → 写方异常、未入队即失败
- [ ] 停机排空：已入队消息执行完毕后才终止；DB 失败完成 exceptional 且后续消息仍可处理
- [ ] fake durable operation 未完成前消息 future 不成功；成功后 fake store 状态已可点读；真实新连接可见性由 T9 验证

**Verification:**
- [ ] 测试过：`./mvnw -pl amoro-ams-v2 test -Dtest=TestBlobStoreActor`

**Dependencies:** T4（BlobStore/MutationCommand 契约）、T7（serde）
**Files likely touched:** `persistence/blob/{BlobStoreActor,BlobMessage}.java`、`test/.../persistence/blob/TestBlobStoreActor.java`
**Estimated scope:** M

---

## Task 9: MyBatisBlobStore + DDL + 表名参数化 + docker-it 机制

**Description:** L7：`ResourceBlobMapper`（注解式五种 SQL，**表名来自 `PersistenceDomain` 域绑定（T4 契约）——枚举白名单防注入，默认域 `amoro_resource`，支持多域多表**）+ `MyBatisBlobStore`（DuplicateKeyException→ResourceAlreadyExists）；`amoro_resource` 三份方言 DDL：MySQL 5.7=`MEDIUMTEXT/DATETIME`，PostgreSQL=`TEXT/TIMESTAMP(3) WITHOUT TIME ZONE`，Derby=`CLOB/TIMESTAMP`，均带 Apache 头注释。MySQL/PG 用 `CREATE TABLE IF NOT EXISTS`；仓库 Derby 10.14.2.0 不支持该语法，Derby 使用 plain `CREATE TABLE`，initializer 先查 metadata 仅在缺表时执行（并只容忍并发建表的 table-exists SQLState）。**pom 依赖自包含**：`mybatis-spring-boot-starter` 显式定版（3.0.x，实现时验证 Boot 3.5 适配）、`mysql-connector-j` 显式 8.x、`jackson-dataformat-yaml`——均在 amoro-ams-v2 pom 内定义；`mysql57` profile 配置（yaml 带协议头）；docker-it 激活机制：pom 属性 `<excludedGroups>${docker-mysql.excluded}</excludedGroups>`（默认 docker-mysql），`-Pdocker-it` profile 置空属性 + 注入 spring.profiles.active，assumption 探测 3307 为第二道保险。

**Acceptance criteria:**
- [ ] 五种 SQL 语义集成测试过（真 MySQL 5.7）
- [ ] **双域双表绑定：两个 PersistenceService 域实例各绑一张表，读写互不串扰**
- [ ] DuplicateKey → ResourceAlreadyExists；重启新实例后全量加载 = 落库内容
- [ ] DB 写异常后的新连接点读按 INSERT/UPDATE/DELETE 分别区分 candidate/previous；DELETE 缺失判成功；第三值/仍不可读时抛 PersistenceOutcomeUnknown 并暴露 fenced-key 指标
- [ ] 懒升级回写：旧 apiVersion 行 → 启动加载 → 行更新为新版本
- [ ] 无 Docker 时 docker-mysql 组自动跳过（assumption），默认 `test` 全绿且**执行数 > 0**（防静默假绿）
- [ ] 建表幂等；schema/yaml 文件过 rat（协议头）
- [ ] 三份 DDL 的 value/last 类型逐字契约测试；Derby plain CREATE 由 metadata guard 可重复初始化，MySQL 5.7 docker 真执行；PostgreSQL 未跑真库前在验证报告明确“仅静态检查、运行时未验证”

**Verification:**
- [ ] 离线：`./mvnw -pl amoro-ams-v2 test`
- [ ] Docker：启动容器后 `./mvnw -pl amoro-ams-v2 test -Pdocker-it`

**Dependencies:** 技术依赖 T7、T8；实施序列前置 T6
**Files likely touched:** `persistence/blob/{MyBatisBlobStore,ResourceBlobMapper}.java`、`src/main/resources/schema-*.sql`、`amoro-ams-v2/pom.xml`、`src/test/resources/application-mysql57.yaml`、`test/.../blob/TestMyBatisBlobStore.java`
**Estimated scope:** M

---

## Task 10: Spring 装配 + 配置键 + 生命周期

**Description:** `ControlPlaneAutoConfiguration`：Scheduler/InMemoryPersistence/BlobStoreActor/ListenerDispatcher/Serde Bean 装配；`AmoroControlProperties`（Spec §7 十键，含 repository timeout 与统一 lifecycle shutdown timeout）；SmartLifecycle 停机定序（先 Scheduler shutdown，再停止 listener 接入并有限排空，最后 actor 排空；三段各自使用同一个 10s 默认上限）；mapper 扫描；dataSourceScriptDatabaseInitializer 建表（@DependsOn 语义）。

**Acceptance criteria:**
- [ ] context 装配测试：Bean 就绪、属性绑定默认值正确
- [ ] listener worker/queue/retry 属性非法值 fail-fast；默认 4/1024/3/1000ms 绑定正确
- [ ] repository timeout 与 lifecycle shutdown timeout 默认均为 10000ms，非正值 fail-fast；RepositoryFacade 实际消费前者，三个生命周期阶段实际消费后者
- [ ] 停机顺序单测：scheduler → listener dispatcher → actor（顺序可观测且均有限排空）
- [ ] schema.sql 自动执行且幂等

**Verification:**
- [ ] 测试过：`./mvnw -pl amoro-ams-v2 test -Dtest=TestControlPlaneAutoConfiguration`

**Dependencies:** T3, T6, T9
**Files likely touched:** `config/{ControlPlaneAutoConfiguration,AmoroControlProperties}.java`、`test/.../config/TestControlPlaneAutoConfiguration.java`
**Estimated scope:** S

---

## Task 11: 端到端流程验证 + 资源无关性证明

**Description:** fake 资源三件套（FakeResource + FakeController + FakeListener，经 L1 Repository 消费）打 `docker-mysql` tag，对真 MySQL 5.7 跑完整生命周期：create→afterCreated→schedule→多轮 invoke（含一次异常退避、一次版本冲突放弃后下轮收敛）→TerminalState 终止→**销毁上下文重建（新 Scheduler+Persistence）→ postStart 从 DB 重放 → 资源续走收敛**。另加第二种 fake 资源（不同 collection/实体/Controller）仅写三件套通过同套用例。

**Acceptance criteria:**
- [ ] 完整生命周期断言全过（含退避与版本冲突分支）
- [ ] 重启重放：重建后资源从 DB 恢复且调度继续（DB 是事实源的运行时证明）
- [ ] 第二种 fake 资源零框架改动接入（资源无关性）
- [ ] 无 Docker 时整组跳过

**Verification:**
- [ ] Docker 全量：`./mvnw -pl amoro-ams-v2 test -Pdocker-it`

**Dependencies:** T10
**Files likely touched:** `test/.../e2e/{FakeResource,FakeController,FakeListener,SecondFake*,TestFrameworkE2E}.java`
**Estimated scope:** M

---

### Checkpoint 3（T8-T11 后）= Spec §10 验收
- [ ] 离线全量绿；`-Pdocker-it` 全量绿
- [ ] `JAVA_HOME=<jdk11> ./mvnw clean compile -Pskip-dashboard-build` 与 `JAVA_HOME=<jdk17> ./mvnw -pl amoro-ams-v2 clean package` 不回归
- [ ] spotless/checkstyle/rat（`./mvnw -pl amoro-ams-v2 verify`）全过

---

## Task 12: 文档与收尾

**Description:** 模块 README 更新（框架结构、配置键、docker-it 运行方式、GJF 约束、**toolchains 前置与模板保全**）；dist/config 样例补充 `amoro.control.*`；更新或新增 v2 GitHub workflow，使 `amoro-ams-v2/**` 变更能触发、同时安装 JDK11/JDK17 并生成 Maven toolchains、运行离线 verify；审计 T1-T11 已逐 Task 完成 Review、验证与本地原子提交，T12 自身通过门禁后单独提交。

**Acceptance criteria:**
- [ ] README 含运行/测试/验证三段命令可复制执行，且保留 JDK17 toolchains 前置说明
- [ ] rat 对新增文件全过（Java/sql/yaml 协议头）
- [ ] CI path filter 覆盖 `amoro-ams-v2/**`；workflow 实际提供 JDK17 toolchain，并执行测试数 >0、无 skipped 的离线 verify

**Verification:**
- [ ] `./mvnw -pl amoro-ams-v2 verify`（含 rat）

**Dependencies:** T11
**Files likely touched:** `amoro-ams-v2/README.md`、`dist/src/main/amoro-bin/conf/config.yaml`（样例注释）、`.github/workflows/*v2*.yml`（或现有 core workflow 的等价修订）
**Estimated scope:** S
