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

# amoro-ams-v2 Process 资源任务清单

> 规格：`tasks/amoro-ams-v2-process-spec.md`；计划：`tasks/ams-v2-process-plan.md`。
>
> P0 技术审核已完成；P7B 前尚待确认本地 action 的迁移放置边界；P1-P8 尚无当前实现。
> 每个任务必须区分“离线验证”“真实 adapter 验证”“Docker E2E”，不能把 skipped tests 或历史提交测试算作通过。
> **实施顺序门禁：先完成 Framework T1-T12，且每个 Framework Task 均通过 Review、JUnit 5 和本地提交；之后才开始 P1。下列细粒度 Dependencies 只解释能力来源，不允许提前穿插 Process 实现。**
> P1-P8 每个 Task 也必须先写会失败的 JUnit 5 行为测试（纯文档除外），再实现 GREEN，完成五轴 Review 和相关离线/真实 adapter/Docker 验证后才本地原子提交；固定实施序列为 **P1 → P2 → P3 → P4 → P5 → P6 → P7A → P7B → P8**，前一实施序列节点未提交不进入下一节点。

---

## P0：Spec / Plan / Todo 审核

**状态：已完成（仅文档）**

- [x] 核实 `amoro-ams-v2` 当前只有 Boot skeleton，没有 Process 实现；
- [x] 核实 v1 十态、事件、引擎接口、REST、前端字段和 Action wire value；
- [x] 将历史提交 `7a60c87db` 标记为候选资产而非当前代码；
- [x] 修正 durable-first、DB 事实源、ControllerKey、earliest deadline 和 listener 修复契约；
- [x] 修正 FAILED 最终谓词、UNKNOWN、desired 单调、retry off-by-one 和取消路径；
- [x] 增加 attempt-bound submission/execution resolution；
- [x] 定稿 REST JSON、`pageSize`、稳定排序、string ID、完整 parameters 与错误码；
- [x] 增加 create Idempotency-Key、资源化 PATCH/submission/execution-resolutions 与请求/远端响应边界校验；
- [x] 增加 v1/v2 差异、灰度、回退和不兼容边界；
- [x] 明确 v1 当前未废弃、无删除日期，生产证明与零使用量是未来 advisory deprecation 门禁；
- [x] 重排 P1-P8 依赖。

**Files changed:** `tasks/amoro-ams-v2-process-spec.md`、`tasks/ams-v2-process-plan.md`、`tasks/ams-v2-process-todo.md`，以及其依赖的 Framework 三份文档。

### L1：P7B 本地执行放置决策（不阻塞 Framework/P1-P7A）

- [ ] 确认 P7B 为 v2 native local action，或迁移期 `AmsLocalEngineAdapter` 代理 v1 内部执行；确认后同步重写 P7B dependencies/files/E2E 与 v1 兼容改动清单。

---

## P1：Process 对象模型与 serde fixture

**目标：** 在不依赖真实引擎和 REST 的前提下定稿资源 schema。

**Dependencies:** Framework T1-T12 全部完成；本任务直接使用 T4、T7 契约。

**工作项：**

- [ ] 实现 `ProcessResource`、`ProcessSpec`、`ProcessStatus`、`ProcessAttempt`、`AttemptSummary`、`ManualResolution`、`ProcessCondition`、`ProcessFailure`；
- [ ] `apiVersion=process/v1`、`collection=process`、name/string ID；
- [ ] 实现 `ProcessFinality`：固定终态与预算内 FAILED 分离；
- [ ] spec 冻结，仅 desired 允许 `RUN -> CANCEL`；拒绝 CANCEL→RUN；
- [ ] `dispatchGeneration=0..maxSubmissionRetries`，`submissionKey=name:retryNumber:dispatchGeneration`；冻结参数不变时同 action attempt 的 requestHash 稳定；
- [ ] create `idempotencyKeyHash/requestHash`：不存 raw key，canonical JSON 字段顺序不影响 hash；
- [ ] `retryNumber <= maxRetries`、attemptHistory `<= maxRetries`、总 action attempt `<= maxRetries+1`；
- [ ] 每 attempt 的 submissionHistory `<= maxSubmissionRetries`，每代 submission resolution 至多一条；execution resolution 每 action attempt 至多一条；retryDisposition=AUTO/ALLOW/FINAL；
- [ ] 墙上时间注入 `java.time.Clock` UTC，不引用调度单调 Clock；
- [ ] Base64(YAML) 最新版本往返、未知字段和大小上限；
- [ ] 全局大小边界：parameters 16KiB、summary.result 8KiB、trackUri/externalId 2048/512B、文本 512B、operator metadata 256B、conditions 唯一且最多 8；最终资源原始序列化 <=65536B；
- [ ] 构造 max-legal-shape：所有字段取合法最大值、4 个 action attempts × 每个 3 generations、8 conditions，并执行最后一次最终 FAILED 的 failure/finishedAt CAS；persistence YAML 与 REST JSON 必须都仍 `<65536B`，否则本 Task 下调 cap/预留 headroom 后重跑；
- [ ] 建立 `process-v1.yaml` golden fixture；首版不虚构 v1→v2 converter；
- [ ] summary 使用有界 action result，不接受无界日志/堆栈。

**Acceptance:**

- [ ] maxRetries=0：最多一次尝试、首次 FAILED 即最终；
- [ ] maxRetries=3：retryNumber 0..3、最多四次、history 上限 3；
- [ ] maxSubmissionRetries=0/2：每 action attempt 分别最多 1/3 个提交代次；history 上限 0/2；完整资源的提交摘要总数有界；
- [ ] FAILED + retryDisposition=FINAL 不重试；ALLOW 仍受 maxRetries/desired CANCEL 限制；
- [ ] Snowflake name JSON/YAML 往返后逐字符一致；
- [ ] frozen spec 任意非 desired 字段修改均拒绝且无副作用；
- [ ] `postStart` 不改写 phase/attempt。

**Likely files:** `process/model/**`、`process/serde/**`、`src/test/resources/process/process-v1.yaml`。

**Verification:** `./mvnw -pl amoro-ams-v2 test -Dtest='TestProcessResource,TestProcessSerde,TestProcessFinality'`

---

## P2：Process 域持久化、不变量与准入原语

**目标：** 建立 `amoro_process` durable-first 存储与单节点并发准入基础。

**Dependencies:** P1；直接使用 Framework T9、T10，T11/T12 已由全局门禁完成。

**工作项：**

- [ ] 注册 `PersistenceDomain(process, amoro_process, YAML)`；
- [ ] 三库 DDL 同构，value 容纳 Base64(YAML)，表名白名单；
- [ ] 实现 `ProcessRepository` 与 `ProcessActiveIndex`：`(tableId,action)→name` 准入 map + `(createdAt,name)` 非最终资源 persistent rank tree，后者供 rescheduler 稳定 cursor/batch 扫描；
- [ ] retained-resource idempotency index：scope=`(tableId,canonicalAction,keyHash)`，重启从 DB 重建；
- [ ] `ProcessReadIndex`：每资源最多四个轻量 view（ALL/action/phase/action+phase），view 内使用带 subtree-size 的 immutable persistent rank tree 按 `(createdAt DESC,name DESC)` 排序；`resourcesByName`、active/idempotency map 与 `viewKey→rankTreeRoot` 顶层都使用结构共享 persistent hash trie/persistent ordered map 或等价有界结构，禁止复制全部 view map；单资源 prepare 总访问/节点分配 `O(log R+log V+log n)`，rank slice `O(log n+pageSize)`，postStart 构造完整新 snapshot；
- [ ] `ProcessExpiryIndex(finishedAt ASC,name ASC)`：只收最终且 finishedAt 非空资源，entry 带 resourceVersion，durable publish/delete 后更新，postStart 重建；
- [ ] `ExecutionHandleReleaseIndex`：`ConcurrentHashMap<HandleKey,ReleaseEntry>` 去重 + `ConcurrentSkipListMap<(nextReleaseAt,engine,externalId),HandleKey>` due order，二者由 HandleKey fixed striped lock 同步维护；任一 local attempt 的执行终态结果+externalId 在 durable publish 后加入、release 成功移除、postStart 从当前 attempt/attemptHistory 重建；reaper 使用 exclusive cursor，selected/stale/in-flight 均计入 batch，到首个未来 deadline/尾部回绕，不扫全 index/cache；
- [ ] `ProcessIndexProjection` 将 `resourcesByName` canonical read map 与 active、idempotency、read、expiry 四类 correctness-sensitive 索引合并为一个 `ProcessResourceIndexSnapshot`：DB 前纯函数 `prepare`，DB 后 same-lane 用单个 AtomicReference O(1) `commit`；ProcessRepository 的 get/list/准入/rescheduler/TTL 一次操作只取得一次 aggregate 引用并从中读取正文和索引，禁止索引命中后跨读 Framework cache；release index 使用独立、最多 `maxRetries+1` 个 handle 的 prepared delta，在同一 striped lock 内更新 dedup/due-order，允许的竞态后果仅为幂等重复 release；listener 不得作为同步索引发布路径；
- [ ] DB INSERT/UPDATE/DELETE 成功后才更新内存、listener 和 CompletionStage；
- [ ] DB/serde/mailbox 失败时 stage exceptional、内存/resourceVersion 不变；
- [ ] `PersistenceOutcomeUnknown` 时保留 `(tableId,action)` admission reservation 并拒绝再次创建；点读/reload 判定后转 active 或释放；
- [ ] `resourceVersion` CAS 与 identity/spec invariant；
- [ ] 实现 `(tableId,canonicalAction)` keyed mutex；临界区覆盖 active select + durable create；
- [ ] JVM mutex 明确标注仅单实例有效；
- [ ] startup 从 DB 重建，converter 回写也遵循 durable-first。

**Acceptance:**

- [ ] fake DB failure：内存未更新、listener 未调用、stage 失败；
- [ ] projection prepare 失败时 DB/内存/全部索引均不变；DB 失败时丢弃 prepared update；DB 成功返回前 `resourcesByName` 与四类资源索引单次 aggregate snapshot 切换；用 publication latch 并发压测 phase 变更/list、delete/list、create replay/准入，只允许完整旧版或完整新版，禁止“旧 phase view+新正文”、旧 name+缺正文及跨投影 replay；
- [ ] 10 万资源且分布于大量 table/action/phase view 时，read-index 插入/phase 变更的总节点访问和分配受 `O(log R+log V+log n)` 上界约束，不复制整表数组或全部顶层 view map；按第 N 页查询访问节点数受 `O(log n+pageSize)` 上界约束，不扫描前 N-1 页；
- [ ] durable create 成功后模拟崩溃，新实例从 DB 恢复同一资源；
- [ ] 两线程分别模拟 REST/scanner 同时创建，恰一成功、另一个 ACTIVE_PROCESS_EXISTS；
- [ ] 同 idempotency key/hash 重放原资源；同 key/不同 hash 冲突；锁等待超时返回 IDEMPOTENCY_IN_PROGRESS；
- [ ] create 提交结果未知时第三次创建仍被 fencing；repair 确认 DB 成功/失败后分别转 active/允许重建；
- [ ] 不同 domain 使用相同 resourceId 不串扰；
- [ ] process table 清理不影响 framework 其他域表。

**Likely files:** `process/persistence/**`、`src/main/resources/schema-*.sql`、`test/.../process/persistence/**`。

**Verification:**

- 离线：`./mvnw -pl amoro-ams-v2 test -Dtest='TestProcessRepository,TestProcessAdmission'`
- Docker：`./mvnw -pl amoro-ams-v2 test -Pdocker-it -Dtest=TestProcessPersistenceMySql`

---

## P3：ProcessEnginePort、结果类型与 fake adapter

**目标：** 先冻结状态机依赖的端口，避免 Transition 绑定 v1 `ExecuteEngine`。

**Dependencies:** P1、P2。Framework 已由全局门禁完成。

**工作项：**

- [ ] `ProcessEnginePort.capabilities/submit/resolveSubmission/observe/cancel/release`；capabilities 是无 I/O immutable snapshot，含两个 support boolean 和 1..128B capabilityVersion；version 由实现/API/相关配置 canonical hash 或等价持久版本生成，相同配置跨重启稳定；release 仅做 execution-result-durable-confirmed handle cleanup；
- [ ] dispatcher 强制 `amoro.process.engine.command-timeout-ms`（默认 30000，>0）：五个 future 到期必须完成；submit 外层 timeout 保守映射 UNKNOWN，其余命令映射 UNAVAILABLE，claimed release 失败后重入 due index；
- [ ] adapter 返回 submission/cancel UNSUPPORTED 时，在 future 完成前原子发布相应 capability=false、version 不变的 snapshot；condition 持久化该 version；只有配置/plugin reload 验证能力恢复并发布新 version+true 后才能恢复调用；
- [ ] 五个端口均返回 `CompletionStage`；实现 `ProcessEngineDispatcher`：SubmissionIdentity 排斥 submit/resolve 直到结果 durable apply 完成，ExecutionIdentity 排斥 observe/cancel，ReleaseIdentity 合并重复 cleanup；Controller 线程禁止 get/join；
- [ ] SubmissionOutcome 五分类（ACK/REJECTED/UNKNOWN/CONFLICT/UNAVAILABLE）；SubmissionResolution 六分类（含 LOST）；Observation 四分类（含 LOST）；CancellationOutcome 五分类；
- [ ] 定义 authoritative NOT_FOUND：仅远端协议明确返回 key/id 不存在时允许；超时/5xx/解析失败不得映射；submission resolution NOT_FOUND 表示未接受，ACK 后 observe/cancel NOT_FOUND 只表示 execution 缺失、必须人工消解；
- [ ] `EngineObservation(remotePhase,trackUri,summaryDelta,EngineFailure?)`：FAILED 必须 failure，非 FAILED 禁止；observe KNOWN 与 cancel ALREADY_TERMINAL 共用；
- [ ] cancel ALREADY_TERMINAL 只接受 SUCCESS/FAILED/CANCELED/KILLED/CLOSED；SUBMITTED/RUNNING 属契约违例并映射 UNAVAILABLE；
- [ ] `SubmissionCommand` 只带冻结 parameters、submissionKey、requestHash；
- [ ] 实现脚本化 `FakeProcessEnginePort`，可按调用序列返回结果并记录调用；
- [ ] contract tests 验证 adapter 异常翻译。

**Acceptance:**

- [ ] fake 可独立驱动所有 ToRun/ToCancel 分支；
- [ ] fake future 可手工延迟完成，验证 Controller invoke 在 future 未完成时已经返回；
- [ ] 慢 submit future 未完成期间 resolve 调用数严格为 0；submit 结果 durable apply 后才释放 SubmissionIdentity flight；进程重启无 flight 时才允许 resolve；
- [ ] 相同 submissionKey 的 requestHash 变化返回 CONFLICT；
- [ ] unavailable 与 not-found 测试不可混淆；
- [ ] submit 在可证明未发送时返回 UNAVAILABLE 并以同 key 退避重投；请求写出后超时/响应丢失返回 UNKNOWN 并禁止盲重投；
- [ ] LOST 不得翻译为 NOT_FOUND/FAILED，也不得触发自动重投；
- [ ] durable DISPATCHING 后本地 action 已派发、ACK 尚未落库即崩溃：新 adapter 的 resolve 返回 LOST，而不是 NOT_FOUND；
- [ ] release 重复调用/未知 handle 为成功 no-op；fake 可断言 CAS 失败前 release 调用数为 0、CAS 成功后调用 1 次；
- [ ] 当前 v1 `HttpRemoteSparkStandAloneSubmit` 不作为 `ProcessEnginePort` 实现编译依赖。

**Likely files:** `process/engine/api/**`、`test/.../process/engine/FakeProcessEnginePort.java`。

**Verification:** `./mvnw -pl amoro-ams-v2 test -Dtest=TestProcessEnginePortContract`

---

## P4：状态机、ProcessListener 与调度修复

**目标：** 完成可恢复的 ToRun/ToCancel 调和闭环。

**Dependencies:** P2、P3；直接使用 Framework T2 的 ControllerKey/earliest deadline/unschedule 与 T6 listener dispatcher/repair。

**工作项：**

- [ ] `ProcessController` 每轮读最新资源，按 desired 选择 Transition；
- [ ] 命中 SubmissionIdentity/ExecutionIdentity flight 返回 COMMAND_IN_FLIGHT 并短延迟重排，不调用 adapter、不改预算；
- [ ] `status.nextReconcileAt` 持久化门控：提前唤醒只重排；desired CANCEL/人工消解将其原子重置为 now；
- [ ] ToRun 表逐行实现；每轮最多一个逻辑步骤；
- [ ] ToCancel 表逐行实现；cancel API 不直接调用引擎；
- [ ] 所有写入使用 expected resourceVersion；CAS 冲突结束本轮；
- [ ] `ProcessResultApplier` 对异步回调重新读取并按 attempt/generation key/hash 语义前置条件合并，保留最新 desired；CAS 冲突有界重读，generation/attempt 已轮换则拒绝迟到结果；
- [ ] 固定终态/最终 FAILED 才抛 TerminalState；
- [ ] UNKNOWN 无派发证据可修复到 PENDING，有派发证据必须 resolve；
- [ ] UNKNOWN/CONFLICT/UNAVAILABLE 不消耗 retry budget；
- [ ] 权威 submission NOT_FOUND：generation 预算内归档旧代次并生成新 key；预算耗尽形成 FAILED/SUBMISSION_NOT_ACCEPTED，再进入 action retry/finality；
- [ ] `EngineUnreachable`/`SubmissionUnresolved`/`CancellationUnsupported` 置位、更新时间与清除；UNAVAILABLE 与 60s unresolved 间隔不得混用；`DataRepaired` 只用于导入/历史终态字段修补，正常迁移禁止置位；
- [ ] 本地 registry/intent 重启丢失时，resolve（ACK 前 crash window）或 observe（ACK 后）返回 LOST，置 `ExecutionUnresolved` 并冻结自动重投；
- [ ] `ExecutionUnresolved` 优先于 phase 分支短路：默认每 5min 只 CAS 刷新 reminder/告警，当前 unresolved identity 的 submit/resolve/observe/cancel 全部为零调用，人工执行消解才清除并立即唤醒；独立 reaper 仍可 release 其他已 durable-terminal attempt；
- [ ] SubmissionUnresolved 遇 capability=false/UNSUPPORTED 时仅 60s 本地复查/告警，禁止重复 resolve；capability 变 true 后才恢复当前 key/hash 的 resolve；
- [ ] `lastCancelAttemptAt` + cancelRetryInterval 将 cancel/observe 分轮执行，避免每轮双 I/O 或无节制重复 kill；
- [ ] 默认常规轮询 3s、SubmissionUnresolved 60s、ExecutionUnresolved reminder 5min、cancelRetryInterval 10s、COMMAND_IN_FLIGHT 250ms；submit/resolve/observe/cancel 四个 `engineBackoffAttempts` 持久化独立计数 0..7，UNAVAILABLE 同 CAS 饱和递增，非 UNAVAILABLE 只归零自身；重启续接原退避；非法非正配置 fail-fast；
- [ ] 固定终态或 FAILED（含可重试）的任意 durable CAS 只通过 `ProcessIndexProjection` 加入 release index；`ExecutionHandleReaper` 是唯一 release caller，ResultApplier/CommandService/REST 均不得直接 release；CAS 失败/仍运行不产生 entry；
- [ ] ACK 后 status 未推进的补偿分支；
- [ ] listener afterCreated/afterModified/postStart 调度；
- [ ] listener 异常重试；实现只按 active-index cursor 分页的 `ActiveProcessRescheduler`（默认 30s/256/1s 上限）安全网，到尾后下轮回绕，不从终态历史或全 cache 过滤；
- [ ] scheduler 同 key 采用最早 deadline；紧急 cancel 不被慢轮询推迟。

**Acceptance:**

- [ ] 十态 × RUN/CANCEL 参数化测试；非法终态迁移为零；
- [ ] FAILED 预算内重试、预算尽结束、desired=CANCEL 停止重试；
- [ ] 新 retry attempt 归零四 operation backoff、清除 attempt-scoped conditions，但保留 DataRepaired；旧 attempt 的 externalId/finishedAt 进入有界 history；
- [ ] observe/cancel callback 进入 SUCCESS/CANCELED/KILLED/CLOSED 时同一 CAS 写 finishedAt；最终 FAILED 同一 CAS 写 failure+finishedAt，预算内 FAILED 不提前写终态时间；
- [ ] 上述所有 FAILED/固定终态同时写 attempt.finishedAt；预算内 FAILED 的顶层 status.finishedAt 仍为 null，重试归档后时间保持不变；
- [ ] 导入/历史固定终态或最终 FAILED 若只缺 attempt.finishedAt、只缺 status.finishedAt 或两者都缺，ToRun/ToCancel 分别用另一时间/Clock now 补齐并置 DataRepaired；已有顶层时间但缺 attempt 时间不得直接 TerminalState，release firstSeenAt 可重建；
- [ ] submit REJECTED、submission generations 耗尽、远端 FAILED 在最终谓词成立时均在产生 FAILED 的同一 CAS 写 failure+finishedAt；正常数据不得依赖下一轮补写；
- [ ] submit UNKNOWN 后重复 reconcile 不产生第二个 key/第二次盲 submit；
- [ ] submit UNAVAILABLE 不置 SubmissionUnresolved、不耗预算，以持久化 submit backoff 延迟后重用同 generation/key；desired CANCEL 时可直接 CANCELED；
- [ ] 慢 submit + 提前 reconcile/cancel 不调用 resolve；不得出现 resolve NOT_FOUND 轮换 generation 后旧 submit 迟到 ACK 的 orphan/重复作业竞态；
- [ ] resolve ACK/NOT_FOUND/UNAVAILABLE/UNSUPPORTED/CONFLICT/LOST 全覆盖；
- [ ] 人工 submission ACK/NOT_FOUND 把 resolve backoff 归零并在四 counter 全零时清 EngineUnreachable；人工 execution 五终态归零四 counter、清 attempt-scoped health conditions；不得残留永不再由 adapter 结果清除的 EngineUnreachable；
- [ ] resolve/cancel UNSUPPORTED future 完成时 capability 已为 false；同 capabilityVersion 重复 reconcile/进程重启只本地复查且 adapter 调用数保持 1；只有新 version+true 后才新增调用；
- [ ] ACK 后 observe authoritative NOT_FOUND/LOST 或 cancel authoritative NOT_FOUND 均置 ExecutionUnresolved，不写 FAILED/CANCELED、不自动 retry/release；只有 submission resolution NOT_FOUND 可轮换 generation；
- [ ] ExecutionUnresolved 下连续提前/到期 reconcile 对当前 identity 的 submit/resolve/observe/cancel 调用数均为 0；只更新 5min reminder/告警，人工 execution resolution 后才恢复状态推进；同时预置旧 terminal attempt，证明 reaper release 仍可执行；
- [ ] 同一 action attempt 连续 NOT_FOUND 时每次使用新 generation key，达到 maxSubmissionRetries 后只形成一次 FAILED；旧 generation 的迟到 ACK/人工命令不得覆盖当前状态；
- [ ] ToCancel 包含 CANCELED/KILLED/CLOSED/SUCCESS/FAILED 实际终态；
- [ ] cancel ALREADY_TERMINAL(FAILED) 必须携带 EngineFailure，并同 CAS 写 attempt.lastError/failure/finishedAt；缺失 failure 的 adapter 结果在边界被拒绝；
- [ ] observe FAILED 与 cancel ALREADY_TERMINAL(FAILED) 的 EngineFailure.retryable=false/true 分别同 CAS 写 retryDisposition=FINAL/ALLOW；false 即使仍有预算也不重试，true 仍受 maxRetries/desired 约束，迟到 callback 不覆盖人工 FINAL；
- [ ] cancel ALREADY_TERMINAL(SUBMITTED/RUNNING) 在 adapter 边界被拒绝为 UNAVAILABLE，不写 finishedAt；
- [ ] CANCELING 在 cancel 到期/未到期时分别只执行 cancel/observe，并能再次重发幂等 cancel；
- [ ] 初始 capabilities.supportsCancellation=false 时首次进入 CANCELING 先本地置 CancellationUnsupported+capabilityVersion，cancel 调用数保持 0；同 version（含重启）或新 version 仍 false 时，即使 cancel deadline 已到也不进入 cancel 分支，只按常规 poll observe；只有新 capabilityVersion+true 后可先清除、下一轮重试；
- [ ] durable CANCEL 后立即崩溃，重启仍继续取消；
- [ ] submit future 未完成时并发 cancel，迟到 ACK 仍能保存 externalId 并进入取消路径，不产生孤儿；
- [ ] 本地 action 派发后、ACK 落库前崩溃，重启 resolve LOST 后不产生第二次 submit；无 externalId 也可通过 execution resolution 收敛；
- [ ] 复写历史 `TestCancelRace` 场景，但测试文件在当前分支新建；
- [ ] listener 首次失败后无需资源再次修改也能被 repair sweep 调度；
- [ ] 慢 60s schedule 不会覆盖已存在的 0s cancel schedule。
- [ ] listener/default schedule 提前唤醒时，nextReconcileAt 之前 engine 调用数与预算消耗均为 0。
- [ ] rescheduler 单轮不超过 batchSize，cursor 可续扫，大量活跃资源时不全缓存遍历。
- [ ] 四 operation 的 UNAVAILABLE 退避依次为 3,3,5,...,55s，重启后不中断；一个 operation 成功只归零自身，四字段全零才清除 EngineUnreachable。
- [ ] 执行终态结果 CAS 失败不生成 release entry；异步或人工固定终态/可重试 FAILED CAS 成功均入 index；唯一 reaper release、重复幂等；模拟 CAS 成功后进程崩溃，重启补偿 release；10 万 release entries 下单轮 visited<=batchSize，不全量排序/扫描，future deadline 停止且到尾回绕。

**Likely files:** `process/control/**`、`test/.../process/control/**`。

**Verification:** `./mvnw -pl amoro-ams-v2 test -Dtest='TestProcessTransitions,TestProcessCancelRace,TestProcessListenerRecovery'`

---

## P5：REST 点查、取消与 attempt-bound 人工 resolution

**目标：** 提供不会旁路状态机的命令/查询 API。

**Dependencies:** P2、P4；直接使用 Framework T10 装配。

**工作项：**

- [ ] `GET /api/ams/v2/processes/{name}`；
- [ ] `PATCH /api/ams/v2/processes/{name}`，首版只允许 desiredState=CANCEL，只做 durable RUN→CANCEL；
- [ ] cancel 命令复用 `ToCancelTransition.requestCancel`；对预算内 FAILED 的 RUN→CANCEL 在同一 CAS 归并 failure+finishedAt；
- [ ] `POST /api/ams/v2/processes/{name}/submission-resolutions`；
- [ ] `POST /api/ams/v2/processes/{name}/execution-resolutions`；
- [ ] 实现唯一的纯函数 `ManualResolutionTransition`；REST/CommandService/ResultApplier 不复制 generation、condition、phase、failure/finishedAt 规则；
- [ ] 两类 resolution 强制新的 Idempotency-Key，并携带与当前 attempt 精确匹配的 submissionKey/requestHash；
- [ ] ACK externalId 必填；NOT_FOUND externalId 禁止；reason 必填并审计；
- [ ] desired RUN/CANCEL 的人工结论分别按 Spec 落库；
- [ ] 相同结论幂等；不同结论 409；
- [ ] 每 dispatchGeneration 的 submission 审计至多一条并随 submissionHistory 归档；每 action attempt 的 execution 审计至多一条并随 attemptHistory 归档；均保存 idempotencyKeyHash + commandHash，同 key 不同 payload 冲突，迟到旧命令只可重放已归档的完全相同命令，否则 PROCESS_ATTEMPT_STALE；
- [ ] execution resolution 覆盖 SUCCESS/FAILED(retryAllowed true/false)/CANCELED/KILLED/CLOSED；
- [ ] submission NOT_FOUND × desired RUN/CANCEL 按 generation 轮换或终态规则在同一 CAS 落库；execution 固定终态同 CAS 写 finishedAt，最终 FAILED 同 CAS 写 failure+finishedAt；
- [ ] JSON API serializer 与 persistence YAML serde 完全分离；
- [ ] 完整返回 spec.parameters；日志和错误 message 脱敏/截断；
- [ ] cancel 与人工消解在同一 modify 中写 `nextReconcileAt=now`，抢占旧等待；
- [ ] 统一异常映射与 traceId。

**Acceptance:**

- [ ] cancel 在 PENDING/RUNNING/FAILED/固定终态重复调用语义正确；
- [ ] PATCH 预算内 FAILED→desired CANCEL 的响应已含 final failure/finishedAt，不依赖后续 Controller 轮次；
- [ ] 接口线程不调用 engine；
- [ ] DB 失败时不返回 2xx，映射 `503 PERSISTENCE_UNAVAILABLE`；
- [ ] manual ACK/NOT_FOUND × desired RUN/CANCEL 四组合通过；
- [ ] manual NOT_FOUND 在 generation 有/无剩余预算两边界通过；CANCEL→CANCELED 与所有人工固定终态均断言同 CAS finishedAt；
- [ ] execution SUCCESS/FAILED(retryAllowed true/false)/CANCELED/KILLED/CLOSED 与 LOST 恢复组合通过，包含没有 externalId 的 ACK 前 crash window；
- [ ] 冲突返回 `409 SUBMISSION_RESOLUTION_CONFLICT`；
- [ ] 旧 attempt 延迟 ACK 不得覆盖新 attempt externalId；执行结论冲突返回 `409 EXECUTION_RESOLUTION_CONFLICT`；
- [ ] Process name 在 JSON 中始终带引号；
- [ ] 当前无鉴权的边界在 API 文档与部署说明中显式存在。

**Likely files:** `controller/ProcessRestController.java`、`process/service/ProcessCommandService.java`、`controller/error/**`。

**Verification:** `./mvnw -pl amoro-ams-v2 test -Dtest=TestProcessRestController`

---

## P6：手工创建、scanner 与 Action registry

**目标：** 两个入口共用同一冻结与准入链路。

**Dependencies:** P2、P5。

**工作项：**

- [ ] `POST /api/ams/v2/tables/{catalog}/{db}/{table}/processes`；
- [ ] 定义 v2 自有 `ManagedTablePort.resolve/scan`、`ManagedTableSnapshot`、`ProcessActionPlugin.evaluateScheduled/validateAndFreezeManual/buildSubmission` 与 `FrozenActionIntent`；
- [ ] 实现 `V1ManagedTableReadAdapter`：v2 自有只读 Mapper 查询当前 `table_identifier INNER JOIN table_metadata`，不依赖 `org.apache.amoro.server.*`、不写 v1 表；只向 snapshot 暴露 canonical coordinates/string tableId/format/action allowlist 配置与非敏感 trigger facts；
- [ ] REST 强制 `Idempotency-Key`；首次 201、重放 200 + `Idempotency-Replayed:true`、key 重用 409、in-flight 409 + Retry-After；
- [ ] `ProcessCreationService` 校验表、格式、canonical action、engine 和 parameters；
- [ ] retryPolicy 由服务端配置冻结：maxRetries 默认 3/范围 0..3，maxSubmissionRetries 默认 2/范围 0..2，retryDelay 默认 30s/范围 1s..1d；客户端不能覆盖，非法启动配置 fail-fast；
- [ ] `ProcessActionRegistry` lower-kebab → 格式 action 显式映射；P6 提供 fake plugin，真实 remote/local plugin 分别由 P7A/P7B 交付；
- [ ] scanner 通过 ManagedTablePort 稳定 cursor/batch 读取事实，逐表调用 action plugin 后进入同一 CreationService；单表 resolve/probe 失败隔离且有指标；
- [ ] scanner intent key 由 tableId/action/scheduled-window 稳定生成，同窗口重扫不创建第二个资源；
- [ ] initial desired=RUN、phase=PENDING、retryNumber=0；
- [ ] 201 只有在 durable create 成功后返回；
- [ ] scanner 单轮失败隔离、指标和下轮重试；
- [ ] 多实例不保证的边界写入配置/README。

**Acceptance:**

- [ ] manual/scanner 对同一 table/action/engine/parameters/retryPolicy 生成相同冻结执行语义；triggerSource、createdAt、name 及 request/idempotency metadata 按各自入口规则明确不同；
- [ ] Derby/MySQL/PostgreSQL metadata schema contract test 锁定 `table_identifier` join `table_metadata` 的 tableId/coordinates/format 映射；P6 至少运行 Derby，未运行的数据库明确标注；
- [ ] keytab/principal secret/完整 Hadoop site 不进入 ManagedTableSnapshot、Process YAML、REST JSON 或日志；
- [ ] retry/recovery 的 `buildSubmission` 只读冻结 Process spec + server engine profile；修改 live table properties 不改变既有 Process 的 submission command/requestHash；
- [ ] `expire-snapshots` 正确映射，`EXPIRE_SNAPSHOTS` 被拒；
- [ ] 不支持的 format/action 返回 INVALID_ACTION；
- [ ] 两入口并发时恰一 201/成功 create；
- [ ] create 响应丢失后以相同 key/hash 重试只得到原 process name；
- [ ] 表不存在 404；未知 engine 400 INVALID_ENGINE；
- [ ] parameters 创建后冻结，adapter 不重新组装。

**Likely files:** `process/table/**`、`process/trigger/**`、`process/action/**`、`controller/ProcessCreationController.java`。

**Verification:** `./mvnw -pl amoro-ams-v2 test -Dtest='TestProcessCreationController,TestProcessTriggerScanner,TestProcessActionRegistry'`

---

## P7A：Remote Spark adapter

**目标：** 用 v2 端口适配现有远端 Spark 服务，不虚构服务端能力。

**Dependencies:** 技术依赖 P3、P4；实施序列前置 P6。

**工作项：**

- [ ] 基于当前代码核实 `/spark/job/submit`、`/spark/job/state`、`/spark/job/kill` payload/response；
- [ ] 为 Paimon `expire-snapshots/clean-orphans` 两个 `remote-spark` pair 实现真实 `ProcessActionPlugin`，把 allowlist table facts/manual inputs 冻结为 canonical parameters，并只由冻结 spec 构造提交命令；
- [ ] submit 超时/响应丢失映射 UNKNOWN；明确拒绝映射 REJECTED；
- [ ] 只有可证明 HTTP 请求未发送的前置失败映射 submit UNAVAILABLE；请求可能已写出后一律 UNKNOWN；
- [ ] 阻塞 HTTP 调用只运行在独立有界 I/O pool，端口调用立即返回 future；
- [ ] 当前无 ledger 时 resolve 返回 UNSUPPORTED；若新增远端契约，单独测试后再启用；
- [ ] state 解析为 observation；HTTP/JSON 错误为 UNAVAILABLE；
- [ ] kill 只返回命令结果，终态由后续 observe 确认；
- [ ] trackUri/summary 有界；日志不记录完整 parameters；
- [ ] trackUri 只接受无 user-info/控制字符的绝对 http/https URI；`javascript:`、`data:`、相对 URI、畸形 URI 丢弃并告警；可选 host allowlist 配置化；
- [ ] 超时、连接池和重试配置化，adapter 不自行生成新 submissionKey。

**Acceptance:**

- [ ] wiremock/假服务覆盖 2xx、4xx、5xx、timeout、malformed JSON；
- [ ] timeout 不误报 REJECTED/NOT_FOUND；
- [ ] resolve UNSUPPORTED 能驱动 SubmissionUnresolved + 人工路径；
- [ ] registry 中 advertised remote format/action 全部通过参数冻结、live table properties 改变不影响既有命令、wire adapter contract；未实现 pair 返回 INVALID_ACTION 而不是延迟到运行期失败；
- [ ] 不直接把当前 `HttpRemoteSparkStandAloneSubmit` 标记为已复用完成。

**Verification:** `./mvnw -pl amoro-ams-v2 test -Dtest=TestRemoteSparkProcessAdapter`

---

## P7B：Local executor adapter

**目标：** 以独立有界线程池执行本地 action，调度 worker 只派发/观测。

**Decision gate:** 当前条目按 v2 native 方案描述，仅为待确认候选；L1 未关闭前禁止实施 P7B。若选择 `AmsLocalEngineAdapter`，本 Task 必须整体改写为内部协议、v1 execution endpoint、跨进程幂等/超时/鉴权边界和双服务 E2E，不能混用以下 native 假设。

**Dependencies:** P3、P4、P7A；P7A 提交后才进入本节点。

**工作项：**

- [ ] 有界 worker pool、明确 rejection 与 shutdown；
- [ ] 为 Iceberg `expire-snapshots/clean-orphans` 与 Paimon `sync-table-meta` 三个 `local` pair 实现真实 `ProcessActionPlugin`；未实现 pair 不注册；
- [ ] submissionKey 对本地 handle 幂等；重复 submit 同 hash 返回同 handle，不同 hash CONFLICT；
- [ ] submit 立即返回，不等待 action 完成；
- [ ] observe 从本地 registry 读取 phase/result；
- [ ] durable DISPATCHING 后本地 action 已派发、ACK 尚未落库即崩溃：重启后 `resolveSubmission` 在 registry/intent 缺失时返回 LOST，不得返回 NOT_FOUND；
- [ ] 重启后已持久化 ACK handle 不在 registry 时 observe 返回 LOST，置 ExecutionUnresolved；不得返回 authoritative NOT_FOUND；
- [ ] cancel 幂等中断；不支持中断的 action 明确 UNSUPPORTED；
- [ ] 完成结果保留到 Process 持久化确认，防止过早丢 handle；
- [ ] 实现幂等 `release(externalId)`：只有该次执行的固定终态或 FAILED 结果 durable CAS 成功后清理 handle；未知/重复 release 成功 no-op；
- [ ] terminal result hard retention 默认 7 天、正值校验；超时未 release 时清理并告警，仍活跃资源后续 observe 返回 LOST；
- [ ] action 只消费 frozen parameters。

**Acceptance:**

- [ ] scheduler worker 不执行 action、不在 Future.get/join 上等待；
- [ ] action pool 在接收前队列满必须返回 `REJECTED(CAPACITY_EXHAUSTED)`，不创建 handle、不映射 UNKNOWN/UNAVAILABLE、不静默丢任务；
- [ ] submit/observe/cancel/terminal 全链通过；
- [ ] registry 中 advertised local format/action 全部通过参数冻结与真实 action contract；未实现 pair 在 create 时 INVALID_ACTION；
- [ ] 相同 key/hash 幂等，不同 hash CONFLICT；
- [ ] ACK 前 crash-window 场景进入 ExecutionUnresolved、无第二次 submit，且无 externalId 的 execution resolution 可收敛；
- [ ] 重启后本地在途任务无法恢复时进入 ExecutionUnresolved，禁止自动重投；通过 attempt-bound execution resolution 才能收敛，不伪造成功/失败。
- [ ] CAS 失败不 release、固定终态/可重试 FAILED CAS 成功 release、重复 release no-op、CAS 成功后崩溃由 ExecutionHandleReaper 补偿、hard-retention 超时告警均通过。

**Verification:** `./mvnw -pl amoro-ams-v2 test -Dtest=TestLocalProcessAdapter`

---

## P8：列表、TTL、迁移文档与端到端验收

**目标：** 收口读模型、生命周期、v1/v2 切换和发布门禁。

**Dependencies:** P5、P6、P7A、P7B；实施序列前置 P7B；Framework T1-T12 已由全局门禁完成。

**工作项：**

- [ ] `GET /api/ams/v2/tables/{catalog}/{db}/{table}/processes`；
- [ ] query=`action,status,page,pageSize`，page 从 1，pageSize 默认 20/最大 50；不提供 unpaged/all 绕过；
- [ ] 稳定排序 `createdAt DESC, name DESC`；items 为完整资源；表不存在 404；
- [ ] 记录前端 v1 字段到 v2 resource 的派生映射，但不在本任务修改前端；
- [ ] `ProcessTtlCleaner` 只从 ProcessExpiryIndex 按 `(finishedAt,name)` cursor 取最终谓词 + cutoff，单轮至多 batchSize，再按当前 resourceVersion delete；不得遍历全 cache；
- [ ] TTL 默认 interval=60s、batchSize=100、retention=30 天；retention 校验不少于 7 天且不短于公开客户端幂等重试窗口；删除同步移除 idempotency/read/expiry index，但 release entries 独立保留至 release 成功/hard retention；
- [ ] TTL delete 前再次校验当前/历史 local handle 均已从 byHandle 因 release success 移除；pending/in-flight/failed entry 阻止删行并以 inclusive cursor 下轮重读；delete prepared projection 只原子移除 resourcesByName/active/idempotency/read/expiry，禁止用不可重建的 volatile release delta 代替 durable 行；Process 域 `DurableDeletionHook` 在 mutation lane 内、delete stage/同名 create 之前直接幂等 unschedule；delete 失败不得 unschedule；hook 本身禁止 engine I/O；
- [ ] 禁止 runtime truncate；失败重试/告警；
- [ ] README 写清完整 parameters 与当前无鉴权边界；
- [ ] v1/v2 灰度开关、活跃排空、读切换、回退和历史保留；
- [ ] 按版本/action 的 create/active/terminal/UNKNOWN/cancel-latency 指标；v1 endpoint usage 通过 Javalin route after-handler 只读 counter 采集，labels 仅 route template/method/status，不含表名/parameters；
- [ ] README 明确 v1 当前未废弃、无删除日期；advisory deprecation 需生产证明、零新建/活跃/调用方和独立批准；
- [ ] Docker MySQL 5.7 完整流程，记录实际执行数和未验证边界。

**Acceptance:**

- [ ] pageSize=0/51、未知 status/action 的错误语义固定；最大 50 个完整资源的响应上界受单资源 65536B cap 约束；
- [ ] 相同 createdAt 用 name 稳定排序，翻页无重复/遗漏；
- [ ] 列表走 ProcessReadIndex，不对全域 Process 每请求全量排序；索引重建后结果一致；
- [ ] action/status 单独或组合过滤直接命中对应 view，不扫描 table 全历史；单资源 index entry 放大固定不超过 4；
- [ ] string ID 不丢精度；完整 parameters 返回；
- [ ] 可重试 FAILED、CANCELING、SubmissionUnresolved、ExecutionUnresolved 不清理；
- [ ] cleaner 一次删除不超过 batchSize；DB 失败不改内存且不撤销调度；成功删除移除索引并直接 unschedule；
- [ ] expiry index cursor 可续扫、postStart 可重建；版本冲突或 release 未完成时同 key inclusive retry，成功/不再 eligible 后 exclusive 前进；10 万终态资源场景下单轮 aggregate load/delete 候选不超过 batchSize；
- [ ] local release pending/failed 时 TTL 不删 Process；release success→DELETE 前崩溃后重启可从行幂等重建 cleanup；DELETE 成功后立即崩溃也不存在无法重建的 handle；
- [ ] delete DB commit 后阻塞旧 cleanup hook、并发同名 recreate 只能排队；旧 hook 完成后新资源才可创建/调度，旧 delete 不会终止新 entry；
- [ ] v1 200 空页 vs v2 404、`list` vs `items`、type vs action 均写入迁移文档；
- [ ] 灰度期间同一 table/action 不同时启用 v1/v2 触发；
- [ ] E2E 覆盖创建→submit→observe→终态、取消竞态、UNKNOWN submission 消解、LOST execution 消解、崩溃重放、TTL；
- [ ] remote/local 两类各至少一条真实 adapter 流程，不以 fake 全绿替代；
- [ ] 未运行 docker-it 时明确“未验证”，不宣称完整成功。

**Likely files:** `controller/ProcessListController.java`、`process/cleanup/ProcessTtlCleaner.java`、`amoro-ams-v2/README.md`、E2E tests。

**Verification:**

- 离线：`./mvnw -pl amoro-ams-v2 test`
- Docker：`./mvnw -pl amoro-ams-v2 test -Pdocker-it`
- 校验：`./mvnw -pl amoro-ams-v2 verify`
- 全仓回归：按仓库 JDK/toolchain 约束执行 `./mvnw clean compile -Pskip-dashboard-build`

---

## 发布前证据清单

- [ ] 当前 HEAD/工作区与测试制品来源已记录；
- [ ] 离线单测实际执行数 > 0，无隐藏 skipped；
- [ ] docker-it 实际执行数 > 0；
- [ ] remote/local contract tests 均通过；
- [ ] 所有 Mermaid 与 API 示例通过文档检查；
- [ ] v1/v2 差异矩阵与灰度操作经 reviewer 确认；
- [ ] 当前网络信任边界可接受完整 parameters；若边界变化，先完成鉴权/字段分级；
- [ ] Draft 只在代码、测试、迁移门禁全部满足后改为 Implemented。
