# amoro-ams-v2 Process 实际调度生命流程（Runbook）

> 基于 `jira/process-dev` 分支实际实现（Framework T1–T12 + Process P1–P4 已交付，提交
> `724ed7be4`…`230cb6187`）整理。权威设计见 `amoro-ams-v2-process-spec.md`；本文描述
> **代码里真实发生的事**，并标注与 spec 的当前差异。

## 0. 组件地图（已实现）

| 层 | 组件 | 职责 |
|---|---|---|
| 调度 | `DefaultScheduler`（control） | DelayQueue 排序 + single-flight（同 key 至多一个 Controller 在飞）、退避 {3,3,5,8,13,21,34,55}s、优雅停机 |
| 持久化 | `InMemoryPersistence` + `BlobStoreActor`（persistence/blob） | 每域单线程 mutation lane：read→detached→CAS→apply→版本→serde→projection prepare→DB→发布→hook→listener |
| 事件 | `ListenerDispatcher`（persistence） | 有界队列 + 按 (listener,domain,name) pair 保序 + 有界重试 |
| serde | `VersionAwareJacksonSerde`（serde） | Base64(YAML/JSON) 文档、版本链、64KiB 上界 |
| Process 域 | `ProcessDomainAssembly`（process） | `amoro_process` 表、聚合索引快照（准入/幂等/过期）、删除 hook=同 lane unschedule |
| 状态机 | `ProcessReconciler`（process） | level-triggered Controller：RUN/CANCEL 全路径 |
| 引擎 | `ProcessEngineDispatcher` + `FakeEngineAdapter`（process/engine） | 命令单飞 + 强制超时（submit 超时→UNKNOWN）；远端 Spark 用 fake 模拟（用户决策） |

## 1. 一次 Process 的完整生命

### 1.1 创建（durable create）

```
REST/Scanner → ProcessDomainAssembly.repository.create(resource v0)
  ├─ 调用线程: serde.detach(resource)          # alias 隔离从入队开始
  └─ mutation lane（单线程）:
       canonical.containsKey? → ResourceAlreadyExists（准入第一道）
       candidate = resource.withVersion(1)
       bytes = serde.serialize(candidate)        # 超过 65536B 直接失败
       projection.prepare(created(detached))     # 纯计算，DB 前失败零副作用
       blobStore.insert(amoro_process, name, Base64(YAML))   # ← durable 界线
       canonical.put + projection.commit         # O(1) 原子切换
       listener handoff(AFTER_CREATED)           # 异步，绝不阻塞/反转 stage
返回 v1，phase=PENDING, desired=RUN
```

数据库行落库后，即使进程当场崩溃，新进程 `postStart` 会从 DB 全量重放并重建一切。

### 1.2 事件 → 调度接入

```
ListenerDispatcher 收 AFTER_CREATED envelope
  → ProcessListener.afterCreated（异步 worker）
  → scheduler.schedule(ProcessReconciler(name))
     single-flight：同 key ("process", name) 至多一个在飞；重复 schedule 合并最早 deadline
```

### 1.3 每轮调和（invoke，50ms~周期由 amoro.control.scheduler.delay-ms 决定）

Reconciler 在 scheduler worker 上执行，**每轮至多一个逻辑步骤**：

```
invoke():
  resource = repository.get(name)        # 读内存快照（detached），零 DB 调用
  if final: throw TerminalState          # 登记表回收，永久停调度
  desired=RUN:
    PENDING/UNKNOWN:
      attempt 为空 → 先 CAS 持久化 attempt（submissionKey=name:retry:0, submitState=CREATED）
                    → 本轮结束，下轮才派发（spec §7.3：durable DISPATCHING 先于 dispatch）
      attempt 已在  → dispatcher.submit(key, requestHash, payload)  # 异步单飞
                      └ ACCEPTED 回调: CAS→SUBMITTED（记 externalId, submittedAt）
                        REJECTED 回调: CAS→FAILED(终态判定)+failure
                        UNKNOWN/CONFLICT/UNAVAILABLE: 本轮不动，下轮重派（engine backoff 节流）
    SUBMITTED/RUNNING:
      dispatcher.observe(externalId)
        KNOWN(SUBMITTED) → 保持（仅刷新 lastObserved）
        KNOWN(RUNNING)   → CAS→RUNNING（startedAt）
        KNOWN(SUCCESS/CANCELED/KILLED/CLOSED) → CAS→终态 + attempt.finishedAt + 顶层 finishedAt
        KNOWN(FAILED)    → CAS→FAILED + lastError; retryable=false ⇒ disposition=FINAL
        NOT_FOUND/LOST   → 首版：日志+下轮继续（spec 要求 ExecutionUnresolved 人工消解，P5 范畴）
    FAILED（预算内）:
      CAS 归档旧 attempt（含 externalId → attemptHistory，供幂等 release）
      retryNumber+1, 新 attempt=null, phase=PENDING
      → Step.WAIT：scheduler.schedule(this, retryDelay)   # 业务门控，非框架退避
    FAILED（desired=CANCEL / 预算尽 / FINAL）:
      throw TerminalState
  desired=CANCEL:
    PENDING/UNKNOWN → 先 submit/resolve 确认是否已派发；确认从未派发 → CAS→CANCELED（终态）
    SUBMITTED/RUNNING → CAS→CANCELING
    CANCELING:
      submitState != CANCEL_REQUESTED → dispatcher.cancel(externalId)
          ACCEPTED          → CAS 标记 CANCEL_REQUESTED（后续轮次不再重复 cancel）
          ALREADY_TERMINAL  → CAS→实际终态 + finishedAt
      submitState == CANCEL_REQUESTED → observe 轮次收敛终态
    FAILED → TerminalState（desired=CANCEL 使 FAILED 即终）
```

### 1.4 CAS 纪律

所有状态写走 `modify(name, expectedVersion, fn)`：
- 版本不匹配 → `PreconditionFailedException` → **本轮直接放弃**（不重试），下一轮重读最新值；
- 迟到的引擎回调按 `attempt.submissionKey` 身份比对，attempt 已轮换则丢弃；
- 因此并发 cancel / observe / 重试永远不会互相覆盖。

### 1.5 删除（TTL 或运维）

```
delete(name, version):
  lane: DB DELETE → cache/index 摘除 → DurableDeletionHook:
        scheduler.unschedule(ControllerKey("process", name))   # 同 lane、delete stage 完成前
  → 之后同名 create 才可能出队；旧 delete 永不误杀新 entry
```

## 2. 重启重放（"DB 是事实源"的运行时证明）

新进程启动：

```
postStart():
  blobStore.forEach(amoro_process)            # 全量游标
    → serde.deserialize（旧版本经 Converter 链懒升级并回写）
    → canonical 重建 + ProcessIndexProjection 逐资源 prepare/commit 重建索引
  → 逐资源 handoff POST_START → listener → scheduler.schedule
调度恢复后 PENDING/SUBMITTED/RUNNING/CANCELING 全部继续调和：
  - submitState=DISPATCHING/UNKNOWN 的，重启后先 resolveSubmission 消解
  - 已 ACK 的直接 observe 续跑
  - 终态资源抛 TerminalState 自动退场
（T11 E2E 对真 MySQL 完整验证了该链路）
```

## 3. 失败与恢复语义速查

| 故障 | 行为 |
|---|---|
| Controller 抛任意异常 | 框架退避 {3,3,5,8,...}s 无限重试，**控制器不丢弃** |
| 引擎命令超时 | dispatcher 强制完成：submit→UNKNOWN（不盲重投），其余→UNAVAILABLE |
| DB 提交结果未知 | 新连接点读三分支：candidate=成功 / previous=失败 / 不可判=fence+repair |
| listener 失败 | 不影响 durable 写；同 pair 有界重试 3 次后告警丢弃，修复扫描补偿 |
| mailbox 满 | 写方快速失败（stage exceptional），绝不先确认后补写 |
| 进程崩溃 | 重启 postStart 全量重放，level-triggered 自然收敛 |

## 4. 当前与 spec 的差异（如实）

1. **读索引**：首版用不可变 Map 快照（语义等价），spec 的 persistent rank tree O(log) 渐进上界延后（`ProcessIndexSnapshot` 可替换）。
2. **ExecutionUnresolved/SubmissionUnresolved 人工消解**（spec §3.4/§8.5/§8.6）：状态机已按"不盲重投"处理，专用 REST 端点与 condition 体系属 P5 未交付部分。
3. **EngineBackoff 持久化计数、conditions、nextReconcileAt 门控**：模型字段齐全，Reconciler 首版以周期轮询替代精确门控（语义收敛等价、效率差异）。
4. **P5–P8 未交付**：REST `/api/ams/v2`、Scanner/ManagedTablePort、真实 HTTP Spark adapter（现为 fake 模拟——用户决策）、TTL cleaner、v1 差异矩阵文档。

## 5. 验证现状

- 离线全量 **153 tests** 绿（`JAVA_HOME=jdk11 ./mvnw -pl amoro-ams-v2 test`）
- 真 MySQL 5.7.44 集成绿（`-Pdocker-it`，`AMORO_V2_MYSQL_*` 环境变量）
- 双 JDK 构建（JDK11 reactor / JDK17 boot jar）与 spotless/checkstyle/rat 全过
- 每个任务均经独立 code review（3 次 Request-changes 全部修复）后本地原子提交
