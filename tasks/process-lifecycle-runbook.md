# amoro-ams-v2 Process 实际调度生命流程（Runbook）

> 基于 `jira/process-dev` 分支实际实现（Framework T1–T12 + Process P1–P8 全部交付，
> 提交 `724ed7be4`…`e63a8bd4f`）整理。权威设计见 `amoro-ams-v2-process-spec.md`；本文描述
> **代码里真实发生的事**，并标注与 spec 的当前差异。离线 183 + docker-it（真 MySQL 5.7）
> 192 测试全绿。

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


## 0.5 图集（架构 / 状态机 / Controller 入口 / RUN·CANCEL 流程）

> 以下四图均按**当前代码实际行为**绘制（含评审修复 190fda699：准入 mutex、durable
> DISPATCHING 两步、UNKNOWN 不盲重投、取消零提交、handle release/TTL gate）。

### 图 1：总体架构

```mermaid
flowchart TB
    subgraph 入口["创建入口（两个，共用同一准入链）"]
        REST["REST /api/ams/v2<br/>POST tables/{c}/{d}/{t}/processes<br/>Idempotency-Key"]
        SCAN["ProcessTriggerScanner<br/>scan→gate→创建（分钟窗口幂等键）"]
    end
    SUPPORT["ProcessRestSupport<br/>per-(tableId,action) admission mutex<br/>幂等重放/单活跃/冻结 retryPolicy"]
    ASM["ProcessDomainAssembly<br/>repository + indexProjection + 删除hook"]
    PERSIST["InMemoryPersistence（L5）<br/>durable-first · resourceVersion CAS · fence/repair"]
    LANE["BlobStoreActor（L6）<br/>每域单线程 mutation lane"]
    BLOB["MyBatisBlobStore（L7）<br/>五种 SQL · 表名白名单"]
    DB[("amoro_process<br/>Base64(YAML)")]
    IDX["ProcessIndexProjection<br/>聚合快照：resourcesByName<br/>+ 准入/幂等/过期视图"]
    LD["ListenerDispatcher<br/>pair 保序 · 有界重试"]
    SCHED["DefaultScheduler<br/>single-flight · DelayQueue · 退避"]
    CTRL["ProcessReconciler（Controller）<br/>每轮至多一步 · version-CAS"]
    DISP["ProcessEngineDispatcher<br/>Submission/Execution 单飞<br/>command-timeout（submit 超时→UNKNOWN）"]
    LOCAL["LocalEngineAdapter<br/>有界 action 池<br/>容量满=权威 REJECTED"]
    FAKE["FakeEngineAdapter<br/>远端 Spark 单测模拟（用户决策）"]
    TTL["ProcessTtlCleaner<br/>expiryOrder 有界批次<br/>handle gate → CAS delete"]
    REG["ExecutionHandleRegistry<br/>终态 durable 后 release"]

    REST --> SUPPORT
    SCAN --> SUPPORT
    SUPPORT -->|"keyed mutex 内: 幂等查索引→活跃查→create"| ASM
    ASM --> PERSIST --> LANE --> BLOB --> DB
    LANE -.->|"DB 成功后 same-lane publish"| IDX
    LANE -.->|"durable 后 handoff 事件"| LD
    LD -.->|"AFTER_CREATED / POST_START"| SCHED
    SCHED -->|"(process,name) 单飞"| CTRL
    CTRL -->|"submit/observe/cancel 异步"| DISP --> LOCAL
    DISP -.-> FAKE
    CTRL -.->|"终态 durable→release"| REG
    TTL -->|"索引快照+gate+delete"| ASM
    DB -.->|"postStart 重放重建"| IDX
```

### 图 2：状态机流转（phase × submitState × desired）

```mermaid
stateDiagram-v2
    direction LR
    [*] --> PENDING: durable create<br/>desired=RUN

    state "RUN 路径（desired=RUN）" as run {
        PENDING --> PENDING: 轮1 CAS attempt(CREATED)<br/>轮2 CAS→DISPATCHING
        PENDING --> SUBMITTED: submit ACK<br/>(记 externalId)
        PENDING --> Unresolved: submit UNKNOWN/CONFLICT<br/>submitState 持久化·停轮·不盲重投
        Unresolved --> SUBMITTED: 人工 ACK<br/>POST /submission-resolutions
        Unresolved --> PENDING: 人工 NOT_FOUND<br/>代次+1（预算内）/ 预算尽→FAILED
        SUBMITTED --> RUNNING: observe RUNNING
        SUBMITTED --> SUCCESS: observe 终态
        RUNNING --> SUCCESS: observe 终态
        SUBMITTED --> FAILED: observe FAILED
        RUNNING --> FAILED: observe FAILED
        FAILED --> PENDING: 预算内·disposition=ALLOW<br/>归档旧 attempt(含 externalId)·retryDelay 后
        FAILED --> FinalFail: desired=CANCEL/预算尽/FINAL
        state Unresolved {
            [*] --> UNKNOWN
            [*] --> CONFLICT
        }
    }

    state "CANCEL 路径（desired=CANCEL）" as cancel {
        PENDING --> CANCELED: submitState=CREATED<br/>零引擎调用直接终态
        PENDING --> CANCELING: DISPATCHING/UNKNOWN<br/>保守转（消解路径收敛）
        SUBMITTED --> CANCELING: CAS desired=CANCEL
        RUNNING --> CANCELING: CAS desired=CANCEL
        CANCELING --> CANCELING: cancel ACCEPTED→标记<br/>CANCEL_REQUESTED 后转 observe 轮
        CANCELING --> CANCELED: observe/cancel 终态
        CANCELING --> KILLED: observe/cancel 终态
        CANCELING --> CLOSED: observe/cancel 终态
        CANCELING --> FAILED: desired=CANCEL 即最终
    }

    SUCCESS --> [*]: TerminalState<br/>+两层 finishedAt
    CANCELED --> [*]
    KILLED --> [*]
    CLOSED --> [*]
    FinalFail --> [*]
```

> 迟到回调（observe/cancel/submit）统一按 `attempt.submissionKey` 身份守卫：attempt 已
> 轮换（retry 或人工消解）则丢弃，绝不把旧结果写进新 attempt。

### 图 3：Controller 入口/出口（调度接入全路径）

```mermaid
flowchart TB
    subgraph 入调度["入口：谁会 schedule(ProcessReconciler)"]
        A["durable create 成功<br/>→ AFTER_CREATED 事件"]
        B["重启 postStart 全量重放<br/>→ POST_START 事件/资源"]
        C["人工消解/取消等命令 CAS<br/>→ AFTER_MODIFIED 事件"]
    end
    LD["ListenerDispatcher<br/>(listener,domain,name) pair 保序"]
    SCH["DefaultScheduler 登记表<br/>ConcurrentHashMap&lt;ControllerKey,ScheduledEntry&gt;"]
    subgraph 合并["single-flight 合并规则"]
        M1["无条目 → 入队新包装"]
        M2["QUEUED → 只缩短 deadline<br/>（earliest 合并，绝不变晚）"]
        M3["在飞(CLAIMED) → 收敛 rescheduleRequested<br/>worker 返回后按最早值重入队"]
        M4["TERMINATED → 移除旧 identity<br/>putIfAbsent 新 generation"]
    end
    W["SchedulerWorker（N 个 daemon）<br/>signal-version 等待，到期 poll"]
    INV["ProcessReconciler.invoke()<br/>每轮至多一个逻辑步骤"]
    subgraph 出口["出口：谁停掉调度"]
        O1["固定终态/最终 FAILED<br/>throw TerminalState → 登记表回收"]
        O2["TTL/运维 delete<br/>→ DurableDeletionHook 同 lane<br/>key-only unschedule（先于同名 create）"]
        O3["优雅停机 shutdown(timeout)<br/>scheduler→dispatcher→lane 定序"]
    end

    A --> LD
    B --> LD
    C --> LD
    LD --> SCH
    SCH --> 合并
    合并 --> W --> INV
    INV -->|"Step.DONE/WAIT/DISPATCHED"| W
    INV --> O1
    O2 -.-> SCH
    O3 -.-> SCH
```

### 图 4a：RUN 全链路时序（含本地引擎）

```mermaid
sequenceDiagram
    participant R as REST/Scanner
    participant S as ProcessRestSupport
    participant L as mutation lane(BlobStoreActor)
    participant DB as amoro_process
    participant E as ListenerDispatcher
    participant K as DefaultScheduler
    participant C as ProcessReconciler
    participant D as EngineDispatcher
    participant G as LocalEngineAdapter

    R->>S: create(Idempotency-Key, action, engine, params)
    Note over S: per-(tableId,action) mutex 内：<br/>幂等重放→活跃检查→冻结参数
    S->>L: create(v0) → serialize(YAML) → projection prepare
    L->>DB: INSERT Base64(YAML)（durable 界线）
    L-->>S: v1, phase=PENDING, attempt=submissionKey name:0:0 (CREATED)
    L->>E: handoff AFTER_CREATED（不阻塞 stage）
    E->>K: schedule(Reconciler)（single-flight 合并）

    Note over K,C: 周期轮（每轮至多一步）
    K->>C: invoke #1：PENDING+attempt 在 → CAS submitState→DISPATCHING
    K->>C: invoke #2：DISPATCHING → D.submit(key,hash,payload)
    D->>G: 单飞派发（容量满=权威 REJECTED）
    G-->>D: ACK(externalId)
    D-->>C: 回调（attempt 身份守卫通过）
    C->>L: CAS→SUBMITTED（externalId, submittedAt）版本+1

    K->>C: invoke #n：SUBMITTED → D.observe(externalId)
    G-->>D: KNOWN(RUNNING)
    C->>L: CAS→RUNNING（startedAt）——相位不变则跳过写
    K->>C: invoke #n+1：RUNNING → observe
    G-->>D: KNOWN(SUCCESS)
    C->>L: CAS→SUCCESS + attempt.finishedAt + 顶层 finishedAt
    C->>D: release(externalId)（终态已 durable）
    C->>K: throw TerminalState → 登记表回收
```

### 图 4b：CANCEL 全链路时序

```mermaid
sequenceDiagram
    participant U as 运维/客户端
    participant S as REST PATCH
    participant L as mutation lane
    participant K as DefaultScheduler
    participant C as ProcessReconciler
    participant D as EngineDispatcher
    participant G as LocalEngineAdapter

    alt 从未派发（submitState=CREATED）
        U->>S: PATCH {desiredState: CANCEL}
        S->>L: CAS desired=CANCEL（+nextReconcileAt=now）
        K->>C: invoke：PENDING+CREATED
        Note over C: spec §7.4：可证从未发送<br/>零引擎调用
        C->>L: CAS→CANCELED + 两层 finishedAt
        C->>K: TerminalState（回收）
    else 已派发在跑（SUBMITTED/RUNNING）
        U->>S: PATCH {desiredState: CANCEL}
        S->>L: CAS desired=CANCEL + phase→CANCELING
        K->>C: invoke：CANCELING → D.cancel(externalId)
        G-->>D: ACCEPTED（协作标记）
        C->>L: CAS attempt submitState→CANCEL_REQUESTED（后续轮不再重复 cancel）
        K->>C: invoke：CANCEL_REQUESTED → D.observe(externalId)
        G-->>D: KNOWN(CANCELED)（action 协作退出）
        C->>L: CAS→CANCELED + finishedAt
        C->>D: release(externalId)
        C->>K: TerminalState
    else 提交未决（UNKNOWN/CONFLICT）
        U->>S: POST /submission-resolutions<br/>{ACK,externalId} 或 {NOT_FOUND}
        alt ACK
            S->>L: CAS→CANCELING（记 externalId+审计同 CAS）
        else NOT_FOUND
            S->>L: CAS→CANCELED + finishedAt（同 CAS 落审计）
        end
    end
    Note over C: 全部回调带 attempt 身份守卫；<br/>desired=CANCEL 使 FAILED 即最终
```

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
      attempt 已在且 submitState=CREATED → 先 CAS submitState→DISPATCHING（durable 先置，崩溃重启进消解）
      attempt=DISPATCHING → dispatcher.submit(key, requestHash, payload)   # 异步单飞
                      └ ACCEPTED 回调: CAS→SUBMITTED（记 externalId, submittedAt）
                        REJECTED 回调: CAS→FAILED(终态判定)+failure
                        UNKNOWN/CONFLICT 回调: CAS submitState→UNKNOWN/CONFLICT 并停轮——
                          绝不盲重投同 key，等待人工提交消解（POST /submission-resolutions）
                        UNAVAILABLE 回调: 可证未发送，下轮同 key 重投
    SUBMITTED/RUNNING:
      dispatcher.observe(externalId)
        KNOWN(SUBMITTED) → 保持（仅刷新 lastObserved）
        KNOWN(RUNNING)   → CAS→RUNNING（startedAt）
        KNOWN(SUCCESS/CANCELED/KILLED/CLOSED) → CAS→终态 + attempt.finishedAt + 顶层 finishedAt
        KNOWN(FAILED)    → CAS→FAILED + lastError; retryable=false ⇒ disposition=FINAL
            NOT_FOUND/LOST   → 首版：日志+下轮继续；人工执行消解（POST /execution-resolutions）收敛
        回调统一带 submissionKey 身份守卫：attempt 已轮换则丢弃，绝不覆盖新 attempt
        终态写 durable 成功 → engine.release(externalId) + ExecutionHandleRegistry 释放（TTL gate 依赖）
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

## 4. P5–P8 已交付内容（本轮更新）

- **P5 REST `/api/ams/v2`**：create（幂等键/服务端冻结 retryPolicy/单活跃准入/终态后重放 200+原资源）、GET 点查、列表（createdAt DESC、单快照 items+total、page≥1/pageSize 1..50）、PATCH 取消（FAILED 归并同 CAS 防 TTL 死角）、submission/execution 人工消解（双身份校验+DISPATCHING 门控+审计同 CAS）；统一错误体 {code,message,timestamp,traceId}，PersistenceException→503、未知字段→400。
- **P6 触发扫描**：`ManagedTablePort`（只读表快照）+ `ProcessActionPlugin`（interval 门控+冻结参数）+ `ProcessTriggerScanner`（复用 REST 准入、per-(tableId,action) mutex、分钟窗口幂等键、表级隔离）。
- **P7 本地引擎**：`LocalEngineAdapter` 有界 action 池（submit 立即 ACK、observe 收敛、容量满=权威 REJECTED、释放后 observe=LOST、协作式 cancel）——真实 adapter 非 fake；远端 Spark 按用户决策以 `FakeEngineAdapter` 单测模拟。
- **P8 TTL**：`ProcessTtlCleaner` 从 expiryOrder 顺序读到 cutoff、有界批次、逐条 CAS delete；真 MySQL E2E 覆盖 REST→本地引擎→终态→TTL→重启重放全链路。

## 5. 当前与 spec 的差异（如实）

1. **读索引**：首版用不可变 Map 快照（语义等价），spec 的 persistent rank tree O(log) 渐进上界延后（`ProcessIndexSnapshot` 可替换）。
2. **EngineBackoff 持久化计数、conditions、nextReconcileAt 精确门控**：模型字段齐全，Reconciler 首版以周期轮询替代精确门控（语义收敛等价、效率差异）。
3. **远端 Spark adapter**：按用户决策单测模拟（FakeEngineAdapter），真实 HTTP 提交未实现。
4. **L2/L3 业务边界待用户确认**：首版 action scope（5 pair 候选）、scheduled trigger 兼容承诺、真实格式维护动作（Iceberg/Paimon 调用）接入。
5. **CI workflow**：现有 core workflow 不含 JDK17 toolchain/path filter，本地验证为当前门禁（README 声明）。

## 6. 验证现状

- 离线全量 **183 tests** 绿（`JAVA_HOME=jdk11 ./mvnw -pl amoro-ams-v2 test`）
- docker-it（真 MySQL 5.7.44 @3306/amoro_v2）**192 tests** 全绿（`AMORO_V2_MYSQL_PASSWORD=... ./mvnw -pl amoro-ams-v2 test -Pdocker-it`）
- 双 JDK 构建（JDK11 reactor / JDK17 boot jar）与 spotless/checkstyle/rat 全过
- 每个任务均经独立 code review（框架+P5 共 4 次 Request-changes 全部修复）后本地原子提交
