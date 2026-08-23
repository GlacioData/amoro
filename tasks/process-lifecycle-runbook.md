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
| 引擎 | `ProcessEngineDispatcher` + `LocalEngineAdapter`/`FakeEngineAdapter`（process/engine） | 引擎命令同键并发去重 + 强制超时（submit 超时归类 UNKNOWN）；本地引擎为真实线程池实现，远端 Spark 用可编程模拟适配器（用户决策） |


## 0.5 图集（架构 / RUN 状态机 / CANCEL 状态机 / Controller 入口 / RUN·CANCEL 时序）

> 六张图均描述**当前代码的实际行为**（含评审修复 190fda699：准入互斥锁、提交前先
> 持久化 DISPATCHING、未知提交不盲目重复提交、未派发取消不调用引擎、终态后释放执行
> 句柄、TTL 删除前检查句柄释放状态）。
>
> 术语约定：**single-flight（同键并发去重）** 指同一 ControllerKey 的并发调度请求合并
> 为至多一次执行；**mutation lane（串行写线程）** 指每个持久化域唯一的单线程写入队
> 列，所有读写-计算-写入在该线程内串行执行；**CAS** 指基于 resourceVersion 的乐观
> 并发控制写入。

### 图 1：总体架构

```mermaid
flowchart TB
    subgraph Entry["创建入口（两个，共用同一准入链路）"]
        REST["REST /api/ams/v2<br/>POST tables/{c}/{d}/{t}/processes<br/>请求头 Idempotency-Key"]
        SCAN["ProcessTriggerScanner<br/>表扫描 → 动作门控 → 创建请求<br/>（按分钟窗口生成幂等键）"]
    end
    SUPPORT["ProcessRestSupport<br/>per-(tableId,action) 准入互斥锁<br/>幂等重放 / 单活跃校验 / 冻结 retryPolicy"]
    ASM["ProcessDomainAssembly<br/>repository + indexProjection + 删除钩子"]
    PERSIST["InMemoryPersistence（L5）<br/>持久化优先写路径 · resourceVersion CAS · 异常结果围栏与修复"]
    LANE["BlobStoreActor（L6）<br/>每域单线程 mutation lane"]
    BLOB["MyBatisBlobStore（L7）<br/>五种 SQL · 表名白名单"]
    DB[("amoro_process<br/>Base64(YAML)")]
    IDX["ProcessIndexProjection<br/>聚合快照：resourcesByName<br/>+ 准入/幂等/过期索引"]
    LD["ListenerDispatcher<br/>按 (listener,domain,name) 保序 · 有界重试"]
    SCHED["DefaultScheduler<br/>同键并发去重 · DelayQueue · 退避"]
    CTRL["ProcessReconciler（Controller）<br/>每轮至多一个逻辑步骤 · 版本 CAS 写"]
    DISP["ProcessEngineDispatcher<br/>提交/执行两类命令并发去重<br/>命令超时（submit 超时归类 UNKNOWN）"]
    LOCAL["LocalEngineAdapter<br/>有界执行线程池<br/>队列满 = 权威 REJECTED"]
    FAKE["FakeEngineAdapter<br/>远端 Spark 单元测试模拟（用户决策）"]
    TTL["ProcessTtlCleaner<br/>按过期索引有界批量删除"]
    REG["ExecutionHandleRegistry<br/>终态持久化后释放执行句柄"]

    REST --> SUPPORT
    SCAN --> SUPPORT
    SUPPORT -->|"互斥锁内：幂等查询→活跃查询→创建"| ASM
    ASM --> PERSIST --> LANE --> BLOB --> DB
    LANE -.->|"数据库成功后同线程发布"| IDX
    LANE -.->|"持久化成功后投递事件"| LD
    LD -.->|"AFTER_CREATED / POST_START"| SCHED
    SCHED -->|"(process,name) 并发去重"| CTRL
    CTRL -->|"submit/observe/cancel 异步命令"| DISP --> LOCAL
    DISP -.-> FAKE
    CTRL -.->|"终态持久化后 release"| REG
    TTL -->|"索引快照 + 句柄检查 + 删除"| ASM
    DB -.->|"postStart 全量重放重建"| IDX
```

### 图 2：状态机 —— desired=RUN 路径

```mermaid
stateDiagram-v2
    direction LR
    [*] --> PENDING: 持久化创建成功<br/>（resourceVersion=1）

    PENDING --> PENDING: 第1轮 CAS 持久化 attempt<br/>（submissionKey=name:retry:0, submitState=CREATED）
    PENDING --> PENDING: 第2轮 CAS submitState<br/>CREATED → DISPATCHING（提交前先持久化）
    PENDING --> SUBMITTED: submit 返回 ACK<br/>（记录 externalId、submittedAt）
    PENDING --> UNKNOWN: submit 返回 UNKNOWN<br/>（submitState 持久化为 UNKNOWN，<br/>停止自动调度，不重复提交同一键）
    PENDING --> CONFLICT: submit 返回 CONFLICT<br/>（同上，等待人工裁决）

    UNKNOWN --> SUBMITTED: 人工裁决 ACK<br/>POST /submission-resolutions
    UNKNOWN --> PENDING: 人工裁决 NOT_FOUND<br/>dispatchGeneration+1（预算内）
    UNKNOWN --> FAILED: 代次预算耗尽<br/>FAILED/SUBMISSION_NOT_ACCEPTED
    CONFLICT --> SUBMITTED: 人工裁决 ACK
    CONFLICT --> PENDING: 人工裁决 NOT_FOUND

    SUBMITTED --> RUNNING: observe 返回 RUNNING<br/>（相位未变化时不执行写操作）
    SUBMITTED --> SUCCESS: observe 返回 SUCCESS
    RUNNING --> SUCCESS: observe 返回 SUCCESS
    SUBMITTED --> FAILED: observe 返回 FAILED
    RUNNING --> FAILED: observe 返回 FAILED

    FAILED --> PENDING: desired=RUN 且重试预算未耗尽<br/>且 disposition ≠ FINAL：<br/>归档旧 attempt（保留 externalId），<br/>retryNumber+1，retryDelay 后重新调度
    FAILED --> FinalFailed: 三条件之一成立<br/>（desired=CANCEL / 预算耗尽 / FINAL）

    SUCCESS --> [*]: 抛出 TerminalState<br/>调度登记表移除该条目
    FinalFailed --> [*]: 同上<br/>（同时写入 failure 与两级 finishedAt）

    note right of UNKNOWN
        迟到的引擎回调按 attempt
        的 submissionKey 校验身份，
        attempt 已轮换则丢弃该结果，
        不覆盖新 attempt 的状态。
    end note
```

### 图 3：状态机 —— desired=CANCEL 路径

```mermaid
stateDiagram-v2
    direction LR
    PENDING --> CANCELED: submitState=CREATED<br/>（可证明从未派发：<br/>直接 CAS 终态，不发起任何引擎请求）
    PENDING --> CANCELING: submitState=DISPATCHING/UNKNOWN<br/>（存在派发痕迹但无 externalId：<br/>保守转入 CANCELING，由人工裁决路径收敛）
    SUBMITTED --> CANCELING: 持久化 desiredState=CANCEL
    RUNNING --> CANCELING: 持久化 desiredState=CANCEL

    CANCELING --> CANCELING: 引擎 cancel 返回 ACCEPTED：<br/>CAS submitState→CANCEL_REQUESTED，<br/>后续轮次改为 observe 轮询（不重复 cancel）
    CANCELING --> CANCELED: observe / cancel 返回终态
    CANCELING --> KILLED: observe / cancel 返回终态
    CANCELING --> CLOSED: observe / cancel 返回终态
    CANCELING --> SUCCESS: observe / cancel 返回终态
    CANCELING --> FAILED: 引擎失败或人工裁决 FAILED<br/>（desired=CANCEL 使 FAILED 即为最终态）
    FAILED --> [*]: desired=CANCEL 时不再重试

    UNKNOWN --> CANCELED: 人工裁决 NOT_FOUND<br/>（同一 CAS 写入终态与审计记录）
    UNKNOWN --> CANCELING: 人工裁决 ACK<br/>（记录 externalId）

    CANCELED --> [*]: TerminalState
    KILLED --> [*]
    CLOSED --> [*]
    SUCCESS --> [*]
```

### 图 4：Controller 的调度入口与出口

```mermaid
flowchart TB
    subgraph In["调度入口（三个，均产生 schedule 调用）"]
        A["资源持久化创建成功<br/>→ AFTER_CREATED 事件"]
        B["进程重启 postStart 全量重放<br/>→ POST_START 事件（每资源一次）"]
        C["取消 / 人工裁决命令 CAS 成功<br/>→ AFTER_MODIFIED 事件"]
    end
    LD["ListenerDispatcher<br/>同一 (listener,domain,name) 内事件按序投递"]
    SCH["DefaultScheduler 调度登记表<br/>ConcurrentHashMap&lt;ControllerKey, ScheduledEntry&gt;"]
    subgraph Merge["同键并发去重（single-flight）合并规则"]
        M1["无登记条目 → 创建新包装并入队"]
        M2["状态 QUEUED → 仅当新到期时间更早时<br/>更新并重新入队（绝不推迟已有任务）"]
        M3["状态 CLAIMED（正在执行）→ 不入队，<br/>将请求的到期时间合并到 rescheduleRequested，<br/>本次执行返回后按最早值重新入队"]
        M4["状态 TERMINATED → 移除旧条目，<br/>以新条目标识注册（generation 隔离）"]
    end
    W["SchedulerWorker（N 个守护线程）<br/>基于信号版本的等待策略，到期后非阻塞取出"]
    INV["ProcessReconciler.invoke()<br/>每轮至多一个逻辑步骤（CAS 写或一个异步命令）"]
    subgraph Out["调度出口（三个）"]
        O1["固定终态 / 最终 FAILED<br/>抛出 TerminalState → 登记表移除条目"]
        O2["TTL 或运维删除<br/>→ DurableDeletionHook 在同一写线程内<br/>执行 scheduler.unschedule（先于同名重建）"]
        O3["优雅停机 shutdown(timeout)<br/>按 scheduler → dispatcher → lane 顺序停机"]
    end

    A --> LD
    B --> LD
    C --> LD
    LD --> SCH
    SCH --> Merge
    Merge --> W --> INV
    INV -->|"返回 DONE / WAIT / DISPATCHED 之一"| W
    INV --> O1
    O2 -.-> SCH
    O3 -.-> SCH
```

### 图 5：RUN 完整调用时序（本地引擎）

```mermaid
sequenceDiagram
    participant R as REST/Scanner
    participant S as ProcessRestSupport
    participant L as 串行写线程(BlobStoreActor)
    participant DB as amoro_process 表
    participant E as ListenerDispatcher
    participant K as DefaultScheduler
    participant C as ProcessReconciler
    participant D as EngineDispatcher
    participant G as LocalEngineAdapter

    R->>S: create（Idempotency-Key, action, engine, parameters）
    Note over S: 在 per-(tableId,action) 互斥锁内执行：<br/>幂等重放查询 → 单活跃校验 → 冻结参数
    S->>L: create(v0)：序列化为 YAML → 索引快照预计算
    L->>DB: INSERT Base64(YAML)（持久化边界：此前任何失败均无副作用）
    L-->>S: 返回 v1（phase=PENDING，attempt 的 submitState=CREATED）
    L->>E: 投递 AFTER_CREATED 事件（不阻塞写阶段）
    E->>K: schedule(Reconciler)（同键请求被合并）

    Note over K,C: 周期性调度，每轮至多一个逻辑步骤
    K->>C: 第1轮 invoke：CAS 将 submitState CREATED → DISPATCHING
    K->>C: 第2轮 invoke：状态为 DISPATCHING，调用 D.submit(key, hash, payload)
    D->>G: 提交命令（同键并发去重；线程池队列满返回权威 REJECTED）
    G-->>D: 返回 ACK（携带 externalId）
    D-->>C: 异步回调（先校验 attempt 身份，未轮换才继续）
    C->>L: CAS → SUBMITTED（记录 externalId、submittedAt，版本 +1）

    K->>C: 后续轮 invoke：SUBMITTED → 调用 D.observe(externalId)
    G-->>D: 返回 KNOWN(RUNNING)
    C->>L: CAS → RUNNING（记录 startedAt；相位未变化时跳过写操作）
    K->>C: 再后续轮 invoke：RUNNING → observe
    G-->>D: 返回 KNOWN(SUCCESS)
    C->>L: CAS → SUCCESS + attempt.finishedAt + 资源级 finishedAt
    C->>D: release(externalId)（终态结果已持久化后释放执行句柄）
    C->>K: 抛出 TerminalState → 调度登记表移除该条目
```

### 图 6：CANCEL 完整调用时序（三种分支）

```mermaid
sequenceDiagram
    participant U as 运维/客户端
    participant S as REST 层
    participant L as 串行写线程
    participant K as DefaultScheduler
    participant C as ProcessReconciler
    participant D as EngineDispatcher
    participant G as LocalEngineAdapter

    alt 分支一：从未派发（submitState=CREATED）
        U->>S: PATCH {desiredState: CANCEL}
        S->>L: CAS desired=CANCEL（nextReconcileAt=now）
        K->>C: invoke：PENDING 且 submitState=CREATED
        Note over C: 可证明提交从未发送：<br/>不发起任何引擎请求
        C->>L: CAS → CANCELED + 两级 finishedAt
        C->>K: TerminalState（登记表移除）
    else 分支二：已派发执行中（SUBMITTED/RUNNING）
        U->>S: PATCH {desiredState: CANCEL}
        S->>L: CAS desired=CANCEL + phase → CANCELING
        K->>C: invoke：CANCELING → 调用 D.cancel(externalId)
        G-->>D: 返回 ACCEPTED（协作式取消标记）
        C->>L: CAS attempt submitState → CANCEL_REQUESTED<br/>（后续轮次不再重复 cancel，改为 observe）
        K->>C: invoke：CANCEL_REQUESTED → 调用 D.observe(externalId)
        G-->>D: 返回 KNOWN(CANCELED)（动作检测到取消标记后退出）
        C->>L: CAS → CANCELED + finishedAt
        C->>D: release(externalId)
        C->>K: TerminalState
    else 分支三：提交结果未知（submitState=UNKNOWN/CONFLICT）
        U->>S: POST /submission-resolutions（携带新 Idempotency-Key、<br/>submissionKey、requestHash、必填 reason）
        alt 裁决为 ACK（携带 externalId）
            S->>L: CAS → CANCELING（externalId 与审计记录同一次 CAS 写入）
        else 裁决为 NOT_FOUND（禁止携带 externalId）
            S->>L: CAS → CANCELED + finishedAt（审计记录同一次 CAS 写入）
        end
    end
    Note over C: 所有引擎回调先按 attempt 的 submissionKey<br/>校验身份；desired=CANCEL 时 FAILED 即最终态
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
       blobStore.insert(amoro_process, name, Base64(YAML))   # ← 持久化边界：此后失败进入结果未知处理
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
      attempt 已存在且 submitState=CREATED → 先 CAS submitState→DISPATCHING（提交前先持久化：崩溃重启后进入裁决路径而非重复提交）
      attempt=DISPATCHING → dispatcher.submit(key, requestHash, payload)   # 异步派发，同键并发请求去重为一次
                      └ ACCEPTED 回调: CAS→SUBMITTED（记 externalId, submittedAt）
                        REJECTED 回调: CAS→FAILED(终态判定)+failure
                        UNKNOWN/CONFLICT 回调: CAS submitState→UNKNOWN/CONFLICT 并停止自动派发——
                          不盲目重复提交同一 submissionKey，等待人工裁决（POST /submission-resolutions）
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
  desired=CANCEL 路径:
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
| 引擎命令超时 | dispatcher 强制完成：submit→UNKNOWN（不盲目重复提交），其余→UNAVAILABLE |
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
