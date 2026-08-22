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

# Amoro Process 控制面 Spec — 方案 C（AppManager 式声明式状态机，可移植边界）

> **历史设计输入，非当前 v2 契约。** 本文记录方案 C 的阶段性推演，已被
> `amoro-ams-v2-process-spec.md` 取代。本文中的 Java 8 / 无 Spring、REST 契约不变、
> 零新表、`CANCEL_CONFIRMED` 等约束不得作为 `/api/ams/v2` 或 `amoro-ams-v2`
> 实现依据；若与权威 Process Spec 冲突，以后者为准。
>
> 2026-08-22。上游决策链：采访确认意图（见 `process-appmanager-redesign-options.md` §0）→ 三变体对比 → **用户已选方案 C** + 确认资产默认假设。
> 参考：SSP AppManager（control-plane 技能家族）。语义复刻，装配手工（Java 8 / 无 Spring）。

---

## 1. 目标与非目标

**目标**：用 spec/status 双状态 + DelayQueue 调度器 + 每资源一个 Controller + 幂等 Transition 单步状态机，重写 External Process 域的调度与控制层；调度层目标 ~10±2 类。

**非目标**：OptimizingQueue 'AMORO' 域（不动，mapper 无条件写路径保留）；Optimizer 生命周期；多节点（无 fencing/ownership/admission-lock）；前端改造（REST 契约不变）。

**Success 标准**（来自采访）：主线 = `Controller.invoke() → Transitions.forState(desired) → Transition 单步`，一个 Process 生命周期顺着 Transition 链可读完；新增 action 类型只写 SPI + Transition；UNKNOWN/取消是状态机内普通迁移。

---

## 2. 双状态映射（零新表）

| AppManager 概念 | Amoro 对应 | 写者 |
|---|---|---|
| spec.state（期望态） | `desired_state`（RUN/CANCEL）+ INSERT 时冻结的 `process_parameters` | 只许 REST/表事件/触发器 |
| status.state（实际态） | `status`（十态）+ `table_process_attempt.submit_state` | 只许状态机（Transition 写回） |
| resourceVersion | `state_version` | CAS 前置校验 |
| transitioning 标记 | 不需要——UNCOMPLETED 结果 + 下一轮排期即等价物 | — |

写权限隔离是硬规则：`ProcessService` 门面上的 register/cancel 只做 INSERT / CAS desired_state，**永不直接写 status**。

---

## 3. 模块结构（方案 C 的可移植边界）

```
amoro-common  org.apache.amoro.process.control          ← 零 AMS 依赖（纯 JDK + 本包类型）
├── Controller            void invoke()
├── Scheduler             schedule(Controller); postStart(); shutdown(timeout)
├── DefaultScheduler      DelayQueue + N SchedulerWorker（daemon）
├── ScheduledController   implements Delayed；processId、nextDesiredTime、backOffAttempts、superseded 标记
├── TerminalState         单例异常，writableStackTrace=false
├── TransitionResult      四值：COMPLETED / FINISHED / UNCOMPLETED / FAILED
├── ProcessTransition     TransitionResult tryTransition(TransitionContext)
├── TransitionContext     仓储端口接口（快照/CAS/attempt 生命周期/引擎查找——只暴露 amoro-common 类型）
└── Clock / BackoffPolicy {3000,3000,5000,8000,13000,21000,34000,55000}ms + ≤250ms 抖动

amoro-ams  org.apache.amoro.server.process
├── control/MyBatisTransitionContext   TransitionContext 实现（委托简化版 ProcessStateRepository）
├── control/ProcessController          每资源一个：读快照→缺失/终态抛 TerminalState→Transitions.forState(desired)
├── control/Transitions                (desired_state) → ToRunTransition / ToCancelTransition 工厂
├── control/ToRunTransition            内部按 status switch（§6 表）
├── control/ToCancelTransition         内部按 status switch（§7 表）
└── ProcessService（收缩为门面）        register = INSERT + schedule；cancelProcess = CAS desired_state + schedule
```

可移植性验收：`org.apache.amoro.process.control` 包 import 白名单 = JDK + amoro-common；Transition 实现只依赖 ExecuteEngine SPI + TransitionContext。未来搬入 SB3 宿主时仅替换 Context 实现与装配。

---

## 4. 调度器（复刻 + 三处明确改良）

复刻语义：

- DelayQueue 无界（元素数 = 活跃资源数）；`take()` 独占 → 执行完计算 nextDesiredTime 才重新入队 → **同一资源天然串行，无并发重入**。
- invoke 三分支：成功 → `backOffAttempts=0`，`next = now + delay(Transition 决定，缺省 poll-interval)`；TerminalState → 不再入队，**永久停止**；其他 Throwable → 退避序列 + 抖动，**无限重试**（调度层永不放弃资源）。
- 线程 daemon、命名 `process-scheduler-worker-%d`。

相对参考架构的**改良偏差**（记录于 §12 保真台账）：

1. **优雅停机**（参考架构无）：`shutdown()` 停止接受 schedule → 停 worker take → 有限等待在途 invoke（默认 10s）→ 超时放行（daemon 随 JVM 退出；在途写依赖重启恢复收敛）。接入现有 `ProcessService.dispose` 链，位置在引擎 close 之前。
2. **worker 不静默丢弃控制器**（参考架构缺陷：catch Throwable 后永久丢弃，资源状态机静默停摆）：worker 捕获异常仅记日志并按退避重新入队。
3. **同 pid 重复 schedule 去重**（参考架构需外部 KeyedLock）：调度器维护 `ConcurrentHashMap<Long, ScheduledController>`；对同 pid 再次 schedule 时将旧包装标记 `superseded=true`，worker take 到 superseded 包装直接丢弃不 invoke。

---

## 5. Controller 模板流程（每轮 reconcile）

1. `snapshot = context.snapshot(processId)`；行缺失或 status 已终态（SUCCESS/CANCELED/KILLED/CLOSED/FAILED 且预算耗尽或 desired=CANCEL）→ `throw TerminalState.INSTANCE`。
2. `Transitions.forState(snapshot.desiredState)` 取 Transition。
3. `result = transition.tryTransition(context)`——内部至多**一次引擎调用 + 一至两次 CAS 写回**。
4. 按 TransitionResult 重新入队：
   - `UNCOMPLETED` → next = transition 建议延迟（缺省 poll-interval；UNAVAILABLE 退避上限 12×interval；重试等待 30s）
   - `COMPLETED` → next = 0（立即下一轮，如 attempt 刚 ACK 需要首观察）
   - `FINISHED` → 终态已落库 + afterComplete（仅 CAS 胜者调用）→ 不再入队
   - `FAILED` → 落 FAILED（含脱敏 last_error）；有预算则下轮走重试迁移，否则 FINISHED 语义
5. CONFLICT 停推特例：不置 FAILED，保持行现状 + 每轮 12×interval 唤醒告警（人工修复 attempt 后自动续走）。

每轮全量读快照重判（level-triggered）：任何一轮丢失/崩溃，重启后仍收敛。

---

## 6. ToRunTransition 迁移表（desired=RUN）

| status | attempt 状态 | 动作（单步） | 结果 / next |
|---|---|---|---|
| PENDING/UNKNOWN | 无或 CREATED | CAS attempt CREATED→DISPATCHING → `engine.submit(冻结 payload, key+hash)` | ACK→attempt=ACK + CAS→SUBMITTED：UNCOMPLETED / next=0；REJECTED→attempt=REJECTED + CAS→FAILED：见 FAILED 行；UNKNOWN→attempt=UNKNOWN：UNCOMPLETED / 退避；CONFLICT→停推特例 |
| PENDING/UNKNOWN | DISPATCHING/UNKNOWN | `engine.resolveSubmission(key)`（resolve 折叠于此） | ACK→同上；NOT_FOUND(幂等引擎)→attempt 回 DISPATCHING 重投同 key；UNAVAILABLE→退避；UNSUPPORTED→12×interval 低频重查 + 告警，**禁止盲重提**；CONFLICT→停推 |
| PENDING/UNKNOWN | ACK 但 status 未推进 | CAS→SUBMITTED 补偿 | UNCOMPLETED / next=0 |
| SUBMITTED/RUNNING | ACK(qid) | `engine.observeProcess(qid)` | KNOWN 运行中→必要时 CAS→RUNNING：UNCOMPLETED / next=poll-interval；KNOWN 终态→CAS 终态 + afterComplete：FINISHED；NOT_FOUND→权威确认→CAS FAILED（消耗预算）：走 FAILED 行；UNAVAILABLE→退避上限 12×interval + 告警，**永不判失败**：UNCOMPLETED |
| FAILED | —（desired=RUN 且 retry<3） | CAS FAILED→PENDING（RETRY_REQUESTED）：retry+1、清 qid、新建 CREATED attempt | UNCOMPLETED / next=30s |
| FAILED | 预算耗尽 | 终态 | FINISHED |
| CANCELING | — | desired=RUN 不复活取消（罕见竞态：人工改回 desired） | 保持 + 告警 / 12×interval |
| 任一终态 | — | — | TerminalState |

重试预算唯一来源 `PROCESS_MAX_RETRY_NUMBER=3`：只有 REJECTED 与权威 NOT_FOUND/远端明确 FAILED 消耗；UNAVAILABLE/UNKNOWN/队列拒绝不消耗。

---

## 7. ToCancelTransition 迁移表（desired=CANCEL）

| status | 前置 | 动作 | 结果 |
|---|---|---|---|
| 任一终态 | — | — | TerminalState（untrack） |
| FAILED（任意预算） | — | 只保持 FAILED（desired=CANCEL 已在 CAS 层挡住 retry 复活），不 kill | FINISHED |
| PENDING/UNKNOWN | attempt 未派发（无 qid） | CAS→CANCELED | FINISHED |
| PENDING/UNKNOWN | attempt DISPATCHING/UNKNOWN | 先走 resolve 取 qid；确认无作业→CAS→CANCELED | FINISHED / UNCOMPLETED |
| SUBMITTED/RUNNING/CANCELING | 有 qid | 首轮同 CAS：→CANCELING（desired 已=CANCEL）→ `engine.tryCancel(qid)`（幂等）→ `observeProcess` | CANCELED/KILLED→CANCEL_CONFIRMED→终态：FINISHED；SUCCESS/FAILED/CLOSED→按远端真实终态落库（FAILED 不再 retry）：FINISHED；UNAVAILABLE→保持 CANCELING 退避重 kill：UNCOMPLETED |

取消意图持久化于 `desired_state=CANCEL`：跨重启存活；store 层 validTransition + retry CAS 谓词双防线拒绝取消后复活（继承 PR2c 语义）。

---

## 8. 重启恢复（启动重放，对应 postStart）

- 独立恢复线程，不阻塞就绪：扫描五活动状态行（UNKNOWN/PENDING/SUBMITTED/RUNNING/CANCELING），按 §6/§7 表由快照直接续走——无 attempt 建 CREATED、DISPATCHING/UNKNOWN 走 resolve、SUBMITTED 无 attempt 补 ACK、CANCELING 继续 kill/observe。
- **错峰**：`next = now + hash(processId) mod 30s`，替代令牌桶（单节点足够；调度本身只触 DB 点查，压力可控）。
- 单行恢复失败：保留原状态 + ERROR 日志 + 10min 低频重试，**不写 FAILED**。

## 9. 持久化（继承 + 简化）

**保留（从 dev 分支搬运）**：`table_process_attempt` 全表（submission_key 唯一 / payload 冻结 / submit_state / state_version）；`table_process` 新列 `state_version`、`desired_state`、生成列唯一活跃索引；`SubmitState` 枚举；`validTransition` PR2c 决策表；`ProcessEvent.CANCEL_CONFIRMED`。

**删除（单节点简化）**：`process_controller_ownership` 表、`process_admission_lock` 表、owner 三列（scope/node/epoch）、全部 EXISTS 谓词、`AmsAssignService` fence-before-publish、`TableRemovalReason` ownership-revoke 语义、`ProcessOwnershipManager`。

**`ProcessStateRepository` 简化保留**：`snapshot / casProcess(带 state_version 谓词) / createAttempt / casAttempt / insertProcessWithinCap / desiredState CAS`——去掉 fence/acquire/claim。准入 = JVM 内计数（tracked-max）+ DB 唯一活跃索引兜底。

**清理**：deleteExpired/deleteBefore 随状态机迁移至新 dispose 链（含 UNKNOWN 态排除条件，继承 P1-5 修复）。

## 10. 配置（AmoroManagementConf，三键）

| key | 默认 | 含义 |
|---|---|---|
| `process.control.poll-interval` | 5s | 常规观察间隔；UNAVAILABLE 退避上限 = 12× |
| `process.control.workers` | 8 | SchedulerWorker 数（引擎调用在 worker 内同步执行，远端 HTTP 超时须 < workers×吞吐预算） |
| `process.control.tracked-max` | 10000 | 新建准入上限（恢复不受限） |

（dev 分支 `process.reconcile.*` 三键从未发布，直接换名，无迁移负担。）

## 11. 可观测性（核心四项，MetricManager）

`process_tracked{status}`、`process_command_latency_ms{command,outcome}`、`process_unavailable_total{engine}`、`process_recovery_backlog{status}`。六条告警压缩为两条 WARN：unresolved（UNKNOWN/CONFLICT attempt）持续 5min；CANCELING 滞留 >600s。

## 12. 保真台账（相对 AppManager 参考的偏差）

| 偏差 | 理由 |
|---|---|
| + 优雅停机 / worker 不丢弃 / superseded 去重 | 修复参考架构已知缺陷（§4） |
| 每轮读 DB 而非内存实体缓存 | Amoro 既有 MyBatis 事实源模式；每唤醒 ≈2 次主键点查，fork 规模可接受；缓存留作后续项 |
| transitioning 标记省略 | UNCOMPLETED + 下轮排期语义等价 |
| spec 写者含内部触发器（ActionCoordinator） | Amoro 触发是内部行为，非 REST-only；写权限隔离规则不变 |
| 退避不区分异常类型 | 与参考一致（无限重试）；业务失败预算由 Transition 分类管理 |

## 13. PR 拆分与验收

| PR | 内容 | 验收 |
|---|---|---|
| A0 | 新分支（基于 jira/process-dev）+ 资产搬运：amoro-common SPI 9 文件、DDL（去 ownership/admission 两表）、简化版 Repository+Mapper+Meta；删除旧 `executor/` 包 | 编译绿；现有测试不回归 |
| A1 | `amoro-common/process/control` 调度器（Controller/Scheduler/ScheduledController/Worker/TerminalState/Backoff） | 单测：串行性、退避序列、TerminalState、superseded 去重、优雅停机 |
| A2 | Transition 体系 + ToRun 主线（submit→observe→terminal、重试、UNKNOWN/resolve 折叠、CONFLICT 停推）；ProcessService 门面重接 | 生命周期端到端测试（替代旧 TestTableProcessExecutor） |
| A3 | ToCancel + 取消竞态不变量测试（重写 TestCancelRace 八场景 + TestAsyncCancelStateMachine） | 八场景全绿 |
| A4 | 启动重放/错峰恢复、清理迁移、四指标两告警、配置键、文档 | 恢复矩阵测试 + 全量回归对齐基线（9 类既有失败不变） |

## 14. 不变量清单（测试重写目标）

1. DB 是事实源：五活动状态全可恢复；内存可全丢。
2. 同表跨 Action 至多一条活动行（DB 唯一索引）。
3. 同一 process 同时刻至多一个 Controller 在 invoke（DelayQueue 串行 + superseded）。
4. attempt 先落库才允许调引擎；submit 结果先持久化再进下一轮。
5. 同 submissionKey 永远同 hash 同 payload；不符即 CONFLICT 停推。
6. UNKNOWN≠失败：UNAVAILABLE/UNSUPPORTED/队列拒绝永不消耗 3 次预算。
7. `desired_state=CANCEL` 后不可能复活（validTransition + retry CAS 谓词双层）。
8. 只有终态 CAS 胜者调 afterComplete；stale 写零副作用（state_version）。
9. CANCELED 后不得再 SUBMITTED；取消后 FAILED 不得业务重试；无 qid 不得留远端孤儿。
10. 优化域（execution_engine='AMORO'）不走新控制面：恢复扫描跳过、不受 tracked-max 限制。
11. 调度层对任意单轮崩溃免疫（kill -9 任一时刻重启可收敛）。
12. REST 契约不变：GET .../processes 字段与分页行为与现版一致。

## 15. 开工前置

- JIRA issue 号（现 `[JSPT-XXXX]` 占位）。
- 远端 Spark submissionKey 幂等契约（X1）状态不变：未落地前 rejectable-codes 默认空集 = 所有非零码 UNKNOWN，系统行为安全（不重投、告警人工介入）。
- 构建环境：`JAVA_HOME` 指向 JDK 11（GJF 1.7 约束，见既有基线记录）。
