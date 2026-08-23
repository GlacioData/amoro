# Amoro AMS v2 Process Control Plane Architecture

## 1. Scope and delivery boundary

This module delivers a single-node, Spring Boot 3 / Java 17 Process control plane. It proves the
complete orchestration path with explicitly enabled simulators:

`create -> durable state -> schedule -> submit -> observe/cancel -> terminal -> release -> TTL`

It does **not** connect, migrate, or implement any Iceberg or Paimon Action. In particular, this
delivery contains no Iceberg `expire-snapshots` / `clean-orphans`, no Paimon `sync-table-meta`, no
real table loading, no metadata or file mutation, and no real Spark submission or remote Spark
protocol. Those capabilities require a separate Action specification and implementation.

The default runtime selects `ProviderMode.REAL`. Because this specification ships no real
providers, the default Engine registry and Action catalog are empty. Dummy Local and Remote
providers are selected only when `amoro.process.simulation.enabled=true`; every dummy terminal
summary contains `simulated=true`.

## 2. Architectural principles

- The database is the source of truth. Memory contains rebuildable projections only.
- Mutations are durable-first: projection publication and listener delivery happen after the DB
  write is confirmed.
- A Process is reconciled from desired and actual state. An event only wakes the reconciler; it is
  never the sole correctness path.
- Every state mutation uses `resourceVersion` compare-and-set semantics.
- The unique active scope is `(tableId, action)`. Manual and scheduled creation share one admission
  service and therefore one constraint.
- A create/list request resolves one atomic `TableIdentity(tableId, tableFormat)` snapshot. The
  persisted `tableFormat` is never re-read during dispatch, so a Process cannot mix identities from
  different catalog observations.
- Engine calls are asynchronous and bounded. Scheduler workers never wait on Action or remote I/O.
- An unknown submission side effect is never treated as “nothing happened” and is never blindly
  submitted again.
- A durable terminal result precedes execution-handle release; successful release precedes TTL
  deletion.
- Provider SPI types are format-neutral. Framework packages do not depend on Iceberg, Paimon,
  Spark, or v1 AMS server classes.

## 3. System context

```mermaid
flowchart LR
    Client["Operator / v2 Client"]
    API["/api/ams/v2 REST"]
    Scanner["Scheduled Dummy Scanner"]
    Create["ProcessCreationService"]
    Command["ProcessCommandService"]
    Repo["Process Repository"]
    DB[("amoro_process_v2")]
    Index["Immutable Aggregate Index"]
    Scheduler["Level-triggered Scheduler"]
    Reconciler["ProcessReconciler"]
    EngineSPI["ProcessEnginePort SPI"]
    LocalSim["Local Simulator"]
    RemoteSim["Remote Simulator\n(no HTTP / no Spark)"]
    Release["ExecutionHandleReaper"]
    TTL["ProcessTtlCleaner"]

    Client --> API
    API --> Create
    API --> Command
    Scanner --> Create
    Create --> Repo
    Command --> Repo
    Repo --> DB
    DB -. "postStart replay" .-> Index
    Repo --> Index
    Index --> Scheduler
    Scheduler --> Reconciler
    Reconciler --> EngineSPI
    EngineSPI --> LocalSim
    EngineSPI --> RemoteSim
    Index --> Release
    Release --> EngineSPI
    Index --> TTL
    TTL --> Repo
```

The existing `amoro-ams` service remains outside this diagram. There is no dual write, table join,
v1 Process proxy, or shared execution state in this delivery.

## 4. Component architecture

```mermaid
flowchart TB
    subgraph HTTP["HTTP boundary"]
      Controller["ProcessApiController"]
      Errors["ApiExceptionHandler"]
      Rest["ProcessRestSupport"]
    end

    subgraph Domain["Process domain"]
      Creation["ProcessCreationService"]
      Commands["ProcessCommandService"]
      Manual["ManualResolutionTransition"]
      Cancel["ToCancelTransition"]
      Reconcile["ProcessReconciler"]
      Apply["ProcessResultApplier"]
      Invariant["ProcessInvariantValidator"]
      Aggregate["ProcessIndexProjection"]
      ReleaseIndex["ExecutionHandleReleaseIndex"]
    end

    subgraph Framework["Generic control-plane framework"]
      Repo["RepositoryFacade"]
      Persistence["InMemoryPersistence"]
      Lane["BlobStoreActor\none mutation lane"]
      Mapper["MyBatisBlobStore / Mapper"]
      Scheduler["DefaultScheduler"]
      Listener["ListenerDispatcher"]
    end

    subgraph Extension["Explicit extension boundary"]
      ActionFactory["ProcessActionPluginFactory SPI"]
      ActionRegistry["ProcessActionRegistry"]
      EngineFactory["ProcessEngineFactory SPI"]
      EngineRegistry["ProcessEngineRegistry"]
      Dispatcher["ProcessEngineDispatcher"]
      EnginePort["ProcessEnginePort"]
      LocalAction["LocalActionFactory / LocalAction seam"]
    end

    Controller --> Rest
    Controller -.-> Errors
    Rest --> Creation
    Rest --> Commands
    Commands --> Manual
    Commands --> Cancel
    Creation --> Repo
    Commands --> Repo
    Reconcile --> Repo
    Reconcile --> Dispatcher
    Dispatcher --> EnginePort
    EngineRegistry --> Dispatcher
    EngineFactory --> EngineRegistry
    ActionFactory --> ActionRegistry
    ActionRegistry --> Rest
    EnginePort -. "future native implementation" .-> LocalAction
    Repo --> Persistence --> Lane --> Mapper
    Lane --> Invariant
    Lane --> Aggregate
    Lane --> ReleaseIndex
    Persistence --> Listener --> Scheduler --> Reconcile
    Apply --> Repo
```

### Responsibilities

| Component | Responsibility | Explicit non-responsibility |
|---|---|---|
| `ProcessCreationService` | Idempotency, single-active admission, immutable spec construction | Does not call an Engine |
| `ProcessCommandService` | Bounded CAS for cancel and manual resolution | Does not duplicate transition logic |
| `ProcessReconciler` | One level-triggered state-machine step per invocation | Does not block for Engine futures |
| `ProcessResultApplier` | Identity-checked, late-result-safe durable callback application | Does not release handles |
| `ProcessIndexProjection` | One immutable snapshot for body, active, idempotency, read, expiry views | Is not a fact store |
| `ExecutionHandleReaper` | Sole caller of `ProcessEnginePort.release` | Does not change business outcome |
| `ProcessTtlCleaner` | Bounded deletion of expired, final, cleanup-complete Process rows | Does not repair terminal state |
| Action / Engine SPI | Selects behavior by canonical `(tableFormat, action, engine)` and provider mode | Does not imply a format Action exists |

## 5. Durable resource and indexes

`amoro_process_v2` stores one Base64-encoded YAML document per Process. The document contains an
immutable execution specification and a mutable status. `resourceVersion` starts at 1 after the
first durable insert and increases for each successful mutation.

```mermaid
classDiagram
    class ProcessResource {
      +String name
      +long resourceVersion
      +ProcessSpec spec
      +ProcessStatus status
    }
    class ProcessSpec {
      +TableRef table
      +String action
      +String executionEngine
      +String triggerSource
      +String desiredState
      +RequestIdentity request
      +Map parameters
      +RetryPolicy retryPolicy
    }
    class TableRef {
      +String catalog
      +String database
      +String table
      +String tableId
      +String tableFormat
    }
    class ProcessStatus {
      +String phase
      +int retryNumber
      +ProcessAttempt attempt
      +List attemptHistory
      +EngineBackoff engineBackoffAttempts
      +List conditions
      +Summary summary
      +String nextReconcileAt
      +String finishedAt
    }
    class ProcessAttempt {
      +int dispatchGeneration
      +String submissionKey
      +String requestHash
      +String submitState
      +String externalId
      +List submissionHistory
      +ManualResolutions manualResolutions
      +String finishedAt
    }
    ProcessResource --> ProcessSpec
    ProcessSpec --> TableRef
    ProcessResource --> ProcessStatus
    ProcessStatus --> ProcessAttempt
```

The aggregate projection publishes all API/admission views with one atomic reference:

- `resourcesByName`: canonical Process body;
- `activeByTableAction`: unique non-final holder of `(tableId, action)`;
- `idempotencyByKey`: create replay identity;
- `activeOrder`: stable rescheduler cursor ordered by `(createdAt, name)`;
- `readViews`: persistent rank trees ordered by `(createdAt DESC, name DESC)`;
- `expiryOrder`: final resources ordered by `(finishedAt, name)`.

Persistent AVL maps/rank trees provide structural sharing. Point updates are `O(log n)`, rank pages
are `O(log n + pageSize)`, and an old snapshot remains internally consistent while a new snapshot
is prepared.

`ExecutionHandleReleaseIndex` is a separate reconstructable cleanup projection. It deduplicates by
`(executionEngine, externalId)`, orders retry deadlines, and tracks Process ownership for the TTL
gate. A restart deliberately rebuilds terminal handles and repeats idempotent release.

## 6. Mutation and publication sequence

```mermaid
sequenceDiagram
    autonumber
    participant Caller
    participant Repo as RepositoryFacade
    participant Lane as Process mutation lane
    participant Inv as Invariant + projection prepare
    participant DB as amoro_process_v2
    participant Cache as Canonical cache + indexes
    participant Events as ListenerDispatcher

    Caller->>Repo: create / expected-version modify / delete
    Repo->>Lane: enqueue bounded command
    Lane->>Lane: read latest and derive detached candidate
    Lane->>Inv: validate and prepare immutable deltas
    Inv-->>Lane: prepared updates
    Lane->>DB: durable INSERT / UPDATE / DELETE
    alt DB outcome confirmed
      DB-->>Lane: success
      Lane->>Cache: publish body and prepared projections
      Lane->>Events: hand off after-commit event
      Lane-->>Caller: complete result
    else outcome cannot be resolved
      Lane->>Lane: fence resource name/scope
      Lane-->>Caller: PERSISTENCE_OUTCOME_UNKNOWN
    else DB write refuted
      Lane-->>Caller: failure; memory remains unchanged
    end
```

Projection preparation happens before DB I/O, but publication happens only after durable success.
A projection prepare error therefore produces no DB or memory change.

## 7. Creation paths and single-active admission

### Manual creation

```mermaid
sequenceDiagram
    participant Client
    participant REST
    participant Table as TableCatalogPort
    participant Action as Selected dummy Action plugin
    participant Create as ProcessCreationService
    participant Index as Aggregate snapshot
    participant Repo

    Client->>REST: POST table/processes + Idempotency-Key
    REST->>Table: resolve simulated table identity
    REST->>Action: validate and freeze dummy parameters
    REST->>Create: canonical ProcessCreateIntent
    Create->>Create: acquire scope lease(tableId, action)
    Create->>Index: check idempotency and active holder
    alt identical idempotency record exists
      Create-->>REST: original resource, replay=true
    else another active Process exists
      Create-->>REST: 409 ACTIVE_PROCESS_EXISTS
    else admitted
      Create->>Repo: durable create(PENDING)
      Repo-->>Create: resourceVersion=1
      Create-->>REST: new resource
    end
```

### Scheduled creation

The scheduled dummy scanner reads `ManagedTablePort` with a stable cursor and asks the selected
dummy Action plugin whether a logical fire time is eligible. It then creates the same canonical
`ProcessCreateIntent` through the same singleton `ProcessCreationService`. It owns no second lock or
write path, so a concurrent REST request and scan cannot both create an active Process for the same
scope.

No real table condition is implemented. `SimulatedManagedTablePort` and dummy evaluation facts are
available only with simulation explicitly enabled; future real facts/probes plug into the neutral
ports.

## 8. Process state machine

Fixed terminal phases are `SUCCESS`, `CANCELED`, `KILLED`, and `CLOSED`. `FAILED` is final only when
the desired state is `CANCEL`, the action retry budget is exhausted, or retry disposition is
`FINAL`. Otherwise it is an active retry decision point.

```mermaid
stateDiagram-v2
    [*] --> PENDING: durable create
    UNKNOWN --> PENDING: repair / authoritative NOT_FOUND
    PENDING --> SUBMITTED: submit or resolve ACK
    PENDING --> FAILED: rejected / submission budget exhausted
    SUBMITTED --> RUNNING: observation
    SUBMITTED --> SUCCESS: terminal observation/manual result
    SUBMITTED --> FAILED: failed observation/manual result
    SUBMITTED --> CANCELING: desired=CANCEL
    RUNNING --> SUCCESS: terminal observation/manual result
    RUNNING --> FAILED: failed observation/manual result
    RUNNING --> CANCELING: desired=CANCEL
    FAILED --> PENDING: retry allowed and budget remains
    CANCELING --> CANCELED: observation/cancel terminal/manual result
    CANCELING --> SUCCESS: action won cancel race
    CANCELING --> FAILED: terminal failure
    SUCCESS --> [*]
    CANCELED --> [*]
    KILLED --> [*]
    CLOSED --> [*]
    FAILED --> [*]: finality predicate true
```

`SubmissionUnresolved` freezes generation rollover until an authoritative ACK or NOT_FOUND.
`ExecutionUnresolved` freezes submit/resolve/observe/cancel for the current identity and requires an
attempt-bound manual result. Unavailability only advances the persisted operation-specific backoff;
it does not consume action retry budget.

## 9. Reconciliation and Engine command sequence

```mermaid
sequenceDiagram
    autonumber
    participant Event as Listener / Rescheduler
    participant S as DefaultScheduler
    participant R as ProcessReconciler
    participant Repo
    participant D as Engine Dispatcher
    participant E as Selected Simulator
    participant Retry as Result persistence retry lane

    Event->>S: schedule ControllerKey(process, name)
    S->>R: invoke (same key never overlaps)
    R->>Repo: read latest Process
    R->>Repo: CAS CREATED -> DISPATCHING
    Note over R,Repo: durable before any engine side effect
    R->>D: async submit(key, requestHash, payload)
    D->>E: simulator submit
    E-->>D: ACK / UNKNOWN / REJECTED / UNAVAILABLE
    D-->>Retry: completed flight, identity remains claimed
    Retry->>Repo: identity-checked ProcessResultApplier CAS
    alt result durably handled or proved stale
      Retry->>D: markDurablyHandled
      Retry->>S: wake same controller
    else persistence temporarily unavailable
      Retry->>Retry: bounded fair retry; flight remains claimed
    end
    R->>D: later async observe/cancel/resolve
```

Before dispatching an Engine side effect, the reconciler reserves bounded callback-persistence
capacity. If the lane is full, it waits without calling the Engine. This avoids the unrecoverable
case where a completed side effect has no in-process owner capable of persisting its result.

### Crash and unknown-submission recovery

- `CREATED` or proven `UNAVAILABLE`: persist `DISPATCHING`, then submit the same generation.
- A later invocation or restart seeing `DISPATCHING` never blindly submits again; it resolves the
  existing `submissionKey/requestHash`.
- ACK persists `externalId` and proceeds to observation.
- Authoritative NOT_FOUND may open the next bounded submission generation.
- UNKNOWN/CONFLICT keeps the same generation and sets `SubmissionUnresolved`.
- LOST sets `ExecutionUnresolved`; only an exact manual command can finish the attempt.

## 10. Cancel and manual resolution

`PATCH desiredState=CANCEL` persists monotonic intent only. The API thread never calls an Engine.
The reconciler then resolves an uncertain submission, transitions an acknowledged execution to
`CANCELING`, and asynchronously cancels or observes it. An action that finishes before cancellation
may legitimately end in `SUCCESS`.

Manual submission/execution commands include `Idempotency-Key`, `submissionKey`, `requestHash`, and
reason. `ManualResolutionTransition` is the single pure derivation implementation. The command
service re-reads and retries bounded CAS conflicts; an identical winner becomes a replay. Current
and archived identities are checked, so a late command cannot mutate a newer attempt.

Engine callbacks use the same identity and audit guards. Once a manual result closes an attempt,
late submit, resolve, observe, or cancel results are treated as durably handled no-ops and cannot
overwrite the operator conclusion.

## 11. Release and TTL sequence

```mermaid
sequenceDiagram
    autonumber
    participant Apply as Result / manual transition
    participant Repo
    participant RIdx as Release index
    participant Reaper
    participant Engine
    participant EIdx as Expiry index
    participant TTL

    Apply->>Repo: CAS terminal or closed FAILED attempt
    Repo-->>RIdx: after DB commit, add externalId
    Repo-->>EIdx: publish final Process eligibility
    Reaper->>RIdx: claim bounded due handles
    Reaper->>Engine: idempotent release(externalId)
    alt release succeeds / unknown handle no-op
      Reaper->>RIdx: remove pending handle
    else timeout/failure
      Reaper->>RIdx: reschedule with bounded backoff
    end
    TTL->>EIdx: stable cursor page before cutoff
    TTL->>RIdx: pending for this Process?
    alt no pending handle and version/finality still match
      TTL->>Repo: expected-version durable delete
    else pending or raced
      TTL-->>TTL: stop at inclusive cursor and retry later
    end
```

Delete cannot synthesize a release entry: all handles must be reconstructable while the Process row
still exists. The same mutation lane runs the durable deletion hook and unschedules the controller,
so a failed delete never unschedules an active Process and an old delete cannot kill a new same-name
controller.

## 12. Thread and queue ownership

| Thread / pool | Bound | Work allowed | Work forbidden |
|---|---:|---|---|
| HTTP request pool | Server-managed | DTO validation, snapshot reads, bounded repository calls | Engine calls, Action execution |
| `amoro-control-worker-*` | `amoro.control.scheduler.workers` | One short reconcile step | Blocking on futures or DB client I/O outside repository facade |
| `process-mutation-lane` | One thread, bounded mailbox | Ordered derive, DB write, projection publish | Engine/network calls |
| `amoro-listener-worker-*` | Bounded workers/queue/retries | Post-commit event delivery | Defining correctness by event delivery alone |
| `amoro-process-local-action-*` | Simulation worker count + bounded queue | Dummy Local execution only | Iceberg/Paimon table or Action calls |
| Engine timeout executor | One per selected Engine | Future completion guards | Business state mutation |
| Result persistence retry lane | One thread, bounded reservations | Fair retry of completed Engine result CAS | New Engine commands |
| Active rescheduler | One thread, bounded cursor/runtime | Repair missed listener wakeups | Full cache scan |
| Execution handle reaper | One thread, bounded due batch | Idempotent release only | Business outcome changes |
| Scheduled dummy trigger | One thread, bounded table pages | Dummy fact evaluation and shared creation service | Real table probing |
| TTL maintenance | One thread, bounded expiry page | Expected-version delete after release gate | TRUNCATE or full-cache traversal |

## 13. Provider SPI and deployment modes

`ServiceLoader` discovers `ProcessEngineFactory`, `ProcessActionPluginFactory`, and the Local Action
seam with an explicit classloader. Factory identity includes canonical name and `ProviderMode`.
Duplicate identities fail application startup. Validated identities are frozen before provider
construction; partial construction failure closes every adapter already created. Registry,
dispatcher, and lifecycle-aware adapters share one bounded shutdown budget.

```mermaid
flowchart LR
    Loader["Java ServiceLoader"] --> Factories["All discovered factories"]
    Factories --> Mode{"Selected ProviderMode"}
    Mode -->|REAL default| Empty["Empty registry in this Spec"]
    Mode -->|SIMULATED explicit| Local["local simulator"]
    Mode -->|SIMULATED explicit| Remote["remote-spark simulator"]
    Mode -->|SIMULATED explicit| Dummy["simulated / dummy-maintenance plugin"]
```

A future real provider implements the same ports in a separate delivery. Registering a wire name is
not sufficient: only the intersection of selected Action support and selected Engine names becomes
an admitted route.

## 14. REST surface

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/ams/v2/tables/{catalog}/{db}/{table}/processes` | Manual idempotent create |
| `GET` | `/api/ams/v2/processes/{name}` | Point query |
| `GET` | `/api/ams/v2/tables/{catalog}/{db}/{table}/processes` | Stable rank-paged list |
| `PATCH` | `/api/ams/v2/processes/{name}` | Monotonic cancel intent |
| `POST` | `/api/ams/v2/processes/{name}/submission-resolutions` | Attempt-bound submission result |
| `POST` | `/api/ams/v2/processes/{name}/execution-resolutions` | Attempt-bound execution result |

Unknown request fields and malformed bodies return `400`. API errors use the stable
`{code,message,timestamp,traceId}` shape. Persistence outcome unknown is distinct from ordinary
unavailability and fences the identity instead of inviting a new side-effecting request.

## 15. Startup, restart, and shutdown

Startup order:

1. validate `amoro.control.*` and `amoro.process.*` values;
2. initialize the dialect-specific schema;
3. create the Process domain, invariant validator, aggregate index, and release index;
4. discover providers and select REAL (default) or SIMULATED (explicit);
5. register the Process scheduling listener;
6. run `postStart`: reload DB rows, rebuild projections, and replay live Process wakeups;
7. start scheduler and bounded maintenance loops;
8. accept normal API traffic.

Shutdown first stops new trigger/repair/release/TTL rounds and waits a bounded interval for an
already-running round. It then stops scheduler reconciliation, closes Engine adapters and the
result-persistence retry lane, drains listener delivery, and finally drains mutation lanes. A
timeout interrupts the corresponding executor; every close path is idempotent so Spring's inferred
destroy callbacks also cover partial startup failures. Any durable `DISPATCHING`, active,
terminal-release, or expiry state left at shutdown is reconstructed from the DB on the next
startup.

## 16. Verification boundary

The release gate requires:

- JDK 17 compilation and the complete offline JUnit 5 suite;
- embedded Derby durable-store and restart tests;
- local Docker Testcontainers MySQL 5.7 tests for SQL semantics and the Process lifecycle;
- Local and Remote simulator contract/lifecycle tests, including cancel, UNKNOWN/LOST, restart,
  manual resolution, release, and TTL;
- concurrent manual/scheduled admission tests proving at most one active Process;
- dependency and source scans proving no Iceberg/Paimon Action invocation, table loading, file or
  metadata mutation, Spark client, or remote submission endpoint.

Passing simulator tests demonstrates orchestration correctness only. It is not evidence that any
format maintenance behavior has been implemented.
