# Amoro AMS v2 (Spring Boot 3)

Spring Boot 3 based next-generation AMS host, containing the generic resource control-plane
framework and the v2 Process orchestration plane. The Process implementation is deliberately a
**simulation-only control-flow delivery**: it proves manual/scheduled creation, durable state,
Local/Remote Engine SPI dispatch, reconciliation, cancellation, manual resolution, restart,
execution-handle release, and TTL.

This module does **not** connect or implement any Iceberg/Paimon Action—including Iceberg
`expire-snapshots` / `clean-orphans` and Paimon `sync-table-meta`—does not load a real table, and
does not submit a real Spark job. Simulation providers are disabled by default and require
`amoro.process.simulation.enabled=true`; the default Engine/Action registries are empty. See
[`ARCHITECTURE.md`](ARCHITECTURE.md) for the complete lifecycle, thread pools, component and
sequence diagrams. Authoritative designs are `tasks/amoro-ams-v2-framework-spec.md` and
`tasks/amoro-ams-v2-process-spec.md`.

The rest of the reactor stays on the Java 8 baseline; this module compiles with
**Java 17 via Maven toolchains**, so the usual JDK 8/11 reactor builds keep working.

## Framework layout (T1–T12)

```
org.apache.amoro
├── control      # T1–T3: Controller/ControllerKey/Scheduler contracts, DefaultScheduler
│                #   (single-flight, earliest-deadline, backoff {3,3,5,8,13,21,34,55}s),
│                #   graceful shutdown; DelayQueue orders, signal-version waits
├── persistence  # T4–T6: contracts (PersistenceService/Listener/Sink/Projection/Hook,
│                #   Repository, MutationCommand, domain whitelist, exceptions),
│                #   InMemoryPersistence (durable-first lane, outcome-unknown fencing,
│                #   deletion hook), ListenerDispatcher (per-pair ordering, bounded retry),
│                #   facade/ (L2 sync RepositoryFacade, L3 single-namespace pass-through)
│                #   blob/ (T8 BlobStoreActor mutation lane, T9 MyBatisBlobStore + mapper)
├── serde        # T7: VersionAwareJacksonSerde (JSON/YAML, converter chains, latest-only
│                #   writes, 64KiB bound), SerdeRegistry (eager validation)
└── config       # T10: amoro.control.* properties, Spring assembly, SmartLifecycle
                 #   bounded shutdown ordering (maintenance -> scheduler -> engines -> listeners -> lanes)
                 #   idempotent schema initializer, domain factory
```

Key invariants (see the framework spec for the full contract): the database is the source
of truth and the in-memory state is a rebuildable projection; a successful stage means the
row is durable; same-key controllers never overlap; unknown commit outcomes fence the name
until repair; listener failures never fail a durable write.

## Prerequisites

A JDK 17+ toolchain registered in `~/.m2/toolchains.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<toolchains>
  <toolchain>
    <type>jdk</type>
    <provides>
      <version>17</version>
    </provides>
    <configuration>
      <jdkHome>/path/to/jdk-17</jdkHome>
    </configuration>
  </toolchain>
</toolchains>
```

## Build & Run

```bash
# 1) Reactor build on JDK 8/11 (the usual flow) — compiles/tests via the JDK 17
#    toolchain, produces a PLAIN jar (spring-boot repackage is skipped):
JAVA_HOME=/path/to/jdk-11 ./mvnw clean package -DskipTests -Pskip-dashboard-build

# 2) Module build with the executable boot jar — Maven itself must run on JDK 17+
JAVA_HOME=/path/to/jdk-17 ./mvnw -pl amoro-ams-v2 clean package

# run locally (port 1640; v1 AMS keeps 1630 during the migration); defaults to an
# embedded Derby datastore, override with AMORO_V2_DATASOURCE_* environment variables
java -jar amoro-ams-v2/target/amoro-ams-v2-*.jar
curl http://localhost:1640/api/ams/v2/health
```

## Test & Verify

```bash
# offline suite (docker-mysql tagged groups excluded by default; Derby-backed tests run
# on an embedded database). Executes with >0 tests — no silent skips:
JAVA_HOME=/path/to/jdk-11 ./mvnw -pl amoro-ams-v2 test

# isolated local-Docker MySQL 5.7 integration through Testcontainers
# (SQL semantics, dialect DDL, Process lifecycle, release/TTL and restart replay):
JAVA_HOME=/path/to/jdk-11 ./mvnw -pl amoro-ams-v2 -Pdocker-it -Dgroups=docker-mysql test

# formatting/checkstyle gates:
JAVA_HOME=/path/to/jdk-11 ./mvnw -pl amoro-ams-v2 validate
```

The docker-mysql group is excluded through a property-driven Surefire configuration.
`-Pdocker-it` clears that exclusion and starts an isolated `mysql:5.7.44` Testcontainers database;
it never probes, truncates, or drops a user-managed local database. Docker unavailability is a test
failure for this explicit profile, not a skipped green build.

## Configuration

`amoro.control.*` (see `AmoroControlProperties`): scheduler workers/period, storage
serialization bound, actor mailbox capacity, listener pool/retry, repository timeout and
the unified lifecycle shutdown budget — all validated fail-fast at startup. The
datasource comes from `spring.datasource.*` (defaults to embedded Derby, override with
`AMORO_V2_DATASOURCE_*`).

`amoro.process.*` controls creation policy, reconcile deadlines, Engine timeout, bounded result
persistence, active rescheduling, execution release and TTL. All values fail fast at startup.
`amoro.process.simulation.enabled` defaults to `false`; only an explicit `true` selects the dummy
Local/Remote providers and simulated table/action facts. A Process freezes `tableFormat` with its
table identity, so dispatch selects the exact `(tableFormat, action, executionEngine)` provider
without loading or re-resolving a table.

## Deployed schema

The shipped DDL creates exactly ONE table, `amoro_process_v2`, which carries the whole
Process domain (persistence AND state tracking). Framework-generic domains that opt in
(e.g. `amoro_resource`) own their table creation — it is not part of the shipped DDL.

## Notes

- `jacoco.skip=true`: the repo-wide jacoco 0.8.7 cannot instrument Java 17 class files.
- Keep source syntax google-java-format 1.7-compatible until the repo-wide formatter is
  upgraded (records/switch-expressions would break `spotless` on this module). GJF does
  not reflow comments: keep javadoc lines ≤ 100 columns.
- Recorded deviations from the specs live in the task-series commit messages (e.g. the
  `last_updated` column rename — `last` is a Derby reserved word — and the Derby test
  runtime 10.16 while the shipped SQL stays 10.14-compatible).
- CI: the repo's current core workflows install JDK 11 only and do not trigger on
  `amoro-ams-v2/**`; until a v2 workflow lands (JDK 11+17 toolchains, path filter, offline
  verify), local verification with the commands above is the release gate.
- No dependencies on other reactor modules. Third-party versions are pinned in this
  module's pom (mybatis-spring-boot-starter 3.0.4, mysql-connector-j 8.4.0 for MySQL 5.7).
