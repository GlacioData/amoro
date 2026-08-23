# Amoro AMS v2 (Spring Boot 3)

Spring Boot 3 based next-generation AMS host, now containing the **generic resource
control-plane framework** (scheduler + seven-layer persistence, Tasks T1–T12 complete).
The **Process control plane** (spec/status + Transition state machine, tasks P1–P8) is being
implemented on top of it. Authoritative designs: `tasks/amoro-ams-v2-framework-spec.md` and
`tasks/amoro-ams-v2-process-spec.md`; implementation companions live next to them in
`tasks/`. Historical design inputs (`tasks/process-appmanager-redesign-options.md`,
`tasks/process-control-plane-spec.md`, `tasks/process-reconciler-architecture.md`) are
superseded background only.

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
                 #   shutdown ordering (scheduler -> dispatcher -> lanes), dialect-aware
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

# real-MySQL integration (five SQL semantics, dialect DDL, E2E restart replay).
# Defaults to localhost:3306/amoro_v2; point AMORO_V2_MYSQL_* at any MySQL 5.7:
export AMORO_V2_MYSQL_PASSWORD=...   # user/url via AMORO_V2_MYSQL_USER/_URL
JAVA_HOME=/path/to/jdk-11 ./mvnw -pl amoro-ams-v2 test -Pdocker-it

# formatting/checkstyle gates:
JAVA_HOME=/path/to/jdk-11 ./mvnw -pl amoro-ams-v2 validate
```

The docker-mysql group is excluded through a property-driven surefire configuration
(`docker-mysql.excluded`); `-Pdocker-it` clears the property instead of relying on
overridable literal exclusions, so a plain `test` run can never silently include or
mis-skip the group. Without a reachable MySQL the group skips explicitly (assumption),
never silently.

## Configuration

`amoro.control.*` (see `AmoroControlProperties`): scheduler workers/period, storage
serialization bound, actor mailbox capacity, listener pool/retry, repository timeout and
the unified lifecycle shutdown budget — all validated fail-fast at startup. The
datasource comes from `spring.datasource.*` (defaults to embedded Derby, override with
`AMORO_V2_DATASOURCE_*`).

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
