# Amoro AMS v2 (Spring Boot 3)

Spring Boot 3 based next-generation AMS host. The current implementation is a Boot
application and health-endpoint skeleton. It is intended to host the **Process control
plane** (spec/status + Transition state machine) and absorb AMS responsibilities module
by module. The authoritative designs awaiting implementation are
`tasks/amoro-ams-v2-framework-spec.md` and `tasks/amoro-ams-v2-process-spec.md`;
their current implementation companions are `tasks/ams-v2-framework-plan.md`,
`tasks/ams-v2-framework-todo.md`, `tasks/ams-v2-process-plan.md`, and
`tasks/ams-v2-process-todo.md`. Historical design inputs are limited to
`tasks/process-appmanager-redesign-options.md`,
`tasks/process-control-plane-spec.md`, and
`tasks/process-reconciler-architecture.md`.

The rest of the reactor stays on the Java 8 baseline; this module compiles with
**Java 17 via Maven toolchains**, so the usual JDK 8/11 reactor builds keep working.

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

Two build modes, both verified:

```bash
# 1) Reactor build on JDK 8/11 (the usual flow) — compiles/tests via the JDK 17
#    toolchain, produces a PLAIN jar (spring-boot repackage is skipped):
JAVA_HOME=/path/to/jdk-11 ./mvnw clean package -DskipTests -Pskip-dashboard-build

# 2) Module build with the executable boot jar — Maven itself must run on JDK 17+
#    (activates the jdk17-boot-jar profile; spotless is skipped for this module
#    because google-java-format 1.7 cannot run on JDK 17+):
JAVA_HOME=/path/to/jdk-17 ./mvnw -pl amoro-ams-v2 clean package

# run locally (port 1640; v1 AMS keeps 1630 during the migration)
java -jar amoro-ams-v2/target/amoro-ams-v2-*.jar

# smoke check
curl http://localhost:1640/api/ams/v2/health
```

## Notes

- `jacoco.skip=true`: the repo-wide jacoco 0.8.7 cannot instrument Java 17 class
  files; revisit when the parent bumps jacoco ≥ 0.8.8.
- Keep source syntax google-java-format 1.7-compatible until the repo-wide formatter
  is upgraded (records/switch-expressions would break `spotless` on this module).
- No dependencies on other reactor modules yet. The generic resource framework and
  Process resource will be implemented incrementally inside this module according to
  the authoritative specs above. The implementation gate is Framework T1-T12 first,
  followed by Process P1-P8; every task requires review and passing JUnit 5 tests before
  its local commit.
