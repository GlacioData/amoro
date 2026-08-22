# Amoro AMS v2 (Spring Boot 3)

Spring Boot 3 based next-generation AMS host. It starts as the home of the **Process
control plane** (spec/status + Transition state machine) and absorbs AMS
responsibilities module by module — see `tasks/process-appmanager-redesign-options.md`
(方案 C) and `tasks/process-control-plane-spec.md`.

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
- No dependencies on other reactor modules yet; the Process control contract is
  planned as a zero-dependency layer in `amoro-common` (spec §3) and will be the
  first dependency this module declares.
