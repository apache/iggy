# Iggy Java SDK Microbenchmarks

JMH-based microbenchmarks for Iggy Java SDK.

## Prerequisites

- JDK 17+

## Usage

### Using Gradle

```bash
# From repository root
cd foreign/java

# Build microbenchmarks
./gradlew :iggy-microbenchmarks:build

# Run all microbenchmarks
./gradlew :iggy-microbenchmarks:jmh

# Run specific benchmark
./gradlew :iggy-microbenchmarks:jmh -PjmhArgs="SimpleDemo"

# Quick test (fewer iterations)
./gradlew :iggy-microbenchmarks:jmh -PjmhArgs="SimpleDemo -f 1 -wi 2 -i 3"

# With GC profiling
./gradlew :iggy-microbenchmarks:jmh -PjmhArgs="SimpleDemo -prof gc"
```

### Direct JAR Execution

```bash
# Build first
./gradlew :iggy-microbenchmarks:build

# Run all microbenchmarks
java -jar microbenchmarks/build/libs/iggy-jmh-microbenchmarks-*-SNAPSHOT.jar

# List all benchmarks
java -jar microbenchmarks/build/libs/iggy-jmh-microbenchmarks-*-SNAPSHOT.jar -l

# Full JMH help
java -jar microbenchmarks/build/libs/iggy-jmh-microbenchmarks-*-SNAPSHOT.jar -h
```

## Troubleshooting

**Verbose logging:** Edit `src/main/resources/logback.xml`:
```xml
<logger name="org.apache.iggy.benchmark" level="DEBUG"/>
```
