# Iggy Java SDK Benchmarks

JMH-based performance benchmarks for Iggy Java SDK.

## Benchmark Types

### Microbenchmarks
Pure client-side performance, no external dependencies:
- **BytesSerializerBenchmarkDemo** - BigInteger U64/U128 conversion performance

## Prerequisites

- JDK 17+

## Usage

### Using Gradle

```bash
# From repository root
cd foreign/java

# Build benchmarks
./gradlew :iggy-benchmarks:build

# Run all benchmarks (via Gradle)
./gradlew :iggy-benchmarks:jmh

# Run specific benchmark
./gradlew :iggy-benchmarks:jmh -PjmhArgs="BytesSerializer"

# Quick test (fast iterations)
./gradlew :iggy-benchmarks:jmh -PjmhArgs="BytesSerializer -f 1 -wi 3 -i 5"

# With GC profiling
./gradlew :iggy-benchmarks:jmh -PjmhArgs="BytesSerializer -prof gc"
```

### Direct JAR Execution

```bash
# Build first
./gradlew :iggy-benchmarks:build

# Run all benchmarks
java -jar benchmarks/build/libs/iggy-jmh-benchmarks-*-SNAPSHOT.jar

# Run specific benchmark
java -jar benchmarks/build/libs/iggy-jmh-benchmarks-*-SNAPSHOT.jar BytesSerializer

# Quick test
java -jar benchmarks/build/libs/iggy-jmh-benchmarks-*-SNAPSHOT.jar BytesSerializer -f 1 -wi 3 -i 5

# List all benchmarks
java -jar benchmarks/build/libs/iggy-jmh-benchmarks-*-SNAPSHOT.jar -l

# Full JMH help
java -jar benchmarks/build/libs/iggy-jmh-benchmarks-*-SNAPSHOT.jar -h
```
