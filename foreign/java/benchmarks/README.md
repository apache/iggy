# Iggy Java SDK Benchmarks

JMH-based performance benchmarks for Iggy Java SDK.

## Prerequisites

- JDK 17+
- Docker (Testcontainers manages the Iggy server automatically)

## Usage
```bash
# From repository root
cd foreign/java

# Run all benchmarks
./gradlew :iggy-benchmarks:jmh

# Run specific benchmark
./gradlew :iggy-benchmarks:jmh -PjmhArgs=".*TcpPoll.*"

# Quick test (no warmup, 1 iteration)
./gradlew :iggy-benchmarks:jmh -PjmhArgs=".*AsyncTcpPoll.* -wi 0 -i 1 -f 1"

# With GC profiling
./gradlew :iggy-benchmarks:jmh -PjmhArgs=".*TcpPoll.* -prof gc"

# Override parameters (see benchmark source code for available options)
./gradlew :iggy-benchmarks:jmh -PjmhArgs=".*TcpPoll.* -p messagesPerPoll=100"
./gradlew :iggy-benchmarks:jmh -PjmhArgs=".*TcpPoll.* -p messagesPerPoll=1,10,100"
```

## Troubleshooting

**Container fails to start:** Make sure Docker is running.

**Verbose logging:** Edit `src/main/resources/logback.xml`:
```xml
<logger name="org.apache.iggy.benchmark" level="DEBUG"/>
```
