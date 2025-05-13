# Java SDK for Iggy

[Iggy GitHub](https://github.com/apache/iggy) | [Website](https://iggy.apache.org) | [Getting started](https://iggy.apache.org/docs/introduction/getting-started) | [Documentation](https://iggy.apache.org/docs) | [Blog](https://iggy.apache.org/blogs) | [Discord](https://discord.gg/C5Sux5NcRa)

[![Tests](https://github.com/apache/iggy/actions/workflows/ci-check-java-sdk.yml/badge.svg)](https://github.com/apache/iggy/actions/workflows/ci-check-java-sdk.yml)
[![x](https://img.shields.io/twitter/follow/iggy_rs_?style=social)](https://x.com/ApacheIggy)

---

Official Java client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

The client currently supports HTTP and TCP protocols with blocking implementation.

### Adding the client to your project

Add dependency to `pom.xml` or `build.gradle` file.

You can find the latest version in Maven Central repository:

https://central.sonatype.com/artifact/org.apache.iggy/iggy-java-sdk

### Implement consumer and producer

You can find examples for
simple [consumer](https://github.com/apache/iggy/blob/master/foreign/java/examples/simple-consumer/src/main/java/org/apache/iggy/SimpleConsumer.java)
and [producer](https://github.com/apache/iggy/blob/master/foreign/java/examples/simple-producer/src/main/java/org/apache/iggy/SimpleProducer.java)
in the repository.
