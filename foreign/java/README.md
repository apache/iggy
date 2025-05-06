# Java SDK for Iggy

[Iggy GitHub](https://github.com/iggy-rs/iggy) | [Website](https://iggy.rs) | [Getting started](https://docs.iggy.rs/introduction/getting-started/) | [Documentation](https://docs.iggy.rs) | [Blog](https://blog.iggy.rs) | [Discord](https://iggy.rs/discord)

[![Tests](https://github.com/iggy-rs/iggy-java-client/actions/workflows/tests.yml/badge.svg)](https://github.com/iggy-rs/iggy-java-client/actions/workflows/tests.yml)
[![x](https://img.shields.io/twitter/follow/iggy_rs_?style=social)](https://twitter.com/iggy_rs_)

---

Official Java client SDK for [Iggy.rs](https://iggy.rs) message streaming.

The client currently supports HTTP and TCP protocols with blocking implementation.

### Adding the client to your project

Add dependency to `pom.xml` or `build.gradle` file.

You can find the latest version in Maven Central repository:

https://central.sonatype.com/artifact/rs.iggy/iggy-java-sdk

### Implement consumer and producer

You can find examples for
simple [consumer](https://github.com/iggy-rs/iggy-java-client/blob/main/examples/simple-consumer/src/main/java/rs/iggy/SimpleConsumer.java)
and [producer](https://github.com/iggy-rs/iggy-java-client/blob/main/examples/simple-producer/src/main/java/rs/iggy/SimpleProducer.java)
in the repository.
