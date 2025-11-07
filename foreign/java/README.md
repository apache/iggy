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

### Auto Consumer Group (New Feature)

The Java SDK now includes support for automatic consumer group management with built-in rebalancing capabilities. This feature simplifies the process of managing consumer groups by automatically handling:

- Heartbeat management
- Partition rebalancing
- Automatic join/leave operations
- Callbacks for partition assignment changes

Example usage:

```java
// Configure consumer group
ConsumerGroupConfig config = ConsumerGroupConfig.builder()
    .sessionTimeout(Duration.ofSeconds(30))
    .heartbeatInterval(Duration.ofSeconds(10))
    .rebalanceStrategy(RebalanceStrategy.ROUND_ROBIN)
    .autoCommit(true)
    .autoCommitInterval(Duration.ofSeconds(5))
    .onPartitionsAssigned(partitions -> {
        System.out.println("Assigned partitions: " + partitions);
        // Initialize partition state
    })
    .onPartitionsRevoked(partitions -> {
        System.out.println("Revoked partitions: " + partitions);
        // Cleanup partition state
    })
    .build();

// Create and initialize consumer group
AutoConsumerGroup consumerGroup = client.consumerGroup()
    .stream("my-stream")
    .topic("my-topic")
    .groupName("my-consumer-group")
    .config(config)
    .build();

try {
    consumerGroup.init();
    
    // Consume messages
    while (running) {
        var messages = consumerGroup.poll(Duration.ofSeconds(1));
        for (var message : messages.messages()) {
            System.out.println("Received message: " + new String(message.payload()));
            // Process message
        }
    }
} finally {
    consumerGroup.close();  // Automatically leaves the consumer group
}
```