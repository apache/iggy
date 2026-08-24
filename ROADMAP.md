- **Mechanism for splitting and merging partitions**

  Split hot partitions and merge underutilized ones without downtime or data loss.

- **Native key-value pair store**

  Built-in KV storage, usable for example to replace the current consumer offset storage.

- **Key-based message partitioner**

  Route messages to partitions by message key, so all messages with the same key land on the same partition.

- **Per-message routing for connector sources**

  Let a source connector choose the target stream, topic, and partition for each produced message instead of a single fixed destination.

- **Wildcard topic subscriptions for consumer groups**

  Subscribe a consumer group to a topic name pattern, automatically picking up new matching topics.

- **Tiered storage**

  Offload older segments to cheaper storage (such as S3-compatible object stores) while serving recent data from local disk.

- **Delayed message delivery**

  Publish messages that become visible to consumers only after a configured delay or at a scheduled time.

- **Deferred responses**

  Complete requests asynchronously, returning the response once the operation finishes instead of blocking the connection.

- **Direct I/O support**

  Bypass the OS page cache for predictable latency and lower memory pressure.

- **Multi-leader cluster support**

  Accept writes on multiple leaders concurrently instead of funneling all writes through a single leader.

- **Dynamically sized clusters**

  Add and remove nodes at runtime with automatic rebalancing of partitions.

- **Storage fault repairs through protocol-aware recovery**

  Detect corrupted or lost data on a replica and repair it from peers using the replication protocol.

- **Compression**

  Implement both client side and server side compression.

- **Log compaction**

  Retain only the most recent record per compaction key in a topic log, while the topic stays consumable as a stream. Requires deciding where the compaction key comes from, since Iggy messages carry user headers but no dedicated key field.
