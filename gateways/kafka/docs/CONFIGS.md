# Topic configuration

`CreateTopics` (19), `DescribeConfigs` (32), `AlterConfigs` (33) and `IncrementalAlterConfigs`
(44) read and write the configuration of Kafka topics. `DescribeConfigs` also answers a broker
resource (type 4).

`kafka-configs --alter` on a topic has sent API 44 since Kafka 2.5. Java
`Admin.incrementalAlterConfigs`, the only alter method left in Kafka 4.0, also sends API 44, as do
MirrorMaker 2 and kafka-ui. API 33 comes from Java `Admin.alterConfigs` before Kafka 4.0 and from
the non-incremental alter calls of other clients, such as librdkafka's `rd_kafka_AlterConfigs`.

With the bridge off, these APIs answer `NOT_CONTROLLER` (41) and do not read or write Iggy.

## Authorization

When SASL is on, `AlterConfigs` and `IncrementalAlterConfigs` require the `manage_topics` (or
`manage_streams`) Iggy permission on the authenticated principal. A principal without it gets
`TOPIC_AUTHORIZATION_FAILED` (29) for every resource in the request, and `update_topic` is never
called. With SASL off, the request has no principal, and nothing is gated here.
`DescribeConfigs` tests no permission of its own. `CreateTopics` has no gate yet.

## The keys

The gateway knows every topic key of Kafka 4.4, and tests each value as Kafka 4.4 tests it, with
Kafka's text. Iggy stores three keys on the topic. The gateway accepts any `cleanup.policy`, and
any value of a key that changes nothing a client sees. It tests four keys against what Iggy does,
and accepts only the Kafka default of `message.timestamp.type`.

| Key | Iggy topic option | Values the gateway accepts |
| --- | --- | --- |
| `retention.ms` | `message_expiry`, in microseconds | -1 and more, as Kafka. -1 never expires |
| `retention.bytes` | `max_topic_size`, the value times the partition count | Any count, as Kafka. A negative count caps nothing. Each partition keeps at least one segment |
| `segment.bytes` | `segment_size`, set only at creation | A multiple of 512 bytes from 1 MiB to 1 GiB |
| `cleanup.policy` | None | Any list of `compact` and `delete`, as Kafka. A describe does not report it |
| `min.insync.replicas` | None | 1 up to the majority of the replication factor, which a describe reports |
| `flush.messages`, `flush.ms` | `durability`, set only at creation | Any value that Kafka accepts. `1` and `0` make a new topic `persisted` |
| `max.message.bytes` | None | `1048588` up to the largest batch that Produce takes, which a describe reports |
| `message.timestamp.type` | None | `CreateTime` only, which a describe reports |
| Any other Kafka 4.4 key, for example `compression.type` | None | Any value that Kafka accepts. A describe reports the Kafka default |

Kafka trims a value and parses it by the type of its key, and so does the gateway. So `+1`,
`0001` and ` 1 ` are the same `INT`, and `TRUE` is the same `BOOLEAN` as `true`.

### `retention.ms`

Iggy stores `message_expiry` as an `IggyExpiry`, in microseconds. Retention applies to sealed
segments only. Iggy does not roll a segment on time, so the active segment does not expire.

- -1 stores `IggyExpiry::NeverExpire`.
- 0 stores one microsecond, because Iggy reads an expiry of 0 as its own default. A describe
  reports 0.
- A count above 18446744073709551 milliseconds does not fit the microsecond counter of Iggy. That
  is more than half a million years. It never expires, and a describe reports -1. Debezium sends
  `9223372036854775807`, which is such a count.

A describe divides the stored microseconds by 1000 only for a whole number of milliseconds. An
Iggy client can store another duration. The describe then answers `INVALID_CONFIG` (40) with a
null value and source `DYNAMIC_TOPIC_CONFIG` (1). The gateway does not round.

### `retention.bytes`

Kafka caps each partition. Iggy caps the whole topic with `max_topic_size` and gives each
partition an equal share. So the gateway stores the value times the partition count, and a
describe reports the share of one partition. A product past 64 bits caps nothing in practice, so
it is stored as no cap.

Iggy always keeps the newest sealed segment of a partition and the active one. For a cap that
small, Kafka keeps only the active segment. So a `retention.bytes` below `segment.bytes` deletes
nothing that Kafka keeps. The gateway accepts such a cap, from 0 up, and logs a
warning. Iggy refuses a topic cap below one segment, so a value times the partition count below
`segment.bytes` is stored as `segment.bytes`. A describe then reports that size divided by the
partition count. To keep less than 1 GiB in a partition, also set `segment.bytes`.

Iggy does not delete a segment past the lowest offset that an Iggy consumer or consumer group
committed on the partition. So an Iggy client that stopped reading holds retention back. The
Kafka consumer groups of the gateway store their offsets as Iggy external groups, which do not
hold retention back.

`CreatePartitions` raises `max_topic_size` in proportion before it adds partitions, so that each
partition keeps its share. The raise and the growth run under the same lock, and an alter that
writes a size cap takes the same lock.

- If Iggy refuses the raise, nothing is added, and the answer is the error of that call.
- If the outcome of the raise is unknown, nothing is added, the old cap is written back, and the
  answer is `REQUEST_TIMED_OUT` (7).
- If the partitions are proven not added, the old cap is written back.
- If the outcome of the growth is unknown, the raised cap stays. It keeps more of each partition,
  never less.
- If the topic ends with more partitions than requested, because another client grew it too, the
  cap is raised again for every partition.

The lock covers one gateway. Another gateway or an Iggy client can change the cap or the
partition count at the same time. A partition can then keep a smaller share. Set
`retention.bytes` again then.

### `segment.bytes`

Iggy sets `segment_size` at the creation of a topic and cannot change it later. So `CreateTopics`
stores the value, and an alter accepts only the current value. A delete of the key succeeds only
while the size is the default, 1 GiB. With another size, it gets `INVALID_CONFIG` (40). An
`AlterConfigs` that leaves the key out keeps the size, and the gateway logs that.

Iggy needs a multiple of 512 bytes from 1 MiB to 1 GiB. Kafka accepts any size from 1 MiB to
2 GiB, so a size above 1 GiB gets `INVALID_CONFIG` (40) here.

### `cleanup.policy`

Kafka accepts a list of `compact` and `delete`, and from Kafka 4.4 also an empty list. The
gateway accepts the same lists. Iggy deletes expired segments and does not compact (#4521).

- A policy with `delete` applies `retention.ms` and `retention.bytes`, as Kafka does.
- A policy without `delete`, which is `compact` or an empty list, keeps every record. The topic
  gets no expiry and no size cap, whatever the retention keys say. Kafka ignores retention there
  too.
- A policy with `compact` logs a warning that names the topic, because nothing compacts it. An
  alter that only repeats the policy of a topic logs at debug level. The Strimzi Topic Operator
  sends the policy again on every pass.

Iggy has no topic option for the policy, so the gateway does not store it. A describe leaves
`cleanup.policy` out. Kafka Connect refuses an internal topic that reports any policy other than
`compact`. When the entry is missing, Connect starts
(`TopicAdmin.verifyTopicCleanupPolicyOnlyCompact`). "Limitations" lists what the missing entry
breaks.

A topic that keeps every record stores no expiry and no size cap, both as values that a client
set. Because the policy is not stored, an alter that does not name `cleanup.policy` keeps such a
topic as it is:

- A `retention.ms` or `retention.bytes` that removes records gets `INVALID_CONFIG` (40). Name
  `cleanup.policy=delete` in the same request to apply it.
- A delete of either key, or an `AlterConfigs` that leaves them out, changes nothing. kafka-ui
  and AKHQ send such a reset.
- `-1` for either key is accepted, because it removes nothing.

A topic created with `retention.ms=-1` and `retention.bytes=-1` under `delete` looks the same, so
an alter of its retention also needs `cleanup.policy` in the request.

### Other keys

Most other keys change nothing that a client of the gateway sees. Examples are the compression
type, the compaction keys and the segment timing. The gateway accepts any value that Kafka
accepts for such a key, and does not apply it. A describe reports the Kafka 4.4 default, from
`DEFAULT_CONFIG` (5), as writable. A value that Kafka refuses gets Kafka's text, for example
`Invalid value 0 for configuration min.insync.replicas: Value must be at least 1`.

Four keys depend on what Iggy does:

- `min.insync.replicas`: Iggy commits a write on a majority of the nodes that store the
  partition. For a replication factor of 3, that is the `min.insync.replicas=2` that most Kafka
  users set. A describe reports that majority. A value up to it is accepted, because a write gets
  at least the copies that it asks for. A larger value gets `INVALID_CONFIG` (40). The test uses
  the factor of the request, or of the topic, else the Iggy default. While the gateway knows
  neither, it accepts any value and reports `1`.
- `flush.messages` and `flush.ms`: Iggy sets the durability of a topic only at its creation. `flush.messages=1` or `flush.ms=0` on a create, a flush of every write, makes the
  messages `persisted`. On an alter, these values are accepted only for a topic that is
  `persisted` already. Any other value is accepted and not applied. A describe of a `persisted`
  topic reports `1` and `0`.
- `max.message.bytes`: the gateway has no size limit for each topic. It takes a record batch up
  to the request frame (`IGGY_KAFKA_MAX_FRAME_SIZE`) and one Iggy send, whichever is smaller,
  in every topic. It accepts a value from `1048588` up to that size, and a describe reports that
  size. MirrorMaker 2 copies a raised value from the source topic. A value below `1048588` asks
  the topic to refuse batches that the gateway takes, so it is refused.

`message.timestamp.type` changes the timestamps that clients get. Iggy keeps the timestamp that
the producer sets, so `LogAppendTime` gets `INVALID_CONFIG` (40) with the reason. A describe
reports `CreateTime` as read only.

A name that Kafka 4.4 does not know gets `INVALID_CONFIG` (40) with Kafka's text,
`Unknown topic config name: <name>`. This includes the keys that Kafka 4.0 removed, such as
`message.format.version`, and names like `retention.hours`, which only exist as broker keys.

## Replication factor

Iggy stores a replication factor for each topic (#4692): the count of nodes that store each
partition. `CreateTopics` stores the factor of the request. A factor of `-1` takes the Iggy
default, which is every node unless an operator set a number.

- Iggy accepts a factor from 1 to 32, and at most the node count of the cluster. Any other factor
  gets `INVALID_REPLICATION_FACTOR` (38), with Kafka's text and the node count in place of the
  broker count.
- A factor of 1 keeps one copy, as in Kafka.
- Iggy commits a write on a majority of these nodes. A factor of 2 therefore stops writes while
  either node is down. Kafka keeps writing with one copy there, because its default
  `min.insync.replicas` is 1. Use 3 for a topic that must take writes while one node is down.

A real create gets the answer from Iggy. A `validate_only` create tests the factor against the
node count that the gateway read last. For a factor of `-1`, it reports the Iggy default that
the gateway read last, or `-1` before the first read. A create that the Iggy SDK sent twice
reports the same way.

From v5, a successful result reports the factor that Iggy stored, the default included. Metadata
reports as many replicas, see "Replicas in Metadata".

A manual replica assignment stays limited to this gateway, and the topic gets the Iggy default.

### Replicas in Metadata

Each gateway reports itself as the leader of every partition, because every gateway can serve
every partition. The replica list of a partition holds as many ids as the factor of the topic.
The Strimzi Topic Operator and Terraform read the factor that Iggy stored from it. It names this
gateway first, then the next gateways in node id order. When the factor is above the number of
gateways that `IGGY_KAFKA_BROKERS` lists, stand-in ids from 65536 fill the rest. No gateway can
take such an id, because a node id is at most 65535. A client reads a stand-in as a replica on a
broker that is not up. Each replica counts as in sync.

The topic list that Metadata reads gives the factor, but not the nodes that store each partition.
So the replica list names gateways and stand-ins, and not the Iggy nodes of the partition. A
topic created before Iggy stored a factor reports this gateway alone.

## Defaults of the gateway

Three variables set what a new topic gets for each key its request leaves out. They apply to
`CreateTopics` and to auto topic creation. They do not apply to topics that Iggy clients create.

| Variable | Kafka broker key | Default |
| --- | --- | --- |
| `IGGY_KAFKA_TOPIC_RETENTION_MS` | `log.retention.ms` | `-1`, never expire. Otherwise a positive count |
| `IGGY_KAFKA_TOPIC_RETENTION_BYTES` | `log.retention.bytes` | `-1`, no cap. Otherwise at least 1 GiB, so that every segment size fits |
| `IGGY_KAFKA_TOPIC_DURABILITY` | None | `replicated`. `persisted` applies to the messages and to the consumer offsets of the topic |

Kafka keeps records for 7 days by default. The gateway keeps them forever unless an operator sets
`IGGY_KAFKA_TOPIC_RETENTION_MS`.

Iggy stores a default like any value that a client sends. So a describe reports a stored
`retention.ms` or `retention.bytes` equal to the default as `STATIC_BROKER_CONFIG` (4), as Kafka
reports a topic that inherits `log.retention.ms`. For a key that a client set, a delete of the
key, or an `AlterConfigs` that leaves it out, writes the default. For a key that no client set, it
changes nothing, as in Kafka. Such a topic keeps the Iggy default that it was created with, also
after an operator sets the variable.

A delete writes the default of the gateway that serves it. Give the three variables the same
value on every gateway, or the same request gives a topic another retention on each gateway.

## What `CreateTopics` does

A topic that exists answers `TOPIC_ALREADY_EXISTS` (36) before any other test, as in Kafka. Kafka
Connect resends its topic configuration on every start and relies on this. Then the configuration
is tested, then the partition count and the replication factor.

- A null value gets `INVALID_CONFIG` (40) with Kafka's text, every name at once:
  `Null value not supported for topic configs: <names>`.
- A repeated name keeps its last value, as the Kafka controller does.
- Each key follows "The keys". The first refused key fails its topic, and nothing is created for
  it. The other topics in the request still run.

`validate_only` runs the same tests and creates nothing.

From v5, a successful result lists the configuration of the new topic, as a describe of it
reports it. It also carries the replication factor that Iggy stored. The Java
`CreateTopicsResult.config` and `CreateTopicsResult.replicationFactor` return them. An older
version carries no configuration, so the gateway does not build it.

## What a describe returns

### Topics

A null or empty `configuration_keys` list returns every topic key of Kafka 4.4 but
`cleanup.policy`, sorted by name. A non-empty list returns each key it names, in request order and
once. A name that the gateway does not report, such as `cleanup.policy` or `no.such`, is left out,
and the resource still answers `NONE` (0).

| Key | Value | Read only | Source |
| --- | --- | --- | --- |
| `retention.ms` | From `message_expiry`. `-1` means that sealed segments never expire | no | If no client set it, `DEFAULT_CONFIG` (5). If it equals the default of the gateway, `STATIC_BROKER_CONFIG` (4). Else `DYNAMIC_TOPIC_CONFIG` (1) |
| `retention.bytes` | `max_topic_size` divided by the partition count, rounded down. `-1` for no cap | no | As for `retention.ms` |
| `segment.bytes` | `segment_size` | yes | If the create set it, `DYNAMIC_TOPIC_CONFIG` (1). Else `DEFAULT_CONFIG` (5) |
| `min.insync.replicas` | The majority of the replication factor of the topic, else of the Iggy default. `1` while the gateway knows neither | no | `DEFAULT_CONFIG` (5) |
| `flush.messages`, `flush.ms` | `1` and `0` for a `persisted` topic, else the Kafka 4.4 default | no | A `persisted` topic under `IGGY_KAFKA_TOPIC_DURABILITY=persisted` reports `STATIC_BROKER_CONFIG` (4). Another `persisted` topic reports `DYNAMIC_TOPIC_CONFIG` (1). A `replicated` topic reports `DEFAULT_CONFIG` (5) |
| `max.message.bytes` | The largest batch that Produce takes | no | `DEFAULT_CONFIG` (5) |
| `message.timestamp.type` | `CreateTime` | yes | `DEFAULT_CONFIG` (5) |
| Every other key | The Kafka 4.4 default | no | `DEFAULT_CONFIG` (5) |

Kafka reports every topic key as writable. The gateway reports the keys whose value it cannot
change as read only, so that tools like kafka-ui do not offer to edit them.

`is_sensitive` is false. From v3, `include_documentation` attaches one sentence to each entry.
With `include_synonyms`, each entry lists itself as its only synonym, with its value and source.
Kafka also lists the broker keys that the value falls back to, such as `log.retention.ms`.

### Brokers

An admin client sends the describe of a broker to that broker, so each gateway answers for its own
`IGGY_KAFKA_NODE_ID`.

- The empty name stands for the dynamic defaults of the cluster. The gateway has none, so it
  answers `NONE` (0) with no entries.
- Its own id answers the entries below.
- Another id gets `INVALID_REQUEST` (42) with Kafka's text, `Unexpected broker id, expected <id>
  or empty string, but received <name>`. A name that is not a number gets `Broker id must be an
  integer, but it is: <name>`.

| Key | Value | Source |
| --- | --- | --- |
| `auto.create.topics.enable` | `IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED`, `false` by default | 4 when on, else 5 |
| `broker.id`, `node.id` | `IGGY_KAFKA_NODE_ID` | 4 |
| `compression.type` | `producer` | 5 |
| `default.replication.factor` | The Iggy default, from the read of the roster. `-1` before the first read, so that Debezium creates its schema history topic with the Iggy default | 4 after the first read, else 5 |
| `delete.topic.enable` | `true` | 5 |
| `log.cleanup.policy` | `delete` | 5 |
| `log.message.timestamp.type` | `CreateTime` | 5 |
| `log.retention.bytes`, `log.retention.ms` | The defaults of the gateway, or `-1` for one that is unset | 4 when set, else 5 |
| `log.segment.bytes` | `1073741824` | 5 |
| `message.max.bytes` | The largest batch that Produce takes | 5 |
| `min.insync.replicas` | The majority of `default.replication.factor`. `1` before the first read | 4 after the first read, else 5 |
| `num.partitions` | `1`, the count that `CreateTopics` gives for `-1`. Auto topic creation uses `IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS` | 5 |
| `unclean.leader.election.enable` | `false` | 5 |

Every entry is read only, because the gateway changes no broker configuration. At startup, the
Strimzi Topic Operator reads `auto.create.topics.enable` and `min.insync.replicas` from every
broker. If either one is missing, it stops. The gateway does not report
`inter.broker.protocol.version`, which Kafka 4.0 removed.

## What `AlterConfigs` does

Kafka's `AlterConfigs` replaces the whole configuration of a topic, and so does the gateway for the
keys it stores. Each named key follows "The keys". Each stored key that a client set and the
resource leaves out goes back to its default: the default of the gateway where one is set, else
the Iggy default. A describe reports the Iggy default from `DEFAULT_CONFIG` (5). An empty
`configs` list therefore resets `retention.ms` and `retention.bytes`. A key that no client set
stays as it is.

`segment.bytes` cannot go back to its default once Iggy created the topic with another size. A
resource that leaves it out keeps the size, and the gateway logs that. KafkaJS, Sarama and
kafka-python send only `AlterConfigs`, and can then change the retention of such a topic.

A null value gets `INVALID_REQUEST` (42) with Kafka's text, `Null value not supported for :
<names>`, before the topic lookup. A repeated name gets `INVALID_CONFIG` (40). One bad key fails
its resource and writes nothing for it. The other resources in the request still run.

`validate_only` runs the same tests, including the topic lookup, and does not call
`update_topic`. A key that goes back to a value it already has is not written.

## What `IncrementalAlterConfigs` does

Each entry carries one operation on one key. The entries of one resource are applied together, in
one write of the topic.

| Operation | `retention.ms`, `retention.bytes` | `segment.bytes` | `cleanup.policy` and the other list keys | Any other key |
| --- | --- | --- | --- | --- |
| `SET` (0) | Stored, as "The keys" says | Accepted only at the current value | Tested as "The keys" says. A policy without `delete` keeps every record | Tested as "The keys" says. An unknown name gets `INVALID_CONFIG` (40) |
| `DELETE` (1) | Back to the default of the gateway, else the Iggy default. If no client set the key, or it has that value already, nothing is written | Refused unless the size is the default | No change | No change, also for an unknown name |
| `APPEND` (2), `SUBTRACT` (3) | `INVALID_CONFIG` (40), `Can't APPEND to key retention.ms because its type is not LIST.` | As for `retention.ms` | Applied to the Kafka default of the list, then tested as a `SET`. For `cleanup.policy`, that default is `delete` | As for `retention.ms` |

`kafka-configs --delete-config` sends an empty value with its delete, and the gateway ignores the
value. An empty operation list writes nothing, and answers `NONE` (0) once the topic lookup
succeeds.

Three request faults are answered `INVALID_REQUEST` (42) before the topic lookup and without any
Iggy call:

- A configuration name that appears twice in one resource: `Error due to duplicate config keys`.
- A null value on anything but a `DELETE`: `Null value not supported for : <names>`.
- An operation byte outside 0-3: `Unknown operations type <n>`.

The first two texts are Kafka's own. The third comes from Kafka's broker-resource test, because
Kafka's topic path has no well-formed answer for an unknown operation. A topic named by more than one resource gets `INVALID_REQUEST` (42) on every
occurrence, with `Each resource must appear at most once.`

The order of the tests is: authorization, the topic cap, the resource type, the topic name, the
request faults, the topic lookup, then the configuration faults. A missing topic is
`UNKNOWN_TOPIC_OR_PARTITION` (3), also for an entry with a bad value, as with `AlterConfigs`.
Kafka orders these tests differently, see "Limitations".

`validate_only` runs every test, including the topic lookup, and writes nothing.

## Limitations

- Iggy applies only `retention.ms`, `retention.bytes`, `segment.bytes` and, at creation, a flush
  of every write. The gateway accepts other values of most other keys without applying them, and
  a describe reports the Kafka default for them. Terraform and the Strimzi Topic Operator then see
  a difference on every read.
- `message.timestamp.type=LogAppendTime` is refused, and so is a `max.message.bytes` outside
  `1048588` and the largest batch. A describe reports the largest batch for every topic.
- `min.insync.replicas` above the majority of the replication factor is refused. A smaller value
  is accepted, and a describe reports the majority.
- `flush.messages=1` and `flush.ms=0` are refused on an alter of a topic that is not
  `persisted`.
- Iggy does not compact (#4521). A topic with `compact` keeps every record, so it grows without
  bound.
- The gateway does not store `cleanup.policy`, and a describe does not report it. Schema Registry
  creates `_schemas` and starts, but on its next start it reads the policy back and stops. Set
  `kafkastore.topic.skip.validation=true` for it. Apicurio Registry 3 reads `delete` back from its
  KafkaSQL topics, so it does not start.
- A compacted topic reports `retention.ms=-1` and `retention.bytes=-1` as values that a client
  set. Kafka reports `cleanup.policy=compact` and the retention defaults there.
- On a topic that keeps every record, an alter of its retention needs `cleanup.policy` in the
  same request. Kafka accepts the alter and ignores the retention while the policy is `compact`.
- A replication factor of 2 stops writes while either node is down. Kafka keeps writing there.
- The replica list in Metadata names gateways and stand-ins, not the Iggy nodes that store the
  partition.
- If a stored `retention.ms` or `retention.bytes` equals the default of the gateway, it reports
  as `STATIC_BROKER_CONFIG` (4), also for a value that a client set. Kafka reports such an
  override as `DYNAMIC_TOPIC_CONFIG` (1). Terraform then shows a change for a topic that sets the
  key to the default of the gateway.
- A topic keeps the default it was created with. Kafka applies a new broker default to each topic
  without an override.
- `retention.bytes` below one segment keeps one segment in each partition, and a describe can
  then report more than the value that a client set. Kafka keeps only the active segment there.
- `segment.bytes` above 1 GiB is refused. Kafka accepts it.
- `segment.bytes` and the durability cannot change after the topic is created.
- A `retention.ms` past the microsecond counter of Iggy reads back as -1.
- Kafka tests the configuration before it tests that the topic exists, and tests duplicate names
  and null values before the topic name. The alters of the gateway look the topic up first. So
  a missing topic with a bad entry answers `UNKNOWN_TOPIC_OR_PARTITION` (3), and Kafka answers
  `INVALID_CONFIG` (40).

## Other resource errors

| Condition | Code | Notes |
| --------- | ---- | ----- |
| Caller lacks `manage_topics` (SASL on, both alter APIs) | `TOPIC_AUTHORIZATION_FAILED` (29) | Tested before the topic lookup. Every resource in the request |
| Resource type other than topic on an alter, or other than topic and broker on a describe | `INVALID_REQUEST` (42) | `only topic resources are supported`, or `only topic and broker resources are supported`. Other resources in the batch still run |
| Topic name fails the same rules as CreateTopics | `INVALID_TOPIC_EXCEPTION` (17) | The message is the reason, and does not repeat the topic name |
| Topic does not exist | `UNKNOWN_TOPIC_OR_PARTITION` (3) | Tested before the configuration keys, so a missing topic is not `INVALID_CONFIG` |
| Null value on `AlterConfigs` | `INVALID_REQUEST` (42) | Kafka's text. Tested before the topic lookup |
| Duplicate configuration name, null value, or unknown operation on `IncrementalAlterConfigs` | `INVALID_REQUEST` (42) | Texts under "What `IncrementalAlterConfigs` does". Tested before the topic lookup, so a missing topic gets this code too |
| More than 100 distinct topic names, or for `DescribeConfigs` more than 100 distinct topic names and broker resources together | `POLICY_VIOLATION` (44) | Every resource in the request, tested before the bridge-availability test. A broker resource counts each time it appears |
| A topic name repeated across resources | `INVALID_REQUEST` (42) | Every occurrence of the duplicate, not only the second one. CreateTopics answers a repeated topic name the same way. `IncrementalAlterConfigs` adds Kafka's message, `Each resource must appear at most once.` |

Unknown tagged fields at the request, resource, or configuration-entry level are ignored, not
rejected.
Kafka's flexible versions define them as a forward-compatible extension point, which a server that
does not know them must skip.
