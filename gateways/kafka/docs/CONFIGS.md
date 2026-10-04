# Topic configuration

`DescribeConfigs` (32) and `AlterConfigs` (33) apply to Kafka topic resources only
(resource type 2). Only the deprecated `Admin.alterConfigs` (API 33) writes
retention. `IncrementalAlterConfigs` (44) is not in this change. It is planned
after this work merges. It is not advertised, and a client that sends 44 still
gets the connection closed. Only `AlterConfigs` (33) writes retention.

With the bridge off, both APIs answer `NOT_CONTROLLER` (41) for each resource and
do not read or write Iggy.

## What a describe returns

A null or empty `configuration_keys` list returns both keys below. A non-empty
list returns only the requested names this gateway knows, in request order. A
name it does not know is omitted. The resource's error code stays `NONE` (0),
and the known keys in that request are still returned.

| Key | Value | Read only | Source |
| --- | ----- | --------- | ------ |
| `retention.ms` | Sealed-segment lifetime from Iggy `message_expiry`, or `-1` when those segments never expire | no | `DEFAULT_CONFIG` (5) for the unset never-expire default; `DYNAMIC_TOPIC_CONFIG` (1) when a client set it or a stored duration cannot be shown as milliseconds |
| `cleanup.policy` | `delete` | yes | `DEFAULT_CONFIG` (5) |

`is_sensitive` is false for both. From v3, `include_documentation` is honored:
true attaches one sentence per known key, false sends no documentation.

`retention.minutes` and `retention.hours` are not listed unless the client names
them in `configuration_keys`. A request that names one of those keys gets that
name back, with the value derived from the stored millisecond count (integer
division toward zero, and `-1` stays `-1`). The stored source of truth is still
`retention.ms`. Naming either key is not `INVALID_CONFIG`.

`include_synonyms` is honored. The synonym list on `retention.ms` is empty when
the flag is false, or when this process does not remember `retention.minutes` or
`retention.hours` for that topic. When the flag is true and an alter on this
process used one or both of those names, the list has one entry per remembered
name. The synonym value is the stored millisecond count divided by 60000 or
3600000, truncating toward zero. `-1` stays `-1`. `cleanup.policy` has no
synonyms. A null or empty `configuration_keys` list still returns only
`retention.ms` and `cleanup.policy`; minutes and hours show up there only as
synonyms.

Iggy stores `message_expiry` as `IggyExpiry`. A duration is `IggyDuration`, and
that type counts microseconds. Retention applies to sealed segments only. Iggy
does not time-roll, so the active segment does not expire. Describe divides by
1000 only when the stored count is a positive whole number of milliseconds.
Anything else, including 0 microseconds, is `INVALID_CONFIG` (40) with a null
value and source `DYNAMIC_TOPIC_CONFIG` (1). That expiry was stored; it is not
the never-expire default. The gateway does not round.

## What an alter stores

`retention.ms`, `retention.minutes`, and `retention.hours` are one writable
duration. Iggy stores milliseconds, through `update_topic`'s `message_expiry`.
Minutes and hours are not stored.

- `retention.ms` is the source of truth.
- `retention.minutes` converts as milliseconds = minutes * 60000. A later
  describe of the synonym divides the stored milliseconds by 60000.
- `retention.hours` converts as milliseconds = hours * 3600000. A later describe
  of the synonym divides the stored milliseconds by 3600000.
- `-1` on any of the three stores `IggyExpiry::NeverExpire`. The server treats
  `0` (`IggyExpiry::ServerDefault`) as "leave the current expiry", so `-1` is
  not sent as 0. A following describe reports `-1` with source
  `DYNAMIC_TOPIC_CONFIG`.
- A positive count must be the canonical decimal of that integer (`1`, not
  `0001` or `+1`). The gateway rejects a product that overflows before it
  multiplies into milliseconds. The resulting duration must be at most
  `u32::MAX` seconds, the same limit `IggyExpiry::from_str` enforces. A longer
  value is `INVALID_CONFIG` (40) and is not stored. A following describe of
  `retention.ms` reports that millisecond count with source
  `DYNAMIC_TOPIC_CONFIG`.
- `0` and every other value are `INVALID_CONFIG` (40).

If one resource sets more than one of the three names and the converted
millisecond values differ, that resource is `INVALID_CONFIG` (40) and nothing
is written. If they convert to the same millisecond count, one write is stored.

Kafka's `AlterConfigs` replaces a topic's full configuration. An empty `configs`
list on that API is a reset to defaults. This gateway patches the named keys
instead of replacing the set, so an empty list is `INVALID_CONFIG` (40) and
writes nothing. A previously set expiry stays in place. A list that names only
keys other than the three retention names fails for that key and also writes
nothing.

`cleanup.policy` and any other key, including `retention.mins`, are
`INVALID_CONFIG` (40). They are not stored. Kafka has no `NOT_CONFIGURABLE`
code. One bad key fails that resource and writes nothing for it. Other
resources in the same request are still processed.

`validate_only` runs the same checks, including the topic lookup, and does not
call `update_topic`. It also does not change which synonym names are remembered.

## Synonym memory

When an alter successfully applies and the request used `retention.minutes`
and/or `retention.hours`, the gateway remembers those names for that topic.
When an alter successfully applies and it used only `retention.ms`, the
remembered names for that topic are cleared. A failed alter does not change
them. A resource that does not store an expiry does not change them either.

The key is the Iggy stream and topic the gateway already resolves from the
Kafka topic name, not the Kafka name itself.

## Limitations

- `IncrementalAlterConfigs` (API 44) is not implemented here. It is planned
  after this work merges. A client that sends 44 still gets the connection
  closed. Only `AlterConfigs` (33) writes retention.
- Synonym memory is process-local. It is lost on gateway restart and is not
  shared across gateway processes. After a restart the synonym list is empty
  until the next alter that uses minutes or hours.
- The memory is not cleared when a topic is deleted outside this gateway
  (`DeleteTopics` is not in this branch). A later recreate can show a stale
  synonym until the next alter.
- Synonym display truncates toward zero. A stored 90000 ms remembered as
  minutes is reported as 1 minute on the synonym, while `retention.ms` still
  shows 90000.
- `retention.ms` remains the only value Iggy stores.

## Other resource errors

| Condition | Code | Notes |
| --------- | ---- | ----- |
| Resource type other than topic | `INVALID_REQUEST` (42) | Message is `only topic resources are supported`. Other resources in the batch still run |
| Topic name fails the same rules as CreateTopics | `INVALID_TOPIC_EXCEPTION` (17) | The message is the validation reason and does not repeat the topic name |
| Topic does not exist | `UNKNOWN_TOPIC_OR_PARTITION` (3) | Checked before config keys, so a missing topic is not `INVALID_CONFIG` |
| Empty `configs` list on alter | `INVALID_CONFIG` (40) | Kafka would replace the set, which is a reset. This gateway patches named keys, so an empty list is rejected and writes nothing |
| More than 100 distinct topic names | `POLICY_VIOLATION` (44) | Every resource in the request |
| A topic name repeated across resources | `INVALID_REQUEST` (42) | Every occurrence of the duplicate, not just the second one. Same choice CreateTopics makes for a repeated topic name |
| Non-empty unknown tagged fields | `INVALID_REQUEST` (42) | Request-level tags fail every resource. A resource or config entry's own tags fail that resource. Empty tagged fields are the normal flexible encoding |
