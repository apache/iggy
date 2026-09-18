# Kafka ACLs and Iggy permissions

Status: proposed. Answers the ACL half of [#3549](https://github.com/apache/iggy/issues/3549).
Depends on SASL, because every answer here is about the principal that authenticated.

## Decision

**Read only.** `DescribeAcls` (29) is implemented. `CreateAcls` (30) and `DeleteAcls` (31) are not,
and are not advertised, so they are refused the way every other unlisted key is.

**A principal can see its own permissions and nothing else.** The gateway holds no administrative
credentials, by design: a Kafka client's credentials are the only ones it ever has. Reading another
user's record needs a permission the gateway cannot supply, so a filter naming a different
principal returns an empty result rather than an error.

**Permissions are read once, at authentication.** The login that verifies the credentials also
fetches the principal's own record, and the connection keeps the result. No second session, no
stored password, one extra round trip on a path that already does one.

**Global permissions only, as wildcard bindings.** Iggy's per-stream and per-topic permissions are
not rendered. See [What is not mapped](#what-is-not-mapped).

## Why this is worth doing before the data plane exists

Authorization cannot be *enforced* yet. Produce and Fetch are stubs
([#3535](https://github.com/apache/iggy/issues/3535),
[#3536](https://github.com/apache/iggy/issues/3536)), so there is no operation to gate, and the
verified identity is currently discarded.

`DescribeAcls` is the one authorization surface that needs neither. It is a read: take the
authenticated principal, take the permissions Iggy already holds for it, and answer in Kafka's
vocabulary. That makes it verifiable end to end today with a real admin client, which nothing else
in the authorization story is. It also gives the discarded identity its first consumer, which is
what turns carrying it onto the connection from speculative plumbing into something with a caller.

## The mapping

Iggy grants permissions to a user. Kafka describes them as bindings of
`(resource, principal, host, operation, permission type)`. Three of those five are constant here.

| Kafka field | Value | Why |
| ------------- | ------- | ----- |
| principal | `User:<username>` | Kafka's own convention for a SASL principal |
| host | `*` | Iggy has no host-scoped permissions |
| permission type | `ALLOW` | Iggy has no deny rules, only grants |

The rest come from Iggy's global permissions, **after applying Iggy's own inheritance**. Its
enforcement is hierarchical, not flag for flag: polling is granted by any of the four read or manage
flags on topics or streams, appending by either manage flag, and reading server state by either
server flag. A literal copy of the flags therefore describes a principal that cannot do things Iggy
will in fact let it do, which is the same falsehood the derived group binding below exists to avoid.
The rules live in `core/metadata/src/permissioner/permissioner_rules/`.

| Iggy global permission | Kafka resource | Operation |
| ------------------------ | ---------------- | ----------- |
| `read_servers` or `manage_servers` | `CLUSTER` `kafka-cluster` | `DESCRIBE` |
| any read or manage flag on topics or streams | `TOPIC` `*` | `DESCRIBE` |
| `manage_topics` or `manage_streams` | `TOPIC` `*` | `CREATE`, `DELETE`, `ALTER` |
| `poll_messages`, or any read or manage flag | `TOPIC` `*` | `READ` |
| `send_messages`, `manage_topics` or `manage_streams` | `TOPIC` `*` | `WRITE` |
| same as the topic `DESCRIBE` row | `GROUP` `*` | `READ` |

Three notes on that table.

**Stream permissions fold into topic ones.** Iggy puts topics inside streams, and Kafka has no
resource above a topic. The topic mapping places every Kafka topic inside one Iggy stream, so a
stream-level grant is in practice a grant over the topics a Kafka client can reach. Rendering it as
a topic binding says the true thing in Kafka's vocabulary; inventing a resource type for it would
not.

**A consumer group grant is derived, not stored.** Iggy has no group-level permission. A Kafka
consumer needs `READ` on its group as well as on the topic, so a principal Iggy would admit to a
group is shown as having it. It is derived from the topic read grant, not from `poll_messages`,
because group *membership* operations route through Iggy's own `get_topic` rule, which admits on the
read and manage flags and never consults polling. Deriving it from polling granted a group to
principals Iggy denies.

Offset commit and fetch are the exception: Iggy routes those through `poll_messages`, while Kafka
gates them on this same `GROUP READ`. A principal holding polling but no read grant is therefore
shown no group binding even though Iggy would let it commit an offset. That under-reports, which is
the safe direction, where deriving from polling would over-report membership.

**`manage_servers` renders nothing of its own.** Iggy reads it in exactly one rule, as an alias for
`read_servers`, so it gates no mutation anywhere. Rendering `CLUSTER ALTER` from it would advertise
an ability with nowhere to be used, and the write ACL APIs that operation authorizes are not even
advertised. This is the same argument that keeps `manage_users` out of the table.

**`manage_users` and `read_users` are not rendered.** Kafka's `USER` resource covers credential and
delegation-token administration, which this gateway does not expose at all. Mapping onto it would
claim an ability that has nowhere to be used.

Wildcards use `LITERAL` with the name `*`, which is how Kafka itself represents "every resource of
this type". `PREFIXED` is never emitted, because nothing in Iggy's model is prefix-scoped.

## Filters

`DescribeAcls` carries a filter. Each field is matched, with Kafka's `ANY` sentinel matching
everything:

- **resource type** filters the table above. `ANY` returns all of it.
- **resource name** matches the rendered name, so `*` matches the wildcard bindings.
- **principal** matches the authenticated principal. Naming anyone else returns empty, per the
  decision above.
- **host** matches `*`.
- **pattern type** selects nothing when it is `PREFIXED` or unknown, because nothing rendered is
  prefix-scoped.
- **operation** and **permission type** filter the rendered rows. Only `ANY` is a wildcard here:
  Kafka's `ALL` is a concrete operation, so a filter naming it selects nothing, because nothing
  rendered is an `ALL` binding.

`MATCH` widens the name comparison rather than narrowing it. A named filter under `MATCH` also
selects a literal pattern named `*`, which is how `kafka-acls.sh --topic orders
--resource-pattern-type match` asks what affects one topic. Since every binding here is a wildcard,
exact comparison alone would answer nothing to the one query that should find them all.

An empty result is `error_code` 0 with no resources, not an error. Kafka draws a firm line between
"no bindings match" and "the request failed", and an admin tool prints them very differently.

## What is not mapped

**Per-stream and per-topic permissions.** Iggy keys them by numeric slab id, not by name, so
rendering a Kafka topic name means resolving every id through a lookup, one call per resource, on
what should be a cheap read. Worse, the resolution can be ambiguous in the direction we need it:
the topic mapping is one-way, from Kafka name to Iggy stream and topic, and an override means an
Iggy topic name does not identify the Kafka name it came from. The same limitation is already
recorded against `OffsetFetch` with a null topic list. Closing it needs a reverse index, which
belongs with whichever change first needs one.

The consequence is honest but worth stating: a principal whose access is granted per topic rather
than globally is described as having no topic bindings. That under-reports rather than
over-reports, which is the safe direction for an authorization view.

**Writes.** `CreateAcls` and `DeleteAcls` would mean updating another user's permissions, which
needs `manage_users` on the *Kafka client's own* Iggy user. Coherent, but it turns a read-only
surface into one that mutates accounts, and it cannot be verified the way a read can. Left out
deliberately rather than half-built.

**Enforcement.** Nothing here gates an operation. This describes what Iggy would allow; Iggy is
still the thing that decides, once there is an operation for it to decide about.

## One divergence from a real broker

A real broker gates `DescribeAcls` on `CLUSTER DESCRIBE`. This gateway answers any authenticated
principal, including one the same response describes as lacking that permission.

That is deliberate rather than an oversight. The only thing a principal can learn here is its own
access, which it can discover anyway by attempting an operation, and enforcing the gate would mainly
hide a principal's own permissions from itself. Recorded so the difference is a decision rather than
a surprise.

## Staleness

The permissions a connection reports are the ones its principal had when it authenticated. A
permission changed afterwards is not visible until the client reconnects.

This is deliberate. The alternative is either holding an Iggy session open per connection, which
the authentication design rejects on cost, or keeping the password to log in again, which it
rejects outright. It also matches what Iggy already does on its own data plane, where a revocation
becomes visible only once the owning shard applies it.

Say so in the README rather than leaving an operator to discover that an ACL view can lag.

## Testing

The state machine and the mapping are pure, so they unit test directly: a permission set in, a set
of bindings out, including the empty and wildcard cases.

End to end, `kafka-acls.sh --list` from the Kafka distribution image, against a gateway with SASL
enabled and a real Iggy server behind it. This is the part that matters, because it is the first
authorization behaviour that can be checked against a real client rather than a stub. It runs five
principals, chosen so that each one fails differently if the mapping is wrong:

| Principal | Holds | What it pins down |
| ----------- | ------- | ------------------- |
| root | everything | every row of the table renders |
| `consumer-only` | `read_topics` | the derived group binding appears |
| `poller-only` | `poll_messages` | and *only* here: a topic read with no group binding. Deriving the group from polling instead makes this the one listing that grows a GROUP section |
| `producer-only` | `send_messages` | a write with no read and no group |
| `no-grants` | nothing | an empty list is a success, not an error |

Each assertion is paired: the binding that must be present and the one that must be absent. A
principal asserted only on absence would also be satisfied by a client container that never
started, which is how an earlier revision of this suite passed over nothing.

`kafka_client_e2e_tests.rs` skips itself when Docker is missing, so a local run without it is green
rather than broken. CI sets `KAFKA_E2E_REQUIRED=1`, which turns that skip into a failure, because
there the missing prerequisite is a broken job rather than a developer without Docker.

## References

- Authentication: [`AUTHENTICATION.md`](AUTHENTICATION.md)
- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md)
- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Iggy permissions: `core/common/src/types/permissions/`
- Self-read exemption: `core/server/src/dispatch/authz.rs`
