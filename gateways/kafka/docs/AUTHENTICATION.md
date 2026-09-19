# Kafka authentication and Iggy identity

Status: proposed. Answers [#3549](https://github.com/apache/iggy/issues/3549) and needs a TLS listener
first (see [Transport security](#transport-security)). The mechanism choice below is forced by how Iggy
stores credentials, not preferred.

## Decision

Four parts.

**Mechanism: SASL/PLAIN only.** SCRAM cannot be implemented against Iggy's credential store at all, and
nothing else in Kafka's mechanism set maps without new server-side storage.

**Credentials pass straight through.** The username and password a Kafka client sends in its PLAIN payload
are an Iggy username and password. The gateway forwards them to a login and keeps no credential store and
no mapping table of its own. An Iggy account is created for each Kafka principal, and the Iggy server stays
the only place a credential is defined or verified.

**Identity: one Iggy user per Kafka principal.** Not one shared account for everyone. Credentials are
verified on every connection, against Iggy, and that verification is deliberately not cached. See
[Verification is per connection](#verification-is-per-connection-and-cannot-be-cached).

**Authorization stays in Iggy.** The gateway does not implement its own ACL model. It authenticates a Kafka
client into an Iggy identity and lets the server's existing permission checks decide what that identity can do.

## Why SCRAM is out

Iggy stores exactly one credential per user, an Argon2id PHC string (`core/metadata/src/stm/user.rs:57`,
hashed at `core/server_common/src/crypto.rs:30`, verified at `core/server/src/dispatch/session_ops.rs:126`).

SCRAM-SHA-256 and SCRAM-SHA-512 require the server to hold `StoredKey` and `ServerKey`, both derived from
`PBKDF2(password, salt, iterations)`, and to send the salt and iteration count to the client in the
server-first message. None of that is derivable from an Argon2 hash, and the stored hash is never exposed
through any API. There is no second credential column and no per-mechanism credential table anywhere in
the tree.

Supporting SCRAM therefore means a new replicated credential table plus derivation at user-creation time,
which is a change to Iggy's own wire format and state machine. That is a product decision and does not
belong to this gateway.

SASL/OAUTHBEARER is the one other candidate, because a bearer token maps onto an Iggy personal access
token, which is verified by a BLAKE3 hash lookup (`core/common/src/utils/hash.rs:25`) rather than Argon2.
It is out of scope here and worth revisiting, since it avoids putting a password on the wire.

## Why not one Iggy session per Kafka connection

The obvious reading of "one-to-one user mapping" is one authenticated Iggy connection behind every Kafka
connection. The cost rules it out.

| Per login | Cost |
| ----------- | ------ |
| Argon2id verify, run inline on the shard thread | 19 MiB, t=2, p=1 |
| Replicated VSR `Register` | one consensus commit |
| Leader settlement round trip, plus a second login if it redirects | one or two round trips |
| Session slots, cluster-wide across all transports | 8192, oldest committed evicted past it |

The Argon2 verify runs on the shard's compio runtime, which has no blocking pool. A burst of Kafka
connections therefore stalls that shard's entire request loop, including produce and fetch traffic that has
nothing to do with authentication. The 8192 ceiling (`core/consensus/src/impls.rs:178`) is cluster-wide, so
connection churn on the Kafka side turns directly into session-table pressure and eviction on the Iggy side.

## Why not a shared service account

The other option in the issue is authenticating every Kafka client against one `kafka-bridge` account. It
removes the cost above and creates a worse problem: every Kafka client would reach Iggy as the same
privileged principal, so Iggy's own permission model could no longer distinguish them. The gateway would
have to carry an ACL model of its own, and become the only thing standing between any authenticated Kafka
client and every stream the bridge account can reach.

That is a larger security surface than the one it removes.

## Verification is per connection, and cannot be cached

A Kafka principal is the SASL `authcid`. Every SASL exchange verifies the presented credentials by
logging into Iggy with them, and that verification happens on every connection.

The gateway reaches Iggy over the binary TCP transport only. Nothing uses the HTTP API, so there is no way
to check a password without establishing a full session. First contact for a connection therefore costs one
Argon2 verify, run inline on the shard thread, and one replicated `Register`.

An earlier revision of this document proposed caching the authenticated client per principal and attaching
later connections to it. That is wrong, and implementing it surfaced why. A cache keyed on the username
alone lets a second connection present *any* password for a principal already seen, which is an
authentication bypass rather than an optimisation. Keying it on the credential instead means holding
something password-equivalent in gateway memory for every principal seen. Neither is acceptable, so
verification stays on the connection path.

Two consequences worth naming rather than discovering:

- **Login rate tracks connection rate.** A client that reconnects frequently drives an Argon2 verify per
  reconnect, on a shard runtime with no blocking pool, where it competes with ordinary produce and fetch
  traffic on that shard. Measured on debug builds, a single login takes about 10 ms and 32 concurrent
  logins take about 75 ms each, with throughput flattening near 410 per second. The shape is the point,
  latency climbing while throughput plateaus, not the absolute numbers. `MANUAL_TESTING.md` has the table.
- **Nothing holds the verified session.** The connection that proved a credential drops it immediately,
  because no handler consumes an Iggy session yet. When Produce and Fetch need one
  ([#3535](https://github.com/apache/iggy/issues/3535),
  [#3536](https://github.com/apache/iggy/issues/3536)), the question of how a principal's data client is
  pooled becomes live, and it is a separate question from how its credentials are verified. Pooling clients
  is gateway-side code over the existing public SDK and needs no SDK change.

If either consequence starts to hurt, the lever is below, and it is a smaller change than a credential
cache because it moves cost off the server without weakening what the gateway checks.

### Personal access tokens, if login cost becomes a problem

Not part of the baseline. The gateway logs in with the username and password the Kafka client presented,
and that is the whole credential path. This section records why a token is the lever to reach for if the
login rate turns out to hurt, and what it costs to pull.

Argon2 is the expensive half of a login and the only half that runs inline on a shard thread with no
blocking pool. A token login replaces it with a hash lookup. The replicated `Register` is unchanged either
way, so tokens buy server CPU, not consensus, and only under churn: a cached client that stays connected
pays neither.

Minting is self-scoped, so a token can only be created for a principal while authenticated as that
principal. First contact therefore always pays the password path regardless, and a token would pay off only
afterwards, on internal SDK reconnects, pool growth and cache repopulation. Credentials also cannot be
swapped on a live client, so a token would apply to the next client built for that principal, never the one
that minted it.

What makes it a real cost rather than a free optimisation: the raw token is returned exactly once and is
never retrievable, so a restarted gateway cannot recover what it minted and must delete before creating
again, handling `PersonalAccessTokenAlreadyExists` (51). Names have to be instance-scoped or two gateways
fight over one token. A user holds at most `max_tokens_per_user` tokens, 100 by default
(`core/server/config.toml:305`), and tokens leaked by instances that never cleaned up count against that.
Token expiry has to outlive the cache TTL with margin. The gateway would also be holding bearer credentials
carrying the full rights of their users with no scope narrowing, in memory, for every principal it has seen.

Measure the login rate before taking any of that on.

## Handshake

Per connection, with SASL enabled. Only ApiVersions and SaslHandshake are legal before authentication.

| State | Input | Action |
| ------- | ------- | -------- |
| `AwaitHandshake` | ApiVersions (18) | answer, stay |
| | SaslHandshake (17) v1, supported mechanism | answer `error_code` 0, go to `AwaitToken` |
| | SaslHandshake (17) v1, unknown mechanism | answer 33 with the mechanism list, then close |
| | SaslHandshake (17) v0 | answer 35 with the mechanism list, then close |
| | anything else | answer 34 shaped for that API, then close |
| `AwaitToken` | SaslAuthenticate (36), credentials valid | answer 0, empty `auth_bytes`, go to `Authenticated` |
| | SaslAuthenticate (36), credentials rejected | answer 58 with a generic message, then close |
| | anything else | answer 34, then close |
| `Authenticated` | any supported API, including ApiVersions again | serve |
| `AwaitHandshake` | a second ApiVersions | answer 34, then close (one is allowed, as on a real broker) |
| `AwaitToken` | SaslAuthenticate above the advertised ceiling | close, no schema exists at that version |
| any | Metadata or Produce while unauthenticated | close with no body (no error field, and acks=0 forbids one) |
| | SaslHandshake or SaslAuthenticate | answer 34, keep the connection |

Notes that decide the implementation.

- **Handshake v1 only.** The handshake version selects the token framing (KIP-152). Refusing v0 means the
  legacy bare-token mode is never entered, so the frame reader stays header-parsing-only. Only pre-1.0
  Kafka clients are excluded.
- **`session_lifetime_ms` is 0.** No KIP-368 re-authentication, which keeps mid-connection identity change
  out of scope. That matters beyond convenience, see the re-authentication note below.
- **ApiVersions is answered twice.** The Java client sends it once before the handshake and again after
  authenticating, so it must stay legal in both states.
- **Produce with `acks=0` stays silent.** Answering an unauthenticated fire-and-forget produce desyncs the
  client's correlation stream, so that case closes without writing. The existing rationale in
  `protocol/api.rs` applies unchanged.
- **Pre-authentication deadline.** An unauthenticated connection currently holds a `max_connections` permit
  for up to the idle timeout, ten minutes by default. Authentication needs its own, much shorter deadline.

## Error mapping

At authentication time a rejected credential becomes `SASL_AUTHENTICATION_FAILED` (58) with a generic
message. An Iggy that cannot be reached, or an overloaded gateway, closes the connection without a body
instead: Kafka clients treat 58 as fatal and raise it to the application, so borrowing it for a transient
condition turns a blip into a permanent failure for credentials that were always correct. A close reads as
a transport failure, which is retriable, and still says nothing about whether the account exists. Iggy's login path already runs a dummy hash for unknown users to avoid a user-enumeration oracle,
so the gateway must not reintroduce one by distinguishing unknown user from wrong password in the message
or by returning early.

After authentication, Iggy reports exactly one permission-denied code, `IggyError::Unauthorized` (41,
`core/common/src/error/iggy_error.rs:97`), alongside `Unauthenticated` (40). Kafka distinguishes
`TOPIC_AUTHORIZATION_FAILED` (29), `GROUP_AUTHORIZATION_FAILED` (30) and `CLUSTER_AUTHORIZATION_FAILED`
(31). The gateway therefore picks the Kafka code from the operation it was performing, not from the Iggy
error, because the Iggy error cannot tell them apart.

Iggy's data-plane permission checks read the local shard's view, so a permission revocation is visible on
the control plane immediately and on the data plane only after that shard applies it. Combined with the
principal cache TTL above, revocation is eventually consistent by two mechanisms rather than one. Say so
in the README rather than leaving an operator to discover it.

## Transport security

SASL/PLAIN puts the password in the clear on the wire, and so does Iggy's own login. Both hops need TLS.

The gateway listener has no TLS at all today, which makes a TLS listener a prerequisite rather than a
follow-up. The Iggy side already supports it on both ends (client at
`core/common/src/types/configuration/tcp_config/tcp_client_config.rs:30`, server at
`core/configs/src/server_config/tcp.rs:31`), shipped disabled.

The two hops are configured independently. `IGGY_KAFKA_IGGY_TLS_ENABLED` and its companions encrypt
the gateway's link to Iggy and are what make the gateway usable at all against a TLS-only Iggy
server, where every verification would otherwise fail as unreachable. The Kafka-side listener is the
half that is still missing.

Two limits worth recording. There is no mTLS anywhere in the tree, every rustls config uses
`with_no_client_auth()`, so certificate-based Kafka client authentication cannot map to an Iggy identity
without an external terminator. And an Iggy password is capped at 100 bytes with a `u8` length prefix on
the wire (`core/common/src/http/users/defaults.rs:20`), so a Kafka client with a longer PLAIN secret is
rejected.

## Re-authentication

Iggy's only correct re-authentication is logout followed by login, which drops and re-mints the VSR session.
Sending a login on a still-bound connection takes a replay branch that verifies the new credentials and
returns the new user id to the caller while leaving the server bound to the previous user
(`core/server/src/dispatch/session_ops.rs:232`). Building Kafka re-authentication on that would silently
cross identities.

Returning `session_lifetime_ms` of 0 avoids the question entirely for now. Whoever implements KIP-368 later
has to resolve it first.

## Out of scope

- SCRAM-SHA-256 and SCRAM-SHA-512, blocked on credential storage that does not exist.
- SASL/OAUTHBEARER and GSSAPI.
- mTLS and certificate-based identity.
- Mapping Kafka ACL administration APIs onto Iggy permissions. Authorization is enforced, but the
  `DescribeAcls` and `CreateAcls` API keys stay unimplemented.
- KIP-368 re-authentication.

## Open questions

1. **Who creates the Iggy accounts?** Each Kafka principal needs an Iggy user, since its credentials are
   the ones the Kafka client sends. An operator can create them with the existing user API
   (`core/common/src/traits/user_client.rs:38`), or the gateway can provision on first SASL login, which
   needs the `manage_users` permission and means the gateway holds a credential that can create users.
   Auto-provisioning also cannot work here: the gateway only ever sees a password it cannot validate
   against anything until a user already exists, so provisioning would accept any credential as a new
   account. Default: an operator creates the accounts, and the gateway only consumes them.
2. **What is the principal cache TTL?** It bounds how long a revoked Iggy user keeps working through the
   gateway. Default: 5 minutes.
3. **What happens when SASL is enabled on a gateway that already serves unauthenticated clients?** Enabling
   it breaks every existing client at once, since the version advertisement changes. Default: SASL is
   off unless configured, and the two SASL keys are not advertised when it is off.

## References

- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md)
- Credential storage: `core/metadata/src/stm/user.rs`, `core/server_common/src/crypto.rs`
- Login and session binding: `core/server/src/dispatch/session_ops.rs`, `core/server/src/session_manager.rs`
- Permissions: `core/common/src/types/permissions/`, `core/metadata/src/permissioner/`
- Listener and dispatch: `gateways/kafka/src/server.rs`, `gateways/kafka/src/protocol/api.rs`
