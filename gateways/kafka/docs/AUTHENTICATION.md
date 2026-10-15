# Kafka authentication and Iggy identity

Status: implemented. Answers [#3549](https://github.com/apache/iggy/issues/3549), and
[#4510](https://github.com/apache/iggy/issues/4510) for the transport: a `SASL_SSL` listener
encrypts the Kafka hop and `IGGY_KAFKA_IGGY_TLS_ENABLED` the Iggy hop, so PLAIN leaves the trusted
network only inside TLS (see [Transport security](#transport-security)). A client certificate on an
`SSL` listener names a principal too (see [Client certificates](#client-certificates)). The
mechanism choice below is forced by how Iggy stores credentials, not preferred.

## Decision

Five parts.

**Mechanism: SASL/PLAIN only.** SCRAM cannot be implemented against Iggy's credential store at all, and
nothing else in Kafka's mechanism set maps without new server-side storage.

**Credentials pass straight through.** The username and password a Kafka client sends in its PLAIN payload
are an Iggy username and password. The gateway forwards them to a login and keeps no credential store and
no mapping table of its own. An Iggy account is created for each Kafka principal, and the Iggy server stays
the only place a credential is defined or verified.

**Identity: one Iggy user per Kafka principal.** Not one shared account for everyone. Credentials are
verified on every connection, against Iggy, and that verification is deliberately not cached. See
[Verification is per connection](#verification-is-per-connection-and-cannot-be-cached).

**Authorization is meant to stay in Iggy.** The gateway will not implement its own ACL model. The intent is
to carry the authenticated Iggy identity onto the data plane and let the server's existing permission checks
decide what it can do. That is later work: today the verified client is shut down as soon as the credential
clears, and nothing carries the principal past authentication.

**A client certificate is an identity too, with the same reach.** On an `SSL` listener that verifies client
certificates, the subject maps to an Iggy username through Kafka's `ssl.principal.mapping.rules` and the
gateway confirms the user exists. Nothing is logged into on the client's behalf, since Iggy's own transports
take no certificate ([#3930](https://github.com/apache/iggy/issues/3930)); the principal gates admission and
answers `DescribeAcls`, exactly as a SASL one does, and acting as it on the data plane is the same later work.

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
- **Nothing holds the verified session.** The connection that proved a credential drops it at once.
  Produce and Fetch act as the bridge's Iggy user for every client. A data client per principal needs
  pooling, a separate question from how credentials are verified. Pooling is gateway-side code over the
  public SDK and needs no SDK change.

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
The gateway would also be holding bearer credentials carrying the full rights of their users with no scope
narrowing, in memory, for every principal it has seen.

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
| `AwaitHandshake` | ApiVersions past the allowance | answer 34, then close (two are allowed: the one a real broker allows, plus the KIP-511 downgrade retry) |
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
- **The pre-authentication allowance counts answers, not successes.** A refusal a client may retry at a
  lower version is one it may also repeat, and every frame resets the pre-authentication deadline, so
  spending the allowance only on a usable answer leaves the connection unbounded.
- **Produce with `acks=0` stays silent.** Answering an unauthenticated fire-and-forget produce desyncs the
  client's correlation stream, so that case closes without writing. The existing rationale in
  `protocol/api.rs` applies unchanged.
- **Pre-authentication deadline.** An unauthenticated connection holds a `max_connections` permit, so it
  gets `IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS` between frames rather than the ten-minute idle timeout.

## Error mapping

At authentication time a rejected credential becomes `SASL_AUTHENTICATION_FAILED` (58) with a generic
message. An Iggy that cannot be reached, an overloaded gateway, or a peer still inside the delay a previous
rejection earned, closes the connection without a body instead: Kafka clients treat 58 as fatal and raise it to the application, so borrowing it for a transient
condition turns a blip into a permanent failure for credentials that were always correct. A close reads as
a transport failure, which is retriable, and still says nothing about whether the account exists. Iggy's login path already runs a dummy hash for unknown users to avoid a user-enumeration oracle,
so the gateway must not reintroduce one by distinguishing unknown user from wrong password in the message
or by returning early.

### Planned: authorization errors

Not implemented. Today `bridge/error.rs` picks the Kafka code from the Iggy error, and neither 30 nor 31
is produced anywhere.

After authentication, Iggy reports exactly one permission-denied code, `IggyError::Unauthorized` (41,
`core/common/src/error/iggy_error.rs:97`), alongside `Unauthenticated` (40). Kafka distinguishes
`TOPIC_AUTHORIZATION_FAILED` (29), `GROUP_AUTHORIZATION_FAILED` (30) and `CLUSTER_AUTHORIZATION_FAILED`
(31). Once handlers act as the authenticated principal, the gateway will have to pick the Kafka code from
the operation it was performing, not from the Iggy error, because the Iggy error cannot tell them apart.

Iggy's data-plane permission checks read the local shard's view, so a permission revocation is visible on
the control plane immediately and on the data plane only after that shard applies it. Say so in the README
rather than leaving an operator to discover it.

## Transport security

SASL/PLAIN puts the password in the clear on the wire, and so does Iggy's own login. Both hops need TLS,
and the two are configured independently because they protect different hops.

**The Kafka hop** is a listener's security protocol. The gateway binds one listener per protocol, the way
Kafka's `listeners` does (`IGGY_KAFKA_LISTENERS`), and `SASL_SSL` is the one for PLAIN outside a trusted
network. TLS is rustls, one certificate for every `SSL` and `SASL_SSL` listener
(`IGGY_KAFKA_TLS_CERT_FILE`, `IGGY_KAFKA_TLS_KEY_FILE`), re-read on an interval so a rotation goes live
without a restart. Each listener advertises its own address, so a client on the TLS port is never
pointed at the plaintext one. A `SASL_PLAINTEXT` listener still exists for trusted networks, and the
gateway warns at startup that it carries the password in the clear.

**The Iggy hop** is `IGGY_KAFKA_IGGY_TLS_ENABLED` and its companions. They cover every connection the
gateway makes to Iggy: the credential check's and the bridge's, which carries `IGGY_KAFKA_IGGY_PASSWORD`.
They are what make SASL usable against a TLS-only Iggy server at all, where every verification would
otherwise fail as unreachable, and are refused while nothing dials Iggy.

Two limits worth recording. Iggy's own transports take no client certificate
([#3930](https://github.com/apache/iggy/issues/3930)), so a certificate identity on the Kafka side is
verified by the gateway and looked up through the bridge's session rather than presented to Iggy. And an
Iggy password is capped at 100 bytes with a `u8` length prefix on the wire
(`core/common/src/http/users/defaults.rs:20`), so a Kafka client with a longer PLAIN secret is rejected.

## Client certificates

`IGGY_KAFKA_TLS_CLIENT_AUTH` is Kafka's `ssl.client.auth`: `none`, `requested` or `required`. It applies
to `SSL` listeners, which verify the client certificate against `IGGY_KAFKA_TLS_CLIENT_CA_FILE` during the
handshake, and is refused when no `SSL` listener exists.

A `SASL_SSL` listener never asks for a client certificate. Kafka applies `ssl.client.auth` to a
`SASL_SSL` listener only when that listener overrides it, and leaves it at `none` otherwise; SASL names
the principal there. A deployment that wants certificate identities uses `SSL`.

On an `SSL` listener the certificate names the principal, in three steps the connection takes before its
first frame:

1. **Render the subject** the way Java's `X500Principal.getName()` does, RFC 2253 with the most specific
   RDN first and no spaces: `CN=alice,OU=eng,O=Example`. Operators write mapping rules against Java's
   rendering, so the gateway follows JDK 11 to 21's `AVA.toRFC2253String`: its keyword set, its escaping
   (NUL as `\00`), Latin-1 for `PrintableString`, `IA5String` and `GeneralString`, and hex for the string
   types Java does not decode, `UniversalString` among them. Malformed UTF-8 or BMP values are the one case
   not checked against Java.
2. **Map it** through `IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES`, Kafka's `ssl.principal.mapping.rules`
   syntax: `RULE:pattern/replacement/[LU]` applies when the pattern matches the whole subject, `DEFAULT`
   keeps it, the first match wins, and a subject no rule matches closes the connection with the subject in
   the log at `warn`. The default is `DEFAULT`. Iggy only requires a username of 3 to 50 characters, but a
   raw subject is usually longer than that and is rarely the name the operator created, so a deployment
   sets at least one rule. A replacement with a bare `$`, or a `${name}` with no such named group, is
   refused at startup, where Kafka would fail the first handshake it applies to. One that names a group
   the pattern lacks is refused too, which is stricter than Kafka: a broker writes it literally, so
   `RULE:^CN=(.*)$/$2/` maps to `$2`. The reverse case, `/$1\$2/`, makes Kafka throw while the gateway
   maps `alice` to `alice$2`, which is harmless because that Iggy user must still exist. The replacement is applied as Java's `replaceAll`, to every match in the subject, so
   `RULE:(.*)/$1@x/` maps `alice` to `alice@x@x` as it does on a broker. Patterns are the subset of
   Java's regex syntax whose meaning the gateway can reproduce: `\d \s \w` and their negations match
   ASCII only, and `.` outside `(?s)` stops at `\r`, U+0085, U+2028 and U+2029 as well as `\n`, both as
   in Java. Rust would read them over Unicode, so a CA-signed `CN=\u212Aafka` (Kelvin sign) would match
   `\w+` and lowercase to `kafka`. Case-insensitive matching (`(?i)`), the `u` and `U` flags, `\p{..}`
   and `\P{..}`, POSIX classes such as `[[:alpha:]]`, word boundaries (`\b`, `\B`), the `m`, `x` and
   `R` flags, `\v`, a quantifier directly after another (`.*+`, possessive in Java) and the `--` and
   `~~` class operations are refused at startup with the construct named: each has no faithful
   translation. Non-ASCII names still map, so
   `CN=Zoë` under `RULE:^CN=([^,]+).*$/$1/` maps to `Zoë` as on a broker.
3. **Look the name up** through the bridge's own session. The user must exist and be active, or the
   connection is closed after the handshake with the name in the log at `warn`: a certificate the CA signed
   for an account that does not exist is a provisioning mistake, not something the client can correct. The
   bridge is therefore required for certificate identities, and its Iggy user needs `read_users`. A failed
   lookup, including one refused for want of that permission, closes the connection too and never admits
   the name unchecked.

With `requested`, a client that presents no certificate is Kafka's `ANONYMOUS` principal, which holds no
permissions: `DescribeAcls` describes it as holding nothing, and `DeleteTopics` and `AlterConfigs`, which
need `manage_topics`, are refused. Everything else is served as the bridge's user. With `required`, rustls
fails the handshake instead.

The same holds on every listener that names nobody once any listener names principals: a `PLAINTEXT`
listener beside a `SASL_SSL` or mTLS one serves `ANONYMOUS`, so it cannot do what an authenticated user
without `manage_topics` cannot. Only a gateway whose listeners all name nobody leaves those requests
ungated.

The principal a certificate yields is the same `AuthenticatedPrincipal` a SASL login yields, with its
permissions read at admission, so everything written about the SASL identity above holds: it is a snapshot,
it is not cached across connections, and nothing acts as it on the data plane yet
([#4518](https://github.com/apache/iggy/issues/4518)).

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
- Presenting a client certificate to Iggy's own transports, which take none
  ([#3930](https://github.com/apache/iggy/issues/3930)); the gateway verifies it and looks the user up
  instead.
- Authorization. No handler asks Iggy about permissions yet, since the verified identity is not carried
  onto the data plane; SASL is an admission gate only. The `DescribeAcls` and `CreateAcls` API keys also
  stay unimplemented.
- KIP-368 re-authentication.

## Open questions

1. **Who creates the Iggy accounts?** Each Kafka principal needs an Iggy user, since its credentials are
   the ones the Kafka client sends. An operator can create them with the existing user API
   (`core/common/src/traits/user_client.rs:38`), or the gateway can provision on first SASL login, which
   needs the `manage_users` permission and means the gateway holds a credential that can create users.
   Auto-provisioning also cannot work here: the gateway only ever sees a password it cannot validate
   against anything until a user already exists, so provisioning would accept any credential as a new
   account. Default: an operator creates the accounts, and the gateway only consumes them.
2. **What happens when SASL is enabled on a gateway that already serves unauthenticated clients?** Enabling
   it breaks every existing client at once, since the version advertisement changes. Default: SASL is
   off unless configured, and the two SASL keys are not advertised when it is off.

## References

- Scope and phases: [`SCOPE.md`](SCOPE.md)
- Record mapping: [`BRIDGE_MAPPING.md`](BRIDGE_MAPPING.md)
- Credential storage: `core/metadata/src/stm/user.rs`, `core/server_common/src/crypto.rs`
- Login and session binding: `core/server/src/dispatch/session_ops.rs`, `core/server/src/session_manager.rs`
- Permissions: `core/common/src/types/permissions/`, `core/metadata/src/permissioner/`
- Listener and dispatch: `gateways/kafka/src/server.rs`, `gateways/kafka/src/protocol/api.rs`
