// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Credentials a client signs in with.
public enum Credentials: Sendable, Hashable {
    case usernamePassword(username: String, password: String)
    case personalAccessToken(String)
}

extension Credentials: CustomStringConvertible {
    /// Never includes the secret.
    public var description: String {
        switch self {
        case .usernamePassword(let username, _): "username \(username)"
        case .personalAccessToken: "a personal access token"
        }
    }
}

/// How the client reconnects after losing its connection.
public struct ReconnectionOptions: Sendable, Hashable {
    /// Whether to reconnect at all. Off, a dropped connection fails the
    /// request that hit it and every later one until `connect()` is called.
    public var enabled: Bool
    /// How many rounds over the known endpoints to attempt; nil means forever.
    public var maxRetries: UInt32?
    /// Pause between rounds.
    public var interval: Duration
    /// Grace period after a successful connection before redialing the same
    /// endpoint, so a flapping server is not hammered.
    public var reestablishAfter: Duration

    public init(enabled: Bool = true, maxRetries: UInt32? = nil, interval: Duration = .seconds(1), reestablishAfter: Duration = .seconds(5)) {
        self.enabled = enabled
        self.maxRetries = maxRetries
        self.interval = interval
        self.reestablishAfter = reestablishAfter
    }
}

/// Configuration of a TCP client.
///
/// ```swift
/// var configuration = ClientConfiguration(address: "localhost:8090")
/// configuration.autoLogin = .usernamePassword(username: "iggy", password: "iggy")
/// configuration.tls = TLSOptions(caFile: "core/certs/iggy_ca_cert.pem", domain: "localhost")
/// ```
public struct ClientConfiguration: Sendable, Hashable {
    /// `host:port` of the server, or of any node of a cluster.
    public var address: String
    /// TLS settings; nil for plain TCP.
    public var tls: TLSOptions?
    /// Credentials to sign in with after every connection, including the
    /// ones a reconnect establishes. Without them a reconnect cannot restore
    /// the session, so a request that hits a dropped connection fails
    /// instead of replaying.
    public var autoLogin: Credentials?
    public var reconnection: ReconnectionOptions
    /// Period of the ping the client sends to stay alive; the server evicts
    /// clients that miss too many.
    public var heartbeatInterval: Duration
    /// Disables Nagle's algorithm. On by default: the lockstep request model
    /// stalls a multi-segment frame behind segment coalescing.
    public var noDelay: Bool
    /// Upper bound on one dial, including the TLS handshake.
    public var connectTimeout: Duration
    /// Upper bound on waiting for one reply across every replay and failover.
    /// Far beyond any healthy round trip; it only trips when the server loses
    /// the reply entirely.
    public var requestTimeout: Duration

    public init(
        address: String = "127.0.0.1:8090", tls: TLSOptions? = nil, autoLogin: Credentials? = nil,
        reconnection: ReconnectionOptions = ReconnectionOptions(), heartbeatInterval: Duration = .seconds(5),
        noDelay: Bool = true, connectTimeout: Duration = .seconds(30), requestTimeout: Duration = .seconds(30)
    ) {
        self.address = address
        self.tls = tls
        self.autoLogin = autoLogin
        self.reconnection = reconnection
        self.heartbeatInterval = heartbeatInterval
        self.noDelay = noDelay
        self.connectTimeout = connectTimeout
        self.requestTimeout = requestTimeout
    }
}

/// Where the client is in its lifecycle.
public enum ClientState: Sendable, Hashable, CustomStringConvertible {
    case shutdown
    case disconnected
    case connecting
    case connected
    case authenticating
    case authenticated

    public var description: String {
        switch self {
        case .shutdown: "shutdown"
        case .disconnected: "disconnected"
        case .connecting: "connecting"
        case .connected: "connected"
        case .authenticating: "authenticating"
        case .authenticated: "authenticated"
        }
    }
}

/// Lifecycle events a client publishes through ``IggyClient/events``.
public enum DiagnosticEvent: Sendable, Hashable, CustomStringConvertible {
    case shutdown
    case disconnected
    case connected
    case signedIn
    case signedOut

    public var description: String {
        switch self {
        case .shutdown: "shutdown"
        case .disconnected: "disconnected"
        case .connected: "connected"
        case .signedIn: "signed_in"
        case .signedOut: "signed_out"
        }
    }
}
