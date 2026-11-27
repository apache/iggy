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

import Foundation
import NIOCore

extension ClientConfiguration {
    /// Creates a configuration from a connection string, the format every
    /// Iggy SDK accepts:
    ///
    /// ```text
    /// iggy://user:password@host:port?tls=true&tls_domain=localhost
    /// iggy+tcp://<personal access token>@[::1]:8090?reconnection_retries=unlimited
    /// ```
    ///
    /// The credentials become ``autoLogin``. The options are `tls`,
    /// `tls_domain`, `tls_ca_file`, `reconnection_retries` (a count or
    /// `unlimited`), `reconnection_interval`, `reestablish_after`,
    /// `heartbeat_interval` (durations such as `5s`, `500ms` or `1h 30m`),
    /// `nodelay`, `max_pending_operations` and `max_pending_operation_bytes`.
    /// An unknown option fails the parse, so one string is valid in every
    /// SDK or in none.
    ///
    /// Some options read differently than in Rust:
    ///
    /// - `tls` and `nodelay` take `true` or `false`, and any other value
    ///   fails, where Rust reads it as false.
    /// - Without `nodelay` the client keeps its own default, Nagle off.
    /// - `reconnection_interval` is the full pause between rounds over the
    ///   endpoints. Rust takes it as the base of a jittered backoff capped at
    ///   10 seconds.
    /// - `reestablish_after` is how long after the last successful
    ///   connection the client waits before redialing that endpoint. Rust
    ///   takes it as the uptime that resets the backoff.
    /// - A request that needs the connection waits for the redial, up to the
    ///   request timeout (30 seconds by default), and fails when the wait, or
    ///   the `reconnection_interval` pause, outlasts it. An explicit
    ///   `connect()` waits it out.
    /// - The pending operation limits are checked and then ignored, because
    ///   the client keeps no operations to replay after a session reset.
    public init(connectionString: String) throws {
        self = try ConnectionString.parse(connectionString)
    }
}

/// The parser of `core/common/src/types/configuration/auth_config/connection_string.rs`
/// and the TCP options of `tcp_connection_string_options.rs`.
enum ConnectionString {
    private static let defaultPrefix = "iggy://"
    private static let transportPrefix = "iggy+"

    static func parse(_ connectionString: String) throws -> ClientConfiguration {
        try checkTransport(connectionString)
        let protocolParts = connectionString.components(separatedBy: "://")
        guard protocolParts.count == 2 else {
            throw IggyError(.invalidConnectionString, context: "expected one ://")
        }
        let parts = protocolParts[1].split(separator: "@", omittingEmptySubsequences: false)
        guard parts.count == 2 else {
            throw IggyError(.invalidConnectionString, context: "expected one @ after the credentials")
        }
        let credentials = try parseCredentials(parts[0])
        let serverAndOptions = parts[1].split(separator: "?", omittingEmptySubsequences: false)
        guard serverAndOptions.count <= 2 else {
            throw IggyError(.invalidConnectionString, context: "expected at most one ?")
        }
        let address = String(serverAndOptions[0])
        try validate(address: address)

        var configuration = ClientConfiguration(address: address, autoLogin: credentials)
        if serverAndOptions.count == 2 {
            try parseOptions(serverAndOptions[1], into: &configuration)
        }
        return configuration
    }

    /// Accepts `iggy://` and `iggy+tcp://`. Only a transport spelled in
    /// letters is named in the error, since a malformed scheme can hold the
    /// credentials.
    private static func checkTransport(_ connectionString: String) throws {
        if connectionString.hasPrefix(defaultPrefix) {
            return
        }
        guard connectionString.hasPrefix(transportPrefix) else {
            throw IggyError(.invalidConnectionString, context: "expected an iggy:// or iggy+tcp:// scheme")
        }
        let transport = connectionString.dropFirst(transportPrefix.count).components(separatedBy: "://")[0]
        guard transport != "tcp" else {
            return
        }
        guard !transport.isEmpty, transport.allSatisfy({ $0.isASCII && $0.isLetter }) else {
            throw IggyError(.invalidConnectionString, context: "expected an iggy:// or iggy+tcp:// scheme")
        }
        throw IggyError(.invalidConnectionString, context: "transport \(transport) is not supported by the Swift SDK, use tcp")
    }

    /// `username:password`, or a personal access token alone.
    private static func parseCredentials(_ text: Substring) throws -> Credentials {
        let pieces = text.split(separator: ":", omittingEmptySubsequences: false)
        switch pieces.count {
        case 1:
            // Rust accepts an empty token and fails at the sign-in instead.
            guard !pieces[0].isEmpty else {
                throw IggyError(.invalidConnectionString, context: "credentials are required")
            }
            return .personalAccessToken(String(pieces[0]))
        case 2:
            guard !pieces[0].isEmpty, !pieces[1].isEmpty else {
                throw IggyError(.invalidConnectionString, context: "username and password are required")
            }
            return .usernamePassword(username: String(pieces[0]), password: String(pieces[1]))
        default:
            throw IggyError(.invalidConnectionString, context: "expected username:password or a personal access token")
        }
    }

    /// `host:port`, or `[v6]:port` with an IPv6 literal in the brackets.
    private static func validate(address: String) throws {
        let host: Substring
        let port: Substring
        if address.hasPrefix("[") {
            guard let close = address.firstIndex(of: "]"), address[address.index(after: close)...].hasPrefix(":") else {
                throw IggyError(.invalidConnectionString, context: "expected [address]:port")
            }
            host = address[address.index(after: address.startIndex)..<close]
            port = address[address.index(close, offsetBy: 2)...]
            guard case .v6? = try? SocketAddress(ipAddress: String(host), port: 0) else {
                throw IggyError(.invalidConnectionString, context: "\(host) is not an IPv6 address")
            }
        } else {
            guard let colon = address.firstIndex(of: ":") else {
                throw IggyError(.invalidConnectionString, context: "expected host:port")
            }
            host = address[..<colon]
            port = address[address.index(after: colon)...]
        }
        guard !host.isEmpty, port.allSatisfy({ $0.isASCII && $0.isNumber }), let number = UInt16(port), number != 0 else {
            throw IggyError(.invalidConnectionString, context: "expected host:port with a port from 1 to 65535")
        }
    }

    private static func parseOptions(_ text: Substring, into configuration: inout ClientConfiguration) throws {
        var tlsEnabled = false
        var tls = TLSOptions()
        var retries = "unlimited"
        var interval = "1s"
        var reestablishAfter = "5s"
        var heartbeatInterval = "5s"
        for option in text.split(separator: "&", omittingEmptySubsequences: false) {
            let pair = option.split(separator: "=", omittingEmptySubsequences: false)
            guard pair.count == 2 else {
                throw IggyError(.invalidConnectionString, context: "expected name=value options")
            }
            let value = String(pair[1])
            switch pair[0] {
            case "max_pending_operations", "max_pending_operation_bytes":
                guard value.allSatisfy({ $0.isASCII && $0.isNumber }), let limit = UInt64(value), limit > 0 else {
                    throw IggyError(.invalidConnectionString, context: "\(pair[0]) must be a positive integer")
                }
            case "tls": tlsEnabled = try flag(value, option: pair[0])
            case "tls_domain": tls.domain = value
            case "tls_ca_file": tls.caFile = value
            case "reconnection_retries": retries = value
            case "reconnection_interval": interval = value
            case "reestablish_after": reestablishAfter = value
            case "heartbeat_interval": heartbeatInterval = value
            case "nodelay": configuration.noDelay = try flag(value, option: pair[0])
            default: throw IggyError(.invalidConnectionString, context: "unknown option \(pair[0])")
            }
        }

        configuration.reconnection.enabled = true
        if retries == "unlimited" {
            configuration.reconnection.maxRetries = nil
        } else if let count = UInt32(retries) {
            configuration.reconnection.maxRetries = count
        } else {
            throw IggyError(.invalidNumberValue, context: "reconnection_retries must be a count or unlimited")
        }
        configuration.reconnection.interval = try duration(interval, option: "reconnection_interval", nonZero: true)
        configuration.reconnection.reestablishAfter = try duration(reestablishAfter, option: "reestablish_after", nonZero: false)
        configuration.heartbeatInterval = try duration(heartbeatInterval, option: "heartbeat_interval", nonZero: true)
        if tlsEnabled {
            configuration.tls = tls
        }
    }

    /// Rust reads any value but `true` as false, so `tls=1` would sign in
    /// over plaintext. The Go, Node, Java and C# parsers refuse it instead.
    private static func flag(_ value: String, option: Substring) throws -> Bool {
        switch value {
        case "true": return true
        case "false": return false
        default: throw IggyError(.invalidConnectionString, context: "\(option) must be true or false")
        }
    }

    /// Rust types the intervals as `NonZeroIggyDuration`, which refuses zero
    /// and anything shorter than the whole microseconds it serializes. A
    /// duration past what those microseconds hold would trap in `Task.sleep`.
    private static func duration(_ text: String, option: String, nonZero: Bool) throws -> Duration {
        guard let value = IggyDuration.parse(text) else {
            throw IggyError(.invalidConnectionString, context: "\(option)=\(text) is not a duration")
        }
        if nonZero, value < .microseconds(1) {
            throw IggyError(.invalidConnectionString, context: "\(option) must be at least one microsecond")
        }
        guard value <= ProducerConfiguration.maxDuration else {
            throw IggyError(.invalidConnectionString, context: "\(option) must be at most \(ProducerConfiguration.maxDuration)")
        }
        return value
    }
}

/// `IggyDuration::from_str` of `core/common/src/utils/duration.rs`: the
/// zero spellings, or the grammar of the humantime crate 2.4 the Rust SDK
/// parses with, on the lowercased text.
enum IggyDuration {
    static func parse(_ text: String) -> Duration? {
        let lowered = text.lowercased()
        if ["0", "unlimited", "disabled", "none"].contains(lowered) {
            return .zero
        }
        var parser = HumantimeParser(lowered)
        guard let (seconds, nanoseconds) = parser.parse(), seconds <= UInt64(Int64.max) else {
            return nil
        }
        return Duration(secondsComponent: Int64(seconds), attosecondsComponent: Int64(nanoseconds) * 1_000_000_000)
    }
}

/// A port of `humantime::duration::Parser`. Amounts and units run together
/// or spaced, as in `1h30m`, `1h 30m` or `5 s`; a fraction must come out to
/// whole nanoseconds, or whole seconds for hours and longer units.
private struct HumantimeParser {
    private let scalars: [Unicode.Scalar]
    private var position = 0
    private var seconds: UInt64 = 0
    private var nanoseconds: UInt64 = 0

    init(_ text: String) {
        scalars = Array(text.unicodeScalars)
    }

    mutating func parse() -> (UInt64, UInt64)? {
        guard var amount = firstDigit() ?? nil else {
            return nil
        }
        outer: while true {
            var fraction: (numerator: UInt64, denominator: UInt64)?
            var offset = position
            while let scalar = next() {
                if let digit = Self.digit(scalar) {
                    guard let grown = Self.append(digit, to: amount) else {
                        return nil
                    }
                    amount = grown
                } else if Self.isUnitLetter(scalar) {
                    break
                } else if scalar == "." {
                    guard let parsed = fractionalPart(&offset) else {
                        return nil
                    }
                    fraction = parsed
                    break
                } else if !scalar.properties.isWhitespace {
                    return nil
                }
                offset = position
            }
            let start = offset
            offset = position
            while let scalar = next() {
                if let digit = Self.digit(scalar) {
                    guard add(amount, fraction, unit: start..<offset) else {
                        return nil
                    }
                    amount = digit
                    continue outer
                } else if scalar.properties.isWhitespace {
                    break
                } else if !Self.isUnitLetter(scalar) {
                    return nil
                }
                offset = position
            }
            guard add(amount, fraction, unit: start..<offset) else {
                return nil
            }
            switch firstDigit() {
            case .some(.some(let digit)): amount = digit
            case .some(.none): return (seconds, nanoseconds)
            case .none: return nil
            }
        }
    }

    /// The first digit after any whitespace: nil for anything else, and
    /// `.some(nil)` at the end of the text.
    private mutating func firstDigit() -> UInt64?? {
        while let scalar = next() {
            if let digit = Self.digit(scalar) {
                return .some(digit)
            }
            guard scalar.properties.isWhitespace else {
                return nil
            }
        }
        return .some(nil)
    }

    private mutating func fractionalPart(_ offset: inout Int) -> (numerator: UInt64, denominator: UInt64)? {
        var numerator: UInt64 = 0
        var denominator: UInt64 = 1
        var leadingZeros = true
        while let scalar = next() {
            if let digit = Self.digit(scalar) {
                guard case (let scaled, false) = denominator.multipliedReportingOverflow(by: 10) else {
                    return nil
                }
                denominator = scaled
                if digit != 0 || !leadingZeros {
                    leadingZeros = false
                    guard let grown = Self.append(digit, to: numerator) else {
                        return nil
                    }
                    numerator = grown
                }
            } else if Self.isUnitLetter(scalar) {
                break
            } else if !scalar.properties.isWhitespace {
                return nil
            }
            offset = position
        }
        // A separator with no digit after it, as in `1.`.
        guard denominator != 1 else {
            return nil
        }
        return (numerator, denominator)
    }

    private mutating func add(_ amount: UInt64, _ fraction: (numerator: UInt64, denominator: UInt64)?, unit range: Range<Int>) -> Bool {
        let unit = String(String.UnicodeScalarView(scalars[range]))
        guard let (secondsPerUnit, nanosecondsPerUnit) = Self.units[unit] else {
            return false
        }
        // Whole amounts of sub-second units are counted in nanoseconds.
        let whole =
            secondsPerUnit == 0
            ? Self.multiply(amount, nanosecondsPerUnit).map { (UInt64(0), $0) }
            : Self.multiply(amount, secondsPerUnit).map { ($0, UInt64(0)) }
        guard let whole, accumulate(whole) else {
            return false
        }
        guard let (numerator, denominator) = fraction else {
            return true
        }
        let part: (UInt64, UInt64)?
        switch unit {
        case "nanos", "nsec", "ns":
            part = nil
        case _ where secondsPerUnit >= 3600:
            part = Self.multiply(numerator, secondsPerUnit).flatMap { Self.divideExactly($0, denominator) }.map { ($0, UInt64(0)) }
        default:
            let nanosecondsPerWhole = secondsPerUnit == 0 ? nanosecondsPerUnit : secondsPerUnit * 1_000_000_000
            part = Self.multiply(numerator, nanosecondsPerWhole).flatMap { Self.divideExactly($0, denominator) }.map { (UInt64(0), $0) }
        }
        guard let part else {
            return false
        }
        return accumulate(part)
    }

    private mutating func accumulate(_ part: (seconds: UInt64, nanoseconds: UInt64)) -> Bool {
        guard case (var total, false) = nanoseconds.addingReportingOverflow(part.nanoseconds) else {
            return false
        }
        var extra = part.seconds
        if total > 1_000_000_000 {
            guard case (let carried, false) = extra.addingReportingOverflow(total / 1_000_000_000) else {
                return false
            }
            extra = carried
            total %= 1_000_000_000
        }
        guard case (let sum, false) = seconds.addingReportingOverflow(extra) else {
            return false
        }
        // `Duration::new` carries a full second of nanoseconds.
        if total == 1_000_000_000 {
            guard case (let carried, false) = sum.addingReportingOverflow(1) else {
                return false
            }
            seconds = carried
            nanoseconds = 0
        } else {
            seconds = sum
            nanoseconds = total
        }
        return true
    }

    private mutating func next() -> Unicode.Scalar? {
        guard position < scalars.count else {
            return nil
        }
        defer { position += 1 }
        return scalars[position]
    }

    /// Seconds per unit, or nanoseconds per unit for units below a second.
    private static let units: [String: (seconds: UInt64, nanoseconds: UInt64)] = {
        var units: [String: (UInt64, UInt64)] = [:]
        for (names, seconds, nanoseconds) in [
            (["nanos", "nsec", "ns"], UInt64(0), UInt64(1)),
            (["usec", "us", "\u{b5}s"], 0, 1_000),
            (["millis", "msec", "ms"], 0, 1_000_000),
            (["seconds", "second", "secs", "sec", "s"], 1, 0),
            (["minutes", "minute", "min", "mins", "m"], 60, 0),
            (["hours", "hour", "hr", "hrs", "h"], 3_600, 0),
            (["days", "day", "d"], 86_400, 0),
            (["weeks", "week", "wk", "wks", "w"], 604_800, 0),
            (["months", "month", "M"], 2_630_016, 0),
            (["years", "year", "yr", "yrs", "y"], 31_557_600, 0),
        ] {
            for name in names {
                units[name] = (seconds, nanoseconds)
            }
        }
        return units
    }()

    private static func isUnitLetter(_ scalar: Unicode.Scalar) -> Bool {
        ("a"..."z").contains(scalar) || ("A"..."Z").contains(scalar) || scalar == "\u{b5}"
    }

    private static func digit(_ scalar: Unicode.Scalar) -> UInt64? {
        ("0"..."9").contains(scalar) ? UInt64(scalar.value - 48) : nil
    }

    private static func append(_ digit: UInt64, to value: UInt64) -> UInt64? {
        multiply(value, 10).flatMap { scaled in
            let (sum, overflow) = scaled.addingReportingOverflow(digit)
            return overflow ? nil : sum
        }
    }

    private static func multiply(_ lhs: UInt64, _ rhs: UInt64) -> UInt64? {
        let (product, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        return overflow ? nil : product
    }

    private static func divideExactly(_ lhs: UInt64, _ rhs: UInt64) -> UInt64? {
        lhs % rhs == 0 ? lhs / rhs : nil
    }
}
