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

import struct Foundation.CharacterSet

/// Parser for the connection strings every Iggy SDK accepts:
///
/// ```text
/// iggy://user:password@host:port?tls=true&tls_domain=localhost&reconnection_retries=unlimited
/// iggy+tcp://<personal_access_token>@host:port
/// ```
///
/// Options: `tls`, `tls_domain`, `tls_ca_file`, `reconnection_retries`
/// (`unlimited` or a number), `reconnection_interval`, `reestablish_after`,
/// `heartbeat_interval` (durations such as `5s`, `500ms`, `1h 1m`), and
/// `nodelay`. An unknown option is rejected.
enum ConnectionString {
    private static let defaultPrefix = "iggy://"
    private static let transportPrefix = "iggy+"

    static func parse(_ connectionString: String) throws -> ClientConfiguration {
        guard !connectionString.isEmpty else {
            throw IggyError(.invalidConnectionString, context: "empty")
        }
        let afterScheme: Substring
        if connectionString.hasPrefix(defaultPrefix) {
            afterScheme = connectionString.dropFirst(defaultPrefix.count)
        } else if connectionString.hasPrefix(transportPrefix) {
            let rest = connectionString.dropFirst(transportPrefix.count)
            guard let schemeEnd = rest.range(of: "://") else {
                throw IggyError(.invalidConnectionString, context: "missing ://")
            }
            let transport = rest[..<schemeEnd.lowerBound]
            guard transport == "tcp" else {
                throw IggyError(.invalidConnectionString, context: "transport \(transport) is not supported by this SDK, use tcp")
            }
            afterScheme = rest[schemeEnd.upperBound...]
        } else {
            throw IggyError(.invalidConnectionString, context: "expected an iggy:// or iggy+tcp:// prefix")
        }
        guard afterScheme.range(of: "://") == nil else {
            throw IggyError(.invalidConnectionString, context: "unexpected second scheme")
        }
        guard let at = afterScheme.lastIndex(of: "@") else {
            throw IggyError(.invalidConnectionString, context: "credentials are required")
        }
        let credentialsPart = afterScheme[..<at]
        let serverPart = afterScheme[afterScheme.index(after: at)...]
        guard !credentialsPart.contains("@") else {
            throw IggyError(.invalidConnectionString, context: "unexpected @")
        }

        let credentials: Credentials
        let pieces = credentialsPart.split(separator: ":", omittingEmptySubsequences: false)
        switch pieces.count {
        case 1:
            guard !pieces[0].isEmpty else {
                throw IggyError(.invalidConnectionString, context: "credentials are required")
            }
            credentials = .personalAccessToken(String(pieces[0]))
        case 2:
            guard !pieces[0].isEmpty, !pieces[1].isEmpty else {
                throw IggyError(.invalidConnectionString, context: "username and password are required")
            }
            credentials = .usernamePassword(username: String(pieces[0]), password: String(pieces[1]))
        default:
            throw IggyError(.invalidConnectionString, context: "malformed credentials")
        }

        let serverAndOptions = serverPart.split(separator: "?", maxSplits: 2, omittingEmptySubsequences: false)
        guard serverAndOptions.count <= 2 else {
            throw IggyError(.invalidConnectionString, context: "unexpected second ?")
        }
        let address = String(serverAndOptions[0])
        _ = try Endpoint(parsing: address)

        var configuration = ClientConfiguration(address: address, autoLogin: credentials)
        var tlsEnabled = false
        var tls = TLSOptions()
        if serverAndOptions.count == 2 {
            for option in serverAndOptions[1].split(separator: "&", omittingEmptySubsequences: false) {
                let pair = option.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
                guard pair.count == 2 else {
                    throw IggyError(.invalidConnectionString, context: "malformed option \(option)")
                }
                let key = pair[0]
                let value = String(pair[1])
                switch key {
                case "tls":
                    tlsEnabled = value == "true"
                case "tls_domain":
                    tls.domain = value
                case "tls_ca_file":
                    tls.caFile = value
                case "reconnection_retries":
                    if value == "unlimited" {
                        configuration.reconnection.maxRetries = nil
                    } else if let retries = UInt32(value) {
                        configuration.reconnection.maxRetries = retries
                    } else {
                        throw IggyError(.invalidNumberValue, context: "reconnection_retries=\(value)")
                    }
                case "reconnection_interval":
                    configuration.reconnection.interval = try nonZeroDuration(value, key: "reconnection_interval")
                case "reestablish_after":
                    configuration.reconnection.reestablishAfter = try duration(value, key: "reestablish_after")
                case "heartbeat_interval":
                    configuration.heartbeatInterval = try nonZeroDuration(value, key: "heartbeat_interval")
                case "nodelay":
                    configuration.noDelay = value == "true"
                default:
                    throw IggyError(.invalidConnectionString, context: "unknown option \(key)")
                }
            }
        }
        if tlsEnabled {
            configuration.tls = tls
        }
        return configuration
    }

    private static func duration(_ text: String, key: String) throws -> Duration {
        guard let value = Duration(humanTime: text) else {
            throw IggyError(.invalidConnectionString, context: "\(key)=\(text) is not a duration")
        }
        return value
    }

    private static func nonZeroDuration(_ text: String, key: String) throws -> Duration {
        let value = try duration(text, key: key)
        guard value > .zero else {
            throw IggyError(.invalidConnectionString, context: "\(key) must be greater than zero")
        }
        return value
    }
}

extension Duration {
    /// Parses the `humantime` format the other SDKs use: a sequence of
    /// `<number><unit>` such as `5s`, `500ms`, `1h 30m`, `2d`. The words
    /// `0`, `unlimited`, `none`, and `disabled` parse to zero.
    public init?(humanTime text: String) {
        let trimmed = text.trimmingCharacters(in: .whitespaces).lowercased()
        if ["0", "unlimited", "none", "disabled"].contains(trimmed) {
            self = .zero
            return
        }
        var total = Duration.zero
        var number = ""
        var unit = ""
        var sawAny = false
        func flush() -> Bool {
            guard !number.isEmpty, let value = Double(number) else { return false }
            let multiplier: Duration
            switch unit {
            case "ns", "nsec": multiplier = .nanoseconds(1)
            case "us", "usec", "µs", "μs": multiplier = .microseconds(1)
            case "ms", "msec": multiplier = .milliseconds(1)
            case "s", "sec", "secs", "second", "seconds": multiplier = .seconds(1)
            case "m", "min", "mins", "minute", "minutes": multiplier = .seconds(60)
            case "h", "hr", "hour", "hours": multiplier = .seconds(3600)
            case "d", "day", "days": multiplier = .seconds(86_400)
            case "w", "week", "weeks": multiplier = .seconds(604_800)
            default: return false
            }
            // Whole units multiply exactly; a fraction goes through Double.
            if value == value.rounded(), value < Double(Int64.max) {
                total += multiplier * Int64(value)
            } else {
                total += multiplier * value
            }
            number = ""
            unit = ""
            sawAny = true
            return true
        }
        for character in trimmed {
            if character.isNumber || character == "." {
                if !unit.isEmpty, !flush() {
                    return nil
                }
                number.append(character)
            } else if character == " " {
                if !unit.isEmpty, !flush() {
                    return nil
                }
            } else {
                unit.append(character)
            }
        }
        guard flush(), sawAny else {
            return nil
        }
        self = total
    }
}
