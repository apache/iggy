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

import Testing

@testable import Iggy

@Suite("Connection strings")
struct ConnectionStringTests {
    @Test(arguments: ["127.0.0.1:8090", "localhost:8090", "iggy.default.svc.cluster.local:8090", "my_iggy_server:8090"])
    func acceptsHostnamesAndIPv4(_ address: String) throws {
        let configuration = try ClientConfiguration(connectionString: "iggy://user:secret@\(address)")
        #expect(configuration.address == address)
        #expect(configuration.autoLogin == .usernamePassword(username: "user", password: "secret"))
        #expect(configuration.tls == nil)
    }

    @Test(arguments: ["[2001:db8::1]:8090", "[2001:db8:0:0:0:0:0:1]:65535", "[::ffff:192.0.2.1]:8090", "[::1]:8090"])
    func acceptsBracketedIPv6(_ address: String) throws {
        let configuration = try ClientConfiguration(connectionString: "iggy://user:secret@\(address)?reconnection_retries=3&heartbeat_interval=10s")
        #expect(configuration.address == address)
        #expect(configuration.reconnection.maxRetries == 3)
        #expect(configuration.heartbeatInterval == .seconds(10))
        _ = try Endpoint(parsing: configuration.address)
    }

    @Test func aLoneSecretIsAPersonalAccessToken() throws {
        let configuration = try ClientConfiguration(connectionString: "iggy+tcp://iggypat-1234567890abcdef@[::1]:8090")
        #expect(configuration.autoLogin == .personalAccessToken("iggypat-1234567890abcdef"))
        #expect(configuration.address == "[::1]:8090")
    }

    @Test func withoutOptionsTheDefaultsMatchRust() throws {
        let configuration = try ClientConfiguration(connectionString: "iggy://user:secret@127.0.0.1:1234")
        #expect(configuration.reconnection == ReconnectionOptions(enabled: true, maxRetries: nil, interval: .seconds(1), reestablishAfter: .seconds(5)))
        #expect(configuration.heartbeatInterval == .seconds(5))
        #expect(configuration.noDelay)
    }

    @Test func readsEveryOption() throws {
        let configuration = try ClientConfiguration(
            connectionString:
                "iggy://user:secret@localhost:8090?tls=true&tls_domain=iggy.example&tls_ca_file=/etc/iggy/ca.pem&reconnection_retries=7"
                + "&reconnection_interval=250ms&reestablish_after=1m 30s&heartbeat_interval=1.5s&nodelay=false"
                + "&max_pending_operations=256&max_pending_operation_bytes=67108864")
        #expect(configuration.tls == TLSOptions(domain: "iggy.example", caFile: "/etc/iggy/ca.pem", validateCertificate: true))
        #expect(configuration.reconnection == ReconnectionOptions(enabled: true, maxRetries: 7, interval: .milliseconds(250), reestablishAfter: .seconds(90)))
        #expect(configuration.heartbeatInterval == .milliseconds(1500))
        #expect(!configuration.noDelay)
    }

    @Test func spellsTheFlagsAsRustDoes() throws {
        // Anything but `true` turns a flag off, and TLS options alone do not
        // turn TLS on.
        let off = try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?tls=yes&tls_domain=localhost&nodelay=1")
        #expect(off.tls == nil)
        #expect(!off.noDelay)
        let on = try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?nodelay=true&tls=true")
        #expect(on.tls == TLSOptions())
        #expect(on.noDelay)
        // The last spelling of an option wins.
        let last = try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?reconnection_retries=2&reconnection_retries=unlimited")
        #expect(last.reconnection.maxRetries == nil)
    }

    @Test func zeroIsAllowedOnlyWhereRustTakesAPlainDuration() throws {
        let configuration = try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?reestablish_after=disabled")
        #expect(configuration.reconnection.reestablishAfter == .zero)
        for option in ["heartbeat_interval", "reconnection_interval"] {
            for value in ["0", "none", "0s", "999ns"] {
                #expect(throws: IggyError(.invalidConnectionString), "\(option)=\(value)") {
                    try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?\(option)=\(value)")
                }
            }
        }
        #expect(try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?heartbeat_interval=1us").heartbeatInterval == .microseconds(1))
    }

    @Test func refusesDurationsTaskSleepCannotWaitOut() throws {
        // u64 microseconds hold about 584,554 years.
        #expect(try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?heartbeat_interval=584000y").heartbeatInterval > .zero)
        #expect(throws: IggyError(.invalidConnectionString)) {
            try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?heartbeat_interval=585000y")
        }
    }

    @Test func aBadRetryCountIsANumberError() {
        for value in ["many", "-1", "4294967296", ""] {
            #expect(throws: IggyError(.invalidNumberValue), "\(value)") {
                try ClientConfiguration(connectionString: "iggy://user:secret@localhost:8090?reconnection_retries=\(value)")
            }
        }
    }

    @Test(arguments: [
        "[::1]", "[::1]:", "[::1]:invalid", "[::1]:65536", "[::1]:0", "[::1]:-1", "[::1]:8090:1234", "[::1:8090", "::1:8090", "2001:db8::1:8090",
        "[]:8090", "[localhost]:8090", "[gggg::1]:8090", "[192.168.1.1]:8090", "localhost", "localhost:0", "127.0.0.1:0", ":8090", "host:+8090",
        "localhost:8090:1",
    ])
    func refusesMalformedAddresses(_ address: String) {
        #expect(throws: IggyError(.invalidConnectionString)) {
            try ClientConfiguration(connectionString: "iggy://user:secret@\(address)")
        }
    }

    @Test(arguments: [
        "", "localhost:8090", "iggy:/user:secret@localhost:8090", "tcp://user:secret@localhost:8090", "iggy+quic://user:secret@localhost:8090",
        "iggy+://user:secret@localhost:8090", "iggy://user:secret@localhost:8090://x", "iggy://@localhost:8090", "iggy://:secret@localhost:8090",
        "iggy://user:@localhost:8090", "iggy://a:b:c@localhost:8090", "iggy://user:secret@localhost:8090?", "iggy://user:secret@localhost:8090?tls",
        "iggy://user:secret@localhost:8090?tls=true=false", "iggy://user:secret@localhost:8090?unknown=1", "iggy://user:secret@localhost:8090?a=1?b=2",
        "iggy://user:secret@localhost:8090?tls=true&", "iggy://user:secret@localhost:8090?heartbeat_interval=soon",
        "iggy://user:secret@localhost:8090?reestablish_after=5", "iggy://us@er:secret@localhost:8090",
        "iggy://user:secret@localhost:8090?max_pending_operations=0", "iggy://user:secret@localhost:8090?max_pending_operation_bytes=lots",
    ])
    func refusesMalformedStrings(_ connectionString: String) {
        #expect(throws: IggyError(.invalidConnectionString)) {
            try ClientConfiguration(connectionString: connectionString)
        }
    }

    @Test func errorsNeverRepeatTheSecret() {
        for connectionString in [
            "iggy+s3cr3t-token://user:s3cr3t-token@localhost:8090", "iggy://user:s3cr3t-token@localhost", "iggy://user:s3cr3t-token:x@localhost:8090",
            "iggy://user:s3cr3t-token@@localhost:8090",
        ] {
            do {
                _ = try ClientConfiguration(connectionString: connectionString)
                Issue.record("\(connectionString) parsed")
            } catch {
                #expect(!"\(error)".contains("s3cr3t"), "\(error)")
            }
        }
        #expect(throws: IggyError(.invalidConnectionString)) { try ClientConfiguration(connectionString: "iggy+quic://u:p@localhost:8080") }
        do {
            _ = try ClientConfiguration(connectionString: "iggy+quic://u:p@localhost:8080")
        } catch {
            #expect("\(error)".contains("quic"))
        }
    }

    @Test func aClientTakesItsConfigurationFromTheString() throws {
        let client = try IggyClient(connectionString: "iggy://iggy:iggy@localhost:8090?heartbeat_interval=2s")
        #expect(client.configuration.address == "localhost:8090")
        #expect(client.configuration.autoLogin == .usernamePassword(username: "iggy", password: "iggy"))
        #expect(client.configuration.heartbeatInterval == .seconds(2))
    }
}

/// The cases of the humantime crate's own tests, plus Iggy's zero spellings.
@Suite("Iggy durations")
struct IggyDurationTests {
    static func duration(_ seconds: Int64, _ nanoseconds: Int64 = 0) -> Duration {
        .seconds(seconds) + .nanoseconds(nanoseconds)
    }

    @Test func parsesEveryUnitSpelling() {
        let cases: [(String, Duration)] = [
            ("17nsec", Self.duration(0, 17)), ("17nanos", Self.duration(0, 17)), ("33ns", Self.duration(0, 33)),
            ("3usec", Self.duration(0, 3000)), ("78us", Self.duration(0, 78000)), ("163\u{b5}s", Self.duration(0, 163_000)),
            ("31msec", Self.duration(0, 31_000_000)), ("31millis", Self.duration(0, 31_000_000)), ("6ms", Self.duration(0, 6_000_000)),
            ("3000s", Self.duration(3000)), ("300sec", Self.duration(300)), ("300secs", Self.duration(300)), ("50seconds", Self.duration(50)),
            ("1second", Self.duration(1)), ("100m", Self.duration(6000)), ("12min", Self.duration(720)), ("12mins", Self.duration(720)),
            ("1minute", Self.duration(60)), ("7minutes", Self.duration(420)), ("2h", Self.duration(7200)), ("7hr", Self.duration(25200)),
            ("7hrs", Self.duration(25200)), ("1hour", Self.duration(3600)), ("24hours", Self.duration(86400)), ("1day", Self.duration(86400)),
            ("2days", Self.duration(172_800)), ("365d", Self.duration(31_536_000)), ("1week", Self.duration(604_800)),
            ("7weeks", Self.duration(4_233_600)), ("104wks", Self.duration(2 * 31_449_600)), ("100wk", Self.duration(60_480_000)),
            ("52w", Self.duration(31_449_600)), ("1month", Self.duration(2_630_016)), ("3months", Self.duration(3 * 2_630_016)),
            ("1year", Self.duration(31_557_600)), ("7years", Self.duration(7 * 31_557_600)), ("15yrs", Self.duration(15 * 31_557_600)),
            ("10yr", Self.duration(10 * 31_557_600)), ("17y", Self.duration(536_479_200)),
            // Iggy lowercases first, so `M` reads as minutes.
            ("12M", Self.duration(720)), ("5S", Self.duration(5)),
        ]
        for (text, expected) in cases {
            #expect(IggyDuration.parse(text) == expected, "\(text)")
        }
    }

    @Test func parsesFractionsThatLandOnWholeUnits() {
        let cases: [(String, Duration)] = [
            ("3.1us", Self.duration(0, 3100)), ("3.001us", Self.duration(0, 3001)),
            ("31.000001ms", Self.duration(0, 31_000_001)), ("300.0s", Self.duration(300)), ("0.0s", .zero), ("1.123456789s", Self.duration(1, 123_456_789)),
            ("12.1min", Self.duration(726)), ("1.5minutes", Self.duration(90)), ("2.5h", Self.duration(9000)), ("0.5h", Self.duration(1800)),
            ("1.5day", Self.duration(129_600)), ("7.120us 3ns", Self.duration(0, 7123)), ("1.234s0.345ms0.678us0ns", Self.duration(1, 234_345_678)),
            ("1.999999999s 0.000000001s", Self.duration(2)), ("999999999ns 1ns", Self.duration(1)),
        ]
        for (text, expected) in cases {
            #expect(IggyDuration.parse(text) == expected, "\(text)")
        }
    }

    @Test func spacesMaySeparateEveryPart() {
        #expect(IggyDuration.parse("1h 30m") == .seconds(5400))
        #expect(IggyDuration.parse("1h30m") == .seconds(5400))
        #expect(IggyDuration.parse("5 s") == .seconds(5))
        #expect(IggyDuration.parse("20 min 17 nsec ") == Self.duration(1200, 17))
        // humantime joins digits across a space.
        #expect(IggyDuration.parse("1 0s") == .seconds(10))
    }

    @Test func zeroSpellings() {
        for text in ["0", "unlimited", "Unlimited", "DISABLED", "none"] {
            #expect(IggyDuration.parse(text) == .zero, "\(text)")
        }
    }

    @Test func refusesWhatRustRefuses() {
        for text in [
            "", " ", "5", "s", ".", "1.", "1..s", ".1s", "1.s", "123", "10 months 1", "5x", "five seconds", "1.5", "-1s", "+1s", "1e3s", "1s,2s",
            "0.000123456789s", "1.0000000002s", "31.0000001ms", "0.5ns", "0.0001h", "0.00000005d", "1\u{3bc}s", "100000000000000000000ns",
            "18446744073709551616s",
        ] {
            #expect(IggyDuration.parse(text) == nil, "\(text)")
        }
    }
}
