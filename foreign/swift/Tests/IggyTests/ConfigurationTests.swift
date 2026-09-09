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
    @Test func usernameAndPasswordWithDefaults() throws {
        let configuration = try ConnectionString.parse("iggy://user:secret@127.0.0.1:1234")
        #expect(configuration.address == "127.0.0.1:1234")
        #expect(configuration.autoLogin == .usernamePassword(username: "user", password: "secret"))
        #expect(configuration.tls == nil)
        #expect(configuration.reconnection.maxRetries == nil)
        #expect(configuration.reconnection.interval == .seconds(1))
        #expect(configuration.reconnection.reestablishAfter == .seconds(5))
        #expect(configuration.heartbeatInterval == .seconds(5))
    }

    @Test func personalAccessToken() throws {
        let configuration = try ConnectionString.parse("iggy+tcp://iggypat-1234567890abcdef@localhost:8090")
        #expect(configuration.autoLogin == .personalAccessToken("iggypat-1234567890abcdef"))
        #expect(configuration.address == "localhost:8090")
    }

    @Test func everyOption() throws {
        let configuration = try ConnectionString.parse(
            "iggy://user:secret@localhost:8090?tls=true&tls_domain=iggy.example&tls_ca_file=/etc/iggy/ca.pem&reconnection_retries=3&reconnection_interval=250ms&reestablish_after=1m&heartbeat_interval=10s&nodelay=false"
        )
        #expect(configuration.tls == TLSOptions(domain: "iggy.example", caFile: "/etc/iggy/ca.pem", validateCertificate: true))
        #expect(configuration.reconnection.maxRetries == 3)
        #expect(configuration.reconnection.interval == .milliseconds(250))
        #expect(configuration.reconnection.reestablishAfter == .seconds(60))
        #expect(configuration.heartbeatInterval == .seconds(10))
        #expect(!configuration.noDelay)
    }

    @Test func unlimitedRetriesAndIPv6() throws {
        let configuration = try ConnectionString.parse("iggy://user:secret@[::1]:8090?reconnection_retries=unlimited")
        #expect(configuration.reconnection.maxRetries == nil)
        #expect(configuration.address == "[::1]:8090")
    }

    @Test(arguments: [
        "", "localhost:8090", "iggy://:secret@host:1", "iggy://user:@host:1", "iggy://user:secret@:1",
        "iggy://user:secret@host", "iggy://user:secret@host:", "iggy://user:secret@host:abc",
        "iggy://user:secret@host:1?unknown=1", "iggy://user:secret@host:1?tls", "iggy://a:b:c@host:1",
        "iggy+quic://user:secret@host:1", "iggy://user:secret@host:1?reconnection_retries=many",
        "iggy://user:secret@host:1?heartbeat_interval=0", "iggy://user:secret@host:1?reconnection_interval=soon",
        "iggy://user:secret@host:1?a=1?b=2",
    ])
    func rejectsMalformedStrings(_ connectionString: String) {
        #expect(throws: IggyError.self) {
            try ConnectionString.parse(connectionString)
        }
    }
}

@Suite("Human durations")
struct HumanDurationTests {
    @Test func parsesTheHumantimeFormat() {
        #expect(Duration(humanTime: "5s") == .seconds(5))
        #expect(Duration(humanTime: "500ms") == .milliseconds(500))
        #expect(Duration(humanTime: "1h 1m 1s") == .seconds(3661))
        #expect(Duration(humanTime: "2d") == .seconds(172_800))
        #expect(Duration(humanTime: "1w") == .seconds(604_800))
        #expect(Duration(humanTime: "10us") == .microseconds(10))
        #expect(Duration(humanTime: "3ns") == .nanoseconds(3))
        #expect(Duration(humanTime: "1.5s") == .milliseconds(1500))
        #expect(Duration(humanTime: "1h30m") == .seconds(5400))
        #expect(Duration(humanTime: "0") == .zero)
        #expect(Duration(humanTime: "unlimited") == .zero)
        #expect(Duration(humanTime: "Disabled") == .zero)
        #expect(Duration(humanTime: "none") == .zero)
    }

    @Test func rejectsGarbage() {
        #expect(Duration(humanTime: "") == nil)
        #expect(Duration(humanTime: "5") == nil)
        #expect(Duration(humanTime: "five seconds") == nil)
        #expect(Duration(humanTime: "5x") == nil)
        #expect(Duration(humanTime: "s") == nil)
    }
}

@Suite("Endpoints")
struct EndpointTests {
    @Test func parsesHostsAndPorts() throws {
        let plain = try Endpoint(parsing: "localhost:8090")
        #expect(plain.host == "localhost")
        #expect(plain.port == 8090)
        #expect(!plain.isIPLiteral)
        let v4 = try Endpoint(parsing: "127.0.0.1:8090")
        #expect(v4.isIPLiteral)
        #expect(v4.description == "127.0.0.1:8090")
        let v6 = try Endpoint(parsing: "[fd00::1]:8091")
        #expect(v6.host == "fd00::1")
        #expect(v6.port == 8091)
        #expect(v6.isIPLiteral)
        #expect(v6.description == "[fd00::1]:8091")
    }

    @Test(arguments: ["", "localhost", ":8090", "localhost:0", "localhost:70000", "[::1]", "[::1]8090", "::1:8090", "host:port"])
    func rejectsMalformedAddresses(_ address: String) {
        #expect(throws: IggyError.self) {
            try Endpoint(parsing: address)
        }
    }

    @Test func spellingComparison() {
        #expect(isSameSpelling("localhost:8090", "127.0.0.1:8090"))
        #expect(isSameSpelling("[::1]:8090", "localhost:8090"))
        #expect(isSameSpelling("Iggy-1:8090", "iggy-1:8090"))
        #expect(!isSameSpelling("localhost:8090", "localhost:8091"))
        #expect(!isSameSpelling("10.0.0.1:8090", "10.0.0.2:8090"))
    }

    @Test func tlsServerNameSkipsIPLiterals() throws {
        let literal = TLSOptions()
        #expect(literal.serverName(for: try Endpoint(parsing: "127.0.0.1:8090")) == nil)
        #expect(literal.serverName(for: try Endpoint(parsing: "[::1]:8090")) == nil)
        #expect(literal.serverName(for: try Endpoint(parsing: "iggy.example:8090")) == "iggy.example")
        let named = TLSOptions(domain: "localhost")
        #expect(named.serverName(for: try Endpoint(parsing: "127.0.0.1:8090")) == "localhost")
    }
}

@Suite("Consumer-group state")
struct ConsumerGroupStateTests {
    let key = ConsumerGroupState.GroupKey(stream: "s", topic: "t", group: "g")
    let topic = ConsumerGroupState.TopicKey(stream: "s", topic: "t")

    @Test func groupPartitionsRoundRobinAndWrap() async {
        let state = ConsumerGroupState()
        await state.setAssignment(key, generation: 1, partitions: [0, 1, 2])
        var picks: [UInt32?] = []
        for _ in 0..<4 {
            picks.append(await state.nextGroupPartition(key))
        }
        #expect(picks == [0, 1, 2, 0])
    }

    @Test func generationChangeResetsTheCursor() async {
        let state = ConsumerGroupState()
        await state.setAssignment(key, generation: 1, partitions: [0, 1, 2])
        #expect(await state.nextGroupPartition(key) == 0)
        #expect(await state.nextGroupPartition(key) == 1)
        await state.setAssignment(key, generation: 2, partitions: [5])
        #expect(await state.nextGroupPartition(key) == 5)
    }

    @Test func balancedRoundRobins() async {
        let state = ConsumerGroupState()
        var picks: [UInt32] = []
        for _ in 0..<4 {
            picks.append(await state.nextBalancedPartition(topic, partitionCount: 3))
        }
        #expect(picks == [0, 1, 2, 0])
        #expect(await state.nextBalancedPartition(topic, partitionCount: 0) == 0)
    }

    @Test func missingAssignmentYieldsNil() async {
        let state = ConsumerGroupState()
        #expect(await !state.hasAssignment(key))
        #expect(await state.nextGroupPartition(key) == nil)
    }

    @Test func memberWithNoPartitionsStaysRegistered() async {
        let state = ConsumerGroupState()
        await state.registerGroup(key)
        await state.setAssignment(key, generation: 1, partitions: [])
        #expect(await !state.hasAssignment(key))
        #expect(await state.isRegistered(key))
        await state.deregisterGroup(key)
        #expect(await !state.isRegistered(key))
    }

    @Test func sessionResetKeepsTopicState() async {
        let state = ConsumerGroupState()
        await state.registerGroup(key)
        await state.setAssignment(key, generation: 1, partitions: [0, 1])
        #expect(await state.nextBalancedPartition(topic, partitionCount: 3) == 0)
        await state.setPartitionCount(topic, 3)
        await state.clearSessionScoped()
        #expect(await state.registeredGroups.isEmpty)
        #expect(await !state.hasAssignment(key))
        #expect(await state.nextBalancedPartition(topic, partitionCount: 3) == 1)
        #expect(await state.partitionCount(topic) == 3)
        await state.invalidatePartitionCount(topic)
        #expect(await state.partitionCount(topic) == nil)
    }
}
