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
import Iggy

/// The endpoints and credentials a suite runs against. A default here would
/// turn a dropped compose variable into a run against whatever listens on
/// the fallback address, so a missing value fails the step instead.
enum Environment {
    static func required(_ name: String) throws -> String {
        guard let value = ProcessInfo.processInfo.environment[name], !value.isEmpty else {
            throw StepFailure("\(name) must be set; run the suite via scripts/run-bdd-tests.sh")
        }
        return value
    }

    static func serverAddress() throws -> String {
        try required("IGGY_TCP_ADDRESS")
    }

    static func noQuorumAddress() throws -> String {
        try required("IGGY_TCP_ADDRESS_NO_QUORUM")
    }

    static func rootCredentials() throws -> (username: String, password: String) {
        (try required("IGGY_ROOT_USERNAME"), try required("IGGY_ROOT_PASSWORD"))
    }
}

struct StepFailure: Error, CustomStringConvertible {
    let description: String

    init(_ description: String) {
        self.description = description
    }
}

/// Fails the step unless `condition` holds.
func check(_ condition: Bool, _ message: @autoclosure () -> String) throws {
    guard condition else {
        throw StepFailure(message())
    }
}

/// Parses a captured decimal group.
func number<Value: FixedWidthInteger>(_ text: String, as type: Value.Type = Value.self) throws -> Value {
    guard let value = Value(text) else {
        throw StepFailure("\(text) is not a \(Value.self)")
    }
    return value
}

/// The state one scenario's steps share. Steps run one after another, so
/// nothing touches it concurrently.
final class World: @unchecked Sendable {
    var serverAddress: String?
    var client: IggyClient?
    /// The outcome of the last call a step made to see it fail.
    var lastError: (any Error)?

    var lastSentMessage: IggyMessage?
    var lastPolled: PolledMessages?
    var lastRawResponse: [UInt8]?
    var lastStreamID: UInt32?
    var lastStreamName: String?
    var lastTopicID: UInt32?
    var lastTopicName: String?
    var lastTopicPartitions: UInt32?

    var streamKey: Data?
    var userID: UInt32?
    var createdStreams: [UInt32] = []
    var createdUsers: [UInt32] = []
    var optionSpecs: [OptionSpec] = []

    var changeGroups: [MetadataChangeGroup] = []
    var changeCursor: MetadataChangeCursor?
    var followedChanges: [MetadataChangeRecord] = []
    var currentItems: [MetadataCurrentItem] = []
    var signedInAsAnotherUser = false

    func requireClient() throws -> IggyClient {
        guard let client else {
            throw StepFailure("the client is not connected; the background must sign in first")
        }
        return client
    }

    func requireStream() throws -> Identifier {
        guard let lastStreamID else {
            throw StepFailure("no stream was created in this scenario")
        }
        return Identifier(numeric: lastStreamID)
    }

    func requireTopic() throws -> Identifier {
        guard let lastTopicID else {
            throw StepFailure("no topic was created in this scenario")
        }
        return Identifier(numeric: lastTopicID)
    }

    func requireUser() throws -> Identifier {
        guard let userID else {
            throw StepFailure("no user was created in this scenario")
        }
        return Identifier(numeric: userID)
    }

    /// A client for `address`, connected but not signed in.
    func connect(to address: String) async throws -> IggyClient {
        let client = IggyClient(address: address)
        self.client = client
        try await client.connect()
        return client
    }

    /// Signs back in as root when a step switched users, deletes what the
    /// scenario created, and shuts the client down. A failed cleanup fails
    /// the scenario, since it would leak into the next one.
    func tearDown() async -> [any Error] {
        guard let client else {
            return []
        }
        var failures: [any Error] = []
        if signedInAsAnotherUser {
            do {
                let root = try Environment.rootCredentials()
                try await client.logout()
                _ = try await client.login(username: root.username, password: root.password)
            } catch {
                failures.append(error)
            }
        }
        for id in createdStreams {
            do { try await client.deleteStream(Identifier(numeric: id)) } catch { failures.append(error) }
        }
        for id in createdUsers {
            do { try await client.deleteUser(Identifier(numeric: id)) } catch { failures.append(error) }
        }
        await client.shutdown()
        self.client = nil
        return failures
    }
}

typealias StepHandler = @Sendable (World, [String]) async throws -> Void

/// The step definitions of one feature, matched by regular expression.
struct StepRegistry {
    private var definitions: [(pattern: NSRegularExpression, handler: StepHandler)] = []

    mutating func step(_ pattern: String, _ handler: @escaping StepHandler) {
        // Anchored, so one text cannot match two definitions that differ only
        // in a suffix.
        guard let expression = try? NSRegularExpression(pattern: "^" + pattern + "$") else {
            preconditionFailure("invalid step pattern \(pattern)")
        }
        definitions.append((expression, handler))
    }

    func match(_ text: String) throws -> (StepHandler, [String]) {
        let range = NSRange(text.startIndex..<text.endIndex, in: text)
        let matches = definitions.compactMap { definition -> (StepHandler, [String])? in
            guard let match = definition.pattern.firstMatch(in: text, range: range) else {
                return nil
            }
            let groups = (1..<match.numberOfRanges).map { index -> String in
                guard let groupRange = Range(match.range(at: index), in: text) else {
                    return ""
                }
                return String(text[groupRange])
            }
            return (definition.handler, groups)
        }
        switch matches.count {
        case 0: throw StepFailure("undefined step")
        case 1: return matches[0]
        default: throw StepFailure("ambiguous step, \(matches.count) definitions match")
        }
    }
}

/// Runs a feature and prints a cucumber-style report. Strict: an undefined
/// or ambiguous step fails its scenario.
struct Runner {
    let registry: StepRegistry

    func run(_ feature: Feature) async -> (passed: Int, failed: Int) {
        print("Feature: \(feature.name)")
        var passed = 0
        var failed = 0
        for scenario in feature.scenarios {
            print("")
            print("  Scenario: \(scenario.name)")
            let world = World()
            var scenarioFailed = false
            for step in feature.background + scenario.steps {
                if scenarioFailed {
                    print("    - \(step.keyword) \(step.text) (skipped)")
                    continue
                }
                do {
                    let (handler, groups) = try registry.match(step.text)
                    try await handler(world, groups)
                    print("    \u{2713} \(step.keyword) \(step.text)")
                } catch {
                    scenarioFailed = true
                    print("    \u{2717} \(step.keyword) \(step.text)  (line \(step.line))")
                    print("      \(error)")
                }
            }
            for failure in await world.tearDown() {
                scenarioFailed = true
                print("    \u{2717} cleanup: \(failure)")
            }
            if scenarioFailed {
                failed += 1
            } else {
                passed += 1
            }
        }
        return (passed, failed)
    }
}
