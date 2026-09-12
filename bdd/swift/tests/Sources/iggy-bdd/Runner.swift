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
/// the fallback address, so a missing value aborts the run instead.
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

/// Fails a step unless `condition` holds.
func check(_ condition: Bool, _ message: @autoclosure () -> String) throws {
    guard condition else {
        throw StepFailure(message())
    }
}

/// The per-scenario state one feature's steps share: a client plus whatever
/// the steps remember between one another.
final class World: @unchecked Sendable {
    var serverAddress: String?
    var client: IggyClient?
    var lastSentMessage: IggyMessage?
    var lastPolled: PolledMessages?
    var lastStreamID: UInt32?
    var lastStreamName: String?
    var lastStreamWasFound = false
    var lastTopicID: UInt32?
    var lastTopicName: String?
    var lastTopicPartitions: UInt32?
    var lastRawResponse: [UInt8]?
    var lastRawError: (any Error)?

    func requireClient() throws -> IggyClient {
        guard let client else {
            throw StepFailure("the client is not connected; the background must sign in first")
        }
        return client
    }

    func tearDown() async {
        try? await client?.shutdown()
        client = nil
    }
}

typealias StepHandler = @Sendable (World, [String]) async throws -> Void

/// The step definitions of one feature, matched by regular expression.
struct StepRegistry {
    private var definitions: [(pattern: NSRegularExpression, handler: StepHandler)] = []

    mutating func step(_ pattern: String, _ handler: @escaping StepHandler) {
        // Anchored, like cucumber expressions, so one text cannot match two
        // definitions that differ only in a suffix.
        let expression = try! NSRegularExpression(pattern: "^" + pattern + "$")
        definitions.append((expression, handler))
    }

    func match(_ text: String) throws -> (StepHandler, [String]) {
        let range = NSRange(text.startIndex..<text.endIndex, in: text)
        let matches = definitions.compactMap { definition -> (StepHandler, [String])? in
            guard let match = definition.pattern.firstMatch(in: text, range: range) else {
                return nil
            }
            let groups = (1..<match.numberOfRanges).map { index -> String in
                let groupRange = match.range(at: index)
                guard groupRange.location != NSNotFound, let swiftRange = Range(groupRange, in: text) else {
                    return ""
                }
                return String(text[swiftRange])
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

/// Runs features and prints a cucumber-style report. Strict: an undefined
/// step fails its scenario.
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
                    print("    \u{2717} \(step.keyword) \(step.text)")
                    print("      \(error)")
                }
            }
            await world.tearDown()
            if scenarioFailed {
                failed += 1
            } else {
                passed += 1
            }
        }
        return (passed, failed)
    }
}
