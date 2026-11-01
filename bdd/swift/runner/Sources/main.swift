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

// Runs every scenario of the feature files in the directory of the first
// argument, `/app/features` by default, or only `<BDD_FEATURE>.feature` when
// BDD_FEATURE names one. A step without a definition fails its scenario.
// Exits with 1 when any scenario failed.

/// The definition whose pattern matches all of `text`, with its captures.
func match(_ text: String) throws -> (StepDefinition, [String])? {
    let range = NSRange(text.startIndex..., in: text)
    for definition in stepDefinitions {
        let expression = try NSRegularExpression(pattern: "^" + definition.pattern + "$")
        guard let result = expression.firstMatch(in: text, range: range) else {
            continue
        }
        let captures = (1..<result.numberOfRanges).compactMap { index in
            Range(result.range(at: index), in: text).map { String(text[$0]) }
        }
        return (definition, captures)
    }
    return nil
}

func run(_ scenario: Scenario, of feature: Feature) async -> Bool {
    let world = World()
    defer { print() }
    print("  Scenario: \(scenario.name)")
    for step in feature.background + scenario.steps {
        do {
            guard let (definition, captures) = try match(step.text) else {
                print("    ✘ \(step.keyword) \(step.text)\n      undefined step at \(feature.path):\(step.line)")
                await world.tearDown()
                return false
            }
            try await definition.run(world, captures)
            print("    ✔ \(step.keyword) \(step.text)")
        } catch {
            print("    ✘ \(step.keyword) \(step.text)\n      \(error)")
            await world.tearDown()
            return false
        }
    }
    await world.tearDown()
    return true
}

let directory = CommandLine.arguments.dropFirst().first ?? "/app/features"
let selected = ProcessInfo.processInfo.environment["BDD_FEATURE"].flatMap { $0 == "all" || $0.isEmpty ? nil : $0 }
let paths = try FileManager.default.contentsOfDirectory(atPath: directory)
    .filter { $0.hasSuffix(".feature") && (selected == nil || $0 == "\(selected ?? "").feature") }
    .sorted()
    .map { "\(directory)/\($0)" }
guard !paths.isEmpty else {
    print("No feature file to run in \(directory)")
    exit(1)
}

var passed = 0
var failed: [String] = []
for path in paths {
    let feature = try Gherkin.parse(try String(contentsOfFile: path, encoding: .utf8), path: path)
    print("Feature: \(feature.name)\n")
    for scenario in feature.scenarios {
        if await run(scenario, of: feature) {
            passed += 1
        } else {
            failed.append("\(path):\(scenario.line) \(scenario.name)")
        }
    }
}
print("\(passed + failed.count) scenarios: \(passed) passed, \(failed.count) failed")
for scenario in failed {
    print("  failed: \(scenario)")
}
exit(failed.isEmpty ? 0 : 1)
