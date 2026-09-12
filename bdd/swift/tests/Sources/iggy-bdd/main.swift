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

// Runs the shared scenarios the Swift SDK supports. `BDD_FEATURE` picks one
// feature or `all`; `BDD_SCENARIOS_DIR` points at the feature files and
// defaults to the repository's bdd/scenarios next to this package.
let supportedFeatures = ["basic_messaging", "raw_command", "stream_crud"]
let selected = ProcessInfo.processInfo.environment["BDD_FEATURE"].flatMap { $0.isEmpty || $0 == "all" ? nil : $0 }
let features: [String]
if let selected {
    guard supportedFeatures.contains(selected) else {
        print("unknown or unsupported BDD_FEATURE=\(selected); supported: \(supportedFeatures.joined(separator: ", "))")
        exit(2)
    }
    features = [selected]
} else {
    features = supportedFeatures
}

let scenariosDirectory: URL
if let configured = ProcessInfo.processInfo.environment["BDD_SCENARIOS_DIR"], !configured.isEmpty {
    scenariosDirectory = URL(fileURLWithPath: configured)
} else {
    scenariosDirectory = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()  // main.swift
        .deletingLastPathComponent()  // iggy-bdd
        .deletingLastPathComponent()  // Sources
        .deletingLastPathComponent()  // tests
        .deletingLastPathComponent()  // swift
        .appendingPathComponent("scenarios")
}

var registry = StepRegistry()
CommonSteps.register(into: &registry)
StreamSteps.register(into: &registry)
MessageSteps.register(into: &registry)
RawCommandSteps.register(into: &registry)
let runner = Runner(registry: registry)

var totalPassed = 0
var totalFailed = 0
for name in features {
    let path = scenariosDirectory.appendingPathComponent("\(name).feature")
    let feature: Feature
    do {
        feature = try GherkinParser.parse(try String(contentsOf: path, encoding: .utf8))
    } catch {
        print("cannot load \(path.path): \(error)")
        exit(2)
    }
    let result = await runner.run(feature)
    totalPassed += result.passed
    totalFailed += result.failed
    print("")
}
print("\(totalPassed + totalFailed) scenarios (\(totalPassed) passed, \(totalFailed) failed)")
exit(totalFailed == 0 ? 0 : 1)
