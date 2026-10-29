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

// Runs the shared scenarios the Swift SDK supports, each feature with its
// own step definitions as the godog suites do. `BDD_FEATURE` picks one
// feature or `all`; `BDD_SCENARIOS_DIR` points at the feature files and
// defaults to the repository's bdd/scenarios.
let features: [(name: String, register: (inout StepRegistry) -> Void)] = [
    ("basic_messaging", BasicMessagingSteps.register),
    ("raw_command", RawCommandSteps.register),
    ("session_contract", SessionContractSteps.register),
    ("stream_options", ResourceOptionsSteps.register),
    ("resource_options_patch", ResourceOptionsSteps.register),
    (
        "metadata_changes",
        { registry in
            ResourceOptionsSteps.register(into: &registry)
            MetadataChangesSteps.register(into: &registry)
        }
    ),
]

let selected = ProcessInfo.processInfo.environment["BDD_FEATURE"].flatMap { $0.isEmpty || $0 == "all" ? nil : $0 }
let toRun = features.filter { selected == nil || $0.name == selected }
guard !toRun.isEmpty else {
    print("unknown BDD_FEATURE=\(selected ?? ""); supported: \(features.map(\.name).joined(separator: ", "))")
    exit(2)
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

var totalPassed = 0
var totalFailed = 0
for feature in toRun {
    let path = scenariosDirectory.appendingPathComponent("\(feature.name).feature")
    let parsed: Feature
    do {
        parsed = try GherkinParser.parse(try String(contentsOf: path, encoding: .utf8))
    } catch {
        print("cannot load \(path.path): \(error)")
        exit(2)
    }
    var registry = StepRegistry()
    CommonSteps.register(into: &registry)
    feature.register(&registry)
    let result = await Runner(registry: registry).run(parsed)
    totalPassed += result.passed
    totalFailed += result.failed
    print("")
}
print("\(totalPassed + totalFailed) scenarios (\(totalPassed) passed, \(totalFailed) failed)")
exit(totalFailed == 0 ? 0 : 1)
