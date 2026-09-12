// swift-tools-version: 6.0
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

import PackageDescription

// The BDD runner for the Swift SDK: a small Gherkin interpreter that walks
// the shared scenarios in bdd/scenarios and drives the SDK against a live
// server, the way the other SDK suites do with cucumber or godog.
let package = Package(
    name: "iggy-bdd",
    platforms: [.macOS(.v13)],
    dependencies: [
        .package(name: "iggy", path: "../../../foreign/swift")
    ],
    targets: [
        .executableTarget(
            name: "iggy-bdd",
            dependencies: [.product(name: "Iggy", package: "iggy")]
        )
    ],
    swiftLanguageModes: [.v6]
)
