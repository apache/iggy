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

// The conformance agent of the Swift SDK, which depends on the SDK by path.
// SwiftPM names a path package after its directory, so the agent lives in
// `agent/`: a package directory named `swift` would clash with the SDK's.
// macOS 15 brings Int128 and UInt128, which the text forms of 128-bit header
// values use.
let package = Package(
    name: "iggy-conformance-agent",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(name: "apache-iggy", path: "../../../../foreign/swift"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.6.0"),
    ],
    targets: [
        .executableTarget(
            name: "iggy-agent",
            dependencies: [
                .product(name: "Iggy", package: "apache-iggy"),
                .product(name: "Logging", package: "swift-log"),
            ]
        )
    ],
    swiftLanguageModes: [.v6]
)
