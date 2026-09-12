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
import Testing

/// Byte-exact fixtures produced by `Tools/golden-vectors` from the Rust
/// protocol crates. Regenerate with:
///
/// ```text
/// cargo run --manifest-path Tools/golden-vectors/Cargo.toml -- Tests/IggyTests/Fixtures/golden.json
/// ```
struct GoldenFixture: Decodable {
    struct HashVector: Decodable {
        let len: Int
        let hash: String
    }

    struct ErrorEntry: Decodable {
        let code: UInt32
        let name: String
    }

    let protocolVersion: UInt32
    let protocolVersionMin: UInt32
    let xxh3_64: [HashVector]
    let xxh32: [HashVector]
    let errors: [ErrorEntry]
    let vectors: [String: String]

    enum CodingKeys: String, CodingKey {
        case protocolVersion = "protocol_version"
        case protocolVersionMin = "protocol_version_min"
        case xxh3_64
        case xxh32
        case errors
        case vectors
    }

    static let shared: GoldenFixture = {
        let url = Bundle.module.url(forResource: "golden", withExtension: "json", subdirectory: "Fixtures")!
        let data = try! Data(contentsOf: url)
        return try! JSONDecoder().decode(GoldenFixture.self, from: data)
    }()

    /// The deterministic input the hash vectors were computed over.
    static func pattern(_ length: Int) -> [UInt8] {
        (0..<length).map { UInt8(truncatingIfNeeded: $0) &* 31 &+ 7 }
    }

    /// Bytes of a named vector; fails the test when the name is unknown.
    func bytes(_ name: String) -> [UInt8] {
        guard let hex = vectors[name] else {
            Issue.record("missing golden vector \(name)")
            return []
        }
        return [UInt8](hex: hex)
    }
}

extension [UInt8] {
    init(hex: String) {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(hex.count / 2)
        var index = hex.startIndex
        while index < hex.endIndex {
            let next = hex.index(index, offsetBy: 2)
            bytes.append(UInt8(hex[index..<next], radix: 16)!)
            index = next
        }
        self = bytes
    }

    var hex: String {
        map { byte in
            let text = String(byte, radix: 16)
            return text.count == 1 ? "0" + text : text
        }.joined()
    }
}
