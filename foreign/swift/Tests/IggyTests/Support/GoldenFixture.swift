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
    struct HashVector {
        let len: Int
        let hash: String
    }

    struct ErrorEntry {
        let code: UInt32
        let name: String
    }

    private let xxh3_64Table: [String: String]
    private let xxh32Table: [String: String]
    private let errorTable: [String: String]

    /// The XXH3-64 vectors, by input length.
    var xxh3_64: [HashVector] { Self.hashVectors(xxh3_64Table) }

    /// The XXH32 vectors, by input length.
    var xxh32: [HashVector] { Self.hashVectors(xxh32Table) }

    /// Every error code the server defines, with its snake_case name.
    var errors: [ErrorEntry] {
        errorTable.map { ErrorEntry(code: UInt32($0.key)!, name: $0.value) }.sorted { $0.code < $1.code }
    }

    private static func hashVectors(_ table: [String: String]) -> [HashVector] {
        table.map { HashVector(len: Int($0.key)!, hash: $0.value) }.sorted { $0.len < $1.len }
    }

    enum CodingKeys: String, CodingKey {
        case xxh3_64Table = "xxh3_64"
        case xxh32Table = "xxh32"
        case errorTable = "errors"
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
}
