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

@Suite("XXH3-64")
struct XXH3Tests {
    @Test("matches the reference implementation for every golden length")
    func matchesGoldenVectors() {
        let fixture = GoldenFixture.shared
        #expect(fixture.xxh3_64.count > 300)
        for vector in fixture.xxh3_64 {
            let input = GoldenFixture.pattern(vector.len)
            let hash = XXH3.hash64(input)
            #expect(String(hash, radix: 16).leftPadded(16) == vector.hash, "length \(vector.len)")
        }
    }

    @Test("hashes slices by their own bounds")
    func hashesSlices() {
        let input = GoldenFixture.pattern(600)
        let slice = input[100..<400]
        #expect(XXH3.hash64(slice) == XXH3.hash64(Array(slice)))
    }

    @Test("empty input has the well-known value")
    func emptyInput() {
        #expect(XXH3.hash64([]) == 0x2D06_8005_38D3_94C2)
    }
}

@Suite("XXH32")
struct XXH32Tests {
    @Test("matches the reference implementation for every golden length")
    func matchesGoldenVectors() {
        for vector in GoldenFixture.shared.xxh32 {
            let hash = XXH32.hash(GoldenFixture.pattern(vector.len))
            #expect(String(hash, radix: 16).leftPadded(8) == vector.hash, "length \(vector.len)")
        }
    }

    @Test("empty input has the well-known value")
    func emptyInput() {
        #expect(XXH32.hash([]) == 0x02CC_5D05)
    }
}

extension String {
    func leftPadded(_ width: Int) -> String {
        count >= width ? self : String(repeating: "0", count: width - count) + self
    }
}
