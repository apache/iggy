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

/// XXH32 with a zero seed: the hash every SDK uses to map a messages key to a
/// partition, so a key lands on the same partition whichever SDK produced it.
enum XXH32 {
    private static let prime1: UInt32 = 0x9E37_79B1
    private static let prime2: UInt32 = 0x85EB_CA77
    private static let prime3: UInt32 = 0xC2B2_AE3D
    private static let prime4: UInt32 = 0x27D4_EB2F
    private static let prime5: UInt32 = 0x1656_67B1

    static func hash(_ bytes: [UInt8]) -> UInt32 {
        bytes.withUnsafeBytes { hash($0) }
    }

    static func hash(_ input: UnsafeRawBufferPointer) -> UInt32 {
        let length = input.count
        var offset = 0
        var hash: UInt32
        if length >= 16 {
            var v1 = prime1 &+ prime2
            var v2 = prime2
            var v3: UInt32 = 0
            var v4 = 0 &- prime1
            while offset + 16 <= length {
                v1 = round(v1, read32(input, offset))
                v2 = round(v2, read32(input, offset + 4))
                v3 = round(v3, read32(input, offset + 8))
                v4 = round(v4, read32(input, offset + 12))
                offset += 16
            }
            hash = rotl(v1, 1) &+ rotl(v2, 7) &+ rotl(v3, 12) &+ rotl(v4, 18)
        } else {
            hash = prime5
        }
        hash &+= UInt32(truncatingIfNeeded: length)
        while offset + 4 <= length {
            hash &+= read32(input, offset) &* prime3
            hash = rotl(hash, 17) &* prime4
            offset += 4
        }
        while offset < length {
            hash &+= UInt32(input[offset]) &* prime5
            hash = rotl(hash, 11) &* prime1
            offset += 1
        }
        hash ^= hash >> 15
        hash &*= prime2
        hash ^= hash >> 13
        hash &*= prime3
        hash ^= hash >> 16
        return hash
    }

    @inline(__always)
    private static func read32(_ buffer: UnsafeRawBufferPointer, _ offset: Int) -> UInt32 {
        UInt32(littleEndian: buffer.loadUnaligned(fromByteOffset: offset, as: UInt32.self))
    }

    @inline(__always)
    private static func rotl(_ value: UInt32, _ count: UInt32) -> UInt32 {
        (value << count) | (value >> (32 - count))
    }

    @inline(__always)
    private static func round(_ accumulator: UInt32, _ lane: UInt32) -> UInt32 {
        rotl(accumulator &+ lane &* prime2, 13) &* prime1
    }
}
