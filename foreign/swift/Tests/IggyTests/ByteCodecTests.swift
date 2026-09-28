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

@testable import Iggy

@Suite("Byte codec")
struct ByteCodecTests {
    @Test func integersRoundTripLittleEndian() throws {
        var writer = ByteWriter()
        writer.write(UInt8(0xAB))
        writer.write(UInt16(0x0102))
        writer.write(UInt32(0x0304_0506))
        writer.write(UInt64(0x0708_090A_0B0C_0D0E))
        writer.write(UInt128Value(low: 1, high: 2))
        writer.write(Float(1.5))
        writer.write(true)
        try writer.writeName("ab")
        try writer.writeLongString("cd")
        #expect(writer.bytes[0..<7] == [0xAB, 0x02, 0x01, 0x06, 0x05, 0x04, 0x03])

        var reader = ByteReader(writer.bytes)
        #expect(try reader.readUInt8() == 0xAB)
        #expect(try reader.readUInt16() == 0x0102)
        #expect(try reader.readUInt32() == 0x0304_0506)
        #expect(try reader.readUInt64() == 0x0708_090A_0B0C_0D0E)
        #expect(try reader.readUInt128() == UInt128Value(low: 1, high: 2))
        #expect(try reader.readFloat() == 1.5)
        #expect(try reader.readBool())
        #expect(try reader.readName() == "ab")
        #expect(try reader.readLongString() == "cd")
        #expect(reader.isAtEnd)
    }

    @Test func truncatedReadsFailInsteadOfTrapping() {
        var reader = ByteReader([1, 2, 3])
        #expect(throws: WireError.truncated(offset: 0, need: 4, have: 3)) {
            try reader.readUInt32()
        }
        #expect(throws: WireError.self) {
            try reader.readBytes(4)
        }
        var name = ByteReader([5, 0x61])
        #expect(throws: WireError.self) {
            try name.readName()
        }
        var empty = ByteReader([0])
        #expect(throws: WireError.self) {
            try empty.readName()
        }
    }

    @Test func namesOutsideTheWireBoundAreRejected() {
        var writer = ByteWriter()
        #expect(throws: WireError.self) { try writer.writeName("") }
        #expect(throws: WireError.self) { try writer.writeName(String(repeating: "x", count: 256)) }
        #expect(writer.bytes.isEmpty)
        #expect(throws: Never.self) { try writer.writeName(String(repeating: "x", count: 255)) }
        #expect(writer.bytes.count == 256)
    }

    @Test func invalidUTF8IsRejected() {
        var reader = ByteReader([2, 0xFF, 0xFE])
        #expect(throws: WireError.invalidUTF8(offset: 1)) {
            try reader.readName()
        }
    }

    @Test func slicesReadRelativeToTheirStart() throws {
        let bytes: [UInt8] = [9, 9, 9, 1, 0, 0, 0]
        var reader = ByteReader(bytes[3...])
        #expect(try reader.readUInt32() == 1)
        #expect(reader.position == 4)
    }
}

@Suite("128-bit values")
struct UInt128ValueTests {
    @Test func decimalRenderingAndParsing() {
        #expect(UInt128Value(low: 0, high: 0).description == "0")
        #expect(UInt128Value(low: UInt64.max, high: 0).description == "18446744073709551615")
        #expect(UInt128Value(low: 0, high: 1).description == "18446744073709551616")
        #expect(UInt128Value(low: UInt64.max, high: UInt64.max).description == "340282366920938463463374607431768211455")
        #expect(UInt128Value("340282366920938463463374607431768211455") == UInt128Value(low: UInt64.max, high: UInt64.max))
        #expect(UInt128Value("18446744073709551616") == UInt128Value(low: 0, high: 1))
        #expect(UInt128Value("340282366920938463463374607431768211456") == nil)
        #expect(UInt128Value("") == nil)
        #expect(UInt128Value("12a") == nil)
    }

    @Test func uuidRoundTrip() {
        let uuid = UUID()
        let value = UInt128Value(uuid: uuid)
        #expect(value.uuid == uuid)
        #expect(UInt128Value(littleEndianBytes: value.littleEndianBytes[...]) == value)
        #expect(!UInt128Value.random().isZero)
        #expect(UInt128Value.random() != UInt128Value.random())
    }

    @Test func ordering() {
        #expect(UInt128Value(low: 0, high: 1) > UInt128Value(low: UInt64.max, high: 0))
        #expect(UInt128Value(low: 2, high: 1) > UInt128Value(low: 1, high: 1))
    }
}
