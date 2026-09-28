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

/// Failure while encoding or decoding wire bytes. Internal to the codec; the
/// protocol layer maps it onto an ``IggyError`` where a reply is decoded.
enum WireError: Error, Equatable {
    case truncated(offset: Int, need: Int, have: Int)
    case invalidUTF8(offset: Int)
    case validation(String)
}

/// Little-endian append-only encoder over a byte array.
///
/// Pass the encoded size to ``init(capacity:)`` when it is known up front,
/// as the batch encoder does, so the buffer is allocated once.
struct ByteWriter {
    private(set) var bytes: [UInt8]

    init(capacity: Int = 0) {
        bytes = []
        bytes.reserveCapacity(capacity)
    }

    mutating func write(_ value: UInt8) {
        bytes.append(value)
    }

    mutating func write(_ value: Bool) {
        bytes.append(value ? 1 : 0)
    }

    mutating func write(_ value: UInt16) {
        withUnsafeBytes(of: value.littleEndian) { bytes.append(contentsOf: $0) }
    }

    mutating func write(_ value: UInt32) {
        withUnsafeBytes(of: value.littleEndian) { bytes.append(contentsOf: $0) }
    }

    mutating func write(_ value: UInt64) {
        withUnsafeBytes(of: value.littleEndian) { bytes.append(contentsOf: $0) }
    }

    mutating func write(_ value: Float) {
        write(value.bitPattern)
    }

    mutating func write(_ value: UInt128Value) {
        write(value.low)
        write(value.high)
    }

    mutating func write(_ value: [UInt8]) {
        bytes.append(contentsOf: value)
    }

    mutating func write(_ value: ArraySlice<UInt8>) {
        bytes.append(contentsOf: value)
    }

    mutating func write(_ value: String) {
        bytes.append(contentsOf: value.utf8)
    }

    /// `[len: u8][utf8]`, the layout of every wire name, which must be 1 to
    /// 255 bytes long.
    mutating func writeName(_ value: String) throws {
        let length = value.utf8.count
        guard length >= 1, length <= Int(UInt8.max) else {
            throw WireError.validation("wire name must be 1-255 bytes, got \(length)")
        }
        bytes.append(UInt8(length))
        bytes.append(contentsOf: value.utf8)
    }

    /// `[len: u32][utf8]`, the layout of longer free-form strings.
    mutating func writeLongString(_ value: String) throws {
        guard let length = UInt32(exactly: value.utf8.count) else {
            throw WireError.validation("string of \(value.utf8.count) bytes exceeds the u32 length prefix")
        }
        write(length)
        bytes.append(contentsOf: value.utf8)
    }
}

/// Little-endian cursor over a byte slice. Every read is bounds-checked and
/// fails with ``WireError/truncated(offset:need:have:)`` instead of trapping,
/// because the bytes come from the network.
struct ByteReader {
    let bytes: ArraySlice<UInt8>
    private(set) var offset: Int

    init(_ bytes: ArraySlice<UInt8>) {
        self.bytes = bytes
        self.offset = bytes.startIndex
    }

    init(_ bytes: [UInt8]) {
        self.init(bytes[...])
    }

    var isAtEnd: Bool { offset >= bytes.endIndex }
    var remaining: Int { bytes.endIndex - offset }
    /// Position relative to the start of the buffer handed to the reader.
    var position: Int { offset - bytes.startIndex }

    private func require(_ count: Int) throws {
        if remaining < count {
            throw WireError.truncated(offset: position, need: count, have: remaining)
        }
    }

    mutating func readUInt8() throws -> UInt8 {
        try require(1)
        let value = bytes[offset]
        offset += 1
        return value
    }

    mutating func readBool() throws -> Bool {
        try readUInt8() != 0
    }

    mutating func readUInt16() throws -> UInt16 {
        UInt16(littleEndianBytes: try readBytes(2))
    }

    mutating func readUInt32() throws -> UInt32 {
        UInt32(littleEndianBytes: try readBytes(4))
    }

    mutating func readUInt64() throws -> UInt64 {
        UInt64(littleEndianBytes: try readBytes(8))
    }

    mutating func readFloat() throws -> Float {
        Float(bitPattern: try readUInt32())
    }

    mutating func readUInt128() throws -> UInt128Value {
        let low = try readUInt64()
        let high = try readUInt64()
        return UInt128Value(low: low, high: high)
    }

    mutating func readBytes(_ count: Int) throws -> ArraySlice<UInt8> {
        try require(count)
        let slice = bytes[offset..<offset + count]
        offset += count
        return slice
    }

    mutating func readString(_ count: Int) throws -> String {
        let start = position
        let slice = try readBytes(count)
        guard let value = String(bytes: slice, encoding: .utf8) else {
            throw WireError.invalidUTF8(offset: start)
        }
        return value
    }

    /// `[len: u8][utf8]` with the 1...255 byte bound every wire name carries.
    mutating func readName() throws -> String {
        let length = Int(try readUInt8())
        if length == 0 {
            throw WireError.validation("wire name must be 1-255 bytes, got 0")
        }
        return try readString(length)
    }

    /// `[len: u32][utf8]`.
    mutating func readLongString() throws -> String {
        let declared = try readUInt32()
        // A length that does not fit the platform's Int cannot be in the
        // buffer either; report it as truncation rather than trapping.
        guard let length = Int(exactly: declared) else {
            throw WireError.truncated(offset: position, need: Int.max, have: remaining)
        }
        return try readString(length)
    }
}

extension UInt16 {
    /// Decodes the first two bytes of `bytes`, which the caller has bounds-checked.
    init(littleEndianBytes bytes: ArraySlice<UInt8>) {
        self = bytes.withUnsafeBytes { UInt16(littleEndian: $0.loadUnaligned(as: UInt16.self)) }
    }
}

extension UInt32 {
    /// Decodes the first four bytes of `bytes`, which the caller has bounds-checked.
    init(littleEndianBytes bytes: ArraySlice<UInt8>) {
        self = bytes.withUnsafeBytes { UInt32(littleEndian: $0.loadUnaligned(as: UInt32.self)) }
    }
}

extension UInt64 {
    /// Decodes the first eight bytes of `bytes`, which the caller has bounds-checked.
    init(littleEndianBytes bytes: ArraySlice<UInt8>) {
        self = bytes.withUnsafeBytes { UInt64(littleEndian: $0.loadUnaligned(as: UInt64.self)) }
    }
}
