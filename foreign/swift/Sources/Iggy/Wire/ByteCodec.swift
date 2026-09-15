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
/// client maps it onto an ``IggyError`` at the API boundary.
enum WireError: Error, Equatable {
    case truncated(offset: Int, need: Int, have: Int)
    case invalidUTF8(offset: Int)
    case invalidDiscriminant(type: String, value: UInt8)
    case validation(String)
    case invalidBatchChecksum(stored: UInt64, computed: UInt64)
    case invalidMessageChecksum(stored: UInt64, computed: UInt64, offset: UInt64)
    case invalidMessageTimestampDelta(UInt64)

    var iggyError: IggyError {
        switch self {
        case .truncated(let offset, let need, let have):
            IggyError(.invalidFormat, context: "unexpected end of buffer at offset \(offset): need \(need) bytes, have \(have)")
        case .invalidUTF8(let offset):
            IggyError(.invalidUtf8, context: "invalid utf-8 at offset \(offset)")
        case .invalidDiscriminant(let type, let value):
            IggyError(.invalidFormat, context: "unknown discriminant \(value) for \(type)")
        case .validation(let message):
            IggyError(.invalidFormat, context: message)
        case .invalidBatchChecksum(let stored, let computed):
            IggyError(.invalidBatchChecksum, context: "stored \(stored), computed \(computed)")
        case .invalidMessageChecksum(let stored, let computed, let offset):
            IggyError(.invalidMessageChecksum, context: "stored \(stored), computed \(computed), offset \(offset)")
        case .invalidMessageTimestampDelta(let delta):
            IggyError(.invalidMessageTimestampDelta, context: "delta \(delta) microseconds")
        }
    }
}

/// Little-endian append-only encoder over a byte array.
struct ByteWriter {
    private(set) var bytes: [UInt8]

    init(capacity: Int = 0) {
        bytes = []
        bytes.reserveCapacity(capacity)
    }

    var count: Int { bytes.count }

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

    /// `[len: u8][utf8]`, the layout of every wire name. The caller guarantees
    /// the length fits.
    mutating func writeName(_ value: String) {
        let utf8 = Array(value.utf8)
        bytes.append(UInt8(utf8.count))
        bytes.append(contentsOf: utf8)
    }

    /// `[len: u32][utf8]`, the layout of longer free-form strings.
    mutating func writeLongString(_ value: String) {
        let utf8 = Array(value.utf8)
        write(UInt32(utf8.count))
        bytes.append(contentsOf: utf8)
    }

    mutating func writeZeros(_ count: Int) {
        bytes.append(contentsOf: repeatElement(0, count: count))
    }

    /// Overwrites `count` bytes at `offset` with `value`; `value` must have
    /// exactly `count` bytes and the range must already exist.
    mutating func overwrite(at offset: Int, with value: [UInt8]) {
        bytes.replaceSubrange(offset..<offset + value.count, with: value)
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
        try require(2)
        let value = bytes.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: UInt16.self) }
        offset += 2
        return UInt16(littleEndian: value)
    }

    mutating func readUInt32() throws -> UInt32 {
        try require(4)
        let value = bytes.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: UInt32.self) }
        offset += 4
        return UInt32(littleEndian: value)
    }

    mutating func readUInt64() throws -> UInt64 {
        try require(8)
        let value = bytes.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: position, as: UInt64.self) }
        offset += 8
        return UInt64(littleEndian: value)
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
        let length = Int(try readUInt32())
        return try readString(length)
    }

    mutating func skip(_ count: Int) throws {
        try require(count)
        offset += count
    }

    /// The bytes not yet consumed, without consuming them.
    var rest: ArraySlice<UInt8> { bytes[offset...] }
}

extension UInt32 {
    init(littleEndianBytes bytes: ArraySlice<UInt8>) {
        self = bytes.withUnsafeBytes { UInt32(littleEndian: $0.loadUnaligned(as: UInt32.self)) }
    }
}

extension UInt64 {
    init(littleEndianBytes bytes: ArraySlice<UInt8>) {
        self = bytes.withUnsafeBytes { UInt64(littleEndian: $0.loadUnaligned(as: UInt64.self)) }
    }

    var littleEndianBytes: [UInt8] {
        withUnsafeBytes(of: littleEndian) { Array($0) }
    }
}

extension UInt32 {
    var littleEndianBytes: [UInt8] {
        withUnsafeBytes(of: littleEndian) { Array($0) }
    }
}

extension UInt16 {
    init(littleEndianBytes bytes: ArraySlice<UInt8>) {
        self = bytes.withUnsafeBytes { UInt16(littleEndian: $0.loadUnaligned(as: UInt16.self)) }
    }

    var littleEndianBytes: [UInt8] {
        withUnsafeBytes(of: littleEndian) { Array($0) }
    }
}
