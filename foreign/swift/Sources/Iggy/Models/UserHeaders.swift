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

/// Type tag of a user header key or value.
public enum HeaderKind: UInt8, Sendable, Hashable, Codable, CaseIterable, CustomStringConvertible {
    case raw = 1
    case string = 2
    case bool = 3
    case int8 = 4
    case int16 = 5
    case int32 = 6
    case int64 = 7
    case int128 = 8
    case uint8 = 9
    case uint16 = 10
    case uint32 = 11
    case uint64 = 12
    case uint128 = 13
    case float32 = 14
    case float64 = 15

    /// Byte length of the fixed-size kinds; nil for `raw` and `string`.
    var expectedSize: Int? {
        switch self {
        case .raw, .string: nil
        case .bool, .int8, .uint8: 1
        case .int16, .uint16: 2
        case .int32, .uint32, .float32: 4
        case .int64, .uint64, .float64: 8
        case .int128, .uint128: 16
        }
    }

    public var description: String {
        switch self {
        case .raw: "raw"
        case .string: "string"
        case .bool: "bool"
        case .int8: "int8"
        case .int16: "int16"
        case .int32: "int32"
        case .int64: "int64"
        case .int128: "int128"
        case .uint8: "uint8"
        case .uint16: "uint16"
        case .uint32: "uint32"
        case .uint64: "uint64"
        case .uint128: "uint128"
        case .float32: "float32"
        case .float64: "float64"
        }
    }

    public init?(name: String) {
        guard let kind = HeaderKind.allCases.first(where: { $0.description == name }) else {
            return nil
        }
        self = kind
    }
}

/// A typed value carried in a user header. The same shape describes option
/// values in resource metadata.
public struct HeaderValue: Sendable, Hashable, Codable, CustomStringConvertible {
    public let kind: HeaderKind
    public let bytes: [UInt8]

    /// Wraps bytes already laid out for `kind`, validating fixed-size kinds.
    public init(kind: HeaderKind, bytes: [UInt8]) throws {
        if let expected = kind.expectedSize, bytes.count != expected {
            throw IggyError(.invalidHeaderValue, context: "\(kind) needs \(expected) bytes, got \(bytes.count)")
        }
        if bytes.count > 255 {
            throw IggyError(.invalidHeaderValue, context: "value exceeds 255 bytes")
        }
        self.kind = kind
        self.bytes = bytes
    }

    init(uncheckedKind kind: HeaderKind, bytes: [UInt8]) {
        self.kind = kind
        self.bytes = bytes
    }

    public static func raw(_ bytes: [UInt8]) throws -> HeaderValue {
        try HeaderValue(kind: .raw, bytes: bytes)
    }

    public static func string(_ value: String) throws -> HeaderValue {
        try HeaderValue(kind: .string, bytes: Array(value.utf8))
    }

    public static func bool(_ value: Bool) -> HeaderValue {
        HeaderValue(uncheckedKind: .bool, bytes: [value ? 1 : 0])
    }

    public static func int8(_ value: Int8) -> HeaderValue {
        HeaderValue(uncheckedKind: .int8, bytes: [UInt8(bitPattern: value)])
    }

    public static func int16(_ value: Int16) -> HeaderValue {
        HeaderValue(uncheckedKind: .int16, bytes: UInt16(bitPattern: value).littleEndianBytes)
    }

    public static func int32(_ value: Int32) -> HeaderValue {
        HeaderValue(uncheckedKind: .int32, bytes: UInt32(bitPattern: value).littleEndianBytes)
    }

    public static func int64(_ value: Int64) -> HeaderValue {
        HeaderValue(uncheckedKind: .int64, bytes: UInt64(bitPattern: value).littleEndianBytes)
    }

    /// A two's-complement 128-bit integer given as its unsigned bit pattern.
    public static func int128(bitPattern value: UInt128Value) -> HeaderValue {
        HeaderValue(uncheckedKind: .int128, bytes: value.littleEndianBytes)
    }

    public static func uint8(_ value: UInt8) -> HeaderValue {
        HeaderValue(uncheckedKind: .uint8, bytes: [value])
    }

    public static func uint16(_ value: UInt16) -> HeaderValue {
        HeaderValue(uncheckedKind: .uint16, bytes: value.littleEndianBytes)
    }

    public static func uint32(_ value: UInt32) -> HeaderValue {
        HeaderValue(uncheckedKind: .uint32, bytes: value.littleEndianBytes)
    }

    public static func uint64(_ value: UInt64) -> HeaderValue {
        HeaderValue(uncheckedKind: .uint64, bytes: value.littleEndianBytes)
    }

    public static func uint128(_ value: UInt128Value) -> HeaderValue {
        HeaderValue(uncheckedKind: .uint128, bytes: value.littleEndianBytes)
    }

    public static func float32(_ value: Float) -> HeaderValue {
        HeaderValue(uncheckedKind: .float32, bytes: value.bitPattern.littleEndianBytes)
    }

    public static func float64(_ value: Double) -> HeaderValue {
        HeaderValue(uncheckedKind: .float64, bytes: value.bitPattern.littleEndianBytes)
    }

    public var stringValue: String? {
        kind == .string ? String(decoding: bytes, as: UTF8.self) : nil
    }

    public var boolValue: Bool? {
        guard kind == .bool, let byte = bytes.first else { return nil }
        return byte != 0
    }

    public var int8Value: Int8? {
        guard kind == .int8, let byte = bytes.first else { return nil }
        return Int8(bitPattern: byte)
    }

    public var int16Value: Int16? {
        kind == .int16 ? Int16(bitPattern: UInt16(littleEndianBytes: bytes[...])) : nil
    }

    public var int32Value: Int32? {
        kind == .int32 ? Int32(bitPattern: UInt32(littleEndianBytes: bytes[...])) : nil
    }

    public var int64Value: Int64? {
        kind == .int64 ? Int64(bitPattern: UInt64(littleEndianBytes: bytes[...])) : nil
    }

    public var int128BitPattern: UInt128Value? {
        kind == .int128 ? UInt128Value(littleEndianBytes: bytes[...]) : nil
    }

    public var uint8Value: UInt8? {
        kind == .uint8 ? bytes.first : nil
    }

    public var uint16Value: UInt16? {
        kind == .uint16 ? UInt16(littleEndianBytes: bytes[...]) : nil
    }

    public var uint32Value: UInt32? {
        kind == .uint32 ? UInt32(littleEndianBytes: bytes[...]) : nil
    }

    public var uint64Value: UInt64? {
        kind == .uint64 ? UInt64(littleEndianBytes: bytes[...]) : nil
    }

    public var uint128Value: UInt128Value? {
        kind == .uint128 ? UInt128Value(littleEndianBytes: bytes[...]) : nil
    }

    public var float32Value: Float? {
        kind == .float32 ? Float(bitPattern: UInt32(littleEndianBytes: bytes[...])) : nil
    }

    public var float64Value: Double? {
        kind == .float64 ? Double(bitPattern: UInt64(littleEndianBytes: bytes[...])) : nil
    }

    /// A readable rendering of the value regardless of its kind.
    public var description: String {
        switch kind {
        case .raw: bytes.map { String($0, radix: 16).leftPadded(to: 2) }.joined()
        case .string: stringValue ?? ""
        case .bool: String(boolValue ?? false)
        case .int8: String(int8Value ?? 0)
        case .int16: String(int16Value ?? 0)
        case .int32: String(int32Value ?? 0)
        case .int64: String(int64Value ?? 0)
        case .int128: (int128BitPattern ?? .zero).description
        case .uint8: String(uint8Value ?? 0)
        case .uint16: String(uint16Value ?? 0)
        case .uint32: String(uint32Value ?? 0)
        case .uint64: String(uint64Value ?? 0)
        case .uint128: (uint128Value ?? .zero).description
        case .float32: String(float32Value ?? 0)
        case .float64: String(float64Value ?? 0)
        }
    }
}

extension HeaderValue: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        do {
            self = try .string(value)
        } catch {
            preconditionFailure("invalid header value literal: \(error)")
        }
    }
}

extension HeaderValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self = .bool(value)
    }
}

extension HeaderValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int64) {
        self = .int64(value)
    }
}

extension HeaderValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: Double) {
        self = .float64(value)
    }
}

/// Key of a user header. Almost always a string, but the wire allows any
/// kind, so the key carries its own tag.
public struct HeaderKey: Sendable, Hashable, Codable, CustomStringConvertible {
    public let kind: HeaderKind
    public let bytes: [UInt8]

    public init(kind: HeaderKind, bytes: [UInt8]) throws {
        if bytes.isEmpty || bytes.count > 255 {
            throw IggyError(.invalidHeaderKey, context: "key must be 1-255 bytes, got \(bytes.count)")
        }
        if let expected = kind.expectedSize, bytes.count != expected {
            throw IggyError(.invalidHeaderKey, context: "\(kind) needs \(expected) bytes, got \(bytes.count)")
        }
        self.kind = kind
        self.bytes = bytes
    }

    init(uncheckedKind kind: HeaderKind, bytes: [UInt8]) {
        self.kind = kind
        self.bytes = bytes
    }

    public init(_ name: String) throws {
        try self.init(kind: .string, bytes: Array(name.utf8))
    }

    public var stringValue: String? {
        kind == .string ? String(decoding: bytes, as: UTF8.self) : nil
    }

    public var description: String {
        stringValue ?? bytes.map { String($0, radix: 16).leftPadded(to: 2) }.joined()
    }
}

extension HeaderKey: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        do {
            try self.init(value)
        } catch {
            preconditionFailure("invalid header key literal: \(error)")
        }
    }
}

extension HeaderKey: Comparable {
    /// Ordered like the Rust `BTreeMap` the reference SDK keeps headers in,
    /// so an encoded block is byte-identical across SDKs.
    public static func < (lhs: HeaderKey, rhs: HeaderKey) -> Bool {
        if lhs.kind != rhs.kind {
            return lhs.kind.rawValue < rhs.kind.rawValue
        }
        return lhs.bytes.lexicographicallyPrecedes(rhs.bytes)
    }
}

/// User headers attached to a message.
public typealias UserHeaders = [HeaderKey: HeaderValue]

extension String {
    fileprivate func leftPadded(to width: Int) -> String {
        count >= width ? self : String(repeating: "0", count: width - count) + self
    }
}

/// The TLV encoding shared by user headers and resource options:
/// `[key_kind:u8][key_len:u32][key][value_kind:u8][value_len:u32][value]`.
enum HeaderTLV {
    static let maxFieldLength = 255

    static func encodedSize(_ headers: UserHeaders) -> Int {
        headers.reduce(0) { $0 + 10 + $1.key.bytes.count + $1.value.bytes.count }
    }

    /// Encodes in key order, the deterministic layout the Rust SDK produces.
    static func encode(_ headers: UserHeaders) -> [UInt8] {
        var writer = ByteWriter(capacity: encodedSize(headers))
        for key in headers.keys.sorted() {
            let value = headers[key]!
            writer.write(key.kind.rawValue)
            writer.write(UInt32(key.bytes.count))
            writer.write(key.bytes)
            writer.write(value.kind.rawValue)
            writer.write(UInt32(value.bytes.count))
            writer.write(value.bytes)
        }
        return writer.bytes
    }

    /// One structurally validated TLV entry. Kind codes are not interpreted so
    /// unknown kinds stay forwardable.
    struct Entry {
        let keyKind: UInt8
        let key: ArraySlice<UInt8>
        let valueKind: UInt8
        let value: ArraySlice<UInt8>
    }

    /// Walks the TLV pairs, checking every kind byte is non-zero, every length
    /// is in `1...255`, nothing overruns, nothing trails, and entries pair up.
    static func validate(_ bytes: ArraySlice<UInt8>) throws -> [Entry] {
        var reader = ByteReader(bytes)
        var fields: [(UInt8, ArraySlice<UInt8>)] = []
        while !reader.isAtEnd {
            let kind = try reader.readUInt8()
            if kind == 0 {
                throw WireError.validation("header kind is 0 (reserved) at offset \(reader.position - 1)")
            }
            let length = Int(try reader.readUInt32())
            if length == 0 || length > maxFieldLength {
                throw WireError.validation("header field length \(length) out of range 1...\(maxFieldLength)")
            }
            fields.append((kind, try reader.readBytes(length)))
        }
        if fields.count % 2 != 0 {
            throw WireError.validation("odd number of TLV entries (\(fields.count)), expected key-value pairs")
        }
        var entries: [Entry] = []
        entries.reserveCapacity(fields.count / 2)
        var index = 0
        while index < fields.count {
            entries.append(Entry(keyKind: fields[index].0, key: fields[index].1, valueKind: fields[index + 1].0, value: fields[index + 1].1))
            index += 2
        }
        return entries
    }

    /// Decodes into typed headers. Unknown kind codes fail unless `skipUnknown`
    /// is set, in which case the entry is dropped and the rest kept.
    static func decode(_ bytes: ArraySlice<UInt8>, skipUnknown: Bool) throws -> UserHeaders {
        var headers: UserHeaders = [:]
        for entry in try validate(bytes) {
            guard let keyKind = HeaderKind(rawValue: entry.keyKind), let valueKind = HeaderKind(rawValue: entry.valueKind) else {
                if skipUnknown {
                    continue
                }
                throw IggyError(.invalidHeaderKind, context: "unknown header kind code")
            }
            if let expected = keyKind.expectedSize, entry.key.count != expected {
                throw IggyError(.invalidHeaderKey)
            }
            if let expected = valueKind.expectedSize, entry.value.count != expected {
                throw IggyError(.invalidHeaderValue)
            }
            headers[HeaderKey(uncheckedKind: keyKind, bytes: Array(entry.key))] = HeaderValue(uncheckedKind: valueKind, bytes: Array(entry.value))
        }
        return headers
    }
}
