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

/// One option of a stream, topic, or user as reported by the server, with
/// whether the client set it explicitly or admission derived it.
public struct OptionValue: Sendable, Hashable, Codable {
    public var value: HeaderValue
    public var explicit: Bool

    public init(value: HeaderValue, explicit: Bool) {
        self.value = value
        self.explicit = explicit
    }

    public static func explicit(_ value: HeaderValue) -> OptionValue {
        OptionValue(value: value, explicit: true)
    }

    public static func derived(_ value: HeaderValue) -> OptionValue {
        OptionValue(value: value, explicit: false)
    }
}

/// Options of a resource keyed by option name.
public typealias ResourceOptions = [String: OptionValue]

/// The option catalog scope a ``IggyClient/describeOptions(scope:)`` call asks
/// about.
public enum OptionsScope: UInt8, Sendable, Hashable, Codable {
    case topic = 1
    case stream = 2
    case user = 3
}

/// One entry of the option catalog: a key the create command accepts, its
/// canonical kind, and the server's current default.
public struct OptionSpec: Sendable, Hashable, Codable {
    public var key: String
    public var kind: HeaderKind
    public var defaultValue: [UInt8]
    public var description: String

    public init(key: String, kind: HeaderKind, defaultValue: [UInt8], description: String) {
        self.key = key
        self.kind = kind
        self.defaultValue = defaultValue
        self.description = description
    }
}

/// Key-value options block for create and update requests: the user-headers
/// TLV with the stricter contract options need. Mirrors
/// `core/binary_protocol/src/primitives/options.rs`.
enum OptionsBlock {
    static let maxOptions = 1024
    static let maxOptionsBytes = 100 * 1000

    /// Encodes only the entries matching `explicit`, in key order.
    static func encode(_ options: ResourceOptions, explicitOnly: Bool = true) throws -> [UInt8] {
        let entries = options.filter { !explicitOnly || $0.value.explicit }.sorted { $0.key.utf8.lexicographicallyPrecedes($1.key.utf8) }
        if entries.isEmpty {
            return []
        }
        if entries.count > maxOptions {
            throw IggyError(.optionsBlockTooLarge, context: "\(entries.count) entries, maximum \(maxOptions)")
        }
        let size = entries.reduce(0) { $0 + 10 + $1.key.utf8.count + $1.value.value.bytes.count }
        if size > maxOptionsBytes {
            throw IggyError(.optionsBlockTooLarge, context: "\(size) bytes, maximum \(maxOptionsBytes)")
        }
        var writer = ByteWriter(capacity: size)
        for (key, option) in entries {
            let keyBytes = Array(key.utf8)
            guard !keyBytes.isEmpty, keyBytes.count <= HeaderTLV.maxFieldLength else {
                throw IggyError(.unsupportedOptionKey, context: "option key must be 1-255 bytes")
            }
            writer.write(HeaderKind.string.rawValue)
            writer.write(UInt32(keyBytes.count))
            writer.write(keyBytes)
            writer.write(option.value.kind.rawValue)
            writer.write(UInt32(option.value.bytes.count))
            writer.write(option.value.bytes)
        }
        return writer.bytes
    }

    /// Structural validation plus the options contract: string keys, no
    /// duplicates, bounded count and size. Returns the entries.
    static func validate(_ bytes: ArraySlice<UInt8>) throws -> [HeaderTLV.Entry] {
        if bytes.count > maxOptionsBytes {
            throw WireError.validation("options block is \(bytes.count) bytes, exceeds maximum \(maxOptionsBytes)")
        }
        let entries = try HeaderTLV.validate(bytes)
        if entries.isEmpty {
            return entries
        }
        if entries.count > maxOptions {
            throw WireError.validation("options block has \(entries.count) entries, exceeds maximum \(maxOptions)")
        }
        var keys = Set<[UInt8]>()
        for entry in entries {
            guard entry.keyKind == HeaderKind.string.rawValue else {
                throw WireError.validation("option key kind \(entry.keyKind) is not a string")
            }
            guard String(bytes: entry.key, encoding: .utf8) != nil else {
                throw WireError.validation("option key is not valid UTF-8")
            }
            guard keys.insert(Array(entry.key)).inserted else {
                throw WireError.validation("duplicate option key: \(String(decoding: entry.key, as: UTF8.self))")
            }
        }
        return entries
    }

    /// Decodes a block into domain options, marking every entry with the
    /// given provenance. Entries with an unknown value kind are skipped so a
    /// resource written by a newer server can still be read back.
    static func decode(_ bytes: ArraySlice<UInt8>, explicit: Bool) throws -> ResourceOptions {
        var options: ResourceOptions = [:]
        for entry in try validate(bytes) {
            guard let kind = HeaderKind(rawValue: entry.valueKind) else {
                continue
            }
            if let expected = kind.expectedSize, entry.value.count != expected {
                throw IggyError(.invalidHeaderValue)
            }
            let key = String(decoding: entry.key, as: UTF8.self)
            options[key] = OptionValue(value: HeaderValue(uncheckedKind: kind, bytes: Array(entry.value)), explicit: explicit)
        }
        return options
    }

    /// Merges the `(explicit, derived)` blocks a response carries.
    static func decodeSplit(explicit: ArraySlice<UInt8>, derived: ArraySlice<UInt8>) throws -> ResourceOptions {
        var options = try decode(derived, explicit: false)
        options.merge(try decode(explicit, explicit: true)) { _, explicitValue in explicitValue }
        return options
    }

    /// Reads a `u32`-length-prefixed block.
    static func readPrefixed(from reader: inout ByteReader) throws -> ArraySlice<UInt8> {
        let length = Int(try reader.readUInt32())
        let block = try reader.readBytes(length)
        _ = try validate(block)
        return block
    }
}
