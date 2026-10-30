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
public struct OptionValue: Sendable, Hashable {
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
public enum OptionsScope: UInt8, Sendable, Hashable {
    case topic = 1
    case stream = 2
    case user = 3
}

/// One entry of a scope's option catalog: a key, its canonical kind, what an
/// update may do with it, and the server's default.
public struct OptionSpec: Sendable, Hashable {
    public var key: String
    public var kind: HeaderKind
    public var flags: OptionFlags
    public var defaultValue: [UInt8]
    public var description: String

    public init(key: String, kind: HeaderKind, flags: OptionFlags = [], defaultValue: [UInt8], description: String) {
        self.key = key
        self.kind = kind
        self.flags = flags
        self.defaultValue = defaultValue
        self.description = description
    }
}

/// What a catalog key allows, as ``IggyClient/describeOptions(scope:)``
/// reports it. A key with none of the first two flags is create-only.
public struct OptionFlags: OptionSet, Sendable, Hashable {
    public let rawValue: UInt8

    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    /// An update may set the key.
    public static let updatable = OptionFlags(rawValue: 1 << 0)
    /// An update may reset the key to its default.
    public static let resettable = OptionFlags(rawValue: 1 << 1)
    /// Write-only: accepted at create and never returned.
    public static let secret = OptionFlags(rawValue: 1 << 2)
    /// Computed by the server and returned on reads, never accepted.
    public static let readOnly = OptionFlags(rawValue: 1 << 3)
    /// A server that does not understand the key must not serve the resource.
    public static let critical = OptionFlags(rawValue: 1 << 4)
}

/// A key of one scope's option catalog, with the flags the server's catalog
/// gives it. Cases are declared in catalog order.
protocol CatalogOptionKey: RawRepresentable, CaseIterable, Hashable where RawValue == String {
    var flags: OptionFlags { get }
}

/// An update's options: the keys to set and the keys to return to their
/// defaults. Every other key keeps its stored value.
struct OptionsPatch {
    /// Reset names the `u8` count of the request carries at most.
    static let maxResetKeys = Int(UInt8.max)

    let set: ResourceOptions
    /// In byte order, as the Rust encoder writes them.
    let reset: [String]

    /// Refuses the patch as server admission would, in the server's order:
    /// the request shape, then the key categories, then the values `values`
    /// encodes. A patch with a raw key or raw reset name gets the shape
    /// checks only, since the server's catalog may know keys or flags this
    /// build does not.
    init<Key: CatalogOptionKey>(
        typed: [Key], raw: [String: String], reset: Set<Key>, rawReset: Set<String>, values: () throws -> ResourceOptions
    ) throws {
        let setKeys = Set(typed.map(\.rawValue)).union(raw.keys)
        let names = Set(reset.map(\.rawValue)).union(rawReset).sorted { $0.utf8.lexicographicallyPrecedes($1.utf8) }
        for name in names where name.isEmpty || name.utf8.count > HeaderTLV.maxFieldLength {
            throw IggyError(.unsupportedOptionKey, context: String(name.prefix(errorKeyPreviewLength)))
        }
        if names.count > Self.maxResetKeys {
            let unknown = names.first { Key(rawValue: $0) == nil }
            throw unknown.map { IggyError(.unsupportedOptionKey, context: String($0.prefix(errorKeyPreviewLength))) }
                ?? IggyError(.invalidCommand, context: "more than \(Self.maxResetKeys) reset keys")
        }
        if setKeys.isEmpty && names.isEmpty {
            throw IggyError(.invalidCommand, context: "the update sets and resets nothing")
        }
        if let name = names.first(where: setKeys.contains) {
            throw IggyError(.invalidCommand, context: "\(name) is both set and reset")
        }
        if raw.isEmpty && rawReset.isEmpty {
            let resetKeys = names.compactMap(Key.init(rawValue:))
            let allKeys = typed.sorted { $0.rawValue.utf8.lexicographicallyPrecedes($1.rawValue.utf8) } + resetKeys
            if let key = allKeys.first(where: { $0.flags.contains(.readOnly) }) {
                throw IggyError(.readOnlyOption, context: key.rawValue)
            }
            if let key = allKeys.first(where: { !$0.flags.contains(.updatable) }) {
                throw IggyError(.optionNotUpdatable, context: key.rawValue)
            }
            if let key = resetKeys.first(where: { !$0.flags.contains(.resettable) }) {
                throw IggyError(.optionNotResettable, context: key.rawValue)
            }
        }
        self.set = try values()
        self.reset = names
    }
}

/// Key-value options block for create and update requests: the user-headers
/// TLV with the stricter contract options need. Mirrors
/// `core/binary_protocol/src/primitives/options.rs`.
enum OptionsBlock {
    static let maxOptions = 1024
    static let maxOptionsBytes = 100 * 1000

    /// Encodes the explicit entries in key order; derived values are the
    /// server's to compute and are never sent back.
    static func encode(_ options: ResourceOptions) throws -> [UInt8] {
        let entries = options.filter { $0.value.explicit }.sorted { $0.key.utf8.lexicographicallyPrecedes($1.key.utf8) }
        if entries.isEmpty {
            return []
        }
        if entries.count > maxOptions {
            throw IggyError(.optionsBlockTooLarge, context: "\(entries.count) entries, maximum \(maxOptions)")
        }
        let size = entries.reduce(0) { $0 + HeaderTLV.entryOverhead + $1.key.utf8.count + $1.value.value.bytes.count }
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
}

/// Characters of an offending key an error message shows,
/// `ERROR_KEY_PREVIEW_LEN` in the Rust crate.
private let errorKeyPreviewLength = 64

/// Keys sent as `String` values, all explicit. Typed fields are inserted
/// afterwards, so they win on collision.
func rawOptions(_ raw: [String: String]) throws -> ResourceOptions {
    var options: ResourceOptions = [:]
    for (key, value) in raw {
        guard !key.isEmpty, key.utf8.count <= HeaderTLV.maxFieldLength else {
            throw IggyError(.unsupportedOptionKey, context: String(key.prefix(errorKeyPreviewLength)))
        }
        guard let headerValue = try? HeaderValue.string(value) else {
            throw IggyError(.invalidOptionValue, context: String(key.prefix(errorKeyPreviewLength)))
        }
        options[key] = .explicit(headerValue)
    }
    return options
}

/// The value of a column key such as a name, refused with the scope's own
/// error when it does not fit an option value.
func columnValue(_ text: String, refusal: IggyErrorCode) throws -> HeaderValue {
    guard let value = try? HeaderValue.string(text) else {
        throw IggyError(refusal, context: "must be 1-\(HeaderTLV.maxFieldLength) bytes, got \(text.utf8.count)")
    }
    return value
}
