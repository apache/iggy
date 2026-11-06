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
import Iggy

/// A value that an argument of the protocol names, read from its JSON form.
/// A form that the agent cannot read throws a ``Fault``, and a value that
/// the SDK refuses throws the SDK's error.
protocol ArgumentForm {
    init(argument: JSON) throws
}

/// The arguments of one call. Each read takes its key, and ``finish()``
/// refuses a key that no read took, so a new argument of the corpus fails
/// instead of being ignored.
struct Arguments {
    private let fields: [String: JSON]
    private var taken: Set<String> = []

    init(_ json: JSON?) throws {
        switch json {
        case nil, .null?: fields = [:]
        case .object(let fields)?: self.fields = fields
        case let other?: throw Fault("arguments are an object, not \(other)")
        }
    }

    mutating func optional<Value: ArgumentForm>(_ key: String) throws -> Value? {
        taken.insert(key)
        guard let value = fields[key], value != .null else {
            return nil
        }
        do {
            return try Value(argument: value)
        } catch let fault as Fault {
            throw Fault("\(key): \(fault.reason)")
        }
    }

    mutating func required<Value: ArgumentForm>(_ key: String) throws -> Value {
        guard let value: Value = try optional(key) else {
            throw Fault("the argument \(key) is missing")
        }
        return value
    }

    func finish() throws {
        if let unknown = fields.keys.sorted().first(where: { !taken.contains($0) }) {
            throw Fault("unknown argument \(unknown)")
        }
    }
}

extension JSON {
    /// A keyword such as `first`, or a one-key object such as `{offset: 3}`.
    func keyed() throws -> (key: String, value: JSON?) {
        switch self {
        case .string(let keyword):
            return (keyword, nil)
        case .object(let fields) where fields.count == 1:
            return (fields.keys.first ?? "", fields.values.first)
        default:
            throw Fault("expected a keyword or a one-key object, got \(self)")
        }
    }
}

extension String: ArgumentForm {
    init(argument: JSON) throws {
        guard case .string(let text) = argument else {
            throw Fault("expected text, got \(argument)")
        }
        self = text
    }
}

extension Bool: ArgumentForm {
    init(argument: JSON) throws {
        guard case .bool(let value) = argument else {
            throw Fault("expected a boolean, got \(argument)")
        }
        self = value
    }
}

extension ArgumentForm where Self: FixedWidthInteger {
    init(argument: JSON) throws {
        guard case .number(let number) = argument, let value = Self(exactly: number) else {
            throw Fault("expected a \(Self.self), got \(argument)")
        }
        self = value
    }
}

extension UInt8: ArgumentForm {}
extension UInt32: ArgumentForm {}
extension UInt64: ArgumentForm {}

extension Array: ArgumentForm where Element: ArgumentForm {
    init(argument: JSON) throws {
        guard case .array(let values) = argument else {
            throw Fault("expected a list, got \(argument)")
        }
        self = try values.map(Element.init(argument:))
    }
}

extension Dictionary: ArgumentForm where Key == String, Value: ArgumentForm {
    init(argument: JSON) throws {
        guard case .object(let fields) = argument else {
            throw Fault("expected an object, got \(argument)")
        }
        self = try fields.mapValues(Value.init(argument:))
    }
}

/// Bytes as hex text.
struct HexBytes: ArgumentForm {
    let bytes: [UInt8]

    init(argument: JSON) throws {
        let digits = Array(try String(argument: argument).utf8)
        guard digits.count.isMultiple(of: 2) else {
            throw Fault("hex text has an even length, not \(digits.count)")
        }
        bytes = try stride(from: 0, to: digits.count, by: 2).map { index in
            guard let high = Self.nibble(digits[index]), let low = Self.nibble(digits[index + 1]) else {
                throw Fault("\(argument) is not hex")
            }
            return high << 4 | low
        }
    }

    private static func nibble(_ digit: UInt8) -> UInt8? {
        switch digit {
        case UInt8(ascii: "0")...UInt8(ascii: "9"): digit - UInt8(ascii: "0")
        case UInt8(ascii: "a")...UInt8(ascii: "f"): digit - UInt8(ascii: "a") + 10
        case UInt8(ascii: "A")...UInt8(ascii: "F"): digit - UInt8(ascii: "A") + 10
        default: nil
        }
    }
}

/// A number for an id or text for a name.
extension Identifier: ArgumentForm {
    init(argument: JSON) throws {
        switch argument {
        case .string(let name):
            try self.init(named: name)
        case .number:
            self.init(numeric: try UInt32(argument: argument))
        default:
            throw Fault("an identifier is a number or text, not \(argument)")
        }
    }
}

/// `first`, `last`, `next`, `{offset: 0}` or `{timestamp: microseconds}`.
extension PollingStrategy: ArgumentForm {
    init(argument: JSON) throws {
        switch try argument.keyed() {
        case ("first", nil): self = .first
        case ("last", nil): self = .last
        case ("next", nil): self = .next
        case ("offset", let value?): self = .offset(try UInt64(argument: value))
        case ("timestamp", let value?): self = .timestamp(IggyTimestamp(microseconds: try UInt64(argument: value)))
        default: throw Fault("unknown strategy \(argument)")
        }
    }
}

/// `balanced`, `{partition_id: 0}` or `{messages_key: text}`.
extension Partitioning: ArgumentForm {
    init(argument: JSON) throws {
        switch try argument.keyed() {
        case ("balanced", nil): self = .balanced
        case ("partition_id", let value?): self = .partition(try UInt32(argument: value))
        case ("messages_key", let value?): self = .messagesKey(try String(argument: value))
        default: throw Fault("unknown partitioning \(argument)")
        }
    }
}

/// `{consumer: id}` or `{group: id}`.
extension Consumer: ArgumentForm {
    init(argument: JSON) throws {
        switch try argument.keyed() {
        case ("consumer", let value?): self = .consumer(try Identifier(argument: value))
        case ("group", let value?): self = .group(try Identifier(argument: value))
        default: throw Fault("unknown consumer \(argument)")
        }
    }
}

/// `never`, `server_default`, or microseconds.
extension IggyExpiry: ArgumentForm {
    init(argument: JSON) throws {
        switch argument {
        case .string("never"): self = .never
        case .string("server_default"): self = .serverDefault
        case .number(let microseconds): self = .after(.microseconds(microseconds))
        default: throw Fault("unknown expiry \(argument)")
        }
    }
}

/// `unlimited`, `server_default`, or bytes.
extension MaxTopicSize: ArgumentForm {
    init(argument: JSON) throws {
        switch argument {
        case .string("unlimited"): self = .unlimited
        case .string("server_default"): self = .serverDefault
        case .number(let bytes): self = .bytes(bytes)
        default: throw Fault("unknown topic size \(argument)")
        }
    }
}

/// `manual`, `on_read` or `after_processing`.
extension CommitPolicy: ArgumentForm {
    init(argument: JSON) throws {
        switch argument {
        case .string("manual"):
            self = .manual
        case .string("on_read"):
            self = .onRead
        case .string("after_processing"):
            var auto = AutoCommit.default
            auto.after = .processing
            self = .auto(auto)
        default:
            throw Fault("unknown commit policy \(argument)")
        }
    }
}

extension UserStatus: ArgumentForm {
    init(argument: JSON) throws {
        switch argument {
        case .string("active"): self = .active
        case .string("inactive"): self = .inactive
        default: throw Fault("unknown user status \(argument)")
        }
    }
}

extension OptionsScope: ArgumentForm {
    init(argument: JSON) throws {
        switch argument {
        case .string("stream"): self = .stream
        case .string("topic"): self = .topic
        case .string("user"): self = .user
        default: throw Fault("unknown scope \(argument)")
        }
    }
}

/// Global permissions only, each flag false unless named.
extension Permissions: ArgumentForm {
    init(argument: JSON) throws {
        var fields = try Arguments(argument)
        let global: GlobalPermissions? = try fields.optional("global")
        try fields.finish()
        self.init(global: global ?? GlobalPermissions())
    }
}

extension GlobalPermissions: ArgumentForm {
    init(argument: JSON) throws {
        var flags = try Arguments(argument)
        func flag(_ key: String) throws -> Bool {
            try flags.optional(key) ?? false
        }
        self.init(
            manageServers: try flag("manage_servers"), readServers: try flag("read_servers"), manageUsers: try flag("manage_users"),
            readUsers: try flag("read_users"), manageStreams: try flag("manage_streams"), readStreams: try flag("read_streams"),
            manageTopics: try flag("manage_topics"), readTopics: try flag("read_topics"), pollMessages: try flag("poll_messages"),
            sendMessages: try flag("send_messages"))
        try flags.finish()
    }
}

/// `earliest`, `latest`, `{after: cursor}` or `{current: page token or null}`.
extension MetadataChangeStart: ArgumentForm {
    init(argument: JSON) throws {
        switch try argument.keyed() {
        case ("earliest", nil): self = .earliest
        case ("latest", nil): self = .latest
        case ("after", let cursor?): self = .after(try MetadataChangeCursor(text: try String(argument: cursor)))
        case ("current", .null?): self = .current(nil)
        case ("current", let token?): self = .current(try MetadataCurrentPageToken(text: try String(argument: token)))
        default: throw Fault("unknown metadata start \(argument)")
        }
    }
}

extension MetadataChangeGroup: ArgumentForm {
    init(argument: JSON) throws {
        guard let group = MetadataChangeGroup(name: try String(argument: argument)) else {
            throw Fault("unknown metadata change group \(argument)")
        }
        self = group
    }
}

/// `{kind, value}` with the value in its text form.
extension HeaderValue: ArgumentForm {
    init(argument: JSON) throws {
        var fields = try Arguments(argument)
        let kindName: String = try fields.required("kind")
        let text: String = try fields.required("value")
        try fields.finish()
        guard let kind = HeaderKind(name: kindName) else {
            throw Fault("unknown header kind \(kindName)")
        }
        try self.init(kind: kind, text: text)
    }
}

/// `{id, payload}` or `{id, payload_hex}`, with optional headers. Without an
/// id the SDK picks one.
extension IggyMessage: ArgumentForm {
    init(argument: JSON) throws {
        var fields = try Arguments(argument)
        let idText: String? = try fields.optional("id")
        let payload: String? = try fields.optional("payload")
        let payloadHex: HexBytes? = try fields.optional("payload_hex")
        let headers: [String: HeaderValue] = try fields.optional("headers") ?? [:]
        try fields.finish()
        let id = try idText.map { text in
            guard let id = MessageID(text) else {
                throw Fault("the message id \(text) is not a u128")
            }
            return id
        }
        let bytes: [UInt8]
        switch (payload, payloadHex) {
        case (let text?, nil): bytes = Array(text.utf8)
        case (nil, let hexBytes?): bytes = hexBytes.bytes
        default: throw Fault("a message needs exactly one of payload and payload_hex")
        }
        let userHeaders = try Dictionary(uniqueKeysWithValues: headers.map { key, value in (try HeaderKey(key), value) })
        try self.init(payload: bytes, id: id ?? .zero, userHeaders: userHeaders)
    }
}
