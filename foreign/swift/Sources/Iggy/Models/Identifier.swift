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

/// Names a stream, topic, user, or consumer group either by its numeric id
/// or by its name.
///
/// Both literals work wherever an identifier is expected:
///
/// ```swift
/// try await client.getTopic(streamID: "orders", topicID: 1)
/// ```
///
/// A name is 1 to 255 bytes of UTF-8. The string-literal form traps on an
/// invalid name; use ``init(named:)`` to validate a runtime value.
public struct Identifier: Sendable, Hashable, Codable, CustomStringConvertible {
    public enum Kind: Sendable, Hashable, Codable {
        case numeric(UInt32)
        case named(String)
    }

    public static let maxNameLength = 255

    public let kind: Kind

    public init(numeric id: UInt32) {
        kind = .numeric(id)
    }

    /// Validates the name length before accepting it.
    public init(named name: String) throws {
        let length = name.utf8.count
        guard length >= 1, length <= Self.maxNameLength else {
            throw IggyError(.invalidIdentifier, context: "name must be 1-\(Self.maxNameLength) bytes, got \(length)")
        }
        kind = .named(name)
    }

    /// Parses text the way the CLI does: a decimal number becomes a numeric
    /// identifier, anything else a name.
    public init(parsing text: String) throws {
        if let number = UInt32(text) {
            self.init(numeric: number)
        } else {
            try self.init(named: text)
        }
    }

    public var numericValue: UInt32? {
        if case .numeric(let id) = kind { id } else { nil }
    }

    public var name: String? {
        if case .named(let name) = kind { name } else { nil }
    }

    public var description: String {
        switch kind {
        case .numeric(let id): String(id)
        case .named(let name): name
        }
    }
}

extension Identifier: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: UInt32) {
        self.init(numeric: value)
    }
}

extension Identifier: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        do {
            try self.init(named: value)
        } catch {
            preconditionFailure("invalid identifier literal: \(error)")
        }
    }
}

extension Identifier {
    private static let numericKindCode: UInt8 = 1
    private static let stringKindCode: UInt8 = 2

    /// `[kind:1][length:1][value:N]`.
    func encode(into writer: inout ByteWriter) {
        switch kind {
        case .numeric(let id):
            writer.write(Self.numericKindCode)
            writer.write(UInt8(4))
            writer.write(id)
        case .named(let name):
            writer.write(Self.stringKindCode)
            writer.writeName(name)
        }
    }

    var encodedSize: Int {
        switch kind {
        case .numeric: 6
        case .named(let name): 2 + name.utf8.count
        }
    }

    static func decode(from reader: inout ByteReader) throws -> Identifier {
        let kindCode = try reader.readUInt8()
        let length = Int(try reader.readUInt8())
        switch kindCode {
        case numericKindCode:
            guard length == 4 else {
                throw WireError.validation("numeric identifier must be 4 bytes, got \(length)")
            }
            return Identifier(numeric: try reader.readUInt32())
        case stringKindCode:
            guard length > 0 else {
                throw WireError.validation("string identifier cannot be empty")
            }
            let name = try reader.readString(length)
            return try Identifier(named: name)
        default:
            throw WireError.invalidDiscriminant(type: "Identifier", value: kindCode)
        }
    }
}

/// Validates a wire name and returns it, so the length prefix can never
/// desync the frame.
func validatedWireName(_ value: String, what: String = "name") throws -> String {
    let length = value.utf8.count
    guard length >= 1, length <= Identifier.maxNameLength else {
        throw IggyError(.invalidFormat, context: "\(what) must be 1-\(Identifier.maxNameLength) bytes, got \(length)")
    }
    return value
}
