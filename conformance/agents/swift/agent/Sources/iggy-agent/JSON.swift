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

/// A JSON value of the protocol. Every number that a request or a result
/// carries is an unsigned integer, so a number is a `UInt64` and reads
/// exactly at its full width.
enum JSON: Sendable, Equatable {
    case null
    case bool(Bool)
    case number(UInt64)
    case string(String)
    case array([JSON])
    case object([String: JSON])

    init(_ number: some UnsignedInteger) {
        self = .number(UInt64(number))
    }
}

extension JSON: Codable {
    init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode(UInt64.self) {
            self = .number(value)
        } else if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode([JSON].self) {
            self = .array(value)
        } else {
            self = .object(try container.decode([String: JSON].self))
        }
    }

    func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .null: try container.encodeNil()
        case .bool(let value): try container.encode(value)
        case .number(let value): try container.encode(value)
        case .string(let value): try container.encode(value)
        case .array(let values): try container.encode(values)
        case .object(let members): try container.encode(members)
        }
    }
}

extension JSON: CustomStringConvertible {
    var description: String {
        guard let data = try? JSONEncoder.compact.encode(self) else {
            return "<unencodable>"
        }
        return String(decoding: data, as: UTF8.self)
    }
}

extension JSONEncoder {
    /// One line per value, with sorted keys.
    static var compact: JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        return encoder
    }
}

/// Lowercase hex without separators.
func hex(_ bytes: some Sequence<UInt8>) -> String {
    let digits = Array("0123456789abcdef".utf8)
    var text: [UInt8] = []
    for byte in bytes {
        text.append(digits[Int(byte >> 4)])
        text.append(digits[Int(byte & 0x0f)])
    }
    return String(decoding: text, as: UTF8.self)
}

/// `text` with leading zeros up to `width` characters.
func zeroPadded(_ text: String, to width: Int) -> String {
    String(repeating: "0", count: max(0, width - text.count)) + text
}
