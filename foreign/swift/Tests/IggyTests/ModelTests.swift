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

@Suite("User headers")
struct UserHeadersTests {
    @Test func typedValuesRoundTrip() throws {
        let headers: UserHeaders = [
            "raw": try .raw([1, 2, 3]),
            "string": "text",
            "bool": true,
            "int8": .int8(-8),
            "int16": .int16(-16),
            "int32": .int32(-32),
            "int64": -64,
            "int128": .int128(bitPattern: UInt128Value(low: 1, high: 2)),
            "uint8": .uint8(8),
            "uint16": .uint16(16),
            "uint32": .uint32(32),
            "uint64": .uint64(64),
            "uint128": .uint128(UInt128Value(low: 3, high: 4)),
            "float32": .float32(1.25),
            "float64": 2.5,
        ]
        let encoded = HeaderTLV.encode(headers)
        let decoded = try HeaderTLV.decode(encoded[...])
        #expect(decoded == headers)
        #expect(decoded["raw"]?.bytes == [1, 2, 3])
        #expect(decoded["string"]?.stringValue == "text")
        #expect(decoded["bool"]?.boolValue == true)
        #expect(decoded["int8"]?.int8Value == -8)
        #expect(decoded["int16"]?.int16Value == -16)
        #expect(decoded["int32"]?.int32Value == -32)
        #expect(decoded["int64"]?.int64Value == -64)
        #expect(decoded["int128"]?.int128BitPattern == UInt128Value(low: 1, high: 2))
        #expect(decoded["uint8"]?.uint8Value == 8)
        #expect(decoded["uint16"]?.uint16Value == 16)
        #expect(decoded["uint32"]?.uint32Value == 32)
        #expect(decoded["uint64"]?.uint64Value == 64)
        #expect(decoded["uint128"]?.uint128Value == UInt128Value(low: 3, high: 4))
        #expect(decoded["float32"]?.float32Value == 1.25)
        #expect(decoded["float64"]?.float64Value == 2.5)
        #expect(decoded["float64"]?.stringValue == nil)
        #expect(HeaderTLV.encodedSize(headers) == encoded.count)
    }

    @Test func encodingIsSortedByKey() {
        let encoded = HeaderTLV.encode(["b": "2", "a": "1"])
        #expect(encoded == HeaderTLV.encode(["a": "1", "b": "2"]))
        #expect(encoded[5] == UInt8(ascii: "a"))
    }

    @Test func structuralValidation() throws {
        #expect(throws: WireError.self) { try HeaderTLV.validate([0, 1, 0, 0, 0, 42]) }
        #expect(throws: WireError.self) { try HeaderTLV.validate([1, 0, 0, 0, 0]) }
        #expect(throws: WireError.self) { try HeaderTLV.validate([1, 2, 0, 0, 0, 97, 98]) }
        var trailing = HeaderTLV.encode(["k": "v"])
        trailing.append(0xFF)
        #expect(throws: WireError.self) { try HeaderTLV.validate(trailing[...]) }
        #expect(try HeaderTLV.validate([]).isEmpty)
    }

    @Test func unknownKindsNameTheCode() throws {
        let unknownValue: [UInt8] = [2, 1, 0, 0, 0, UInt8(ascii: "k"), 200, 1, 0, 0, 0, 1]
        #expect(throws: IggyError(.invalidHeaderKind)) { try HeaderTLV.decode(unknownValue[...]) }
        do {
            _ = try HeaderTLV.decode(unknownValue[...])
        } catch let error as IggyError {
            #expect(error.context == "unknown value kind 200")
        }
        let unknownKey: [UInt8] = [201, 1, 0, 0, 0, UInt8(ascii: "k"), 2, 1, 0, 0, 0, UInt8(ascii: "v")]
        do {
            _ = try HeaderTLV.decode(unknownKey[...])
        } catch let error as IggyError {
            #expect(error.context == "unknown key kind 201")
        }
    }

    /// A length above 255 is refused as a `UInt32`, before it could trap a
    /// 32-bit `Int` conversion.
    @Test func hugeFieldLengthsAreValidationErrors() {
        let huge: [UInt8] = [2, 0x00, 0x00, 0x00, 0x80]
        #expect(throws: WireError.validation("header field length 2147483648 out of range 1...255")) { try HeaderTLV.validate(huge[...]) }
        let oddPair: [UInt8] = [2, 1, 0, 0, 0, UInt8(ascii: "k")]
        #expect(throws: WireError.validation("odd number of TLV entries (1), expected key-value pairs")) { try HeaderTLV.validate(oddPair[...]) }
    }

    @Test func emptyValuesAreRejectedLikeTheWireDoes() {
        #expect(throws: IggyError(.invalidHeaderValue)) { try HeaderValue.string("") }
        #expect(throws: IggyError(.invalidHeaderValue)) { try HeaderValue.raw([]) }
        #expect(throws: IggyError(.invalidHeaderValue)) { try HeaderValue.raw([UInt8](repeating: 1, count: 256)) }
        #expect(throws: Never.self) { try HeaderValue.raw([UInt8](repeating: 1, count: 255)) }
    }

    @Test func fixedSizeKindsAreChecked() {
        #expect(throws: IggyError.self) { try HeaderValue(kind: .uint32, bytes: [1, 2, 3]) }
        #expect(throws: IggyError.self) { try HeaderKey(kind: .string, bytes: []) }
        #expect(throws: IggyError.self) { try HeaderValue.string(String(repeating: "a", count: 256)) }
    }
}

@Suite("Options block")
struct OptionsBlockTests {
    /// A newer server's option kind is skipped rather than failing the read.
    @Test func unknownValueKindsAreSkippedOnDecode() throws {
        var block = HeaderTLV.encode(["known": "v"])
        block += [2, 7, 0, 0, 0] + Array("unknown".utf8) + [200, 1, 0, 0, 0, 9]
        let options = try OptionsBlock.decode(block[...], explicit: true)
        #expect(options == ["known": .explicit("v")])
    }

    /// A prefixed block whose declared length exceeds the buffer is reported
    /// as truncation, even when the length would not fit a 32-bit `Int`.
    @Test func prefixedBlocksAreBoundsChecked() {
        var reader = ByteReader([0xFF, 0xFF, 0xFF, 0xFF, 1, 2])
        #expect(throws: WireError.truncated(offset: 4, need: 4_294_967_295, have: 2)) { try OptionsBlock.readPrefixed(from: &reader) }
    }

    @Test func rejectsNonStringAndDuplicateKeys() throws {
        let numericKey: [UInt8] = [12, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 118]
        #expect(throws: WireError.self) { try OptionsBlock.validate(numericKey[...]) }
        let entry = HeaderTLV.encode(["segment_size": "1 GiB"])
        #expect(throws: WireError.self) { try OptionsBlock.validate((entry + entry)[...]) }
        #expect(try OptionsBlock.validate(entry[...]).count == 1)
    }

    @Test func prefixedBlocksRoundTrip() throws {
        let options: ResourceOptions = ["key": .explicit("value")]
        let block = try OptionsBlock.encode(options)
        var writer = ByteWriter()
        writer.write(UInt32(block.count))
        writer.write(block)
        var reader = ByteReader(writer.bytes)
        let decoded = try OptionsBlock.decode(try OptionsBlock.readPrefixed(from: &reader), explicit: true)
        #expect(decoded == options)
        #expect(reader.isAtEnd)
        var truncated = ByteReader(Array(writer.bytes.dropLast()))
        #expect(throws: WireError.self) { try OptionsBlock.readPrefixed(from: &truncated) }
    }

    @Test func derivedEntriesAreNotSentAndExplicitWinsOnMerge() throws {
        let options: ResourceOptions = ["a": .explicit("1"), "b": .derived("2")]
        let encoded = try OptionsBlock.encode(options)
        #expect(try OptionsBlock.decode(encoded[...], explicit: true) == ["a": .explicit("1")])
        let merged = try OptionsBlock.decodeSplit(explicit: encoded[...], derived: try OptionsBlock.encode(["a": .explicit("9"), "c": .explicit("3")])[...])
        #expect(merged == ["a": .explicit("1"), "c": .derived("3")])
    }

    @Test func limitsAreEnforced() throws {
        var many: ResourceOptions = [:]
        for index in 0...OptionsBlock.maxOptions {
            many["key_\(index)"] = .explicit("v")
        }
        #expect(throws: IggyError(.optionsBlockTooLarge, context: "\(OptionsBlock.maxOptions + 1) entries, maximum \(OptionsBlock.maxOptions)")) {
            try OptionsBlock.encode(many)
        }
        var big: ResourceOptions = [:]
        let value = try HeaderValue.string(String(repeating: "v", count: 255))
        for index in 0..<400 {
            big[String(repeating: "k", count: 250) + String(format: "%05d", index)] = .explicit(value)
        }
        #expect(throws: IggyError.self) { try OptionsBlock.encode(big) }
    }
}

@Suite("Identifiers")
struct IdentifierTests {
    @Test func literalsAndParsing() throws {
        let numeric: Identifier = 7
        let named: Identifier = "orders"
        #expect(numeric.numericValue == 7)
        #expect(named.name == "orders")
        #expect(try Identifier(parsing: "12").numericValue == 12)
        #expect(try Identifier(parsing: "abc").name == "abc")
        #expect(throws: IggyError.self) { try Identifier(named: "") }
        #expect(throws: IggyError.self) { try Identifier(named: String(repeating: "x", count: 256)) }
        #expect("\(numeric) \(named)" == "7 orders")
    }

    /// A multi-byte name is length-prefixed by its UTF-8 byte count, not its
    /// character count.
    @Test func namesArePrefixedByByteCount() throws {
        var writer = ByteWriter()
        let named: Identifier = "café"
        try named.encode(into: &writer)
        #expect(writer.bytes == [2, 5, 0x63, 0x61, 0x66, 0xC3, 0xA9])
        writer = ByteWriter()
        try Identifier(numeric: UInt32.max).encode(into: &writer)
        #expect(writer.bytes == [1, 4, 0xFF, 0xFF, 0xFF, 0xFF])
    }
}

@Suite("Messages")
struct IggyMessageTests {
    struct Order: Codable, Equatable {
        let id: Int
        let item: String
    }

    @Test func textAndJSONPayloads() throws {
        let text = try IggyMessage("hello", id: MessageID(low: 1, high: 0))
        #expect(text.payloadString == "hello")
        #expect(text.id == MessageID(low: 1, high: 0))
        #expect(text.userHeaders == nil && text.rawUserHeaders == nil)

        let order = Order(id: 7, item: "book")
        let json = try IggyMessage(encoding: order)
        #expect(try json.decode(Order.self) == order)
        #expect(try IggyMessage(payload: [0xFF]).payloadString == "\u{FFFD}")
    }

    /// A received message keeps headers it cannot interpret as raw bytes and
    /// reports no decoded headers, so one unknown kind does not fail a poll.
    @Test func undecodableHeadersStayRaw() {
        let unknownKind: [UInt8] = [2, 1, 0, 0, 0, UInt8(ascii: "k"), 200, 1, 0, 0, 0, 1]
        let received = IggyMessage(id: .zero, offset: 3, timestamp: .zero, originTimestamp: .zero, checksum: 0, payload: [1], rawUserHeaders: unknownKind)
        #expect(received.rawUserHeaders == unknownKind)
        #expect(received.userHeaders == nil)
        #expect(received[header: "k"] == nil)
        let known = IggyMessage(
            id: .zero, offset: 3, timestamp: .zero, originTimestamp: .zero, checksum: 0, payload: [1], rawUserHeaders: HeaderTLV.encode(["k": "v"]))
        #expect(known.userHeaders == ["k": "v"])
        #expect(known[header: "k"] == "v")
        let empty = IggyMessage(id: .zero, offset: 0, timestamp: .zero, originTimestamp: .zero, checksum: 0, payload: [1], rawUserHeaders: [])
        #expect(empty.rawUserHeaders == nil)
    }

    @Test func descriptionShowsABoundedPreview() throws {
        let short = try IggyMessage("hello")
        #expect(short.description == "[0] ID:0 'hello'")
        // 46 ASCII bytes then a two-byte "é" straddling the 47-byte cut.
        let straddling = try IggyMessage(String(repeating: "a", count: 46) + "éé" + String(repeating: "b", count: 20))
        #expect(straddling.description == "[0] ID:0 '\(String(repeating: "a", count: 46))... (70B)'")
        let binary = try IggyMessage(payload: [UInt8](repeating: 0xFF, count: 100))
        #expect(binary.description == "[0] ID:0 <binary 100B>")
    }

    @Test func headersTravelEncodedAndDecodeBack() throws {
        let headers: UserHeaders = ["trace-id": "abc", "attempt": .uint32(3)]
        let message = try IggyMessage("hello", userHeaders: headers)
        #expect(message.rawUserHeaders == HeaderTLV.encode(headers))
        #expect(message.userHeaders == headers)
        #expect(try IggyMessage("hello", userHeaders: [:]).rawUserHeaders == nil)
    }

    @Test func limitsAreEnforced() {
        #expect(throws: IggyError(.invalidMessagePayloadLength)) { try IggyMessage(payload: []) }
        #expect(throws: IggyError(.tooBigMessagePayload)) {
            try IggyMessage(payload: [UInt8](repeating: 0, count: IggyMessage.maxPayloadSize + 1))
        }
    }
}

@Suite("Partitioning")
struct PartitioningTests {
    func encode(_ partitioning: Partitioning) throws -> [UInt8] {
        var writer = ByteWriter()
        try partitioning.encode(into: &writer)
        return writer.bytes
    }

    @Test func messagesKeyMustFitTheLengthByte() throws {
        #expect(try encode(.messagesKey([UInt8](repeating: 7, count: 255))).count == 257)
        #expect(throws: IggyError(.invalidKeyValueLength)) { try encode(.messagesKey([])) }
        #expect(throws: IggyError(.invalidKeyValueLength)) { try encode(.messagesKey([UInt8](repeating: 7, count: 256))) }
        #expect(throws: IggyError(.invalidKeyValueLength)) { try encode(.messagesKey(String(repeating: "k", count: 300))) }
    }

    /// The key width is part of the label because it is part of the key.
    @Test func integerKeysCarryTheirWidth() throws {
        #expect(try encode(.messagesKey(uint32: 42)) == [3, 4, 42, 0, 0, 0])
        #expect(try encode(.messagesKey(uint64: 42)) == [3, 8, 42, 0, 0, 0, 0, 0, 0, 0])
        #expect(try encode(.messagesKey(uint128: UInt128Value(low: 42, high: 0))).count == 18)
        #expect(try encode(.balanced) == [1, 0])
        #expect(try encode(.partition(9)) == [2, 4, 9, 0, 0, 0])
    }

    /// The poll sentinel is compared against the value the Rust generator
    /// dumped from `core/common`, so the golden lane catches drift.
    @Test func noAssignedPartitionMatchesTheServerConstant() {
        #expect(PolledMessages.noAssignedPartition == GoldenFixture.shared.noAssignedPartition)
        #expect(GoldenFixture.shared.resyncRequiredPartition != GoldenFixture.shared.noAssignedPartition)
    }

    /// Offsets are shared with the Rust, Go, Node and C# defaults only if the
    /// default consumer id matches theirs.
    @Test func defaultConsumerIsIdZero() throws {
        #expect(Consumer.default == .consumer(0))
        var writer = ByteWriter()
        try Consumer.default.encode(into: &writer)
        #expect(writer.bytes == [1, 1, 4, 0, 0, 0, 0])
    }

    @Test func timestampsFromDatesClamp() {
        #expect(IggyTimestamp(date: Date(timeIntervalSince1970: -5)).microseconds == 0)
        #expect(IggyTimestamp(date: Date(timeIntervalSince1970: 1.5)).microseconds == 1_500_000)
        #expect(IggyTimestamp(date: Date(timeIntervalSince1970: 1e15)).microseconds == UInt64.max)
        #expect(IggyTimestamp(date: Date(timeIntervalSince1970: .infinity)).microseconds == UInt64.max)
        #expect(IggyTimestamp(date: Date(timeIntervalSince1970: .nan)).microseconds == 0)
    }
}

@Suite("Permissions")
struct PermissionsTests {
    @Test func roundTripWithNestedTopics() throws {
        let permissions = Permissions(
            global: .all,
            streams: [
                3: StreamPermissions(readStream: true, topics: [1: TopicPermissions(readTopic: true), 2: TopicPermissions(sendMessages: true)]),
                1: StreamPermissions(manageStream: true),
            ])
        let encoded = permissions.encode()
        var reader = ByteReader(encoded)
        #expect(try Permissions.decode(from: &reader) == permissions)
        #expect(reader.isAtEnd)
        for cut in 0..<encoded.count {
            var truncated = ByteReader(Array(encoded[..<cut]))
            #expect(throws: WireError.self) { try Permissions.decode(from: &truncated) }
        }
    }

    @Test func globalOnlyIsElevenBytes() {
        #expect(Permissions(global: .all).encode().count == 11)
    }
}

@Suite("Topic options")
struct TopicOptionsTests {
    @Test func durabilityIsAlwaysSentAndRawMayStrengthenIt() throws {
        let defaults = try TopicCreateOptions().toResourceOptions()
        #expect(defaults[TopicOptionKey.durability] == .explicit("replicated"))
        #expect(defaults[TopicOptionKey.consumerOffsetDurability] == .explicit("replicated"))
        let raw = try TopicCreateOptions(raw: [TopicOptionKey.durability: "persisted"]).toResourceOptions()
        #expect(raw[TopicOptionKey.durability] == .explicit("persisted"))
        // The server parses the policy case-insensitively; the canonical
        // spelling is what travels.
        let spelled = try TopicCreateOptions(raw: [TopicOptionKey.durability: "Persisted"]).toResourceOptions()
        #expect(spelled[TopicOptionKey.durability] == .explicit("persisted"))
        #expect(throws: IggyError.self) {
            try TopicCreateOptions(durability: .persisted, raw: [TopicOptionKey.durability: "replicated"]).toResourceOptions()
        }
        #expect(throws: IggyError.self) {
            try TopicCreateOptions(raw: [TopicOptionKey.durability: "sometimes"]).toResourceOptions()
        }
    }

    /// Zero and negative expiries would travel as 0, which the server reads
    /// as its default retention, so they are refused before encoding.
    @Test func expiryMustBePositive() throws {
        #expect(throws: IggyError(.invalidOptionValue)) { try TopicCreateOptions(messageExpiry: .after(.seconds(-5))).encode() }
        #expect(throws: IggyError(.invalidOptionValue)) { try TopicCreateOptions(messageExpiry: .after(.zero)).encode() }
        #expect(throws: IggyError(.invalidOptionValue)) { try TopicUpdateOptions(messageExpiry: .after(.milliseconds(-1))).encode() }
        #expect(throws: IggyError(.invalidOptionValue)) { try TopicCreateOptions(messageExpiry: .after(.nanoseconds(500))).encode() }
        #expect(throws: IggyError(.invalidOptionValue)) { try TopicCreateOptions(messageExpiry: .after(.seconds(1 << 58))).encode() }
        let options = try TopicCreateOptions(messageExpiry: .after(.seconds(1))).toResourceOptions()
        #expect(options[TopicOptionKey.messageExpiry] == .explicit(.uint64(1_000_000)))
        #expect(try TopicCreateOptions(messageExpiry: .after(.microseconds(1))).toResourceOptions()[TopicOptionKey.messageExpiry] == .explicit(.uint64(1)))
        #expect(try TopicCreateOptions(messageExpiry: .never).toResourceOptions()[TopicOptionKey.messageExpiry] == .explicit(.uint64(UInt64.max)))
        #expect(try TopicCreateOptions(messageExpiry: .serverDefault).toResourceOptions()[TopicOptionKey.messageExpiry] == nil)
    }
}
