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
        writer.writeName("ab")
        writer.writeLongString("cd")
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

@Suite("Consensus session")
struct ConsensusSessionTests {
    @Test func freshSessionIsUnbound() {
        let session = ConsensusSession()
        #expect(!session.isBound)
        #expect(session.session == nil)
        #expect(!session.clientID.isZero)
        #expect(!session.hasActivity)
    }

    @Test func requestIDsAreMonotonicAfterBinding() throws {
        var session = ConsensusSession(clientID: 1)
        #expect(session.beginRegister() == 0)
        try session.bind(10)
        #expect(try session.nextRequestID() == 1)
        #expect(try session.nextRequestID() == 2)
        #expect(session.currentRequestID == 3)
        #expect(session.hasActivity)
    }

    @Test func nextRequestBeforeBindingFails() {
        var session = ConsensusSession(clientID: 1)
        #expect(throws: IggyError(.unauthenticated, context: "request id taken before the session is bound")) {
            try session.nextRequestID()
        }
    }

    @Test func doubleBindAndZeroSessionFail() throws {
        var session = ConsensusSession(clientID: 1)
        try session.bind(10)
        #expect(throws: IggyError(.alreadyAuthenticated)) {
            try session.bind(20)
        }
        var other = ConsensusSession(clientID: 1)
        #expect(throws: IggyError.self) {
            try other.bind(0)
        }
    }

    @Test func reRegisterMintsAFreshIdentity() throws {
        var session = ConsensusSession(clientID: 7)
        _ = session.beginRegister()
        #expect(session.clientID == 7)
        try session.bind(42)
        #expect(session.beginRegister() == 0)
        #expect(!session.isBound)
        #expect(session.clientID != 7)
    }

    @Test func resetDropsEverything() throws {
        var session = ConsensusSession(clientID: 7)
        _ = session.beginRegister()
        try session.bind(42)
        session.reset()
        #expect(!session.isBound)
        #expect(session.clientID != 7)
        #expect(session.currentRequestID == 1)
    }
}

@Suite("Reply decoding")
struct ReplyDecodingTests {
    func replyHeader(operation: ConsensusOperation, bodyLength: Int, status: UInt32 = 0, request: UInt64 = 1) -> [UInt8] {
        var header = [UInt8](repeating: 0, count: VSRFrame.headerSize)
        header.replaceSubrange(48..<52, with: UInt32(VSRFrame.headerSize + bodyLength).littleEndianBytes)
        header[60] = VSRFrame.Command.reply.rawValue
        header.replaceSubrange(200..<208, with: request.littleEndianBytes)
        header[208] = operation.rawValue
        header.replaceSubrange(216..<220, with: status.littleEndianBytes)
        return header
    }

    func successBody(_ payload: [UInt8]) -> [UInt8] {
        UInt32(0).littleEndianBytes + payload
    }

    func rejectionBody(_ code: UInt32) -> [UInt8] {
        UInt32(1).littleEndianBytes + UInt32(0).littleEndianBytes + code.littleEndianBytes
    }

    @Test func metadataSuccessStripsTheResultSection() throws {
        let body = successBody([7, 8, 9])
        let out = try VSRFrame.decodeReply(header: replyHeader(operation: .createStream, bodyLength: body.count)[...], body: body[...])
        #expect(Array(out) == [7, 8, 9])
    }

    @Test func metadataRejectionMapsTheCommittedCode() {
        let body = rejectionBody(IggyErrorCode.streamIdNotFound.rawValue)
        #expect(throws: IggyError(.streamIdNotFound)) {
            try VSRFrame.decodeReply(header: replyHeader(operation: .deleteStream, bodyLength: body.count)[...], body: body[...])
        }
    }

    @Test func transientCodesSurfaceAsTyped() {
        let body = rejectionBody(IggyErrorCode.transientNotCommitted.rawValue)
        #expect(throws: IggyError(.transientNotCommitted)) {
            try VSRFrame.decodeReply(header: replyHeader(operation: .createStream, bodyLength: body.count)[...], body: body[...])
        }
    }

    @Test func truncatedResultSectionIsNeverSuccess() {
        let body = UInt32(1).littleEndianBytes
        #expect(throws: IggyError.self) {
            try VSRFrame.decodeReply(header: replyHeader(operation: .createStream, bodyLength: body.count)[...], body: body[...])
        }
    }

    @Test func nonMetadataBodiesPassThrough() throws {
        let body = rejectionBody(IggyErrorCode.invalidOffset.rawValue)
        let out = try VSRFrame.decodeReply(header: replyHeader(operation: .sendMessages, bodyLength: body.count)[...], body: body[...])
        #expect(Array(out) == body)
        let read = try VSRFrame.decodeReply(header: replyHeader(operation: .nonReplicated, bodyLength: 3)[...], body: [1, 2, 3])
        #expect(Array(read) == [1, 2, 3])
    }

    @Test func consumerOffsetOpsAreResultFramed() throws {
        let body = rejectionBody(IggyErrorCode.consumerOffsetNotFound.rawValue)
        #expect(throws: IggyError(.consumerOffsetNotFound)) {
            try VSRFrame.decodeReply(header: replyHeader(operation: .deleteConsumerOffset, bodyLength: body.count)[...], body: body[...])
        }
        let out = try VSRFrame.decodeReply(header: replyHeader(operation: .storeConsumerOffset, bodyLength: 4)[...], body: successBody([])[...])
        #expect(out.isEmpty)
    }

    @Test func registerRepliesAreFramedOnlyWhenNonEmpty() throws {
        let out = try VSRFrame.decodeReply(header: replyHeader(operation: .register, bodyLength: 0)[...], body: [])
        #expect(out.isEmpty)
        let body = successBody([1, 2])
        let framed = try VSRFrame.decodeReply(header: replyHeader(operation: .register, bodyLength: body.count)[...], body: body[...])
        #expect(Array(framed) == [1, 2])
    }

    @Test func emptySendReplyIsRejected() {
        #expect(throws: IggyError.self) {
            try VSRFrame.decodeReply(header: replyHeader(operation: .sendMessages, bodyLength: 0)[...], body: [])
        }
    }

    @Test func statusDenialWinsBeforeTheBody() {
        let header = replyHeader(operation: .createStream, bodyLength: 0, status: IggyErrorCode.unauthorized.rawValue)
        #expect(throws: IggyError(.unauthorized)) {
            try VSRFrame.decodeReply(header: header[...], body: [])
        }
    }

    @Test func unknownStatusKeepsTheRawCode() {
        let header = replyHeader(operation: .createStream, bodyLength: 0, status: 65_000)
        do {
            _ = try VSRFrame.decodeReply(header: header[...], body: [])
            Issue.record("expected a failure")
        } catch let error as IggyError {
            #expect(error.code == .error)
            #expect(error.rawCode == 65_000)
        } catch {
            Issue.record("unexpected error \(error)")
        }
    }

    @Test func invalidFramesAreRejected() {
        var bogus = replyHeader(operation: .createStream, bodyLength: 0)
        bogus[60] = 99
        #expect(throws: IggyError(.invalidCommand, context: "unexpected frame command")) {
            try VSRFrame.decodeReply(header: bogus[...], body: [])
        }
        var short = replyHeader(operation: .createStream, bodyLength: 0)
        short.replaceSubrange(48..<52, with: UInt32(10).littleEndianBytes)
        #expect(throws: IggyError.self) {
            try VSRFrame.decodeReply(header: short[...], body: [])
        }
        var unknownOperation = replyHeader(operation: .createStream, bodyLength: 0)
        unknownOperation[208] = 200
        #expect(throws: IggyError.self) {
            try VSRFrame.decodeReply(header: unknownOperation[...], body: [])
        }
        #expect(throws: IggyError.self) {
            try VSRFrame.decodeReply(header: replyHeader(operation: .createStream, bodyLength: 8)[...], body: [1, 2])
        }
    }

    @Test func unknownEvictionReasonIsNotReconnectable() {
        var header = [UInt8](repeating: 0, count: VSRFrame.headerSize)
        header[60] = VSRFrame.Command.eviction.rawValue
        header[255] = 200
        let eviction = VSRFrame.readEviction(header[...])
        #expect(eviction.reason == nil)
        #expect(eviction.error.code == .invalidCommand)
        #expect(!eviction.error.isReconnectable)
    }

    @Test func evictionReasonsMapLikeTheRustSDK() {
        func error(_ reason: VSRFrame.EvictionReason) -> IggyErrorCode {
            var header = [UInt8](repeating: 0, count: VSRFrame.headerSize)
            header[60] = VSRFrame.Command.eviction.rawValue
            header[255] = reason.rawValue
            return VSRFrame.readEviction(header[...]).error.code
        }
        #expect(error(.invalidCredentials) == .invalidCredentials)
        #expect(error(.invalidToken) == .invalidPersonalAccessToken)
        #expect(error(.noSession) == .unauthenticated)
        #expect(error(.sessionTooLow) == .unauthenticated)
        #expect(error(.userInactive) == .unauthenticated)
        #expect(error(.staleClient) == .staleClient)
        #expect(error(.malformedLogin) == .invalidFormat)
        #expect(error(.invalidRequestBody) == .invalidCommand)
        // An incompatible-protocol frame with an unusable window degrades to
        // an authentication error rather than trusting the remote frame.
        #expect(error(.incompatibleProtocol) == .unauthenticated)
    }
}

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
        let decoded = try HeaderTLV.decode(encoded[...], skipUnknown: false)
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

    @Test func unknownKindsAreRejectedOrSkipped() throws {
        var bytes: [UInt8] = [2, 1, 0, 0, 0, UInt8(ascii: "k"), 200, 1, 0, 0, 0, 1]
        #expect(throws: IggyError.self) { try HeaderTLV.decode(bytes[...], skipUnknown: false) }
        #expect(try HeaderTLV.decode(bytes[...], skipUnknown: true).isEmpty)
        bytes += HeaderTLV.encode(["x": "y"])
        #expect(try HeaderTLV.decode(bytes[...], skipUnknown: true) == ["x": "y"])
    }

    @Test func fixedSizeKindsAreChecked() {
        #expect(throws: IggyError.self) { try HeaderValue(kind: .uint32, bytes: [1, 2, 3]) }
        #expect(throws: IggyError.self) { try HeaderKey(kind: .string, bytes: []) }
        #expect(throws: IggyError.self) { try HeaderValue.string(String(repeating: "a", count: 256)) }
    }
}

@Suite("Options block")
struct OptionsBlockTests {
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

    @Test func wireRoundTrip() throws {
        for id: Identifier in [1, "my-stream", "café", Identifier(numeric: UInt32.max)] {
            var reader = ByteReader(Requests.identifierOnly(id))
            #expect(try Identifier.decode(from: &reader) == id)
        }
        var badKind = ByteReader([0xFF, 4, 1, 0, 0, 0])
        #expect(throws: WireError.self) { try Identifier.decode(from: &badKind) }
        var badLength = ByteReader([1, 3, 1, 0, 0])
        #expect(throws: WireError.self) { try Identifier.decode(from: &badLength) }
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
