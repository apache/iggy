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

@Suite("Consensus session")
struct ConsensusSessionTests {
    @Test func freshSessionIsUnbound() {
        let session = ConsensusSession()
        #expect(!session.isBound)
        #expect(session.session == nil)
        #expect(!session.clientID.isZero)
    }

    @Test func requestIDsAreMonotonicAfterBinding() throws {
        var session = ConsensusSession(clientID: 1)
        #expect(session.beginRegister() == 0)
        try session.bind(10)
        #expect(try session.nextRequestID() == 1)
        #expect(try session.nextRequestID() == 2)
        #expect(session.currentRequestID == 3)
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

    @Test func emptySendReplyIsADuplicateWithNoConfirmations() throws {
        let body = try VSRFrame.decodeReply(header: replyHeader(operation: .sendMessages, bodyLength: 0)[...], body: [])
        #expect(body.isEmpty)
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
        // A header shorter than a frame is an error, never a trap, and the
        // raw accessors answer nil for it.
        let partial = replyHeader(operation: .createStream, bodyLength: 0).prefix(100)
        #expect(throws: IggyError(.invalidCommand)) { try VSRFrame.decodeReply(header: partial, body: []) }
        #expect(VSRFrame.readReplyRequestID(partial) == nil)
        #expect(VSRFrame.readReplyStatus(partial) == nil)
        #expect(VSRFrame.readReplyOperation(partial) == nil)
        #expect(VSRFrame.readEviction(partial) == nil)
        #expect(VSRFrame.peekCommand(partial.prefix(10)) == nil)
    }

    @Test func unknownEvictionReasonMapsToInvalidCommand() throws {
        var header = [UInt8](repeating: 0, count: VSRFrame.headerSize)
        header[60] = VSRFrame.Command.eviction.rawValue
        header[255] = 200
        let eviction = try #require(VSRFrame.readEviction(header[...]))
        #expect(eviction.reason == nil)
        #expect(eviction.error.code == .invalidCommand)
    }

    @Test func evictionReasonsMapLikeTheRustSDK() {
        func error(_ reason: VSRFrame.EvictionReason) -> IggyErrorCode? {
            var header = [UInt8](repeating: 0, count: VSRFrame.headerSize)
            header[60] = VSRFrame.Command.eviction.rawValue
            header[255] = reason.rawValue
            return VSRFrame.readEviction(header[...])?.error.code
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
