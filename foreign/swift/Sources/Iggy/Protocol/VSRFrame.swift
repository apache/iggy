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

/// The consensus wire framing the SDK speaks over TCP: every frame in both
/// directions is a fixed 256-byte header followed by the body, with no outer
/// length prefix. A port of `core/binary_protocol/src/consensus/header.rs`.
///
/// Fields are addressed by byte offset because the header leads with
/// unaligned 128-bit values.
enum VSRFrame {
    /// Length of every consensus frame header, both directions.
    static let headerSize = 256
    /// Upper bound on a frame the client is willing to read; a corrupt size
    /// field must not turn into a multi-gigabyte allocation.
    static let maxFrameSize = 1 << 30

    /// Request header field offsets the client writes.
    enum RequestOffset {
        static let size = 48
        static let command = 60
        static let client = 128
        static let requestChecksum = 144
        static let timestamp = 160
        static let request = 168
        static let operation = 176
        static let session = 184
        static let userID = 192
        static let reserved = 196
    }

    /// Reply header field offsets the client reads.
    enum ReplyOffset {
        static let size = 48
        static let command = 60
        static let request = 200
        static let operation = 208
        static let status = 216
    }

    /// Eviction header field offsets the client reads.
    enum EvictionOffset {
        static let client = 128
        static let serverProtocolVersion = 144
        static let serverProtocolVersionMin = 148
        static let reason = 255
    }

    /// Frame discriminant, at the same offset in every consensus header.
    enum Command: UInt8 {
        case request = 5
        case reply = 8
        case eviction = 13
    }

    /// Terminal reason carried by an eviction frame. Pinned wire values.
    enum EvictionReason: UInt8 {
        case reserved = 0
        case noSession = 1
        case clientReleaseTooLow = 2
        case clientReleaseTooHigh = 3
        case invalidRequestOperation = 4
        case invalidRequestBody = 5
        case invalidRequestBodySize = 6
        case sessionTooLow = 7
        case sessionReleaseMismatch = 8
        case invalidCredentials = 9
        case invalidToken = 10
        case userInactive = 11
        case sessionError = 12
        case staleClient = 13
        case incompatibleProtocol = 14
        case malformedLogin = 15
    }

    /// A request frame ready for the wire, plus the id it was stamped with.
    struct EncodedRequest {
        var bytes: [UInt8]
        let requestID: UInt64
        let operation: ConsensusOperation
    }

    /// Builds the complete request frame for a command code and its encoded
    /// payload, deriving sequencing from the command code and the session.
    ///
    /// The request checksum is stamped for the operations the server's client
    /// table deduplicates, like the Rust SDK does; partition and
    /// non-replicated operations leave it zero, which the server reads as
    /// unstamped.
    static func encodeRequest(session: inout ConsensusSession, code: UInt32, payload: [UInt8]) throws -> EncodedRequest {
        let totalSize = headerSize + payload.count
        guard totalSize <= UInt32.max else {
            throw IggyError(.invalidConfiguration, context: "request of \(totalSize) bytes exceeds the frame size limit")
        }
        let operation = ConsensusOperation.forCode(code)
        let requestID: UInt64
        let sessionID: UInt64
        switch operation {
        case .register:
            requestID = session.beginRegister()
            sessionID = 0
        case .nonReplicated:
            requestID = session.currentRequestID
            sessionID = session.session ?? 0
        default:
            guard let bound = session.session else {
                throw IggyError(.unauthenticated)
            }
            sessionID = bound
            requestID = try session.nextRequestID()
        }
        let requestChecksum: UInt64 =
            (operation.isPartition || operation == .nonReplicated) ? 0 : XXH3.hash64(payload)

        var frame = [UInt8](repeating: 0, count: totalSize)
        frame.withUnsafeMutableBytes { buffer in
            buffer.storeBytes(of: UInt32(totalSize).littleEndian, toByteOffset: RequestOffset.size, as: UInt32.self)
            buffer[RequestOffset.command] = Command.request.rawValue
            buffer.storeBytes(of: session.clientID.low.littleEndian, toByteOffset: RequestOffset.client, as: UInt64.self)
            buffer.storeBytes(of: session.clientID.high.littleEndian, toByteOffset: RequestOffset.client + 8, as: UInt64.self)
            buffer.storeBytes(of: requestChecksum.littleEndian, toByteOffset: RequestOffset.requestChecksum, as: UInt64.self)
            buffer.storeBytes(of: requestID.littleEndian, toByteOffset: RequestOffset.request, as: UInt64.self)
            buffer[RequestOffset.operation] = operation.rawValue
            buffer.storeBytes(of: sessionID.littleEndian, toByteOffset: RequestOffset.session, as: UInt64.self)
            if operation == .nonReplicated {
                buffer.storeBytes(of: code.littleEndian, toByteOffset: RequestOffset.reserved, as: UInt32.self)
            }
        }
        if !payload.isEmpty {
            frame.replaceSubrange(headerSize..., with: payload)
        }
        return EncodedRequest(bytes: frame, requestID: requestID, operation: operation)
    }

    /// Reads the request id back out of a stamped request frame.
    static func stampedRequestID(_ frame: [UInt8]) -> UInt64 {
        UInt64(littleEndianBytes: frame[RequestOffset.request..<RequestOffset.request + 8])
    }

    /// Reads the total frame length, header included, from any consensus
    /// header. Returns nil when the buffer is too short.
    static func readSize(_ header: ArraySlice<UInt8>) -> UInt32? {
        guard header.count >= ReplyOffset.size + 4 else {
            return nil
        }
        let start = header.startIndex + ReplyOffset.size
        return UInt32(littleEndianBytes: header[start..<start + 4])
    }

    static func peekCommand(_ header: ArraySlice<UInt8>) -> Command? {
        Command(rawValue: header[header.startIndex + ReplyOffset.command])
    }

    static func readReplyRequestID(_ header: ArraySlice<UInt8>) -> UInt64 {
        let start = header.startIndex + ReplyOffset.request
        return UInt64(littleEndianBytes: header[start..<start + 8])
    }

    static func readReplyStatus(_ header: ArraySlice<UInt8>) -> UInt32 {
        let start = header.startIndex + ReplyOffset.status
        return UInt32(littleEndianBytes: header[start..<start + 4])
    }

    static func readReplyOperation(_ header: ArraySlice<UInt8>) -> ConsensusOperation? {
        ConsensusOperation(rawValue: header[header.startIndex + ReplyOffset.operation])
    }

    /// The decoded fields of an eviction frame.
    struct Eviction {
        let reason: EvictionReason?
        let rawReason: UInt8
        let serverProtocolVersion: UInt32
        let serverProtocolVersionMin: UInt32

        /// Maps the eviction to the typed error the Rust SDK raises for it.
        var error: IggyError {
            switch reason {
            case .invalidCredentials:
                return IggyError(.invalidCredentials)
            case .invalidToken:
                return IggyError(.invalidPersonalAccessToken)
            case .noSession, .sessionTooLow, .sessionReleaseMismatch, .userInactive, .sessionError:
                return IggyError(.unauthenticated)
            case .staleClient:
                return IggyError(.staleClient)
            case .incompatibleProtocol:
                if serverProtocolVersionMin == 0 || serverProtocolVersion < serverProtocolVersionMin {
                    return IggyError(.unauthenticated)
                }
                let client = ProtocolVersion.current
                let min = ProtocolVersion(packed: serverProtocolVersionMin)
                let max = ProtocolVersion(packed: serverProtocolVersion)
                return IggyError(
                    .incompatibleProtocolVersion,
                    context: "client \(client), server accepts [\(min), \(max)]")
            case .malformedLogin:
                return IggyError(.invalidFormat)
            default:
                // An unknown reason must not map to a reconnectable error, or a
                // newer server's new reason drives a disconnect and re-login loop.
                return IggyError(.invalidCommand, context: "evicted with reason \(rawReason)")
            }
        }
    }

    static func readEviction(_ header: ArraySlice<UInt8>) -> Eviction {
        let start = header.startIndex
        let rawReason = header[start + EvictionOffset.reason]
        let version = UInt32(littleEndianBytes: header[start + EvictionOffset.serverProtocolVersion..<start + EvictionOffset.serverProtocolVersion + 4])
        let versionMin = UInt32(
            littleEndianBytes: header[start + EvictionOffset.serverProtocolVersionMin..<start + EvictionOffset.serverProtocolVersionMin + 4])
        return Eviction(reason: EvictionReason(rawValue: rawReason), rawReason: rawReason, serverProtocolVersion: version, serverProtocolVersionMin: versionMin)
    }

    /// Turns one complete consensus frame into its reply body. The order is
    /// fixed: frame discriminant, declared size, pre-commit denial status
    /// before any body read, then the committed result section.
    static func decodeReply(header: ArraySlice<UInt8>, body: ArraySlice<UInt8>) throws -> ArraySlice<UInt8> {
        precondition(header.count == headerSize)
        switch peekCommand(header) {
        case .eviction:
            throw readEviction(header).error
        case .reply:
            break
        default:
            throw IggyError(.invalidCommand, context: "unexpected frame command")
        }
        guard let size = readSize(header), size >= headerSize, Int(size) <= maxFrameSize else {
            throw IggyError(.invalidCommand, context: "reply declares an invalid frame size")
        }
        let expected = Int(size) - headerSize
        guard body.count >= expected else {
            throw IggyError(.invalidCommand, context: "reply body is shorter than declared")
        }
        let status = readReplyStatus(header)
        if status != 0 {
            throw IggyError(wireCode: status)
        }
        guard let operation = readReplyOperation(header) else {
            throw IggyError(.invalidCommand, context: "reply carries an unknown operation")
        }
        if operation == .sendMessages && expected == 0 {
            // A committed send always carries a confirmation section. The server
            // answers a replicated request on a dead session with an empty
            // status-0 reply and expects the decoder to reject it.
            throw IggyError(.invalidCommand, context: "empty reply to a send")
        }
        return try splitMetadataResult(operation: operation, body: body.prefix(expected))
    }

    /// Strips the committed result section leading a result-framed reply body:
    /// `[count: u32]` then `count` x `{index: u32, result: u32}`. Success is
    /// count 0 followed by the payload; a committed rejection carries one
    /// entry and no payload. A register reply is result-framed too, except a
    /// terminal register failure ships an empty body, which passes through so
    /// the typed decode fails.
    static func splitMetadataResult(operation: ConsensusOperation, body: ArraySlice<UInt8>) throws -> ArraySlice<UInt8> {
        let framed = operation.isResultFramed || (operation == .register && !body.isEmpty)
        guard framed else {
            return body
        }
        guard body.count >= 4 else {
            throw IggyError(.invalidCommand, context: "result section is truncated")
        }
        let start = body.startIndex
        let count = Int(UInt32(littleEndianBytes: body[start..<start + 4]))
        let sectionLength = 4 + count * 8
        guard body.count >= sectionLength else {
            throw IggyError(.invalidCommand, context: "result section claims more entries than the body holds")
        }
        if count > 0 {
            let code = UInt32(littleEndianBytes: body[start + 8..<start + 12])
            if code != 0 {
                throw IggyError(wireCode: code)
            }
        }
        return body[(start + sectionLength)...]
    }
}
