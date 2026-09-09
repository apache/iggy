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

/// A message: what a producer sends and what a consumer receives.
///
/// Create one from a payload and optional user headers:
///
/// ```swift
/// let message = try IggyMessage("hello", userHeaders: ["trace-id": "abc"])
/// let event = try IggyMessage(encoding: order)  // any Encodable, as JSON
/// ```
///
/// The server stamps `offset`, `timestamp`, and `checksum` when the batch is
/// persisted; they are zero on a message that has not been sent yet. The id
/// is minted by the producer when left zero.
public struct IggyMessage: Sendable, Hashable, Codable {
    /// Largest payload the server accepts.
    public static let maxPayloadSize = 64 * 1000 * 1000
    /// Largest encoded user-headers block the server accepts.
    public static let maxUserHeadersSize = 100 * 1000

    /// Identifier, unique within the partition. Zero until the producer mints one.
    public var id: MessageID
    /// Offset within the partition, assigned by the server.
    public var offset: UInt64
    /// When the server persisted the message.
    public var timestamp: IggyTimestamp
    /// When the producer created the message.
    public var originTimestamp: IggyTimestamp
    /// Checksum the server verified, zero before the message is sent.
    public var checksum: UInt64
    /// The payload bytes, never empty.
    public var payload: [UInt8]
    /// The user headers as sent, or nil when there are none or they could not
    /// be interpreted (see ``rawUserHeaders``).
    public var userHeaders: UserHeaders?
    /// The user headers exactly as they travelled, when any.
    public var rawUserHeaders: [UInt8]?

    /// Creates a message to send.
    public init(payload: [UInt8], id: MessageID = .zero, userHeaders: UserHeaders? = nil) throws {
        if payload.isEmpty {
            throw IggyError(.invalidMessagePayloadLength, context: "payload cannot be empty")
        }
        if payload.count > Self.maxPayloadSize {
            throw IggyError(.tooBigMessagePayload)
        }
        var raw: [UInt8]?
        if let userHeaders, !userHeaders.isEmpty {
            let encoded = HeaderTLV.encode(userHeaders)
            if encoded.count > Self.maxUserHeadersSize {
                throw IggyError(.tooBigUserHeaders)
            }
            raw = encoded
        }
        self.id = id
        self.offset = 0
        self.timestamp = .zero
        self.originTimestamp = .now
        self.checksum = 0
        self.payload = payload
        self.userHeaders = raw == nil ? nil : userHeaders
        self.rawUserHeaders = raw
    }

    /// Creates a message whose payload is the UTF-8 encoding of `text`.
    public init(_ text: String, id: MessageID = .zero, userHeaders: UserHeaders? = nil) throws {
        try self.init(payload: Array(text.utf8), id: id, userHeaders: userHeaders)
    }

    /// Creates a message whose payload is `value` encoded as JSON.
    public init<T: Encodable>(encoding value: T, encoder: JSONEncoder = JSONEncoder(), id: MessageID = .zero, userHeaders: UserHeaders? = nil) throws {
        try self.init(payload: Array(try encoder.encode(value)), id: id, userHeaders: userHeaders)
    }

    /// Builds a received message from its decoded frame.
    init(id: MessageID, offset: UInt64, timestamp: IggyTimestamp, originTimestamp: IggyTimestamp, checksum: UInt64, payload: [UInt8], rawUserHeaders: [UInt8]?)
    {
        self.id = id
        self.offset = offset
        self.timestamp = timestamp
        self.originTimestamp = originTimestamp
        self.checksum = checksum
        self.payload = payload
        self.rawUserHeaders = rawUserHeaders
        if let rawUserHeaders, !rawUserHeaders.isEmpty {
            // A header this SDK cannot interpret leaves the map nil rather than
            // failing the whole poll; the raw bytes stay available.
            userHeaders = try? HeaderTLV.decode(rawUserHeaders[...], skipUnknown: false)
        } else {
            userHeaders = nil
        }
    }

    /// The payload interpreted as UTF-8, replacing invalid sequences.
    public var payloadString: String {
        String(decoding: payload, as: UTF8.self)
    }

    /// The payload decoded as JSON.
    public func decode<T: Decodable>(_ type: T.Type = T.self, decoder: JSONDecoder = JSONDecoder()) throws -> T {
        try decoder.decode(type, from: Data(payload))
    }

    /// One user header by its string key.
    public subscript(header key: String) -> HeaderValue? {
        guard let headerKey = try? HeaderKey(key) else {
            return nil
        }
        return userHeaders?[headerKey]
    }
}

extension IggyMessage: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        do {
            try self.init(value)
        } catch {
            preconditionFailure("invalid message literal: \(error)")
        }
    }
}

extension IggyMessage: CustomStringConvertible {
    public var description: String {
        if let text = String(bytes: payload, encoding: .utf8) {
            let preview = text.count > 50 ? "\(text.prefix(47))... (\(payload.count)B)" : text
            return "[\(offset)] ID:\(id) '\(preview)'"
        }
        return "[\(offset)] ID:\(id) <binary \(payload.count)B>"
    }
}

/// The messages one poll returned.
public struct PolledMessages: Sendable, Hashable, Codable {
    /// The partition that was read. Empty consumer-group polls can carry a
    /// sentinel instead of a real id, see ``noAssignedPartition``.
    public var partitionID: UInt32
    /// The newest offset the partition had when the batch was read.
    public var currentOffset: UInt64
    /// Number of messages the server reported.
    public var count: UInt32
    public var messages: [IggyMessage]

    public init(partitionID: UInt32, currentOffset: UInt64, count: UInt32, messages: [IggyMessage]) {
        self.partitionID = partitionID
        self.currentOffset = currentOffset
        self.count = count
        self.messages = messages
    }

    public static let empty = PolledMessages(partitionID: 0, currentOffset: 0, count: 0, messages: [])

    /// Partition id of an empty poll by a consumer-group member that currently
    /// holds no partitions.
    public static let noAssignedPartition: UInt32 = UInt32.max
    /// Partition id of an empty poll that tells the member to re-sync its
    /// assignment: the coordinator moved to a new generation.
    static let resyncRequiredPartition: UInt32 = UInt32.max - 1
}

/// One confirmation of a committed batch.
public struct SendConfirmation: Sendable, Hashable, Codable {
    public var streamID: UInt32
    public var topicID: UInt32
    public var partitionID: UInt32
    /// Offset the first message of the batch landed at.
    public var baseOffset: UInt64

    public init(streamID: UInt32, topicID: UInt32, partitionID: UInt32, baseOffset: UInt64) {
        self.streamID = streamID
        self.topicID = topicID
        self.partitionID = partitionID
        self.baseOffset = baseOffset
    }
}

/// Per-partition commit confirmations of a send. The list may be empty: the
/// server can commit a batch it has no offsets to describe.
public struct SendMessagesResponse: Sendable, Hashable, Codable {
    public var confirmations: [SendConfirmation]

    public init(confirmations: [SendConfirmation] = []) {
        self.confirmations = confirmations
    }
}
