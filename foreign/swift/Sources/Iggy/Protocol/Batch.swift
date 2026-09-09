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

/// The message batch: the one layout a send body, the replicated prepare,
/// the persisted segment record, and the poll reply all share. A port of
/// `core/binary_protocol/src/batch.rs`.
///
/// ```text
/// [batch header: 256 bytes][blob: message frames]
/// frame = [header: 48 bytes][payload][user_headers]
/// ```
enum Batch {
    static let headerSize = 256
    static let messageHeaderSize = 48
    static let checksumOffset = 40
    static let messageCountOffset = 48
    static let reservedOffset = 52
    /// A message's timestamp delta is a `u32` of microseconds, so one batch
    /// spans at most about 71 minutes of producer clock.
    static let maxTimestampDelta = UInt64(UInt32.max)

    struct Header: Equatable {
        var partitionID: UInt64
        var baseOffset: UInt64
        var baseTimestamp: UInt64
        var originTimestamp: UInt64
        /// Total batch size: header plus blob.
        var batchLength: UInt64
        var batchChecksum: UInt64
        var messageCount: UInt32

        var blobLength: Int { Int(batchLength) - Batch.headerSize }

        static func decode(_ bytes: ArraySlice<UInt8>) throws -> Header {
            guard bytes.count >= Batch.headerSize else {
                throw WireError.truncated(offset: 0, need: Batch.headerSize, have: bytes.count)
            }
            let start = bytes.startIndex
            let batchLength = UInt64(littleEndianBytes: bytes[start + 32..<start + 40])
            guard batchLength >= Batch.headerSize, batchLength <= UInt64(Int.max) else {
                throw WireError.validation("batch length must cover the batch header")
            }
            // Every encoder zeroes the reserved region; admitting nonzero bytes
            // would let unchecksummed data ride the header.
            guard bytes[start + Batch.reservedOffset..<start + Batch.headerSize].allSatisfy({ $0 == 0 }) else {
                throw WireError.validation("batch header reserved bytes must be zero")
            }
            return Header(
                partitionID: UInt64(littleEndianBytes: bytes[start..<start + 8]),
                baseOffset: UInt64(littleEndianBytes: bytes[start + 8..<start + 16]),
                baseTimestamp: UInt64(littleEndianBytes: bytes[start + 16..<start + 24]),
                originTimestamp: UInt64(littleEndianBytes: bytes[start + 24..<start + 32]),
                batchLength: batchLength,
                batchChecksum: UInt64(littleEndianBytes: bytes[start + Batch.checksumOffset..<start + Batch.checksumOffset + 8]),
                messageCount: UInt32(littleEndianBytes: bytes[start + Batch.messageCountOffset..<start + Batch.messageCountOffset + 4]))
        }

        func encode() -> [UInt8] {
            var writer = ByteWriter(capacity: Batch.headerSize)
            writer.write(partitionID)
            writer.write(baseOffset)
            writer.write(baseTimestamp)
            writer.write(originTimestamp)
            writer.write(batchLength)
            writer.write(batchChecksum)
            writer.write(messageCount)
            writer.writeZeros(Batch.headerSize - Batch.reservedOffset)
            return writer.bytes
        }

        /// The bytes the batch checksum covers ahead of the frame checksums.
        fileprivate func checksumPrefix() -> [UInt8] {
            var writer = ByteWriter(capacity: 44)
            writer.write(partitionID)
            writer.write(baseOffset)
            writer.write(baseTimestamp)
            writer.write(originTimestamp)
            writer.write(batchLength)
            writer.write(messageCount)
            return writer.bytes
        }
    }

    struct MessageHeader: Equatable {
        var checksum: UInt64
        var id: UInt128Value
        var offsetDelta: UInt32
        var timestampDelta: UInt32
        var userHeadersLength: UInt32
        var payloadLength: UInt32

        var totalSize: Int { Batch.messageHeaderSize + Int(userHeadersLength) + Int(payloadLength) }

        static func decode(_ bytes: ArraySlice<UInt8>) throws -> MessageHeader {
            guard bytes.count >= Batch.messageHeaderSize else {
                throw WireError.truncated(offset: 0, need: Batch.messageHeaderSize, have: bytes.count)
            }
            let start = bytes.startIndex
            guard UInt64(littleEndianBytes: bytes[start + 40..<start + 48]) == 0 else {
                throw WireError.validation("message frame reserved bytes must be zero")
            }
            return MessageHeader(
                checksum: UInt64(littleEndianBytes: bytes[start..<start + 8]),
                id: UInt128Value(littleEndianBytes: bytes[start + 8..<start + 24]),
                offsetDelta: UInt32(littleEndianBytes: bytes[start + 24..<start + 28]),
                timestampDelta: UInt32(littleEndianBytes: bytes[start + 28..<start + 32]),
                userHeadersLength: UInt32(littleEndianBytes: bytes[start + 32..<start + 36]),
                payloadLength: UInt32(littleEndianBytes: bytes[start + 36..<start + 40]))
        }
    }

    /// Batch checksum: XXH3-64 over the six header meta fields followed by
    /// every frame's stored 8-byte checksum field in message order. Bodies are
    /// bound transitively through the frame checksums.
    static func checksum(header: Header, frameChecksums: [UInt8]) -> UInt64 {
        XXH3.hash64(header.checksumPrefix() + frameChecksums)
    }

    /// A message prepared for the wire.
    struct OutgoingMessage {
        var id: UInt128Value
        var originTimestamp: UInt64
        var payload: [UInt8]
        var userHeaders: [UInt8]?
    }

    /// Encodes the body of a send request:
    /// `[metadata_len:u32][stream][topic][partitioning][count:u32][batch]`.
    static func encodeSend(streamID: Identifier, topicID: Identifier, partitioning: Partitioning, messages: [OutgoingMessage]) throws -> [UInt8] {
        guard !messages.isEmpty else {
            throw IggyError(.invalidMessagesCount, context: "cannot encode an empty message batch")
        }
        let metadataLength = streamID.encodedSize + topicID.encodedSize + partitioning.encodedSize + 4
        let blobLength = messages.reduce(0) { $0 + messageHeaderSize + $1.payload.count + ($1.userHeaders?.count ?? 0) }
        guard headerSize + blobLength <= UInt32.max else {
            throw IggyError(.tooBigMessagePayload, context: "batch exceeds the frame size limit")
        }
        var writer = ByteWriter(capacity: 4 + metadataLength + headerSize + blobLength)
        writer.write(UInt32(metadataLength))
        streamID.encode(into: &writer)
        topicID.encode(into: &writer)
        partitioning.encode(into: &writer)
        writer.write(UInt32(messages.count))

        let originTimestamp = messages.map(\.originTimestamp).min() ?? 0
        let headerStart = writer.count
        writer.writeZeros(headerSize)

        var frameChecksums = [UInt8]()
        frameChecksums.reserveCapacity(messages.count * 8)
        for (index, message) in messages.enumerated() {
            let timestampDelta = message.originTimestamp - originTimestamp
            guard timestampDelta <= maxTimestampDelta else {
                throw IggyError(.invalidMessageTimestampDelta, context: "delta \(timestampDelta) microseconds exceeds the per-batch limit")
            }
            let headers = message.userHeaders ?? []
            let frameStart = writer.count
            writer.write(UInt64(0))
            writer.write(message.id)
            writer.write(UInt32(index))
            writer.write(UInt32(timestampDelta))
            writer.write(UInt32(headers.count))
            writer.write(UInt32(message.payload.count))
            writer.write(UInt64(0))
            writer.write(message.payload)
            writer.write(headers)
            let checksum = XXH3.hash64(writer.bytes[(frameStart + 8)...])
            let checksumBytes = checksum.littleEndianBytes
            writer.overwrite(at: frameStart, with: checksumBytes)
            frameChecksums.append(contentsOf: checksumBytes)
        }

        var header = Header(
            partitionID: 0, baseOffset: 0, baseTimestamp: 0, originTimestamp: originTimestamp, batchLength: UInt64(headerSize + blobLength), batchChecksum: 0,
            messageCount: UInt32(messages.count))
        header.batchChecksum = Self.checksum(header: header, frameChecksums: frameChecksums)
        writer.overwrite(at: headerStart, with: header.encode())
        return writer.bytes
    }

    /// Decodes the batch records of a poll reply body into messages. Layout
    /// is always validated; checksums only when asked, matching the Rust
    /// SDK's poll path.
    static func decodeMessages(_ bytes: ArraySlice<UInt8>, expectedCount: UInt32, verifyChecksums: Bool = false) throws -> [IggyMessage] {
        var messages: [IggyMessage] = []
        messages.reserveCapacity(Int(min(expectedCount, 4096)))
        var position = bytes.startIndex
        while position < bytes.endIndex {
            let header = try Header.decode(bytes[position...])
            let batchEnd = position + Int(header.batchLength)
            guard batchEnd <= bytes.endIndex else {
                throw WireError.truncated(offset: position - bytes.startIndex, need: Int(header.batchLength), have: bytes.endIndex - position)
            }
            var cursor = position + headerSize
            var frameChecksums = [UInt8]()
            var counted: UInt32 = 0
            while cursor < batchEnd {
                let frame = try MessageHeader.decode(bytes[cursor..<batchEnd])
                let payloadStart = cursor + messageHeaderSize
                let payloadEnd = payloadStart + Int(frame.payloadLength)
                let headersEnd = payloadEnd + Int(frame.userHeadersLength)
                guard headersEnd <= batchEnd else {
                    throw WireError.truncated(offset: payloadStart - bytes.startIndex, need: headersEnd - payloadStart, have: batchEnd - payloadStart)
                }
                if verifyChecksums {
                    let computed = XXH3.hash64(bytes[(cursor + 8)..<headersEnd])
                    guard computed == frame.checksum else {
                        throw WireError.invalidMessageChecksum(
                            stored: frame.checksum, computed: computed, offset: header.baseOffset &+ UInt64(frame.offsetDelta))
                    }
                    frameChecksums.append(contentsOf: bytes[cursor..<cursor + 8])
                }
                let payload = Array(bytes[payloadStart..<payloadEnd])
                let userHeaders = frame.userHeadersLength > 0 ? Array(bytes[payloadEnd..<headersEnd]) : nil
                messages.append(
                    IggyMessage(
                        id: frame.id,
                        offset: header.baseOffset &+ UInt64(frame.offsetDelta),
                        timestamp: IggyTimestamp(microseconds: header.baseTimestamp),
                        originTimestamp: IggyTimestamp(microseconds: header.originTimestamp &+ UInt64(frame.timestampDelta)),
                        checksum: frame.checksum,
                        payload: payload,
                        rawUserHeaders: userHeaders))
                counted += 1
                cursor = headersEnd
            }
            if verifyChecksums {
                guard counted == header.messageCount else {
                    throw WireError.validation("batch frames do not tile message_count exactly")
                }
                let computed = checksum(header: header, frameChecksums: frameChecksums)
                guard computed == header.batchChecksum else {
                    throw WireError.invalidBatchChecksum(stored: header.batchChecksum, computed: computed)
                }
            }
            position = batchEnd
        }
        return messages
    }
}
