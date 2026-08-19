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
//

import { xxh3 } from "@node-rs/xxhash";
import { u128LEBufToBigint } from "../number.utils.js";

/**
 * Size of the batch header in bytes.
 * Layout: u64 (partitionId) + u64 (baseOffset) + u64 (baseTimestamp) +
 * u64 (originTimestamp) + u64 (batchLength) + u64 (batchChecksum) +
 * u32 (messageCount) + zero padding up to 256 bytes.
 */
export const BATCH_HEADER_SIZE = 256;

/**
 * Size of the per-message frame header in bytes.
 * Layout: u64 (checksum) + u128 (id) + u32 (offsetDelta) +
 * u32 (timestampDelta) + u32 (userHeadersLength) + u32 (payloadLength) +
 * u64 (reserved).
 */
export const FRAME_HEADER_SIZE = 48;

/** Size of the frame checksum field prefixing the frame header. */
const FRAME_CHECKSUM_SIZE = 8;

/**
 * Batch header describing a run of message frames.
 */
export type BatchHeader = {
  /** Partition the batch belongs to (zero when sent by a client) */
  partitionId: bigint;
  /** Offset of the first message in the batch (zero when sent by a client) */
  baseOffset: bigint;
  /** Server timestamp of the batch in microseconds (zero when sent by a client) */
  baseTimestamp: bigint;
  /** Smallest origin timestamp of the batched messages in microseconds */
  originTimestamp: bigint;
  /** Total batch size in bytes, header included */
  batchLength: bigint;
  /** XXH3-64 checksum of the batch header fields and frame checksums */
  batchChecksum: bigint;
  /** Number of message frames in the batch */
  messageCount: number;
};

/**
 * Per-message frame header.
 */
export type FrameHeader = {
  /** XXH3-64 checksum of the frame past this field, payload and user headers included */
  checksum: bigint;
  /** Unique message identifier */
  id: bigint;
  /** Index of the message within the batch */
  offsetDelta: number;
  /** Message origin timestamp minus batch origin timestamp in microseconds */
  timestampDelta: number;
  /** Length of user-defined headers in bytes */
  userHeadersLength: number;
  /** Length of message payload in bytes */
  payloadLength: number;
  /** Reserved for future use, must be zero */
  reserved: bigint;
};

/**
 * Serializes a batch header to its 256-byte wire format.
 *
 * @param header - Batch header to serialize
 * @returns Serialized batch header buffer
 */
export const serializeBatchHeader = (header: BatchHeader): Buffer => {
  const b = Buffer.alloc(BATCH_HEADER_SIZE);
  b.writeBigUInt64LE(header.partitionId, 0);
  b.writeBigUInt64LE(header.baseOffset, 8);
  b.writeBigUInt64LE(header.baseTimestamp, 16);
  b.writeBigUInt64LE(header.originTimestamp, 24);
  b.writeBigUInt64LE(header.batchLength, 32);
  b.writeBigUInt64LE(header.batchChecksum, 40);
  b.writeUInt32LE(header.messageCount, 48);
  return b;
};

/**
 * Deserializes a batch header from a buffer.
 *
 * @param b - Buffer containing the serialized batch header
 * @param pos - Starting position in the buffer
 * @returns Parsed BatchHeader object
 */
export const deserializeBatchHeader = (b: Buffer, pos = 0): BatchHeader => ({
  partitionId: b.readBigUInt64LE(pos),
  baseOffset: b.readBigUInt64LE(pos + 8),
  baseTimestamp: b.readBigUInt64LE(pos + 16),
  originTimestamp: b.readBigUInt64LE(pos + 24),
  batchLength: b.readBigUInt64LE(pos + 32),
  batchChecksum: b.readBigUInt64LE(pos + 40),
  messageCount: b.readUInt32LE(pos + 48),
});

/**
 * Deserializes a frame header from a buffer.
 *
 * @param b - Buffer containing the serialized frame header
 * @param pos - Starting position in the buffer
 * @returns Parsed FrameHeader object
 */
export const deserializeFrameHeader = (b: Buffer, pos = 0): FrameHeader => ({
  checksum: b.readBigUInt64LE(pos),
  id:
    b.readBigUInt64LE(pos + 8) | (b.readBigUInt64LE(pos + 16) << 64n),
  offsetDelta: b.readUInt32LE(pos + 24),
  timestampDelta: b.readUInt32LE(pos + 28),
  userHeadersLength: b.readUInt32LE(pos + 32),
  payloadLength: b.readUInt32LE(pos + 36),
  reserved: b.readBigUInt64LE(pos + 40),
});

/**
 * Computes the XXH3-64 checksum of a complete frame.
 * Covers everything past the checksum field: the remaining frame header,
 * the payload, and the user headers.
 *
 * @param frame - Complete frame buffer [header][payload][user headers]
 * @returns Frame checksum
 */
export const frameChecksum = (frame: Buffer): bigint =>
  xxh3.xxh64(frame.subarray(FRAME_CHECKSUM_SIZE));

/**
 * Computes the XXH3-64 checksum of a batch.
 * Covers the batch header fields up to the checksum, the message count,
 * and each frame checksum in message order.
 *
 * @param header - Batch header fields, batchChecksum ignored
 * @param frameChecksums - Frame checksums in message order
 * @returns Batch checksum
 */
export const batchChecksum = (
  header: Omit<BatchHeader, "batchChecksum" | "messageCount">,
  frameChecksums: bigint[],
): bigint => {
  const b = Buffer.allocUnsafe(44 + frameChecksums.length * 8);
  b.writeBigUInt64LE(header.partitionId, 0);
  b.writeBigUInt64LE(header.baseOffset, 8);
  b.writeBigUInt64LE(header.baseTimestamp, 16);
  b.writeBigUInt64LE(header.originTimestamp, 24);
  b.writeBigUInt64LE(header.batchLength, 32);
  b.writeUInt32LE(frameChecksums.length, 40);
  frameChecksums.forEach((checksum, index) => {
    b.writeBigUInt64LE(checksum, 44 + index * 8);
  });
  return xxh3.xxh64(b);
};

/**
 * Iggy message header containing metadata for each polled message.
 */
export type IggyMessageHeader = {
  /** Message checksum for integrity verification */
  checksum: bigint;
  /** Unique message identifier (UUID or numeric) */
  id: string | bigint;
  /** Message offset within the partition */
  offset: bigint;
  /** Server-assigned timestamp */
  timestamp: Date;
  /** Client-provided origin timestamp */
  originTimestamp: Date;
  /** Length of user-defined headers in bytes */
  userHeadersLength: number;
  /** Length of message payload in bytes */
  payloadLength: number;
  /** Reserved for future use */
  reserved: bigint;
};

/**
 * Deserializes a message ID from a 16-byte buffer to BigInt.
 *
 * @param b - 16-byte buffer containing the message ID
 * @returns Message ID as BigInt
 */
export const deserialiseMessageId = (b: Buffer) => u128LEBufToBigint(b);
