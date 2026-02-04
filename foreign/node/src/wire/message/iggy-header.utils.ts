/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { toDate } from "../serialize.utils.js";
import { u128LEBufToBigint } from "../number.utils.js";

/**
 * Iggy message header containing metadata for each message.
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
 * Size of the Iggy message header in bytes.
 * Layout: u64 (checksum) + u128 (id) + u64 (offset) + u64 (timestamp) + u64 (originTimestamp) + u32 (userHeadersLength) + u32 (payloadLength) + u64 (reserved)
 */
export const IGGY_MESSAGE_HEADER_SIZE = 8 + 16 + 8 + 8 + 8 + 4 + 4 + 8;

/**
 * Serializes an Iggy message header to wire format.
 * Sets checksum, offset, and timestamp to zero (filled by server).
 *
 * @param id - Message ID as 16-byte buffer
 * @param payload - Message payload
 * @param userHeaders - Serialized user headers
 * @returns Serialized header buffer
 */
export const serializeIggyMessageHeader = (
  id: Buffer,
  payload: Buffer,
  userHeaders: Buffer,
) => {
  const b = Buffer.allocUnsafe(IGGY_MESSAGE_HEADER_SIZE);
  b.writeBigUInt64LE(0n, 0); // checksum u64
  b.fill(id, 8, 24); // id u128
  b.writeBigUInt64LE(0n, 24); // offset u64
  b.writeBigUInt64LE(0n, 32); // timestamp u64
  b.writeBigUint64LE(BigInt(new Date().getTime()), 40); // originTimestamp u64
  b.writeUInt32LE(userHeaders.length, 48); // userHeaders len u32
  b.writeUInt32LE(payload.length, 52); // payload len u32
  b.writeBigUInt64LE(0n, 56); // reserved u64
  return b;
};

/**
 * Deserializes a message ID from a 16-byte buffer to BigInt.
 *
 * @param b - 16-byte buffer containing the message ID
 * @returns Message ID as BigInt
 */
export const deserialiseMessageId = (b: Buffer) => u128LEBufToBigint(b);

/**
 * Deserializes Iggy message headers from a buffer.
 *
 * @param b - Buffer containing the serialized header
 * @returns Parsed IggyMessageHeader object
 * @throws Error if buffer length doesn't match expected header size
 */
export const deserializeIggyMessageHeaders = (b: Buffer) => {
  if (b.length !== IGGY_MESSAGE_HEADER_SIZE)
    throw new Error(
      `deserialize message headers error, length = ${b.length} ` +
        `expected ${IGGY_MESSAGE_HEADER_SIZE}`,
    );
  const headers: IggyMessageHeader = {
    checksum: b.readBigUInt64LE(0),
    id: deserialiseMessageId(b.subarray(8, 24)),
    offset: b.readBigUInt64LE(24),
    timestamp: toDate(b.readBigUInt64LE(32)),
    originTimestamp: toDate(b.readBigUInt64LE(40)),
    userHeadersLength: b.readUInt32LE(48),
    payloadLength: b.readUInt32LE(52),
    reserved: b.readBigUInt64LE(56),
  };
  return headers;
};
