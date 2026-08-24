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

import { uuidv4 } from 'uuidv7';
import { uint32ToBuf, u128ToBuf, uint8ToBuf } from '../number.utils.js';
import { serializeHeaders, type Headers } from './header.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { serializePartitioning, type Partitioning } from './partitioning.utils.js';
import { parse as parseUUID } from '../uuid.utils.js';
import {
  BATCH_HEADER_SIZE,
  FRAME_HEADER_SIZE,
  batchChecksum,
  frameChecksum,
  serializeBatchHeader,
} from './iggy-header.utils.js';

/** Size of the message ID in bytes (u128) */
const MESSAGE_ID_SIZE = 16;

/** Largest representable frame timestamp delta (u32, microseconds) */
const MAX_TIMESTAMP_DELTA = 0xFFFF_FFFFn;

/** Valid types for message ID: numeric, bigint, or UUID string */
export type MessageIdKind = number | bigint | string;

/**
 * Message creation parameters.
 */
export type CreateMessage = {
  /** Optional message ID (auto-generated if not provided) */
  id?: MessageIdKind,
  /** Optional user-defined headers */
  headers?: Headers,
  /** Message payload as string or Buffer */
  payload: string | Buffer,
  /** Optional origin timestamp in microseconds (defaults to send time) */
  originTimestamp?: bigint
};

/**
 * A message prepared for batch encoding.
 */
export type MessageToEncode = {
  /** Message ID as a 16-byte little-endian buffer */
  id: Buffer,
  /** Message payload */
  payload: Buffer,
  /** Serialized user headers */
  userHeaders: Buffer,
  /** Origin timestamp in microseconds */
  originTimestamp: bigint
};

/**
 * Type guard to check if a value is a valid message ID.
 *
 * @param x - Value to check
 * @returns True if the value is a valid MessageIdKind
 */
export const isValidMessageId = (x?: unknown): x is MessageIdKind =>
  x === undefined ||
  'string' === typeof x ||
  'bigint' === typeof x ||
  'number' === typeof x;

/**
 * Serializes a message ID to a 16-byte little-endian buffer.
 * Supports undefined (zero), numeric, bigint, and UUID string formats.
 *
 * @param id - Message ID to serialize
 * @returns 16-byte buffer containing the serialized ID
 * @throws Error if the ID format is invalid
 */
export const serializeMessageId = (id?: unknown) => {

  if(!isValidMessageId(id))
    throw new Error(`invalid message id: '${id}' (use uuid string | number | bigint >= 0)`)

  if(id === undefined)
    return Buffer.alloc(MESSAGE_ID_SIZE, 0); // 0u128

  if ('bigint' === typeof id || 'number' === typeof id) {
    if (id < 0)
      throw new Error(`invalid message id: '${id}' (numeric id must be >= 0)`)

    const idValue = 'number' === typeof id ? BigInt(id) : id;
    return u128ToBuf(idValue);
  }

  try {
    const uuid = parseUUID(id);
    return u128ToBuf(BigInt(`0x${uuid.toHex()}`));
  } catch (err) {
    throw new Error(
      `invalid message id: '${id}' (use uuid string | number | bigint >= 0)`,
      { cause: err }
    )
  }

}

/**
 * Serializes a message ID, minting a random UUID when the ID is
 * absent or zero.
 *
 * @param id - Optional message ID
 * @returns 16-byte little-endian buffer containing a non-zero ID
 */
const resolveMessageId = (id?: MessageIdKind): Buffer => {
  const bId = serializeMessageId(id);
  return bId.every((byte) => byte === 0)
    ? u128ToBuf(BigInt(`0x${uuidv4().replaceAll('-', '')}`))
    : bId;
};

/**
 * Serializes a single message frame.
 * Format: [frame header][payload][user headers]
 *
 * @param message - Message to serialize
 * @param index - Index of the message within the batch
 * @param batchOriginTimestamp - Origin timestamp of the batch in microseconds
 * @returns Serialized frame buffer
 * @throws Error if the timestamp delta exceeds u32
 */
const serializeMessageFrame = (
  { id, payload, userHeaders, originTimestamp }: MessageToEncode,
  index: number,
  batchOriginTimestamp: bigint
): Buffer => {
  if (id.length !== MESSAGE_ID_SIZE)
    throw new Error(
      `invalid message id length: ${id.length}, expected ${MESSAGE_ID_SIZE}`
    );
  const timestampDelta = originTimestamp - batchOriginTimestamp;
  if (timestampDelta > MAX_TIMESTAMP_DELTA)
    throw new Error(
      `message timestamp delta ${timestampDelta} exceeds u32 range`
    );

  const frame = Buffer.alloc(
    FRAME_HEADER_SIZE + payload.length + userHeaders.length
  );
  id.copy(frame, 8);
  frame.writeUInt32LE(index, 24);
  frame.writeUInt32LE(Number(timestampDelta), 28);
  frame.writeUInt32LE(userHeaders.length, 32);
  frame.writeUInt32LE(payload.length, 36);
  payload.copy(frame, FRAME_HEADER_SIZE);
  userHeaders.copy(frame, FRAME_HEADER_SIZE + payload.length);
  frame.writeBigUInt64LE(frameChecksum(frame), 0);
  return frame;
};

/**
 * Encodes messages into the canonical batch format.
 * Format: [batch header][frames], one frame per message.
 *
 * @param messages - Messages to encode
 * @returns Serialized batch buffer
 * @throws Error if the batch is empty
 */
export const encodeMessagesBatch = (messages: MessageToEncode[]): Buffer => {
  if (messages.length === 0)
    throw new Error('cannot encode an empty message batch');

  const originTimestamp = messages.reduce(
    (min, message) =>
      message.originTimestamp < min ? message.originTimestamp : min,
    messages[0].originTimestamp
  );
  const frames = messages.map((message, index) =>
    serializeMessageFrame(message, index, originTimestamp)
  );
  const framesLength = frames.reduce((sum, frame) => sum + frame.length, 0);
  const batchLength = BigInt(BATCH_HEADER_SIZE + framesLength);
  const header = {
    partitionId: 0n,
    baseOffset: 0n,
    baseTimestamp: 0n,
    originTimestamp,
    batchLength,
  };

  return Buffer.concat([
    serializeBatchHeader({
      ...header,
      batchChecksum: batchChecksum(
        header,
        frames.map((frame) => frame.readBigUInt64LE(0))
      ),
      messageCount: messages.length,
    }),
    ...frames,
  ]);
};

/**
 * Serializes a send messages command payload.
 * Format: [metadata length][stream id][topic id][partitioning]
 * [messages count][batch].
 *
 * @param streamId - Stream identifier
 * @param topicId - Topic identifier
 * @param messages - Array of messages to send
 * @param partitioning - Optional partitioning strategy
 * @returns Serialized command payload
 * @throws Error if the message array is empty
 */
export const serializeSendMessages = (
  streamId: Id,
  topicId: Id,
  messages: CreateMessage[],
  partitioning?: Partitioning,
) => {
  if (messages.length === 0)
    throw new Error('cannot send an empty message batch');

  const streamIdentifier = serializeIdentifier(streamId);
  const topicIdentifier = serializeIdentifier(topicId);
  const bPartitioning = serializePartitioning(partitioning);
  const bMessagesCount = uint32ToBuf(messages.length);
  const bMetadataLen = uint32ToBuf(
    streamIdentifier.length + topicIdentifier.length +
      bPartitioning.length + bMessagesCount.length
  );

  const sendTimestamp = BigInt(Date.now()) * 1000n;
  const bBatch = encodeMessagesBatch(messages.map(
    ({ id, headers, payload, originTimestamp }) => ({
      id: resolveMessageId(id),
      payload: 'string' === typeof payload ? Buffer.from(payload) : payload,
      userHeaders: serializeHeaders(headers),
      originTimestamp: originTimestamp ?? sendTimestamp
    })
  ));

  return Buffer.concat([
    bMetadataLen,
    streamIdentifier,
    topicIdentifier,
    bPartitioning,
    bMessagesCount,
    bBatch
  ]);
};

/**
 * Serializes a flush unsaved buffers command payload.
 *
 * @param streamId - Stream identifier
 * @param topicId - Topic identifier
 * @param partitionId - Partition ID to flush
 * @param fsync - Whether to force sync to disk
 * @returns Serialized command payload
 */
export const serializeFlushUnsavedBuffers = (
  streamId: Id,
  topicId: Id,
  partitionId: number,
  fsync = false
) => {
  const streamIdentifier = serializeIdentifier(streamId);
  const topicIdentifier = serializeIdentifier(topicId);
  const bPartitionId = uint32ToBuf(partitionId);
  const bFSync = uint8ToBuf(fsync ? 1 : 0);

  return Buffer.concat([
    streamIdentifier,
    topicIdentifier,
    bPartitionId,
    bFSync
  ]);
};
