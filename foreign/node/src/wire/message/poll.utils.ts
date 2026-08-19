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

import { type Id } from "../identifier.utils.js";
import { type ValueOf, reverseRecord } from "../../type.utils.js";
import { toDate } from "../serialize.utils.js";
import { serializeGetOffset, type Consumer } from "../offset/offset.utils.js";
import { deserializeHeaders, type ParsedHeaderEntry } from "./header.utils.js";
import { Transform, type TransformCallback } from "node:stream";
import {
  BATCH_HEADER_SIZE,
  FRAME_HEADER_SIZE,
  deserializeBatchHeader,
  deserializeFrameHeader,
  type IggyMessageHeader,
} from "./iggy-header.utils.js";

/**
 * Enumeration of message polling strategies.
 */
export const PollingStrategyKind = {
  /** Poll from a specific offset */
  Offset: 1,
  /** Poll from a specific timestamp */
  Timestamp: 2,
  /** Poll from the first message */
  First: 3,
  /** Poll from the last message */
  Last: 4,
  /** Poll the next unconsumed message */
  Next: 5,
} as const;

/** Type alias for the PollingStrategyKind object */
export type PollingStrategyKind = typeof PollingStrategyKind;
/** String literal type of polling strategy names */
export type PollingStrategyKindId = keyof PollingStrategyKind;
/** Numeric values of polling strategies */
export type PollingStrategyKindValue = ValueOf<PollingStrategyKind>;

/** Polling from a specific offset */
export type OffsetPollingStrategy = {
  kind: PollingStrategyKind["Offset"];
  /** Offset to start polling from */
  value: bigint;
};

/** Polling from a specific timestamp */
export type TimestampPollingStrategy = {
  kind: PollingStrategyKind["Timestamp"];
  /** Timestamp in microseconds */
  value: bigint;
};

/** Polling from the first message */
export type FirstPollingStrategy = {
  kind: PollingStrategyKind["First"];
  value: 0n;
};

/** Polling from the last message */
export type LastPollingStrategy = {
  kind: PollingStrategyKind["Last"];
  value: 0n;
};

/** Polling the next unconsumed message */
export type NextPollingStrategy = {
  kind: PollingStrategyKind["Next"];
  value: 0n;
};

/** Union of all polling strategy types */
export type PollingStrategy =
  | OffsetPollingStrategy
  | TimestampPollingStrategy
  | FirstPollingStrategy
  | LastPollingStrategy
  | NextPollingStrategy;

/** Next polling strategy constant */
const Next: NextPollingStrategy = {
  kind: PollingStrategyKind.Next,
  value: 0n,
};

/** First polling strategy constant */
const First: FirstPollingStrategy = {
  kind: PollingStrategyKind.First,
  value: 0n,
};

/** Last polling strategy constant */
const Last: LastPollingStrategy = {
  kind: PollingStrategyKind.Last,
  value: 0n,
};

/**
 * Creates an offset polling strategy.
 *
 * @param n - Offset to start from
 * @returns Offset polling strategy
 */
const Offset = (n: bigint): OffsetPollingStrategy => ({
  kind: PollingStrategyKind.Offset,
  value: n,
});

/**
 * Creates a timestamp polling strategy.
 *
 * @param n - Timestamp in microseconds
 * @returns Timestamp polling strategy
 */
const Timestamp = (n: bigint): TimestampPollingStrategy => ({
  kind: PollingStrategyKind.Timestamp,
  value: n,
});

/**
 * Factory object for creating polling strategies.
 */
export const PollingStrategy = {
  Next,
  First,
  Last,
  Offset,
  Timestamp,
};

/**
 * Serializes a poll messages command payload.
 *
 * @param streamId - Stream identifier
 * @param topicId - Topic identifier
 * @param consumer - Consumer configuration
 * @param partitionId - Partition ID (null for all partitions)
 * @param pollingStrategy - Strategy for selecting messages
 * @param count - Maximum number of messages to poll
 * @param autocommit - Whether to auto-commit offset after polling
 * @returns Serialized command payload
 */
export const serializePollMessages = (
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId: number | null,
  pollingStrategy: PollingStrategy, // default to OffsetPollingStrategy
  count = 10,
  autocommit = false,
) => {
  const b = Buffer.allocUnsafe(14);
  b.writeUInt8(pollingStrategy.kind, 0);
  b.writeBigUInt64LE(pollingStrategy.value, 1);
  b.writeUInt32LE(count, 9);
  b.writeUInt8(!!autocommit ? 1 : 0, 13);

  return Buffer.concat([
    serializeGetOffset(streamId, topicId, consumer, partitionId),
    b,
  ]);
};

/**
 * Enumeration of message states.
 */
export const MessageState = {
  /** Message is available for consumption */
  Available: 1,
  /** Message is temporarily unavailable */
  Unavailable: 10,
  /** Message processing failed */
  Poisoned: 20,
  /** Message is scheduled for deletion */
  MarkedForDeletion: 30,
};

/** Type alias for the MessageState object */
type MessageState = typeof MessageState;
/** String literal type of message state names */
type MessageStateId = keyof MessageState;
/** Numeric values of message states */
type MessageStateValue = ValueOf<MessageState>;
/** Reverse mapping from numeric value to state name */
const ReverseMessageState = reverseRecord(MessageState);

/**
 * Maps a numeric message state to its string identifier.
 *
 * @param k - Numeric state value
 * @returns State identifier string
 * @throws Error if the state is unknown
 */
export const mapMessageState = (k: number): MessageStateId => {
  if (!ReverseMessageState[k as MessageStateValue])
    throw new Error(`unknown message state: ${k}`);
  return ReverseMessageState[k as MessageStateValue];
};

/**
 * A polled message with headers, payload, and user headers.
 */
export type Message = {
  /** Iggy message header metadata */
  headers: IggyMessageHeader;
  /** Message payload data */
  payload: Buffer;
  /** User-defined headers */
  userHeaders: ParsedHeaderEntry[];
};

/**
 * Response from a poll messages command.
 */
export type PollMessagesResponse = {
  /** Partition the messages came from */
  partitionId: number;
  /** Current offset in the partition */
  currentOffset: bigint;
  /** Number of messages returned */
  count: number;
  /** Array of polled messages */
  messages: Message[];
};

/**
 * A message frame decoded from a batch record, with absolute values
 * resolved against the record header.
 */
export type BatchMessage = {
  /** Frame checksum, passed through unverified */
  checksum: bigint;
  /** Unique message identifier */
  id: bigint;
  /** Absolute message offset within the partition */
  offset: bigint;
  /** Server timestamp of the record in microseconds */
  timestamp: bigint;
  /** Message origin timestamp in microseconds */
  originTimestamp: bigint;
  /** Message payload data */
  payload: Buffer;
  /** Raw user header bytes */
  userHeaders: Buffer;
};

/**
 * Deserializes batch records into message frames with absolute values.
 * Each record is [batch header][frames], walked by the record's batch
 * length. Records may be server-sliced, so the first frame of a record
 * can carry a non-zero offset delta.
 *
 * @param b - Buffer containing serialized batch records
 * @param pos - Starting position in the buffer
 * @returns Array of decoded message frames
 * @throws Error if a record or frame is malformed
 */
export const deserializeBatchMessages = (
  b: Buffer,
  pos = 0,
): BatchMessage[] => {
  const messages: BatchMessage[] = [];
  const len = b.length;
  while (pos < len) {
    if (pos + BATCH_HEADER_SIZE > len)
      throw new Error("truncated batch header in poll response");
    const batch = deserializeBatchHeader(b, pos);
    const recordEnd = pos + Number(batch.batchLength);
    if (Number(batch.batchLength) < BATCH_HEADER_SIZE || recordEnd > len)
      throw new Error(
        `invalid batch length ${batch.batchLength} in poll response`,
      );
    pos += BATCH_HEADER_SIZE;
    while (pos < recordEnd) {
      if (pos + FRAME_HEADER_SIZE > recordEnd)
        throw new Error("truncated message frame in poll response");
      const frame = deserializeFrameHeader(b, pos);
      if (frame.reserved !== 0n)
        throw new Error(
          `non-zero reserved field ${frame.reserved} in message frame`,
        );
      const payloadEnd = pos + FRAME_HEADER_SIZE + frame.payloadLength;
      const frameEnd = payloadEnd + frame.userHeadersLength;
      if (frameEnd > recordEnd)
        throw new Error("truncated message frame in poll response");
      messages.push({
        checksum: frame.checksum,
        id: frame.id,
        offset: batch.baseOffset + BigInt(frame.offsetDelta),
        timestamp: batch.baseTimestamp,
        originTimestamp:
          batch.originTimestamp + BigInt(frame.timestampDelta),
        payload: b.subarray(pos + FRAME_HEADER_SIZE, payloadEnd),
        userHeaders: b.subarray(payloadEnd, frameEnd),
      });
      pos = frameEnd;
    }
  }
  return messages;
};

/**
 * Deserializes an array of messages from a buffer of batch records.
 *
 * @param b - Buffer containing serialized batch records
 * @param pos - Starting position in the buffer
 * @returns Array of deserialized messages
 */
export const deserializeMessages = (b: Buffer, pos = 0): Message[] =>
  deserializeBatchMessages(b, pos).map((message) => ({
    headers: {
      checksum: message.checksum,
      id: message.id,
      offset: message.offset,
      timestamp: toDate(message.timestamp),
      originTimestamp: toDate(message.originTimestamp),
      userHeadersLength: message.userHeaders.length,
      payloadLength: message.payload.length,
      reserved: 0n,
    },
    payload: message.payload,
    userHeaders:
      message.userHeaders.length > 0
        ? deserializeHeaders(message.userHeaders)
        : ([] as ParsedHeaderEntry[]),
  }));

/**
 * Deserializes a poll messages response from a buffer.
 *
 * @param r - Response buffer
 * @param pos - Starting position
 * @returns Parsed PollMessagesResponse
 */
export const deserializePollMessages = (r: Buffer, pos = 0) => {
  const partitionId = r.readUInt32LE(pos);
  const currentOffset = r.readBigUInt64LE(pos + 4);
  const count = r.readUInt32LE(pos + 12);
  const messages = deserializeMessages(r, pos + 16);

  return {
    partitionId,
    currentOffset,
    count,
    messages,
  };
};

/**
 * Creates a Transform stream for deserializing poll messages responses.
 *
 * @returns Transform stream that outputs PollMessagesResponse objects
 */
export const deserializePollMessagesTransform = () =>
  new Transform({
    objectMode: true,
    transform(chunk: Buffer, encoding: BufferEncoding, cb: TransformCallback) {
      try {
        return cb(null, deserializePollMessages(chunk));
      } catch (err: unknown) {
        cb(
          new Error("deserializePollMessage::transform error", { cause: err }),
          null,
        );
      }
    },
  });
