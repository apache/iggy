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

import { type Id } from "../identifier.utils.js";
import { type ValueOf, reverseRecord } from "../../type.utils.js";
import { serializeGetOffset, type Consumer } from "../offset/offset.utils.js";
import { deserializeHeaders, type ParsedHeaderEntry } from "./header.utils.js";
import { Transform, type TransformCallback } from "node:stream";
import {
  deserializeIggyMessageHeaders,
  IGGY_MESSAGE_HEADER_SIZE,
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
    throw new Error(`unknow message state: ${k}`);
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
 * Deserializes an array of messages from a buffer.
 *
 * @param b - Buffer containing serialized messages
 * @returns Array of deserialized messages
 */
export const deserializeMessages = (b: Buffer) => {
  const messages: Message[] = [];
  let pos = 0;
  const len = b.length;
  while (pos < len) {
    if (pos + IGGY_MESSAGE_HEADER_SIZE > len) break;
    const bHead = b.subarray(pos, pos + IGGY_MESSAGE_HEADER_SIZE);
    const headers = deserializeIggyMessageHeaders(bHead);
    pos += IGGY_MESSAGE_HEADER_SIZE;
    const plEnd = pos + headers.payloadLength;
    if (plEnd > len) break;
    const payload = b.subarray(pos, plEnd);
    pos += headers.payloadLength;
    let userHeaders: ParsedHeaderEntry[] = [];
    if (
      headers.userHeadersLength > 0 &&
      plEnd + headers.userHeadersLength <= len
    ) {
      userHeaders = deserializeHeaders(
        b.subarray(plEnd, plEnd + headers.userHeadersLength),
      );
      pos += headers.userHeadersLength;
    }
    messages.push({
      headers,
      payload,
      userHeaders,
    });
  }
  return messages;
};

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
  const messages = deserializeMessages(r.subarray(16));

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
