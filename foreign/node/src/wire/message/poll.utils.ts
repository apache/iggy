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


import { type Id } from '../identifier.utils.js';
import { type ValueOf, reverseRecord } from '../../type.utils.js';
import { serializeGetOffset, type Consumer } from '../offset/offset.utils.js';
import { deserializeHeaders, type HeadersMap } from './header.utils.js';
import { Transform, type TransformCallback } from 'node:stream';
import {
  deserializeIggyMessageHeaders,
  IGGY_MESSAGE_HEADER_SIZE,
  IggyMessageHeader
} from './iggy-header.utils.js';

export const PollingStrategyKind = {
  Offset: 1,
  Timestamp: 2,
  First: 3,
  Last: 4,
  Next: 5
} as const;

export type PollingStrategyKind = typeof PollingStrategyKind;
export type PollingStrategyKindId = keyof PollingStrategyKind;
export type PollingStrategyKindValue = ValueOf<PollingStrategyKind>

export type OffsetPollingStrategy = {
  kind: PollingStrategyKind['Offset'],
  value: bigint
}

export type TimestampPollingStrategy = {
  kind: PollingStrategyKind['Timestamp'],
  value: bigint
}

export type FirstPollingStrategy = {
  kind: PollingStrategyKind['First'],
  value: 0n
}

export type LastPollingStrategy = {
  kind: PollingStrategyKind['Last'],
  value: 0n
}

export type NextPollingStrategy = {
  kind: PollingStrategyKind['Next'],
  value: 0n
}

export type PollingStrategy =
  OffsetPollingStrategy |
  TimestampPollingStrategy |
  FirstPollingStrategy |
  LastPollingStrategy |
  NextPollingStrategy;


const Next: NextPollingStrategy = {
  kind: PollingStrategyKind.Next,
  value:0n
};

const First: FirstPollingStrategy = {
  kind: PollingStrategyKind.First,
  value:0n
};

const Last: LastPollingStrategy = {
  kind: PollingStrategyKind.Last,
  value:0n
};

const Offset = (n: bigint): OffsetPollingStrategy => ({
  kind: PollingStrategyKind.Offset,
  value: n
});

const Timestamp = (n: bigint): TimestampPollingStrategy => ({
  kind: PollingStrategyKind.Timestamp,
  value: n
});

// helper
export const PollingStrategy = {
  Next,
  First,
  Last,
  Offset,
  Timestamp
};


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
    b
  ]);
};

export const MessageState = {
  Available: 1,
  Unavailable: 10,
  Poisoned: 20,
  MarkedForDeletion: 30
}

type MessageState = typeof MessageState;
type MessageStateId = keyof MessageState;
type MessageStateValue = ValueOf<MessageState>;
const ReverseMessageState = reverseRecord(MessageState);

export const mapMessageState = (k: number): MessageStateId => {
  if(!ReverseMessageState[k as MessageStateValue])
    throw new Error(`unknow message state: ${k}`);
  return ReverseMessageState[k as MessageStateValue];
}

export type Message = {
  headers: IggyMessageHeader,
  payload: Buffer,
  userHeaders: HeadersMap
};

export type PollMessagesResponse = {
  partitionId: number,
  currentOffset: bigint,
  count: number,
  messages: Message[]
};

export const deserializeMessages = (b: Buffer) => {
  const messages: Message[] = [];
  let pos = 0;
  const len = b.length;
  while (pos < len) {
    if(pos + IGGY_MESSAGE_HEADER_SIZE > len)
      break;
    const bHead = b.subarray(pos, pos + IGGY_MESSAGE_HEADER_SIZE);
    const headers = deserializeIggyMessageHeaders(bHead);
    pos += IGGY_MESSAGE_HEADER_SIZE;
    const plEnd = pos + headers.payloadLength;
    if(plEnd > len)
      break;
    const payload = b.subarray(pos, plEnd);
    pos += headers.payloadLength;
    let userHeaders: HeadersMap = {};
    if(headers.userHeadersLength > 0 && plEnd + headers.userHeadersLength <= len) {
      userHeaders = deserializeHeaders(b.subarray(plEnd, plEnd + headers.userHeadersLength));
      pos += headers.userHeadersLength;
    }
    messages.push({
      headers,
      payload,
      userHeaders
    });
  }
  return messages;
}

export const deserializePollMessages = (r: Buffer, pos = 0) => {
  const partitionId = r.readUInt32LE(pos);
  const currentOffset = r.readBigUInt64LE(pos + 4);
  const count = r.readUInt32LE(pos + 12);
  const messages = deserializeMessages(r.subarray(16));

  return {
    partitionId,
    currentOffset,
    count,
    messages
  }
};


export const deserializePollMessagesTransform = () => new Transform({
  objectMode: true,
  transform(chunk: Buffer, encoding: BufferEncoding, cb: TransformCallback) {
    try {
      return cb(null, deserializePollMessages(chunk));
    } catch (err: unknown) {
      cb(new Error('deserializePollMessage::transform error', { cause: err }), null);
    }
  }
})
