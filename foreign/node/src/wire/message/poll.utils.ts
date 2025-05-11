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
import { deserializeUUID, toDate } from '../serialize.utils.js';
import { serializeGetOffset, type Consumer } from '../offset/offset.utils.js';
import { deserializeHeaders, type HeadersMap } from './header.utils.js';
import { Transform, type TransformCallback } from 'node:stream';

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
  partitionId: number,              // default to 1
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
  id: string,
  state: string,
  timestamp: Date,
  offset: bigint,
  headers: HeadersMap,
  payload: Buffer,
  checksum: number,
};

export type PollMessagesResponse = {
  partitionId: number,
  currentOffset: bigint,
  messageCount: number,
  messages: Message[]
};

export const deserializePollMessages = (r: Buffer, pos = 0) => {
  const len = r.length;
  const partitionId = r.readUInt32LE(pos);
  const currentOffset = r.readBigUInt64LE(pos + 4);
  const messageCount = r.readUInt32LE(pos + 12);

  const messages: Message[] = [];
  pos += 16;

  if (pos >= len) {
    return {
      partitionId,
      currentOffset,
      messageCount,
      messages
    }
  }

  while (pos < len) {
    const offset = r.readBigUInt64LE(pos);
    const state = mapMessageState(r.readUInt8(pos + 8));
    const timestamp = toDate(r.readBigUInt64LE(pos + 9));
    const id = deserializeUUID(r.subarray(pos + 17, pos + 17 + 16));
    const checksum = r.readUInt32LE(pos + 33);
    const headersLength = r.readUInt32LE(pos + 37);
    const headers = deserializeHeaders(r.subarray(pos + 41, pos + 41 + headersLength));
    pos += headersLength;
    const messageLength = r.readUInt32LE(pos + 41)
    const payload = r.subarray(pos + 45, pos + 45 + messageLength);
    pos += 45 + messageLength;
    messages.push({
      id,
      state,
      timestamp,
      offset,
      headers,
      payload,
      checksum
    });
  }

  return {
    partitionId,
    currentOffset,
    messageCount,
    messages
  }
};

export const deserializePollMessagesTransform = () => new Transform({
  objectMode: true,
  transform(chunk: Buffer, encoding: BufferEncoding, cb: TransformCallback) {
    try {
      return cb(null, deserializePollMessages(chunk));
    } catch (err: unknown) {
      cb(new Error('deserializePollMessage::transform error', {cause: err}), null);
    }
  }
})
