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

import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getTestClient } from './test-client.utils.js';
import { ReadError } from '../stream/partition-reader.js';
import { COMMAND_CODE } from '../wire/command.code.js';
import { Consumer, Partitioning, PollingStrategy } from '../wire/index.js';
import { fetchMessages } from '../wire/message/fetch-messages.command.js';
import { BATCH_HEADER_SIZE, FRAME_HEADER_SIZE } from '../wire/message/iggy-header.utils.js';
import { encodeMessagesBatch, mintMessageId, serializeSendMessages } from '../wire/message/message.utils.js';
import { generateMessages } from '../tcp.sm.utils.js';

describe('e2e -> decode-skip', async () => {
  const client = getTestClient();
  const streamId = 'e2e-decode-skip-stream';
  const topicId = 'e2e-decode-skip-topic';
  const partition = Partitioning.PartitionId(0);
  /** User headers whose first key has length zero. The server stores them, the client cannot decode them. */
  const UNPARSABLE_HEADERS = Buffer.from([1, 0, 0, 0, 0]);

  await client.stream.create({ name: streamId });
  await client.topic.create({ streamId, name: topicId, partitionCount: 1, compressionAlgorithm: 1 });

  after(async () => {
    await client.stream.delete({ streamId });
    await client.session.logout();
    await client.destroy();
  });

  const send = (count: number) => client.message.send({
    streamId, topicId, messages: generateMessages(count), partition
  });

  /** Sends one message with user headers that do not decode, through the raw command. */
  const sendUnparsable = async () => {
    const payload = Buffer.from('unparsable-headers');
    const plain = serializeSendMessages(streamId, topicId, [{ payload }], partition);
    const head = plain.subarray(0, plain.length - BATCH_HEADER_SIZE - FRAME_HEADER_SIZE - payload.length);
    const batch = encodeMessagesBatch([{
      id: mintMessageId(), payload, userHeaders: UNPARSABLE_HEADERS, originTimestamp: BigInt(Date.now()) * 1000n
    }]);
    await client.sendBinaryRequest(COMMAND_CODE.SendMessages, Buffer.concat([head, batch]));
  };

  /** The frame of the message at `offset` as a fetch serves it, undecoded. */
  const frameAsServed = async (offset: bigint): Promise<Buffer> => {
    const [section] = await fetchMessages(client.clientProvider)({
      streamId, topicId, consumer: Consumer.Single, count: 1, autocommit: false,
      targets: [{ partitionId: 0, pollingStrategy: PollingStrategy.Offset(offset) }]
    });
    const frame = section.records.subarray(BATCH_HEADER_SIZE);
    const headersLength = frame.readUInt32LE(32);
    const payloadLength = frame.readUInt32LE(36);
    return frame.subarray(0, FRAME_HEADER_SIZE + payloadLength + headersLength);
  };

  it('e2e -> decode-skip::a message that does not decode is reported with its offset and frame, and a seek passes it',
    async () => {
      await send(1);
      await sendUnparsable();
      await send(1);
      const reader = await client.message.reader({
        streamId, topicId, consumer: Consumer.Single, partitions: [0], start: PollingStrategy.First,
        options: { count: 10, maxWait: 1_000 }
      });
      try {
        const prefix = await reader.next();
        const error = await reader.next().then(() => undefined, (failure: unknown) => failure);
        assert.ok(error instanceof ReadError, String(error));
        assert.deepEqual(prefix.messages.map(({ headers }) => headers.offset), [0n]);
        assert.deepEqual([error.partitionId, error.offset, error.skip?.value], [0, 1n, 2n], String(error.error));
        assert.deepEqual(error.record, await frameAsServed(1n));
        await reader.seek(0, error.skip!);
        const resumed = await reader.next();
        assert.deepEqual(resumed.messages.map(({ headers }) => headers.offset), [2n]);
      } finally {
        await reader.close();
      }
    });
});
