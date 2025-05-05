
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { Readable } from 'node:stream';
import { groupConsumerStream } from '../stream/consumer-stream.js';
import { Client } from '../client/index.js';
import {
  PollingStrategy, Partitioning,
  type PollMessagesResponse
} from '../wire/index.js';
import { sendSomeMessages } from '../tcp.sm.utils.js';

describe('e2e -> consumer-stream', async () => {

  const credentials = { username: 'iggy', password: 'iggy' };
  const opt = {
    transport: 'TCP' as const,
    options: { port: 8090, host: '127.0.0.1' },
    credentials
  };

  const c = new Client(opt);

  const streamId = 137;
  const topicId = 123;
  const groupId = 12;

  await c.stream.create({ streamId, name: `e2e-stream-${streamId}` })
  await c.topic.create({
    streamId,
    topicId,
    name: 'test-topic-stream-cg',
    partitionCount: 3,
    compressionAlgorithm: 1
  });

  const ct = 1000;

  it('e2e -> consumer-stream::send-messages', async () => {
    for (let i = 0; i <= ct; i += 100) {
      await sendSomeMessages(c.clientProvider)(
        streamId, topicId, Partitioning.MessageKey(`k-${i % 400}`)
      );
    }
  });

  it('e2e -> consumer-stream::poll-stream', async () => {
    // POLL MESSAGE
    const pollReq = {
      groupId,
      streamId,
      topicId,
      pollingStrategy: PollingStrategy.Next,
      count: 100,
      interval: 500
    };

    const pollStream = groupConsumerStream(opt);
    const s = await pollStream(pollReq);
    const s2 = await pollStream(pollReq);
    let recv = 0;
    const dataCb = (str: Readable) => (d: PollMessagesResponse) => {      
      recv += d.messageCount;
      if (recv === ct)
        str.destroy();
    };

    s.on('data', dataCb(s));
    s2.on('data', dataCb(s2));

    // promise prevent test from finishing before end is handled
    await new Promise((resolve, reject) => {
      s.on('error', (err) => {
        console.error('=>>Stream ERROR::', err)
        s.destroy(err);
        reject(err);
      });

      s.on('end', async () => {
        resolve(assert.equal(ct, recv));
      });
    });

    after(async () => {
      assert.ok(await c.stream.delete({ streamId }));
      assert.ok(await c.session.logout());
      await c.destroy();
    });

  });
});
