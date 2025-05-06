
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';
import { ConsumerKind, PollingStrategy, Partitioning } from '../wire/index.js';
import { generateMessages } from '../tcp.sm.utils.js';

describe('e2e -> message', async () => {

  const c = new Client({
    transport: 'TCP',
    options: { port: 8090, host: '127.0.0.1' },
    credentials: { username: 'iggy', password: 'iggy' }
  });

  const streamId = 934;
  const topicId = 832;
  const partitionId = 1;

  const stream = {
    streamId,
    name: 'e2e-send-message-stream'
  };

  const topic = {
    streamId,
    topicId,
    name: 'e2e-send-message-topic',
    partitionCount: 1,
    compressionAlgorithm: 1
  };

  const msg = {
    streamId,
    topicId,
    messages: generateMessages(6),
    partition: Partitioning.PartitionId(partitionId)
  };

  await c.stream.create(stream);
  await c.topic.create(topic);

  it('e2e -> message::send', async () => {
    assert.ok(await c.message.send(msg));
  });

  it('e2e -> message::poll/last', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Single, id: 12 },
      partitionId,
      pollingStrategy: PollingStrategy.Last,
      count: 10,
      autocommit: false
    };
    const { messages, ...resp } = await c.message.poll(pollReq);
    assert.equal(messages.length, resp.messageCount);
    assert.equal(messages.length, msg.messages.length)
  });

  it('e2e -> message::poll/first', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Single, id: 12 },
      partitionId,
      pollingStrategy: PollingStrategy.First,
      count: 10,
      autocommit: false
    };
    const { messages, ...resp } = await c.message.poll(pollReq);
    assert.equal(messages.length, resp.messageCount);
    assert.equal(messages.length, msg.messages.length)
  });

  it('e2e -> message::poll/next', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Single, id: 12 },
      partitionId,
      pollingStrategy: PollingStrategy.Next,
      count: 10,
      autocommit: false
    };
    const { messages, ...resp } = await c.message.poll(pollReq);
    assert.equal(messages.length, resp.messageCount);
    assert.equal(messages.length, msg.messages.length)
  });

  it('e2e -> message::poll/next+commit', async () => {
    const pollReq = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Single, id: 12 },
      partitionId,
      pollingStrategy: PollingStrategy.Next,
      count: 10,
      autocommit: true
    };
    const { messages, ...resp } = await c.message.poll(pollReq);
    assert.equal(messages.length, resp.messageCount);
    assert.equal(messages.length, msg.messages.length)

    const r2 = await c.message.poll(pollReq);
    assert.equal(r2.messageCount, 0);
    assert.equal(r2.messages.length, 0)
  });

  it('e2e -> message::cleanup', async () => {
    assert.ok(await c.stream.delete(stream));
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
