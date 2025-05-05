
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';

describe('e2e -> topic', async () => {

  const c = new Client({
    transport: 'TCP',
    options: { port: 8090, host: '127.0.0.1' },
    credentials: { username: 'iggy', password: 'iggy' }
  });

  const streamId = 111;
  const topicId = 123;

  await c.stream.create({ streamId, name: 'e2e-tcp-topic-stream' });

  it('e2e -> topic::create', async () => {
    const topic = await c.topic.create({
      streamId, topicId,
      name: 'e2e-tcp-topic',
      partitionCount: 0,
      compressionAlgorithm: 1,
      messageExpiry: 0n,
      replicationFactor: 1
    });
    assert.ok(topic);
  });

  it('e2e -> topic::list', async () => {
    const topics = await c.topic.list({ streamId });
    assert.ok(topics.length > 0);
  });

  it('e2e -> topic::get', async () => {
    const topic = await c.topic.get({ streamId, topicId });
    assert.ok(topic);
  });

  it('e2e -> topic::createPartition', async () => {
    const cp = await c.partition.create({
      streamId, topicId,
      partitionCount : 22
    });
    assert.ok(cp);
  });

  it('e2e -> topic::deletePartition', async () => {
    const dp = await c.partition.delete({
      streamId, topicId,
      partitionCount : 19
    });
    assert.ok(dp);
  });

  it('e2e -> topic::update', async () => {
    const topic = await c.topic.get({ streamId, topicId });
    const u2 = await c.topic.update({
      streamId, topicId,
      name: topic.name,
      messageExpiry: 42n
    });
    assert.ok(u2);
  });

  it('e2e -> topic::purge', async () => {
    assert.ok(await c.topic.purge({ streamId, topicId }));
  });

  it('e2e -> topic::delete', async () => {
    assert.ok(await c.topic.delete({ streamId, topicId, partitionsCount: 0 }));
  });

  it('e2e -> topic::cleanup', async () => {
    assert.ok(await c.stream.delete({ streamId }));
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
