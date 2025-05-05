
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';

describe('e2e -> stream', async () => {

  const c = new Client({
    transport: 'TCP',
    options: { port: 8090, host: '127.0.0.1' },
    credentials: { username: 'iggy', password: 'iggy' }
  });

  const streamId = 164;
  const name = 'e2e-tcp-stream';
  const name2 = `${name}-updated`;

  it('e2e -> stream::create', async () => {
    const stream = await c.stream.create({ streamId, name });
    assert.ok(stream);
  });

  it('e2e -> stream::list', async () => {
    const streams = await c.stream.list();
    assert.ok(streams.length > 0);
  });

  it('e2e -> stream::get', async () => {
    const stream = await c.stream.get({ streamId });
    assert.ok(stream);
  });

  it('e2e -> stream::update', async () => {
    const stream = await c.stream.update({
      streamId,
      name: name2
    });
    assert.ok(stream);
  });

  it('e2e -> stream::purge', async () => {
    assert.ok(await c.stream.purge({ streamId }));
  });

  it('e2e -> stream::delete', async () => {
    assert.ok(await c.stream.delete({ streamId }));
  });

  it('e2e -> stream::cleanup', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
