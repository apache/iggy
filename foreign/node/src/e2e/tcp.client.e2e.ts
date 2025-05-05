
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';

describe('e2e -> client', async () => {

  const c = new Client({
    transport: 'TCP',
    options: { port: 8090, host: '127.0.0.1' },
    credentials: { username: 'iggy', password: 'iggy' }
  });

  it('e2e -> client::getMe', async () => {
    const cli = await c.client.getMe();
    assert.ok(cli);
  });

  it('e2e -> client::get#id', async () => {
    const cli = await c.client.getMe();
    const cli2 = await c.client.get({ clientId: cli.clientId })
    assert.ok(cli);
    assert.deepEqual(cli, cli2);
  });

  it('e2e -> client::list', async () => {
    const clis = await c.client.list();
    assert.ok(clis.length > 0);
  });

  it('e2e -> client::cleanup', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
