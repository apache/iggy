
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';

describe('e2e -> token', async () => {

  const c = new Client({
    transport: 'TCP',
    options: { port: 8090, host: '127.0.0.1' },
    credentials: { username: 'iggy', password: 'iggy' }
  });

  const tokenName = 'yolo-token-test';
  
  it('e2e -> token::create', async () => {
    const tk = await c.token.create({ name: tokenName, expiry: 1800n });
    assert.ok(tk.token.length > 1);
  });

  it('e2e -> token::list', async () => {
    const tks = await c.token.list();
    assert.ok(tks.length > 0);
  });

  it('e2e -> token::delete', async () => {
    assert.ok(await c.token.delete({name: tokenName}));
  });
  
  it('e2e -> token::logout', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
